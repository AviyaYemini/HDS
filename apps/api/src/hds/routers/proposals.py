"""Proposals router: review a run's proposal as a change, edit it, undo an edit (008 US1, US2).

Layer: routers. Delegates to ``ProposalService``, which ``hds.wiring`` builds; HTTP only. Managers
and Admins only; a Manager only on runs whose projects they all manage (007 T108).
"""

import uuid
from datetime import date, datetime
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, ConfigDict

from hds.core.deps import ProposalIfMatch as IfMatch
from hds.core.deps import proposal_version as version_of
from hds.core.deps import require_manager
from hds.core.types import text
from hds.models.schedule_job import ScheduleJob
from hds.models.user import User
from hds.services.proposal import ProposalService
from hds.wiring import proposals

router = APIRouter(prefix="/api/v1", tags=["proposals"])


class Override(BaseModel):
    """Why a person keeps a rule break in the proposal, and which rules it covers."""

    codes: list[str]
    reason: str


class Placed(BaseModel):
    """A person the proposal places on a slot."""

    user_id: uuid.UUID
    by: Literal["solver", "person"]
    locked: bool
    override: Override | None


class Mark(BaseModel):
    """One change mark against the roster when the run was made (FR-001)."""

    mark: Literal["added", "changed", "removed", "unchanged"]
    user_id: uuid.UUID
    replaces: uuid.UUID | None
    accepted: bool


class ReviewSlot(BaseModel):
    """A slot of the proposal under review."""

    key: str
    shift_id: uuid.UUID | None
    project_id: uuid.UUID
    shift_type_id: uuid.UUID
    date: date
    starts_at: datetime
    ends_at: datetime
    headcount: int
    kept: list[uuid.UUID]
    people: list[Placed]
    places: int
    marks: list[Mark]
    part_accepted: bool


class Counts(BaseModel):
    """How many marks of each kind the proposal holds."""

    added: int
    changed: int
    removed: int
    unchanged: int


class Coverage(BaseModel):
    """Places filled over places needed, for one day of one project (FR-002)."""

    date: date
    project_id: uuid.UUID
    filled: int
    needed: int


class Review(BaseModel):
    """A proposal as the review reads it."""

    job_id: uuid.UUID
    version: int
    state: Literal["open", "accepted", "discarded", "expired", "partly_accepted"]
    optimal: bool | None
    settled: bool
    counts: Counts
    coverage: list[Coverage]
    slots: list[ReviewSlot]
    unfilled: list[dict[str, Any]]
    employees: list[dict[str, Any]]
    no_availability: list[uuid.UUID]


class Warning(BaseModel):
    """A rule the edit's person breaks on a slot it touched, and whether an override excuses it."""

    key: str
    user_id: uuid.UUID | None
    code: str
    values: dict[str, str]
    excused: bool


class EditAnswer(BaseModel):
    """The proposal after an edit, and the warnings of the people it placed."""

    proposal: Review
    warnings: list[Warning]


class EditRequest(BaseModel):
    """One edit: assign, unassign, move or swap a person, with a reason to keep a rule break."""

    op: Literal["assign", "unassign", "move", "swap"]
    key: Annotated[str, text(200, min_length=1)]
    user_id: uuid.UUID
    to_key: Annotated[str, text(200, min_length=1)] | None = None
    other_user_id: uuid.UUID | None = None
    override_reason: Annotated[str, text(500, min_length=1)] | None = None

    model_config = ConfigDict(extra="forbid")


def review(job: ScheduleJob, state: str | None, body: dict[str, Any]) -> Review:
    """The review answer for ``job`` with its view ``body``."""
    return Review(
        job_id=job.id,
        version=job.version,
        state=state or "open",  # type: ignore[arg-type]
        optimal=job.optimal,
        **body,
    )


def answer(content: BaseModel, version: int) -> Response:
    """A JSON answer carrying the proposal's version as its ``ETag``."""
    return JSONResponse(content.model_dump(mode="json"), headers={"ETag": f'"{version}"'})


@router.get(
    "/schedule-jobs/{id}/proposal",
    response_model=Review,
    summary="Review a proposal",
    description=(
        "Manager or Admin. The proposal's working copy over the roster as it was when the run was "
        "made: every assignment marked added, changed, removed or unchanged, with counts; places "
        "filled over needed per day and project; who placed each person, and any override. The "
        "`ETag` is the `version` that edits and accepts send in `If-Match`. 409 "
        "(`schedule.not_ready`) before the job has a proposal; 403 "
        "(`roster.project_not_managed`) for a Manager who does not manage every project of the "
        "run (007 T108)."
    ),
)
async def read_proposal(
    id: uuid.UUID,
    current_user: User = Depends(require_manager),
    svc: ProposalService = Depends(proposals),
) -> Response:
    """Review a proposal.

    Manager or Admin, of every project in the run.
    """
    job, body = await svc.read(current_user, id)
    return answer(review(job, svc.state(job), body), job.version)


@router.post(
    "/schedule-jobs/{id}/edits",
    response_model=EditAnswer,
    summary="Edit a proposal",
    description=(
        "Manager or Admin, of every project in the run. Assign, unassign, move or swap a person in "
        "an open proposal; the roster does not change until it is accepted. The answer gives the "
        "rules the placed people break on the shifts the edit touched. A break the roster lets a "
        "person override needs `override_reason` (422 `schedule.override_reason_required`, with "
        "the breaks in `errors`); an inactive employee is refused (422 "
        "`warnings.employee_inactive`); an edit that does not fit the proposal is 422 "
        "(`schedule.edit_invalid`); a shift with a part accepted is 409 "
        "(`schedule.part_accepted`); a stale `If-Match` is 412 (`schedule.version_stale`)."
    ),
)
async def edit_proposal(
    id: uuid.UUID,
    body: EditRequest,
    if_match: str | None = IfMatch,
    current_user: User = Depends(require_manager),
    svc: ProposalService = Depends(proposals),
) -> Response:
    """Edit a proposal.

    Manager or Admin, of every project in the run.
    """
    op = body.model_dump(mode="json", exclude_none=True)
    job, view, warnings = await svc.edit(current_user, id, op, version_of(if_match))
    content = EditAnswer(
        proposal=review(job, svc.state(job), view),
        warnings=[Warning(**w) for w in warnings],
    )
    return answer(content, job.version)


@router.post(
    "/schedule-jobs/{id}/edits/undo",
    response_model=Review,
    summary="Undo the last edit of a proposal",
    description=(
        "Manager or Admin, of every project in the run. Undo the proposal's last edit not yet "
        "undone. 409 (`schedule.nothing_to_undo`) when there is none, or its shifts have a part "
        "accepted since; 412 (`schedule.version_stale`) for a stale `If-Match`."
    ),
)
async def undo_edit(
    id: uuid.UUID,
    if_match: str | None = IfMatch,
    current_user: User = Depends(require_manager),
    svc: ProposalService = Depends(proposals),
) -> Response:
    """Undo the last edit of a proposal.

    Manager or Admin, of every project in the run.
    """
    job, view = await svc.undo(current_user, id, version_of(if_match))
    return answer(review(job, svc.state(job), view), job.version)
