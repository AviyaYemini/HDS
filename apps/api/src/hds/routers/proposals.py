"""Proposals router: review a run's proposal as a change, edit it, undo an edit (008 US1, US2).

Layer: routers. Delegates to ``ProposalService``, which ``hds.wiring`` builds; HTTP only. Managers
and Admins only; a Manager only on runs whose projects they all manage (007 T108).
"""

import uuid
from datetime import date, datetime
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, Path
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


class CoverOption(BaseModel):
    """One person who could cover the slot, or the rule that keeps them off it (FR-014, FR-016).

    ``free`` people break no rule there; otherwise ``code`` names the rule, with the numbers for
    rest (``have``, ``need``) and the weekly maximum (``week_minutes``, ``slot_minutes``,
    ``limit``), in minutes. ``close`` and ``open``: a "would rather not" or an "I can" covers it.
    ``minutes``, ``nights`` and ``weekends``: what they would work in the run's range after it.
    """

    user_id: uuid.UUID
    free: bool
    code: str | None
    have: int | None
    need: int | None
    week_minutes: int | None
    slot_minutes: int | None
    limit: int | None
    close: bool
    open: bool
    minutes: int
    nights: int
    weekends: int


class CoverTotals(BaseModel):
    """The active staff's count and total minutes, nights and weekend shifts, before the pick.

    Each total over ``people`` is the average an option's numbers are set against.
    """

    people: int
    minutes: int
    nights: int
    weekends: int


class Cover(BaseModel):
    """Who else could cover one slot of a proposal, best first."""

    key: str
    options: list[CoverOption]
    totals: CoverTotals


class EditRequest(BaseModel):
    """One edit of a person: assign, unassign, move, swap, lock or unlock.

    A rule break needs a reason to be kept; a lock marks someone later runs keep (008 FR-018).
    """

    op: Literal["assign", "unassign", "move", "swap", "lock", "unlock"]
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


@router.get(
    "/schedule-jobs/{id}/slots/{key}/cover",
    response_model=Cover,
    summary="Who else could cover a slot",
    description=(
        "Manager or Admin, of every project in the run. Every active staff member not on the "
        "proposal's slot `key`, whatever project or kind of project they work on, judged over all "
        "their shifts in every project, the roster's drafts and the working copy. People free come "
        'first: no "would rather not" on it, then the fewest nights, minutes and weekend shifts '
        "after it, then name; people who would break a rule follow, with the rule and its numbers. "
        "Codes and numbers only: never pay, a site or another project's shift (008 FR-014, "
        "FR-016). 404 for a slot not in the proposal; 409 (`schedule.not_ready`) before the job "
        "has one."
    ),
)
async def cover_slot(
    id: uuid.UUID,
    key: Annotated[str, Path(max_length=200)],
    current_user: User = Depends(require_manager),
    svc: ProposalService = Depends(proposals),
) -> Cover:
    """Who else could cover a slot.

    Manager or Admin, of every project in the run.
    """
    return Cover(**await svc.cover(current_user, id, key))


class Share(BaseModel):
    """One person's share of the range, and which of it stands out (008 FR-025)."""

    user_id: uuid.UUID
    minutes: int
    nights: int
    weekends: int
    hours_outlier: bool
    nights_outlier: bool
    weekends_outlier: bool


class Totals(BaseModel):
    """The run's people and their totals; each total over ``people`` is its average."""

    people: int
    minutes: int
    nights: int
    weekends: int


class Tradeoffs(BaseModel):
    """What the proposal gave up, as counts the summary words (008 FR-024)."""

    empty_places: int
    open_wishes_missed: int
    close_wishes_given: int
    minutes_low: int
    minutes_high: int
    nights_low: int
    nights_high: int
    chips_not_met: int


class ChipState(BaseModel):
    """A chip of the run, and whether the proposal meets it now (008 FR-021, FR-022)."""

    kind: str
    user_id: uuid.UUID
    count: int | None = None
    project_id: uuid.UUID | None = None
    site_id: uuid.UUID | None = None
    weekday: int | None = None
    shift_type_id: uuid.UUID | None = None
    state: Literal["met", "not_met", "not_applying"]
    cause: Literal["coverage", "conflict", "fixed"] | None = None


class Fairness(BaseModel):
    """How fair the proposal is, what it traded off, its chips, and what it keeps.

    008 FR-023 to FR-026.
    """

    shares: list[Share]
    totals: Totals
    tradeoffs: Tradeoffs
    chips: list[ChipState]
    overridden: int
    locks: int


@router.get(
    "/schedule-jobs/{id}/fairness",
    response_model=Fairness,
    summary="Fairness and trade-offs of a proposal",
    description=(
        "Manager or Admin, of every project in the run. Judged on the working copy, so it follows "
        "every edit (008 FR-026): each person's hours (in minutes), nights and weekend shifts in "
        "the range against the run's totals, with outliers; the trade-off counts; each chip met, "
        "not met (with what prevented it) or no longer applying; how many assignments break a "
        "rule by a person's override; and how many locks it keeps. Numbers only, never a score. "
        "409 (`schedule.not_ready`) before the job has a proposal."
    ),
)
async def fairness_of(
    id: uuid.UUID,
    current_user: User = Depends(require_manager),
    svc: ProposalService = Depends(proposals),
) -> Fairness:
    """Fairness and trade-offs of a proposal.

    Manager or Admin, of every project in the run.
    """
    return Fairness(**await svc.fairness(current_user, id))
