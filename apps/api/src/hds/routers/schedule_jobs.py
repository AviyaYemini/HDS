"""Schedule jobs router: ask for a roster, follow the run, read the proposal (004 US2, US4).

Layer: routers. Delegates to ``ScheduleJobService``; HTTP only. Managers and Admins only.
"""

import base64
import uuid
from datetime import date, datetime
from typing import Any, Literal

from fastapi import APIRouter, Depends, Header, Query
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, ConfigDict, Field

from hds.core.config import get_settings
from hds.core.deps import require_manager
from hds.core.exceptions import DomainValidationError
from hds.core.types import StrictDate
from hds.models.schedule_job import ScheduleJob
from hds.models.user import User
from hds.services.idempotency import IdempotencyService
from hds.services.schedule_job import ScheduleJobService
from hds.wiring import idempotency, schedule_jobs

router = APIRouter(prefix="/api/v1", tags=["schedule jobs"])

Status = Literal["queued", "running", "succeeded", "failed"]
ProposalState = Literal["open", "accepted", "discarded", "expired"]


class JobCreateRequest(BaseModel):
    """What to generate: a range of dates, and optionally some projects and the solver."""

    first: StrictDate = Field(alias="from")
    last: StrictDate = Field(alias="to")
    project_ids: list[uuid.UUID] = Field(default_factory=list, max_length=500)
    solver: Literal["greedy", "cpsat"] = "cpsat"

    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class Limits(BaseModel):
    """The limits a job ran with."""

    min_rest_hours: int
    max_weekly_hours: int


class Reason(BaseModel):
    """One reason places stayed empty, and how many candidates it excluded."""

    code: str
    employees: int


class ProposedSlot(BaseModel):
    """A slot of the proposal: an existing draft (``shift_id``) or a new shift."""

    key: str
    shift_id: uuid.UUID | None
    project_id: uuid.UUID
    shift_type_id: uuid.UUID
    date: date
    starts_at: datetime
    ends_at: datetime
    headcount: int
    assigned: list[uuid.UUID]
    kept: list[uuid.UUID]


class UnfilledSlot(BaseModel):
    """A slot with empty places, and why."""

    key: str
    places: int
    reasons: list[Reason]


class EmployeeShare(BaseModel):
    """A candidate's share of the range."""

    user_id: uuid.UUID
    minutes: int
    nights: int
    open_met: int
    close_not_met: int


class ShortPublished(BaseModel):
    """A published shift with fewer people than its need; reported, not changed."""

    shift_id: uuid.UUID
    needed: int
    assigned: int


class Change(BaseModel):
    """A person on a slot, added or removed against the current drafts."""

    key: str
    user_id: uuid.UUID


class Changes(BaseModel):
    """The proposal against the current drafts."""

    added: list[Change]
    removed: list[Change]


class Proposal(BaseModel):
    """Who works each slot, what stays empty and why, and what it changes; employees by id."""

    slots: list[ProposedSlot]
    unfilled: list[UnfilledSlot]
    employees: list[EmployeeShare]
    no_availability: list[uuid.UUID]
    short_published: list[ShortPublished]
    changes: Changes


class JobSummary(BaseModel):
    """A job without its proposal."""

    id: uuid.UUID
    status: Status
    first: date = Field(serialization_alias="from")
    last: date = Field(serialization_alias="to")
    project_ids: list[uuid.UUID]
    solver: str
    requested_by: uuid.UUID
    settings: Limits
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    error_code: str | None
    optimal: bool | None
    proposal_state: ProposalState | None
    time_limit_seconds: int = Field(
        description="How long the solver may search: the determinate progress bar's whole "
        "(007 FR-074). Elapsed is now minus `started_at`."
    )


class JobRead(JobSummary):
    """A job and, once it has succeeded, its proposal."""

    proposal: Proposal | None


class JobPage(BaseModel):
    """A page of jobs, newest first."""

    items: list[JobSummary]
    next_cursor: str | None


def _summary(job: ScheduleJob, state: str | None) -> dict[str, Any]:
    return {
        "id": job.id,
        "status": job.status,
        "first": job.first_day,
        "last": job.last_day,
        "project_ids": job.project_ids,
        "solver": job.solver,
        "requested_by": job.requested_by,
        "settings": job.settings,
        "created_at": job.created_at,
        "started_at": job.started_at,
        "finished_at": job.finished_at,
        "error_code": job.error_code,
        "optimal": job.optimal,
        "proposal_state": state,
        "time_limit_seconds": get_settings().SCHEDULE_TIME_LIMIT_SECONDS,
    }


def job_read(job: ScheduleJob, state: str | None) -> JobRead:
    """The full answer for one job."""
    proposal = Proposal.model_validate(job.result) if job.result is not None else None
    return JobRead(**_summary(job, state), proposal=proposal)


def _cursor(job: ScheduleJob) -> str:
    text = f"{job.created_at.isoformat()}|{job.id}"
    return base64.urlsafe_b64encode(text.encode()).decode().rstrip("=")


def _position(cursor: str | None) -> tuple[datetime, uuid.UUID] | None:
    if cursor is None:
        return None
    try:
        text = base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4)).decode()
        moment, id = text.split("|")
        return datetime.fromisoformat(moment), uuid.UUID(id)
    except (ValueError, UnicodeDecodeError):
        raise DomainValidationError(
            code="schedule.cursor_invalid", detail="The cursor is not one this API returned."
        ) from None


@router.post(
    "/schedule-jobs",
    status_code=202,
    response_model=JobRead,
    summary="Ask for a roster",
    description=(
        "Manager or Admin. Queue a run that proposes a roster for `from` to `to` (at most 31 "
        "days) over every active project, or the `project_ids` given. A worker solves it; follow "
        "it with `GET /schedule-jobs/{id}`. Nothing in the roster changes until the proposal is "
        "accepted. A backwards or too long range is refused with 422 (`schedule.range_invalid`), "
        "a project that is not active with 422 (`schedule.project_not_active`). With an "
        "`Idempotency-Key`, a repeat of the same request answers the first job, and the same key "
        "with another request is refused with 422 (`idempotency.key_reused`)."
    ),
)
async def create_job(
    body: JobCreateRequest,
    idempotency_key: str | None = Header(default=None, min_length=1, max_length=255),
    current_user: User = Depends(require_manager),
    svc: ScheduleJobService = Depends(schedule_jobs),
    retries: IdempotencyService = Depends(idempotency),
) -> Response:
    """Ask for a roster.

    Manager or Admin. The job is queued and a worker solves it; the roster changes only when the
    proposal is accepted. A repeat with the same `Idempotency-Key` answers the first job.
    """

    async def run() -> tuple[int, Any]:
        """Queue the job and build the answer to store and send."""
        job = await svc.create(
            current_user,
            {
                "first_day": body.first,
                "last_day": body.last,
                "project_ids": body.project_ids,
                "solver": body.solver,
            },
        )
        return 202, job_read(job, svc.state(job)).model_dump(mode="json", by_alias=True)

    status, content = await retries.run_once(
        current_user.id,
        idempotency_key,
        {"operation": "schedule_job", "body": body.model_dump(mode="json", by_alias=True)},
        run,
    )
    return JSONResponse(status_code=status, content=content)


@router.get(
    "/schedule-jobs",
    response_model=JobPage,
    summary="List schedule jobs",
    description=(
        "Manager or Admin. The organisation's jobs, newest first, without their proposals; "
        "`status` filters them. `limit` (1 to 200, default 50) per page; pass `next_cursor` back "
        "as `cursor` for the next."
    ),
)
async def list_jobs(
    status: Status | None = None,
    cursor: str | None = Query(default=None, max_length=200),
    limit: int = Query(default=50, ge=1, le=200),
    current_user: User = Depends(require_manager),
    svc: ScheduleJobService = Depends(schedule_jobs),
) -> Response:
    """List schedule jobs.

    Manager or Admin. Newest first, without their proposals.
    """
    jobs = await svc.page(current_user, status, _position(cursor), limit + 1)
    page = JobPage(
        items=[JobSummary(**_summary(j, svc.state(j))) for j in jobs[:limit]],
        next_cursor=_cursor(jobs[limit - 1]) if len(jobs) > limit else None,
    )
    return JSONResponse(page.model_dump(mode="json", by_alias=True))


@router.get(
    "/schedule-jobs/{id}",
    response_model=JobRead,
    summary="Read a schedule job",
    description=(
        "Manager or Admin. The job's status, range, projects, limits and times; once it has "
        "succeeded, whether its proposal is proven optimal, the proposal's state (open, accepted, "
        "discarded or expired after 7 days) and the proposal itself."
    ),
)
async def read_job(
    id: uuid.UUID,
    current_user: User = Depends(require_manager),
    svc: ScheduleJobService = Depends(schedule_jobs),
) -> Response:
    """Read a schedule job.

    Manager or Admin. Its status and, once it has succeeded, its proposal.
    """
    job = await svc.get(current_user, id)
    return JSONResponse(job_read(job, svc.state(job)).model_dump(mode="json", by_alias=True))


class AcceptAnswer(BaseModel):
    """What accepting wrote to the roster, as drafts."""

    shifts_created: int
    shifts_updated: int
    assignments_added: int
    assignments_removed: int


@router.post(
    "/schedule-jobs/{id}/accept",
    response_model=AcceptAnswer,
    summary="Accept a proposal",
    description=(
        "Manager or Admin. Store the proposal as draft shifts and assignments, all or nothing; "
        "nothing is published. An existing draft keeps its id and headcount. Refused with 409 "
        "when the job has no proposal (`schedule.not_ready`), the proposal was accepted, "
        "discarded or has expired (`schedule.proposal_closed`), or anything it was made from has "
        "changed since (`schedule.proposal_out_of_date`); nothing changes then."
    ),
)
async def accept_job(
    id: uuid.UUID,
    current_user: User = Depends(require_manager),
    svc: ScheduleJobService = Depends(schedule_jobs),
) -> AcceptAnswer:
    """Accept a proposal.

    Manager or Admin. Its shifts and assignments become drafts, all or nothing.
    """
    return AcceptAnswer(**await svc.accept(current_user, id))


@router.post(
    "/schedule-jobs/{id}/discard",
    response_model=JobRead,
    summary="Discard a proposal",
    description=(
        "Manager or Admin. Close the proposal without storing it. Refused with 409 when the job "
        "has no proposal (`schedule.not_ready`) or it is already closed "
        "(`schedule.proposal_closed`)."
    ),
)
async def discard_job(
    id: uuid.UUID,
    current_user: User = Depends(require_manager),
    svc: ScheduleJobService = Depends(schedule_jobs),
) -> Response:
    """Discard a proposal.

    Manager or Admin. It can no longer be accepted.
    """
    job = await svc.discard(current_user, id)
    return JSONResponse(job_read(job, svc.state(job)).model_dump(mode="json", by_alias=True))
