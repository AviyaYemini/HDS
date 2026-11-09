"""Schedule jobs router: ask for a roster, follow the run, read the proposal (004 US2, US4).

Layer: routers. Delegates to ``ScheduleJobService``; HTTP only. Managers and Admins only.
"""

import base64
import uuid
from datetime import date, datetime
from typing import Annotated, Any, Literal

from fastapi import APIRouter, Depends, Header, Query
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, ConfigDict, Field, StrictBool

from hds.core.config import get_settings
from hds.core.deps import ProposalIfMatch as IfMatch
from hds.core.deps import proposal_version as version_of
from hds.core.deps import require_manager
from hds.core.exceptions import DomainValidationError
from hds.core.types import WHOLE_NUMBER, StrictDate, text
from hds.models.schedule_job import ScheduleJob
from hds.models.user import User
from hds.services.idempotency import IdempotencyService
from hds.services.schedule_job import ScheduleJobService
from hds.wiring import idempotency, schedule_jobs

router = APIRouter(prefix="/api/v1", tags=["schedule jobs"])

Status = Literal["queued", "running", "succeeded", "failed", "cancelled"]
ProposalState = Literal["open", "accepted", "discarded", "expired", "partly_accepted"]


class Controls(BaseModel):
    """The run's two plain controls (008 FR-017); untouched, today's order.

    ``keep_changes_small``: "Keep changes small" (true) or "Rebalance freely" (false).
    ``wishes_strictly``: respect "would rather not" strictly, before the balance of hours and
    nights (true), or gently, after it (false).
    """

    keep_changes_small: StrictBool = False
    wishes_strictly: StrictBool = True

    model_config = ConfigDict(extra="forbid")


class ChipIn(BaseModel):
    """One preference for the run about one person (008 FR-020).

    ``max_shifts``, ``max_nights`` and ``max_hours`` take ``count``; ``keep_on`` takes
    ``project_id`` or ``site_id``; ``keep_off`` and ``prefer`` take ``weekday`` (0 is Sunday) or
    ``shift_type_id``. A chip without what its kind needs is refused (422
    ``schedule.chip_invalid``).
    """

    kind: Literal["max_shifts", "max_nights", "max_hours", "keep_on", "keep_off", "prefer"]
    user_id: uuid.UUID
    count: Annotated[int, Field(ge=0, le=744), WHOLE_NUMBER] | None = None
    project_id: uuid.UUID | None = None
    site_id: uuid.UUID | None = None
    weekday: Annotated[int, Field(ge=0, le=6), WHOLE_NUMBER] | None = None
    shift_type_id: uuid.UUID | None = None

    model_config = ConfigDict(extra="forbid")


class JobCreateRequest(BaseModel):
    """What to generate: a range of dates, and optionally some projects and the solver.

    008: ``controls``, up to 20 ``chips``, and ``based_on``, an earlier run this one reruns: it
    starts from that run's working copy and keeps its locks, and copies its chips when ``chips``
    is absent (FR-019, FR-021a).
    """

    first: StrictDate = Field(alias="from")
    last: StrictDate = Field(alias="to")
    project_ids: list[uuid.UUID] = Field(default_factory=list, max_length=500)
    solver: Literal["greedy", "cpsat"] = "cpsat"
    controls: Controls = Field(default_factory=Controls)
    chips: list[ChipIn] | None = Field(default=None, max_length=20)
    based_on: uuid.UUID | None = None

    model_config = ConfigDict(extra="forbid", populate_by_name=True)


class Limits(BaseModel):
    """The limits a job ran with."""

    min_rest_hours: int
    max_weekly_hours: int


class Excluded(BaseModel):
    """A person rest or the weekly maximum kept off a slot, with the numbers (008 FR-013)."""

    user_id: uuid.UUID
    code: str
    have: int | None = None
    need: int | None = None
    week_minutes: int | None = None
    slot_minutes: int | None = None
    limit: int | None = None


class Reason(BaseModel):
    """One reason places stayed empty, how many candidates it excluded, and who, where it names."""

    code: str
    employees: int
    # Absent in results stored before 008's PR 2.
    people: list[Excluded] = []


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
    # How many locked assignments the run kept (008 FR-018); absent in results made earlier.
    locks_kept: int = 0


class Progress(BaseModel):
    """Places filled out of those needed, and when it last improved (008 FR-027)."""

    filled: int | None
    needed: int
    at: datetime | None


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
        "(007 FR-074)."
    )
    elapsed_seconds: int | None = Field(
        description="Whole seconds the job has searched by the server's clock, to now while it "
        "runs and to `finished_at` once done; null before it starts. The bar's value is this over "
        "`time_limit_seconds` (007 FR-074)."
    )
    progress: Progress | None = Field(
        description="The best so far while it runs, the final count once done (008 FR-027)."
    )
    ended: Literal["finished", "stopped", "timed_out", "cancelled", "failed"] | None = Field(
        description="How the run ended: stopped is with the best so far (FR-028)."
    )
    stop_requested: bool = Field(description="Someone asked it to stop (FR-028).")
    controls: Controls = Field(description="The run's two plain controls (008 FR-017).")
    chips: list[ChipIn] = Field(description="The manager's preferences for the run (FR-020).")
    based_on: uuid.UUID | None = Field(description="The earlier run this one reruns (FR-019).")


class JobRead(JobSummary):
    """A job and, once it has succeeded, its proposal."""

    proposal: Proposal | None


class JobPage(BaseModel):
    """A page of jobs, newest first."""

    items: list[JobSummary]
    next_cursor: str | None


def _summary(job: ScheduleJob, svc: ScheduleJobService) -> dict[str, Any]:
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
        "proposal_state": svc.state(job),
        "time_limit_seconds": get_settings().SCHEDULE_TIME_LIMIT_SECONDS,
        "elapsed_seconds": svc.elapsed_seconds(job),
        "progress": (
            {"filled": job.best_filled, "needed": job.needed, "at": job.progress_at}
            if job.needed is not None
            else None
        ),
        "ended": job.ended,
        "stop_requested": job.stop_requested_at is not None,
        "controls": job.controls or {},
        "chips": job.chips or [],
        "based_on": job.based_on_id,
    }


def job_read(job: ScheduleJob, svc: ScheduleJobService) -> JobRead:
    """The full answer for one job."""
    proposal = Proposal.model_validate(job.result) if job.result is not None else None
    return JobRead(**_summary(job, svc), proposal=proposal)


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
        "days) over the `project_ids` given, or, when none are, the caller's: a Manager's managed "
        "active projects, an Admin's every active one; the job keeps the list it covers. A worker "
        "solves it; follow it with `GET /schedule-jobs/{id}`. Nothing in the roster changes until "
        "the proposal is accepted. A Manager naming a project they do not manage is refused with "
        "403 (`roster.project_not_managed`) and no job is made (007 T108). A backwards or too "
        "long range is refused with 422 (`schedule.range_invalid`), a project that is not active "
        "with 422 (`schedule.project_not_active`), and no project to schedule with 422 "
        "(`schedule.no_projects`). With an "
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
                "controls": body.controls.model_dump(),
                "chips": (
                    None
                    if body.chips is None
                    else [c.model_dump(mode="json", exclude_none=True) for c in body.chips]
                ),
                "based_on": body.based_on,
            },
        )
        return 202, job_read(job, svc).model_dump(mode="json", by_alias=True)

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
        items=[JobSummary(**_summary(j, svc)) for j in jobs[:limit]],
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
    return JSONResponse(job_read(job, svc).model_dump(mode="json", by_alias=True))


class AcceptAnswer(BaseModel):
    """What accepting wrote to the roster, as drafts, and the proposal after it (008)."""

    shifts_created: int
    shifts_updated: int
    assignments_added: int
    assignments_removed: int
    version: int
    state: Literal["open", "accepted"]


class PartPair(BaseModel):
    """One person on one slot of the proposal."""

    key: Annotated[str, text(200, min_length=1)]
    user_id: uuid.UUID

    model_config = ConfigDict(extra="forbid")


class AcceptRequest(BaseModel):
    """The part to accept: days, projects or assignments; nothing named accepts all still open."""

    days: list[StrictDate] = Field(default_factory=list, max_length=31)
    project_ids: list[uuid.UUID] = Field(default_factory=list, max_length=500)
    assignments: list[PartPair] = Field(default_factory=list, max_length=2000)

    model_config = ConfigDict(extra="forbid")


@router.post(
    "/schedule-jobs/{id}/accept",
    response_model=AcceptAnswer,
    summary="Accept a proposal, or part of it",
    description=(
        "Manager or Admin. Store the proposal, or the part the body names (`days`, `project_ids` "
        "or `assignments`; nothing named accepts everything still open), as draft shifts and "
        "assignments, all or nothing; nothing is published, and what is not named stays open "
        "(008 FR-003, FR-004). An existing draft keeps its id and headcount. Refused with 409 "
        "when the job has no proposal (`schedule.not_ready`), the proposal is closed "
        "(`schedule.proposal_closed`), nothing named is still open (`schedule.nothing_to_accept`), "
        "an untouched proposal was made from something that has changed "
        "(`schedule.proposal_out_of_date`), or the part's own drafts changed or it would break a "
        "rule the proposal does not override (`schedule.part_out_of_date`, each in `errors`); "
        "nothing changes then. 412 (`schedule.version_stale`) for a stale `If-Match`. A Manager "
        "who does not manage every project of the run, now, is refused the whole of it with 403 "
        "(`roster.project_not_managed`, 007 T108)."
    ),
)
async def accept_job(
    id: uuid.UUID,
    body: AcceptRequest | None = None,
    if_match: str | None = IfMatch,
    current_user: User = Depends(require_manager),
    svc: ScheduleJobService = Depends(schedule_jobs),
) -> AcceptAnswer:
    """Accept a proposal, or part of it.

    Manager or Admin. What it names becomes drafts, all or nothing.
    """
    part = body.model_dump(mode="json") if body is not None else None
    return AcceptAnswer(**await svc.accept(current_user, id, part, version_of(if_match)))


@router.post(
    "/schedule-jobs/{id}/discard",
    response_model=JobRead,
    summary="Discard a proposal",
    description=(
        "Manager or Admin. Close the proposal without storing what is still open of it; a "
        "proposal with parts accepted closes as `partly_accepted`, and those parts stay drafts "
        "(008 FR-005). 412 (`schedule.version_stale`) for a stale `If-Match`. Refused with 409 "
        "when the job "
        "has no proposal (`schedule.not_ready`) or it is already closed "
        "(`schedule.proposal_closed`); with 403 (`roster.project_not_managed`) for a Manager who "
        "does not manage every project of the run (007 T108)."
    ),
)
async def discard_job(
    id: uuid.UUID,
    if_match: str | None = IfMatch,
    current_user: User = Depends(require_manager),
    svc: ScheduleJobService = Depends(schedule_jobs),
) -> Response:
    """Discard a proposal, or what is still open of it.

    Manager or Admin. It can no longer be accepted; parts already accepted stay drafts.
    """
    job = await svc.discard(current_user, id, version_of(if_match))
    return JSONResponse(job_read(job, svc).model_dump(mode="json", by_alias=True))


class RecordItem(BaseModel):
    """One step of a run's record: what, who and when (008 FR-030)."""

    seq: int
    kind: str
    at: datetime
    actor_id: uuid.UUID | None
    version: int | None
    body: dict[str, Any]


class RecordPage(BaseModel):
    """A page of a run's record, oldest first; ``next_after`` reads the next."""

    items: list[RecordItem]
    next_after: int | None


@router.post(
    "/schedule-jobs/{id}/stop",
    response_model=JobRead,
    summary="Stop a run",
    description=(
        "Manager or Admin, of every project in the run. A queued run is cancelled at once; a "
        "running one ends within a few seconds with its best result so far, labelled stopped, "
        "or cancelled with no proposal when it has none yet (008 FR-028). Asking twice changes "
        "nothing. 409 (`schedule.not_running`) for a run already over."
    ),
)
async def stop_job(
    id: uuid.UUID,
    current_user: User = Depends(require_manager),
    svc: ScheduleJobService = Depends(schedule_jobs),
) -> JobRead:
    """Stop a run.

    Manager or Admin, of every project in the run.
    """
    return job_read(await svc.stop(current_user, id), svc)


@router.get(
    "/schedule-jobs/{id}/record",
    response_model=RecordPage,
    summary="A run's record",
    description=(
        "Manager or Admin, of every project in the run. The run's record, oldest first: who ran "
        "it and with what, how it ended, each edit, override, lock, accept and discard, each "
        "with who and when. It only grows (008 FR-030, FR-031, ADR 020)."
    ),
)
async def read_record(
    id: uuid.UUID,
    after: int | None = Query(default=None, ge=0, description="The last `seq` already read."),
    limit: int = Query(default=50, ge=1, le=200),
    current_user: User = Depends(require_manager),
    svc: ScheduleJobService = Depends(schedule_jobs),
) -> RecordPage:
    """A run's record.

    Manager or Admin, of every project in the run.
    """
    rows = await svc.record_page(current_user, id, after, limit)
    items = [
        RecordItem(
            seq=r.seq, kind=r.kind, at=r.at, actor_id=r.actor_id, version=r.version, body=r.body
        )
        for r in rows
    ]
    return RecordPage(items=items, next_after=items[-1].seq if len(items) == limit else None)
