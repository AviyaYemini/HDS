"""Schedule jobs: asking for a roster, the worker's run, and reading the proposal (004 US2, US4).

Layer: services. Depends on ``hds.core``, ``hds.scheduling`` (pure), ``hds.models`` and the
repositories and services it is given; it MUST NOT import routers or know about HTTP.

Rules: Managers and Admins ask for a roster for 1 to 31 days over every active project or chosen
active ones (FR-004, FR-005). The job is queued; a worker claims it, builds the problem, solves it
outside any transaction and stores the proposal, or marks the job failed (FR-008, FR-010). A
proposal changes nothing in the roster (FR-022); one left open for 7 days reads as expired (FR-025).
"""

import asyncio
import logging
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any

from hds.core.authz import MANAGER_ROLES, ensure_role
from hds.core.exceptions import ConflictError, DomainValidationError, NotFoundError
from hds.models.schedule_job import ScheduleJob
from hds.models.user import User
from hds.repositories.organisation import OrganisationRepository
from hds.repositories.project import ProjectRepository
from hds.repositories.schedule_job import ScheduleJobRepository
from hds.repositories.shift import AssignmentRepository, ShiftRepository
from hds.repositories.shift_type import ShiftTypeRepository
from hds.repositories.user import UserRepository
from hds.scheduling import Result, generate
from hds.services.schedule_inputs import Built, ScheduleInputService, surroundings

logger = logging.getLogger(__name__)

MAX_DAYS = 31
CALENDAR_MARGIN = timedelta(days=14)
"""How far a range must stay from the calendar's first and last dates: the run's surroundings
reach the Sunday before the range less the minimum rest, and the Sunday after it plus the rest
(research G9), and asyncpg stores ``date.min`` and ``date.max`` as minus and plus infinity."""
LEASE = timedelta(minutes=5)
"""How long a claim lasts; a worker that dies leaves its job to be claimed again after it. Longer
than the longest solve (``SCHEDULE_TIME_LIMIT_SECONDS`` is at most 60) plus building the problem."""
MAX_ATTEMPTS = 3
EXPIRY = timedelta(days=7)


class JobFailedError(Exception):
    """A job that cannot be run, with the code it fails with."""

    def __init__(self, code: str) -> None:
        """Keep the error code."""
        super().__init__(code)
        self.code = code


@dataclass(frozen=True)
class Claim:
    """A job a worker holds: enough to build, solve and store it without the row."""

    id: uuid.UUID
    organization_id: uuid.UUID
    first_day: date
    last_day: date
    project_ids: tuple[uuid.UUID, ...]
    solver: str
    attempt: int
    settings: dict[str, int]
    lease_until: datetime | None = None


def proposal_state(job: ScheduleJob, now: datetime) -> str | None:
    """``open``, ``accepted``, ``discarded`` or ``expired``; ``None`` before the job succeeds."""
    if job.proposal_state == "open" and job.finished_at and job.finished_at < now - EXPIRY:
        return "expired"
    return job.proposal_state


def proposal_json(built: Built, result: Result) -> dict[str, Any]:
    """The stored form of a proposal (data-model.md ``result``)."""
    return {
        "slots": [
            {
                "key": s.key,
                "shift_id": str(s.shift_id) if s.shift_id else None,
                "project_id": str(s.project_id),
                "shift_type_id": str(s.shift_type_id),
                "date": s.day.isoformat(),
                "starts_at": s.starts_at.astimezone(UTC).isoformat(),
                "ends_at": s.ends_at.astimezone(UTC).isoformat(),
                "headcount": s.headcount,
                "assigned": [str(p) for p in result.assigned.get(s.key, ())],
                "kept": [str(p) for p in s.kept],
            }
            for s in built.problem.slots
        ],
        "unfilled": [
            {
                "key": u.key,
                "places": u.places,
                "reasons": [{"code": r.code, "employees": r.employees} for r in u.reasons],
            }
            for u in result.unfilled
        ],
        "employees": [
            {
                "user_id": str(e.user_id),
                "minutes": e.minutes,
                "nights": e.nights,
                "open_met": e.open_met,
                "close_not_met": e.close_not_met,
            }
            for e in result.summary
        ],
        "no_availability": [str(p) for p in built.no_availability],
        "short_published": [
            {"shift_id": str(s.shift_id), "needed": s.needed, "assigned": s.assigned}
            for s in built.short_published
        ],
        "changes": {
            "added": [{"key": k, "user_id": str(p)} for k, p in result.changes.added],
            "removed": [{"key": k, "user_id": str(p)} for k, p in result.changes.removed],
        },
    }


class ScheduleJobService:
    """Jobs for one request's or one worker step's session."""

    def __init__(
        self,
        job_repo: ScheduleJobRepository,
        organisation_repo: OrganisationRepository,
        inputs: ScheduleInputService,
        project_repo: ProjectRepository,
        shift_repo: ShiftRepository,
        assignment_repo: AssignmentRepository,
        user_repo: UserRepository,
        shift_type_repo: ShiftTypeRepository,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        """Wire the service.

        Args:
            job_repo: Jobs, which are also the queue.
            organisation_repo: Organisations, for the limits a job records.
            inputs: Builds the problem a job solves.
            project_repo: Projects, locked while a proposal is accepted.
            shift_repo: Shifts, where an accepted proposal's drafts are written.
            assignment_repo: Assignments, likewise.
            user_repo: Users, locked while a proposal is accepted.
            shift_type_repo: Shift types, share-locked while a proposal is accepted.
            now: The current UTC time.
        """
        self._jobs = job_repo
        self._orgs = organisation_repo
        self._inputs = inputs
        self._projects = project_repo
        self._shifts = shift_repo
        self._assignments = assignment_repo
        self._users = user_repo
        self._types = shift_type_repo
        self._now = now

    async def create(self, actor: User, data: dict[str, Any]) -> ScheduleJob:
        """Ask for a roster (Manager or Admin); the job is queued.

        Args:
            actor: The caller.
            data: ``first_day``, ``last_day``, ``project_ids`` (empty for every active project)
                and ``solver``.

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
            DomainValidationError: ``schedule.range_invalid`` for a range that ends before it
                starts, covers more than 31 days, or lies within 14 days of either end of the
                calendar; ``schedule.project_not_active`` for a project
                that is not an active one of the organisation (HTTP 422).
        """
        ensure_role(actor, *MANAGER_ROLES)
        first, last = data["first_day"], data["last_day"]
        at_the_ends = first < date.min + CALENDAR_MARGIN or last > date.max - CALENDAR_MARGIN
        if at_the_ends or last < first or (last - first).days >= MAX_DAYS:
            raise DomainValidationError(
                code="schedule.range_invalid",
                detail="The range must not end before it starts, and covers at most 31 days.",
            )
        ids = list(dict.fromkeys(data.get("project_ids") or []))
        if ids:
            found = await self._inputs.active_projects(actor.organization_id, ids)
            if len(found) != len(ids):
                raise DomainValidationError(
                    code="schedule.project_not_active",
                    detail="Every project must be an active project of the organisation.",
                )
        org = await self._orgs.get_by_id(actor.organization_id)
        if org is None:
            raise NotFoundError(resource="Organisation", id=actor.organization_id)
        return await self._jobs.create(
            actor.organization_id,
            {
                "requested_by": actor.id,
                "first_day": first,
                "last_day": last,
                "project_ids": [str(i) for i in ids],
                "solver": data.get("solver") or "cpsat",
                "settings": {
                    "min_rest_hours": org.min_rest_hours,
                    "max_weekly_hours": org.max_weekly_hours,
                },
            },
        )

    async def get(self, actor: User, id: uuid.UUID) -> ScheduleJob:
        """A job of the organisation (Manager or Admin).

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
            NotFoundError: Not in the organisation (HTTP 404).
        """
        ensure_role(actor, *MANAGER_ROLES)
        job = await self._jobs.get(id, actor.organization_id)
        if job is None:
            raise NotFoundError(resource="ScheduleJob", id=id)
        return job

    async def page(
        self,
        actor: User,
        status: str | None,
        after: tuple[datetime, uuid.UUID] | None,
        limit: int,
    ) -> list[ScheduleJob]:
        """The organisation's jobs, newest first (Manager or Admin).

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
        """
        ensure_role(actor, *MANAGER_ROLES)
        return await self._jobs.page(actor.organization_id, status, after, limit)

    def state(self, job: ScheduleJob) -> str | None:
        """The proposal's state now, ``expired`` included."""
        return proposal_state(job, self._now())

    async def _open(self, actor: User, id: uuid.UUID) -> ScheduleJob:
        """The job, locked, if its proposal can still be decided."""
        ensure_role(actor, *MANAGER_ROLES)
        job = await self._jobs.get_locked(id, actor.organization_id)
        if job is None:
            raise NotFoundError(resource="ScheduleJob", id=id)
        if job.status != "succeeded":
            raise ConflictError(
                code="schedule.not_ready", detail="The job has no proposal to decide on."
            )
        if self.state(job) != "open":
            raise ConflictError(
                code="schedule.proposal_closed",
                detail="The proposal was already accepted, discarded, or has expired.",
            )
        return job

    async def accept(self, actor: User, id: uuid.UUID) -> dict[str, int]:
        """Store a proposal as draft shifts and assignments, all or nothing (Manager or Admin).

        Locks, in order: the job; the organisation's row (its limits); the run's projects (every
        active one when the job named none); the shift types; every shift in the run's
        surroundings; every staff account, in id order. Then the problem is built again and its
        fingerprint compared, so nothing it used can change between the check and the writes
        (research G9, G10). While it runs, staff sign-ins wait on their own rows; accepting is rare
        and short, and this is what makes the check exact. An existing draft keeps its id and
        headcount and gets the proposal's people; a new slot becomes a new draft. Nothing is
        published.

        Returns:
            How many shifts were created and updated, and assignments added and removed.

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
            NotFoundError: Not in the organisation (HTTP 404).
            ConflictError: ``schedule.not_ready``, ``schedule.proposal_closed`` or
                ``schedule.proposal_out_of_date`` (HTTP 409); nothing changes.
        """
        job = await self._open(actor, id)
        org_id = actor.organization_id
        org = await self._orgs.get_by_id(org_id)
        if org is None:
            raise NotFoundError(resource="Organisation", id=org_id)
        proposal = job.result or {"slots": []}
        run_ids = [uuid.UUID(p) for p in job.project_ids]
        # Lock everything the fingerprint covers, in the order the other paths take these locks
        # (an assignment locks its shift, then its user), before rebuilding (research G9): the
        # limits; the run's projects, fully, so needs, shift types and new shifts there wait; the
        # shift types; every shift in the surroundings, so publishing, changing, deleting,
        # assigning and unassigning wait; then every staff account.
        await self._orgs.lock(org_id)
        await self._projects.lock_for_shifts(org_id, run_ids or None)
        await self._types.share_lock_all(org_id)
        _, _, window = surroundings(
            org.timezone, job.first_day, job.last_day, timedelta(hours=org.min_rest_hours)
        )
        await self._shifts.lock_between(org_id, *window)
        await self._users.lock_staff(org_id)
        built = await self._inputs.build(org_id, job.first_day, job.last_day, run_ids)
        if built.fingerprint != job.fingerprint:
            raise ConflictError(
                code="schedule.proposal_out_of_date",
                detail="Something the proposal was made from has changed; generate it again.",
            )

        counts = dict.fromkeys(
            ("shifts_created", "shifts_updated", "assignments_added", "assignments_removed"), 0
        )
        slots = {s.key: s for s in built.problem.slots}
        new_shifts: list[dict[str, Any]] = []
        new_people: list[dict[str, Any]] = []
        for item in proposal["slots"]:
            chosen = [uuid.UUID(p) for p in item["assigned"]]
            if item["shift_id"] is not None:
                shift_id = uuid.UUID(item["shift_id"])
                now_on = set(built.problem.current.get(item["key"], ()))
                leaving = sorted(now_on - set(chosen), key=str)
                joining = [p for p in chosen if p not in now_on]
                counts["assignments_removed"] += await self._assignments.remove_people(
                    shift_id, leaving
                )
                if leaving or joining:
                    counts["shifts_updated"] += 1
            else:
                slot = slots[item["key"]]
                shift_id = uuid.uuid4()
                new_shifts.append(
                    {
                        "id": shift_id,
                        "project_id": slot.project_id,
                        "shift_type_id": slot.shift_type_id,
                        "starts_at": slot.starts_at,
                        "ends_at": slot.ends_at,
                        "headcount": slot.headcount,
                    }
                )
                joining = chosen
                counts["shifts_created"] += 1
            new_people += [
                {"shift_id": shift_id, "user_id": p, "assigned_by": actor.id, "source": "generated"}
                for p in joining
            ]
        await self._shifts.create_many(org_id, new_shifts)
        await self._assignments.create_many(org_id, new_people)
        counts["assignments_added"] = len(new_people)
        await self._jobs.decide(job, "accepted", actor.id, self._now())
        return counts

    async def discard(self, actor: User, id: uuid.UUID) -> ScheduleJob:
        """Close an open proposal without storing it (Manager or Admin).

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
            NotFoundError: Not in the organisation (HTTP 404).
            ConflictError: ``schedule.not_ready`` or ``schedule.proposal_closed`` (HTTP 409).
        """
        job = await self._open(actor, id)
        await self._jobs.decide(job, "discarded", actor.id, self._now())
        return job

    # ------------------------------------------------------------------ the worker's steps

    async def counts_by_state(self) -> dict[str, int]:
        """How many jobs are in each state; a state with none is absent (the worker's metrics).

        ``JobMetrics.set_counts`` reports an absent state as 0, so the states are listed once.
        """
        return await self._jobs.count_by_state()

    async def claim(self) -> Claim | None:
        """Claim the next job (worker); ``None`` when there is none. The caller commits."""
        job, lost = await self._jobs.claim(self._now(), LEASE, MAX_ATTEMPTS)
        for job_id in lost:
            logger.warning("schedule job %s lost its worker %d times; failed", job_id, MAX_ATTEMPTS)
        if job is None:
            return None
        return Claim(
            job.id,
            job.organization_id,
            job.first_day,
            job.last_day,
            tuple(uuid.UUID(i) for i in job.project_ids),
            job.solver,
            job.attempts,
            dict(job.settings),
            job.lease_until,
        )

    async def build(self, claim: Claim) -> Built:
        """The problem the claimed job solves, read now, with the job's own limits (worker).

        Raises:
            JobFailedError: ``jobs.project_not_active`` when a project the job named is no longer
                active, so a proposal never quietly leaves one out.
        """
        built = await self._inputs.build(
            claim.organization_id,
            claim.first_day,
            claim.last_day,
            claim.project_ids,
            limits=claim.settings,
        )
        if claim.project_ids and len(built.project_ids) != len(set(claim.project_ids)):
            raise JobFailedError("jobs.project_not_active")
        return built

    async def succeed(self, claim: Claim, built: Built, result: Result) -> bool:
        """Store the proposal (worker); whether the job was still this run's. The caller commits."""
        return await self._jobs.finish(
            claim.id,
            claim.attempt,
            {
                "status": "succeeded",
                "result": proposal_json(built, result),
                "fingerprint": built.fingerprint,
                "optimal": result.optimal,
                "proposal_state": "open",
                "finished_at": self._now(),
            },
        )

    async def fail(self, claim: Claim, code: str) -> bool:
        """Mark the claimed job failed with ``code`` (worker). The caller commits."""
        return await self._jobs.finish(
            claim.id,
            claim.attempt,
            {"status": "failed", "error_code": code, "finished_at": self._now()},
        )


async def solve(claim: Claim, built: Built, time_limit: timedelta) -> Result:
    """Run the solver in a thread, so the worker's event loop keeps its database connections."""
    return await asyncio.to_thread(
        generate,
        built.problem,
        solver=claim.solver,  # type: ignore[arg-type]
        time_limit=time_limit,
    )
