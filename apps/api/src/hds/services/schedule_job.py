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
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime, timedelta
from typing import Any

from hds.core.authz import MANAGER_ROLES, ensure_role
from hds.core.capabilities import Capability, has_on_project
from hds.core.exceptions import (
    ConflictError,
    DomainValidationError,
    ForbiddenError,
    NotFoundError,
    PreconditionFailedError,
)
from hds.core.trace_context import current_traceparent
from hds.models.schedule_job import ScheduleJob
from hds.models.user import User, UserRole
from hds.repositories.organisation import OrganisationRepository
from hds.repositories.project import ProjectRepository
from hds.repositories.project_manager import ProjectManagerRepository
from hds.repositories.roster_extras import RuleOverrideRepository
from hds.repositories.schedule_job import ScheduleJobRepository
from hds.repositories.schedule_job_event import ScheduleJobEventRepository
from hds.repositories.shift import AssignmentRepository, ShiftRepository
from hds.repositories.shift_type import ShiftTypeRepository
from hds.repositories.user import UserRepository
from hds.scheduling import Employee, Result, generate, validate
from hds.scheduling.problem import Chip, Controls
from hds.services import proposal_copy
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


def check_version(job: ScheduleJob, if_match: int | None) -> None:
    """Refuse a change made against an older version of the working copy (008 research P8).

    Raises:
        PreconditionFailedError: ``schedule.version_stale`` (HTTP 412) when ``if_match`` is given
            and is not the copy's version.
    """
    if if_match is not None and if_match != job.version:
        raise PreconditionFailedError(
            code="schedule.version_stale",
            detail="The proposal changed since you read it; read it again.",
        )


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
    organisation_id: uuid.UUID
    first_day: date
    last_day: date
    project_ids: tuple[uuid.UUID, ...]
    solver: str
    attempt: int
    settings: dict[str, int]
    lease_until: datetime | None = None
    trace_context: str | None = None
    # 008: the run's controls and chips as stored, and the earlier run it reruns, if any.
    controls: dict[str, Any] = field(default_factory=dict)
    chips: tuple[dict[str, Any], ...] = ()
    based_on_id: uuid.UUID | None = None


def proposal_state(job: ScheduleJob, now: datetime) -> str | None:
    """``open``, ``accepted``, ``discarded``, ``partly_accepted`` or ``expired``.

    ``None`` before the job succeeds. An open proposal past its 7 days reads as expired, or as
    partly accepted when some of it was accepted (008 FR-004).
    """
    if job.proposal_state == "open" and job.finished_at and job.finished_at < now - EXPIRY:
        if job.current is not None and proposal_copy.any_accepted(job.current):
            return "partly_accepted"
        return "expired"
    return job.proposal_state


def proposal_json(built: Built, result: Result) -> dict[str, Any]:
    """The stored form of a proposal (data-model.md ``result``).

    The people a rerun held for the earlier proposal's locks (``built.held``) are stored as the
    proposal's own, added where the drafts do not have them and marked locked (008 FR-019).
    """
    held = built.held
    current = built.problem.current
    added = [*result.changes.added] + [
        (key, p) for key, people in held.items() for p in people if p not in current.get(key, ())
    ]
    return {
        "slots": [
            {
                "key": s.key,
                "shift_id": str(s.shift_id) if s.shift_id else None,
                "zone": s.zone,
                "project_id": str(s.project_id),
                "site_id": str(s.site_id) if s.site_id else None,
                "shift_type_id": str(s.shift_type_id),
                "date": s.day.isoformat(),
                "starts_at": s.starts_at.astimezone(UTC).isoformat(),
                "ends_at": s.ends_at.astimezone(UTC).isoformat(),
                "headcount": s.headcount,
                "assigned": [
                    str(p) for p in (*result.assigned.get(s.key, ()), *held.get(s.key, ()))
                ],
                "kept": [str(p) for p in s.kept if p not in held.get(s.key, ())],
                "locked": [str(p) for p in held.get(s.key, ())],
            }
            for s in built.problem.slots
        ],
        "unfilled": [
            {
                "key": u.key,
                "places": u.places,
                "reasons": [
                    {
                        "code": r.code,
                        "employees": r.employees,
                        "people": [
                            {
                                "user_id": str(b.user_id),
                                "code": b.code,
                                "have": b.have,
                                "need": b.need,
                                "week_minutes": b.week_minutes,
                                "slot_minutes": b.slot_minutes,
                                "limit": b.limit,
                            }
                            for b in r.people
                        ],
                    }
                    for r in u.reasons
                ],
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
        # How many locked assignments the run kept, which it states (008 FR-018).
        "locks_kept": built.locks,
        "short_published": [
            {"shift_id": str(s.shift_id), "needed": s.needed, "assigned": s.assigned}
            for s in built.short_published
        ],
        "changes": {
            "added": [
                {"key": k, "user_id": str(p)}
                for k, p in sorted(added, key=lambda kp: (kp[0], str(kp[1])))
            ],
            "removed": [{"key": k, "user_id": str(p)} for k, p in result.changes.removed],
        },
    }


def stored_chip(stored: dict[str, Any]) -> Chip:
    """A stored chip as the scheduling package takes it."""

    def ref(name: str) -> uuid.UUID | None:
        value = stored.get(name)
        return uuid.UUID(str(value)) if value else None

    return Chip(
        kind=str(stored["kind"]),
        user_id=uuid.UUID(str(stored["user_id"])),
        count=stored.get("count"),
        project_id=ref("project_id"),
        site_id=ref("site_id"),
        weekday=stored.get("weekday"),
        shift_type_id=ref("shift_type_id"),
    )


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
        project_manager_repo: ProjectManagerRepository,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
        override_repo: RuleOverrideRepository | None = None,
        event_repo: ScheduleJobEventRepository | None = None,
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
            project_manager_repo: Who manages which project: a run's scope (007 T108).
            now: The current UTC time.
            override_repo: Rule overrides, written for a person's overrides when accepting (008).
            event_repo: The run record (008 FR-030); steps are not recorded without it (tests).
        """
        self._jobs = job_repo
        self._orgs = organisation_repo
        self._inputs = inputs
        self._projects = project_repo
        self._shifts = shift_repo
        self._assignments = assignment_repo
        self._users = user_repo
        self._types = shift_type_repo
        self._managers = project_manager_repo
        self._now = now
        self._overrides = override_repo
        self._events = event_repo

    async def create(self, actor: User, data: dict[str, Any]) -> ScheduleJob:
        """Ask for a roster (Manager or Admin); the job is queued with the current trace, if any.

        Args:
            actor: The caller.
            data: ``first_day``, ``last_day``, ``project_ids`` (empty for every active project)
                and ``solver``.

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
            ForbiddenError: ``roster.project_not_managed`` for a Manager naming a project they
                do not manage (HTTP 403); no job is created.
            DomainValidationError: ``schedule.range_invalid`` for a range that ends before it
                starts, covers more than 31 days, or lies within 14 days of either end of the
                calendar; ``schedule.project_not_active`` for a project
                that is not an active one of the organisation; ``schedule.no_projects`` when
                no project is named and the caller has none to schedule (HTTP 422).

        No project named means the caller's: a Manager's managed active projects, an Admin's
        every active one. The resolved list is stored on the job, so its page shows what it
        covered (reviewer's ruling, 2026-11-08, from O9 and T098).
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
            found = await self._inputs.active_projects(actor.organisation_id, ids)
            if len(found) != len(ids):
                raise DomainValidationError(
                    code="schedule.project_not_active",
                    detail="Every project must be an active project of the organisation.",
                )
            await self._require_scope(actor, ids)
        else:
            active = [p.id for p in await self._inputs.active_projects(actor.organisation_id, None)]
            ids = await self._in_scope(actor, active)
            if not ids:
                raise DomainValidationError(
                    code="schedule.no_projects",
                    detail="There is no active project of yours to schedule.",
                )
        org = await self._orgs.get_by_id(actor.organisation_id)
        if org is None:
            raise NotFoundError(resource="Organisation", id=actor.organisation_id)
        based_on = data.get("based_on")
        chips = data.get("chips")
        if based_on is not None:
            # An earlier run of this organisation only; another's is not found (FR-035).
            earlier = await self._jobs.get(based_on, actor.organisation_id)
            if earlier is None:
                raise NotFoundError(resource="ScheduleJob", id=based_on)
            if chips is None:
                chips = list(earlier.chips or [])
        chips = await self._checked_chips(actor, chips or [])
        job = await self._jobs.create(
            actor.organisation_id,
            {
                "requested_by": actor.id,
                "first_day": first,
                "last_day": last,
                "project_ids": [str(i) for i in ids],
                "solver": data.get("solver") or "cpsat",
                "controls": dict(data.get("controls") or {}),
                "chips": chips,
                "based_on_id": based_on,
                "settings": {
                    "min_rest_hours": org.min_rest_hours,
                    "max_weekly_hours": org.max_weekly_hours,
                },
                # The request's trace, so the worker's run of the job joins it (FR-008a).
                "trace_context": current_traceparent(),
            },
        )
        # The record starts with who ran it, the range, projects, controls and chips (FR-030).
        await self.record(
            job,
            "created",
            {
                "from": first.isoformat(),
                "to": last.isoformat(),
                "project_ids": [str(i) for i in ids],
                "controls": dict(data.get("controls") or {}),
                "chips": chips,
                "based_on": str(based_on) if based_on else None,
            },
            actor=actor,
        )
        return job

    async def _checked_chips(
        self, actor: User, chips: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """The chips, each with what its kind needs and naming staff of the organisation.

        Staff deleted or deactivated since still pass: their chip no longer applies (FR-022).

        Raises:
            DomainValidationError: ``schedule.chip_invalid`` for a chip without its value, or
                naming someone who is not staff of the organisation (HTTP 422).
        """
        needs = {
            "max_shifts": ("count",),
            "max_nights": ("count",),
            "max_hours": ("count",),
            "keep_on": ("project_id", "site_id"),
            "keep_off": ("weekday", "shift_type_id"),
            "prefer": ("weekday", "shift_type_id"),
        }
        people = {uuid.UUID(str(c["user_id"])) for c in chips}
        staff = (
            {
                u.id
                for u in await self._users.get_many(people, actor.organisation_id)
                # Anyone of the organisation's staff, even deleted or inactive since: such a chip is
                # reported as no longer applying and never fails a run (FR-022, R42).
                if u.role != UserRole.client
            }
            if people
            else set()
        )
        for index, chip in enumerate(chips):
            fields = needs.get(chip.get("kind", ""), ())
            if not any(chip.get(f) is not None for f in fields):
                raise DomainValidationError(
                    code="schedule.chip_invalid",
                    detail="A preference needs the value its kind asks for.",
                    errors=[{"chip": index, "field": "value"}],
                )
            if uuid.UUID(str(chip["user_id"])) not in staff:
                raise DomainValidationError(
                    code="schedule.chip_invalid",
                    detail="A preference must name a member of the organisation's staff.",
                    errors=[{"chip": index, "field": "user_id"}],
                )
        return [{k: str(v) if isinstance(v, uuid.UUID) else v for k, v in c.items()} for c in chips]

    async def stop(self, actor: User, id: uuid.UUID) -> ScheduleJob:
        """Stop a run (008 FR-028).

        A queued one is cancelled at once; a running one ends with its best result so far, or
        cancelled when it has none yet, within a few seconds. A Manager only on runs whose
        projects they all manage (007 T108).

        Raises:
            ForbiddenError: ``users.manager_required`` or ``roster.project_not_managed`` (HTTP 403).
            NotFoundError: Not in the organisation (HTTP 404).
            ConflictError: ``schedule.not_running`` for a run already over (HTTP 409).
        """
        job = await self.get(actor, id)
        await self.require_scope_of(actor, job)
        locked = await self._jobs.get_locked(id, actor.organisation_id)
        assert locked is not None, "get found it"
        if locked.status == "queued":
            await self._jobs.cancel_queued(locked.id, self._now())
            await self.record(locked, "cancelled", {"before_any_result": True}, actor=actor)
        elif locked.status == "running":
            if locked.stop_requested_at is None:
                await self._jobs.ask_stop(locked.id, self._now())
                await self.record(locked, "stopped", {"asked": True}, actor=actor)
        else:
            raise ConflictError(code="schedule.not_running", detail="The run has already ended.")
        refreshed = await self._jobs.get(id, actor.organisation_id)
        assert refreshed is not None
        return refreshed

    async def record_page(
        self, actor: User, id: uuid.UUID, after: int | None, limit: int
    ) -> list[Any]:
        """A page of the run's record, oldest first (008 FR-030, FR-031).

        An Admin reads every run's; a Manager, the runs whose projects they all manage.

        Raises:
            ForbiddenError: ``users.manager_required`` or ``roster.project_not_managed`` (HTTP 403).
            NotFoundError: Not in the organisation (HTTP 404).
        """
        job = await self.get(actor, id)
        await self.require_scope_of(actor, job)
        if self._events is None:
            return []
        return await self._events.page(job.organisation_id, job.id, after, limit)

    async def recorded(self, claim: Claim, kind: str, body: dict[str, Any]) -> None:
        """Add how a worker's run ended to its record (worker, FR-030)."""
        if self._events is not None:
            await self._events.append(claim.organisation_id, claim.id, kind, self._now(), body)

    async def get(self, actor: User, id: uuid.UUID) -> ScheduleJob:
        """A job of the organisation (Manager or Admin).

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
            NotFoundError: Not in the organisation (HTTP 404).
        """
        ensure_role(actor, *MANAGER_ROLES)
        job = await self._jobs.get(id, actor.organisation_id)
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
        return await self._jobs.page(actor.organisation_id, status, after, limit)

    def state(self, job: ScheduleJob) -> str | None:
        """The proposal's state now, ``expired`` included."""
        return proposal_state(job, self._now())

    def elapsed_seconds(self, job: ScheduleJob) -> int | None:
        """How long the job has searched, by the server's clock.

        Whole seconds, to now while it runs, to its end once finished; None before it starts (007
        FR-074: the progress bar never reads a device's clock).
        """
        if job.started_at is None:
            return None
        end = job.finished_at or self._now()
        return max(0, (end - job.started_at) // timedelta(seconds=1))

    async def _in_scope(self, actor: User, ids: list[uuid.UUID]) -> list[uuid.UUID]:
        """Those of ``ids`` the actor may schedule: every one for an Admin, a Manager's own."""
        if actor.role == UserRole.admin:
            return list(ids)
        managed = await self._managers.manages(actor.id, ids)
        return [
            i for i in ids if has_on_project(actor.role, Capability.ROSTER_EDIT_PROJECT, i, managed)
        ]

    async def _require_scope(self, actor: User, ids: list[uuid.UUID]) -> None:
        """Refuse unless the actor may schedule every one of ``ids`` (007 T108)."""
        if len(await self._in_scope(actor, ids)) != len(ids):
            raise ForbiddenError(
                code="roster.project_not_managed",
                detail="A Manager schedules only the projects they manage.",
            )

    async def _open(self, actor: User, id: uuid.UUID) -> ScheduleJob:
        """The job, locked, if its proposal can still be decided by ``actor``.

        Deciding takes the same scope as asking, checked again now: management may have changed
        since the run was asked for, and a refusal is of the whole proposal, never part of it. A
        job stored without projects (before 007 T108) covers every active one.
        """
        ensure_role(actor, *MANAGER_ROLES)
        job = await self._jobs.get_locked(id, actor.organisation_id)
        if job is None:
            raise NotFoundError(resource="ScheduleJob", id=id)
        if actor.role != UserRole.admin:
            # An Admin may decide on every run; a Manager's is checked against what it covers.
            covered = [uuid.UUID(p) for p in job.project_ids] or [
                p.id for p in await self._inputs.active_projects(actor.organisation_id, None)
            ]
            await self._require_scope(actor, covered)
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

    async def accept(
        self,
        actor: User,
        id: uuid.UUID,
        part: Mapping[str, Any] | None = None,
        if_match: int | None = None,
    ) -> dict[str, Any]:
        """Store a proposal, or a part of it, as draft shifts and assignments (Manager or Admin).

        A part names ``days`` (ISO dates), ``project_ids`` or ``assignments`` (``key`` and
        ``user_id`` pairs); nothing named means everything still open (008 FR-003). One accept is
        all or nothing; what it does not name stays open (FR-004).

        Locks, in order: the job; the organisation's row (its limits); the run's projects (every
        active one when the job named none); the shift types; every shift in the run's
        surroundings; every staff account, in id order. Then the problem is built again (research
        G9, G10). When its fingerprint still matches, nothing the proposal was made from has
        changed and the part is stored. Otherwise the part is checked on its own (008 research
        P9): none of its drafts may have changed since the proposal, and it may break no rule on
        today's roster that the working copy does not already override. An existing draft keeps
        its id and headcount and gets the proposal's people; a new slot becomes a new draft; a
        person's override becomes a rule override on the new assignment. Nothing is published.

        Returns:
            How many shifts were created and updated, and assignments added and removed; the
            working copy's new ``version``; and the proposal's ``state``.

        Raises:
            ForbiddenError: ``users.manager_required`` or ``roster.project_not_managed`` (HTTP 403).
            NotFoundError: Not in the organisation (HTTP 404).
            PreconditionFailedError: ``schedule.version_stale`` (HTTP 412).
            ConflictError: ``schedule.not_ready``, ``schedule.proposal_closed``,
                ``schedule.nothing_to_accept``, ``schedule.proposal_out_of_date`` or
                ``schedule.part_out_of_date`` (HTTP 409); nothing changes.
        """
        job = await self._open(actor, id)
        check_version(job, if_match)
        org_id = actor.organisation_id
        org = await self._orgs.get_by_id(org_id)
        if org is None:
            raise NotFoundError(resource="Organisation", id=org_id)
        work = proposal_copy.of(job.current, job.result)
        part = part or {}
        chosen = proposal_copy.selected(
            work,
            days=part.get("days", ()),
            project_ids=part.get("project_ids", ()),
            pairs=[(a["key"], a["user_id"]) for a in part.get("assignments", ())],
        )
        if not chosen:
            if any(part.get(k) for k in ("days", "project_ids", "assignments")):
                raise ConflictError(
                    code="schedule.nothing_to_accept",
                    detail="Nothing the part names is still open in the proposal.",
                )
            # Nothing left to write (the proposal matches the roster): accepting closes it, as
            # before 008, writing only the locks set in the review.
            await self._write_locks(work, None, actor)
            job.version += 1
            await self._jobs.decide(job, "accepted", actor.id, self._now())
            await self.record(job, "accepted", {"part": {}}, actor=actor, version=job.version)
            return {
                "shifts_created": 0,
                "shifts_updated": 0,
                "assignments_added": 0,
                "assignments_removed": 0,
                "version": job.version,
                "state": "accepted",
            }
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
        named = any(part.get(k) for k in ("days", "project_ids", "assignments"))
        untouched = job.version == 0 and not proposal_copy.any_accepted(work) and not named
        if untouched and built.fingerprint != job.fingerprint:
            # The whole of an untouched proposal, as before 008: any change refuses it.
            raise ConflictError(
                code="schedule.proposal_out_of_date",
                detail="Something the proposal was made from has changed; generate it again.",
            )
        if not untouched:
            # Anything else is judged on today's roster, even when the fingerprint still matches:
            # it does not cover people placed by hand who were not candidates (their shifts
            # elsewhere, their entries, whether they are active), nor a part that takes one pair
            # of a slot and leaves another.
            placed = await self._people(org_id, {p for who in chosen.values() for p in who})
            judged = await self._inputs.build(
                org_id, job.first_day, job.last_day, run_ids, also=set(placed)
            )
            await self._check_part(org.timezone, judged, work, chosen, placed)

        counts: dict[str, Any] = dict.fromkeys(
            ("shifts_created", "shifts_updated", "assignments_added", "assignments_removed"), 0
        )
        slots = {s.key: s for s in built.problem.slots}
        new_shifts: list[dict[str, Any]] = []
        new_people: list[dict[str, Any]] = []
        overrides: list[dict[str, Any]] = []
        for slot in work["slots"]:
            take = chosen.get(slot["key"])
            if not take:
                continue
            adding, dropping = proposal_copy.open_pairs(slot)
            joining = sorted(adding & take)
            leaving = sorted(dropping & take)
            if slot["shift_id"] is not None:
                shift_id = uuid.UUID(slot["shift_id"])
                counts["assignments_removed"] += await self._assignments.remove_people(
                    shift_id, [uuid.UUID(p) for p in leaving]
                )
                counts["shifts_updated"] += 1
                # A changed staffing is a changed shift: its roster ETag moves, so a reader's
                # If-Match goes stale and "changed since publish" sees it (R84).
                await self._shifts.touch(shift_id, org_id)
            else:
                known = slots.get(slot["key"])
                shift_id = uuid.uuid4()
                new_shifts.append(
                    {
                        "id": shift_id,
                        "project_id": uuid.UUID(slot["project_id"]),
                        "shift_type_id": uuid.UUID(slot["shift_type_id"]),
                        "starts_at": known.starts_at
                        if known
                        else datetime.fromisoformat(slot["starts_at"]),
                        "ends_at": known.ends_at
                        if known
                        else datetime.fromisoformat(slot["ends_at"]),
                        # Today's need: the part check refuses one whose headcount changed (R5).
                        "headcount": known.headcount if known else slot["headcount"],
                    }
                )
                slot["shift_id"] = str(shift_id)
                counts["shifts_created"] += 1
            by_person = {p["user_id"]: p for p in slot["people"]}
            for person in joining:
                entry = by_person[person]
                assignment_id = uuid.uuid4()
                new_people.append(
                    {
                        "id": assignment_id,
                        "shift_id": shift_id,
                        "user_id": uuid.UUID(person),
                        "assigned_by": actor.id,
                        "source": "manual" if entry["by"] == "person" else "generated",
                        # A lock in the proposal reaches the roster with it (008 FR-018).
                        "locked_by": actor.id if entry.get("locked") else None,
                        "locked_at": self._now() if entry.get("locked") else None,
                    }
                )
                if entry.get("override"):
                    for code in entry["override"]["codes"]:
                        overrides.append(
                            {
                                "assignment_id": assignment_id,
                                "warning_code": code,
                                "reason": entry["override"]["reason"],
                                "created_by": actor.id,
                            }
                        )
            slot["stored"] = sorted((set(slot["stored"]) | set(joining)) - set(leaving))
        await self._shifts.create_many(org_id, new_shifts)
        await self._assignments.create_many(org_id, new_people)
        # A lock set in the review on someone already on the draft reaches the roster too (R38).
        await self._write_locks(work, None if not named else set(chosen), actor)
        if self._overrides is not None:
            for row in overrides:
                await self._overrides.create(org_id, row)
        counts["assignments_added"] = len(new_people)
        job.current = work
        job.version += 1
        state = "accepted" if proposal_copy.is_settled(work) else "open"
        if state == "accepted":
            await self._jobs.decide(job, "accepted", actor.id, self._now())
        else:
            await self._jobs.save_copy(job)
        await self.record(
            job,
            "accepted",
            {"part": {k: v for k, v in part.items() if v}, **counts},
            actor=actor,
            version=job.version,
        )
        return {**counts, "version": job.version, "state": state}

    async def _write_locks(
        self, work: Mapping[str, Any], keys: set[str] | None, actor: User
    ) -> None:
        """Lock on the roster the people the copy locks on drafts (``keys``; every slot if None)."""
        for slot in work["slots"]:
            if not slot.get("shift_id") or (keys is not None and slot["key"] not in keys):
                continue
            locked = {uuid.UUID(p["user_id"]) for p in slot["people"] if p.get("locked")}
            await self._assignments.lock_people(
                uuid.UUID(slot["shift_id"]), locked, actor.id, self._now()
            )

    async def _people(self, org_id: uuid.UUID, ids: set[str]) -> dict[uuid.UUID, Employee]:
        """The people a part places, as the rules see them: deleted or inactive is inactive (R2)."""
        users = await self._users.get_many({uuid.UUID(p) for p in ids}, org_id) if ids else []
        return {u.id: Employee(u.id, u.is_active and not u.is_deleted) for u in users}

    async def _check_part(
        self,
        zone: str,
        built: Built,
        work: Mapping[str, Any],
        chosen: Mapping[str, set[str]],
        known: Mapping[uuid.UUID, Employee],
    ) -> None:
        """Refuse a part whose drafts changed, or that breaks a rule not overridden (research P9).

        Raises:
            ConflictError: ``schedule.part_out_of_date``, listing each slot, person and cause.
        """
        rebuilt = {s.key: s for s in built.problem.slots}
        causes: list[dict[str, Any]] = []
        for slot in work["slots"]:
            if slot["key"] not in chosen:
                continue
            if slot["shift_id"] is None:
                # A new slot: its need must still be one, of the same headcount, with no draft
                # made in its place (R5).
                need = rebuilt.get(slot["key"])
                if need is None or need.headcount != slot["headcount"]:
                    causes.append({"key": slot["key"], "date": slot["date"], "cause": "changed"})
                continue
            key = f"draft:{slot['shift_id']}"
            now = rebuilt.get(key)
            if now is None:
                causes.append({"key": slot["key"], "date": slot["date"], "cause": "removed"})
                continue
            on_now = {str(p) for p in (*built.problem.current.get(key, ()), *now.kept)}
            moved = (now.starts_at, now.ends_at) != (
                datetime.fromisoformat(slot["starts_at"]),
                datetime.fromisoformat(slot["ends_at"]),
            )
            if moved or on_now != {*slot["stored"], *slot.get("kept", [])}:
                causes.append({"key": slot["key"], "date": slot["date"], "cause": "changed"})
        if not causes:
            for warning in self._part_breaks(zone, built, work, chosen, known):
                causes.append(warning)
        if causes:
            raise ConflictError(
                code="schedule.part_out_of_date",
                detail="Part of what you chose changed since the proposal; nothing was accepted.",
                errors=causes,
            )

    def _part_breaks(
        self,
        zone: str,
        built: Built,
        work: Mapping[str, Any],
        chosen: Mapping[str, set[str]],
        known: Mapping[uuid.UUID, Employee],
    ) -> list[dict[str, Any]]:
        """The rules the part would break on today's roster that the copy does not override."""
        layout: dict[str, list[str]] = {}
        for slot in work["slots"]:
            stored = set(slot["stored"])
            take = chosen.get(slot["key"], set())
            adding, dropping = proposal_copy.open_pairs(slot)
            layout[slot["key"]] = sorted((stored | (adding & take)) - (dropping & take))
        shifts = proposal_copy.shifts(work, zone, layout)
        ours = {s.id for s in shifts}
        extra = proposal_copy.unlisted(built.problem, work, ours)
        focus = {proposal_copy.shift_id_of(s) for s in work["slots"] if s["key"] in chosen}
        found = validate(
            focus=focus,
            shifts=[*built.problem.fixed, *extra, *shifts],
            entries=built.problem.entries,
            employees=proposal_copy.everyone(
                built.problem, [*built.problem.fixed, *extra, *shifts], known
            ),
            settings=built.problem.settings,
        )
        return proposal_copy.unexcused(found, work, chosen)

    async def discard(self, actor: User, id: uuid.UUID, if_match: int | None = None) -> ScheduleJob:
        """Close an open proposal without storing what is still open (Manager or Admin).

        A proposal with parts accepted closes as ``partly_accepted``; those parts stay drafts
        (008 FR-005).

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
            NotFoundError: Not in the organisation (HTTP 404).
            PreconditionFailedError: ``schedule.version_stale`` (HTTP 412).
            ConflictError: ``schedule.not_ready`` or ``schedule.proposal_closed`` (HTTP 409).
        """
        job = await self._open(actor, id)
        check_version(job, if_match)
        work = proposal_copy.of(job.current, job.result)
        state = "partly_accepted" if proposal_copy.any_accepted(work) else "discarded"
        await self._jobs.decide(job, state, actor.id, self._now())
        await self.record(job, "discarded", {"state": state}, actor=actor, version=job.version)
        return job

    async def record(
        self,
        job: ScheduleJob,
        kind: str,
        body: dict[str, Any],
        *,
        actor: User | None = None,
        version: int | None = None,
    ) -> None:
        """Add a step to the run's record (008 FR-030), when the record is wired."""
        if self._events is not None:
            await self._events.append(
                job.organisation_id,
                job.id,
                kind,
                self._now(),
                body,
                actor_id=actor.id if actor else None,
                version=version,
            )

    async def save(self, job: ScheduleJob) -> None:
        """Write a locked job's working copy and version (008, ADR 020)."""
        await self._jobs.save_copy(job)

    async def open_for(self, actor: User, id: uuid.UUID) -> ScheduleJob:
        """The job, locked, if ``actor`` may still change its proposal (see ``_open``)."""
        return await self._open(actor, id)

    async def require_scope_of(self, actor: User, job: ScheduleJob) -> None:
        """Refuse unless ``actor`` may act on every project ``job`` covers (007 T108)."""
        if actor.role == UserRole.admin:
            return
        covered = [uuid.UUID(p) for p in job.project_ids] or [
            p.id for p in await self._inputs.active_projects(job.organisation_id, None)
        ]
        await self._require_scope(actor, covered)

    # ------------------------------------------------------------------ the worker's steps

    async def counts_by_state(self) -> dict[str, int]:
        """How many jobs are in each state; a state with none is absent (the worker's metrics).

        ``JobMetrics.set_counts`` reports an absent state as 0, so the states are listed once.
        """
        return await self._jobs.count_by_state()

    async def claim(self) -> Claim | None:
        """Claim the next job (worker); ``None`` when there is none. The caller commits."""
        job, lost = await self._jobs.claim(self._now(), LEASE, MAX_ATTEMPTS)
        for job_id, org_id in lost:
            logger.warning("schedule job %s lost its worker %d times; failed", job_id, MAX_ATTEMPTS)
            # How it ended joins its record, as every other end does (008 FR-030).
            if self._events is not None:
                body = {"code": "jobs.worker_lost"}
                await self._events.append(org_id, job_id, "failed", self._now(), body)
        if job is None:
            return None
        return Claim(
            job.id,
            job.organisation_id,
            job.first_day,
            job.last_day,
            tuple(uuid.UUID(i) for i in job.project_ids),
            job.solver,
            job.attempts,
            dict(job.settings),
            job.lease_until,
            job.trace_context,
            dict(job.controls or {}),
            tuple(job.chips or ()),
            job.based_on_id,
        )

    async def build(self, claim: Claim) -> Built:
        """The problem the claimed job solves, read now, with the job's own limits (worker).

        Raises:
            JobFailedError: ``jobs.project_not_active`` when a project the job named is no longer
                active, so a proposal never quietly leaves one out.
        """
        built = await self._inputs.build(
            claim.organisation_id,
            claim.first_day,
            claim.last_day,
            claim.project_ids,
            limits=claim.settings,
        )
        if claim.project_ids and len(built.project_ids) != len(set(claim.project_ids)):
            raise JobFailedError("jobs.project_not_active")
        return await self._steered(claim, built)

    async def _steered(self, claim: Claim, built: Built) -> Built:
        """``built`` with the job's controls and chips, and a rerun's starting point and locks.

        A rerun (``based_on``) starts from the earlier run's working copy, slot by slot, and keeps
        the people locked in it as it keeps locks on the roster (008 FR-018, FR-019, research P2,
        P6). A slot the earlier run did not have starts from the drafts.
        """
        problem = built.problem
        start: dict[str, tuple[uuid.UUID, ...]] = dict(problem.current)
        kept_more: dict[str, tuple[uuid.UUID, ...]] = {}
        if claim.based_on_id is not None:
            earlier = await self._jobs.get(claim.based_on_id, claim.organisation_id)
            if earlier is not None and earlier.result is not None:
                work = proposal_copy.of(earlier.current, earlier.result)
                candidates = {c.id for c in problem.candidates}
                for slot in work["slots"]:
                    locked = {p["user_id"] for p in slot["people"] if p.get("locked")}
                    start[slot["key"]] = tuple(
                        uuid.UUID(p) for p in proposal_copy.people(slot) if p not in locked
                    )
                    kept_more[slot["key"]] = tuple(
                        uuid.UUID(p) for p in sorted(locked) if uuid.UUID(p) in candidates
                    )
        slots = tuple(
            replace(s, kept=(*s.kept, *(p for p in kept_more.get(s.key, ()) if p not in s.kept)))
            for s in problem.slots
        )
        steered = replace(
            problem,
            slots=slots,
            start=start,
            controls=Controls(**claim.controls),
            chips=tuple(stored_chip(c) for c in claim.chips),
        )
        held = {
            s.key: tuple(p for p in kept_more.get(s.key, ()) if p not in s.kept)
            for s in problem.slots
            if kept_more.get(s.key)
        }
        added = sum(len(v) for v in held.values())
        return replace(built, problem=steered, locks=built.locks + added, held=held)

    async def progress(self, claim: Claim, needed: int, filled: int | None) -> bool:
        """Write the run's best so far (worker); whether to stop. The caller commits.

        008 FR-027: the places filled out of those needed, and when they last improved, so
        ``filled`` is given only when it has; FR-028: stop when Stop was asked, or when the run
        is no longer this worker's (another reclaimed it), so it stops solving for nothing.
        """
        values: dict[str, Any] = {"needed": needed}
        if filled is not None:
            values |= {"best_filled": filled, "progress_at": self._now()}
        ours, asked = await self._jobs.progress(claim.id, claim.attempt, values)
        return asked or not ours

    async def succeed(self, claim: Claim, built: Built, result: Result) -> bool:
        """Store the proposal (worker); whether the job was still this run's. The caller commits.

        ``ended`` says how (008 FR-028): see :func:`ended_of`.
        """
        filled = sum(len(on) for on in result.assigned.values())
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
                "ended": ended_of(claim, result),
                "needed": sum(s.places for s in built.problem.slots),
                "best_filled": filled,
            },
        )

    async def cancel(self, claim: Claim) -> bool:
        """End a run stopped before it had any result: cancelled, no proposal (worker, FR-028)."""
        return await self._jobs.finish(
            claim.id,
            claim.attempt,
            {"status": "cancelled", "ended": "cancelled", "finished_at": self._now()},
        )

    async def release(self, claim: Claim) -> bool:
        """Give the claimed job back, unsolved (worker shutdown). The caller commits."""
        return await self._jobs.release(claim.id, claim.attempt)

    async def fail(self, claim: Claim, code: str) -> bool:
        """Mark the claimed job failed with ``code`` (worker). The caller commits."""
        return await self._jobs.finish(
            claim.id,
            claim.attempt,
            {"status": "failed", "error_code": code, "finished_at": self._now(), "ended": "failed"},
        )


def ended_of(claim: Claim, result: Result) -> str:
    """How a solved run ended (008 FR-028).

    ``stopped`` by a person with its best so far; ``timed_out`` when CP-SAT ran out of time before
    proving its result optimal; else ``finished`` (the greedy solver never proves optimality, so it
    always finishes).
    """
    if result.stopped:
        return "stopped"
    if claim.solver == "cpsat" and not result.optimal:
        return "timed_out"
    return "finished"


async def solve(
    claim: Claim,
    built: Built,
    time_limit: timedelta,
    progress: Callable[[int, int], None] | None = None,
    should_stop: Callable[[], bool] | None = None,
) -> Result:
    """Run the solver in a thread, so the worker's event loop keeps its database connections.

    ``progress`` and ``should_stop`` are called from that thread (008 FR-027, FR-028).
    """
    return await asyncio.to_thread(
        generate,
        built.problem,
        solver=claim.solver,  # type: ignore[arg-type]
        time_limit=time_limit,
        progress=progress,
        should_stop=should_stop,
    )
