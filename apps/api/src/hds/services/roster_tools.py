"""The manager's roster tools: overrides, the publish review, copy week, templates, bulk edit.

Layer: services. Depends on ``hds.core`` (errors, roles, capabilities), ``hds.models``, the
repositories it is given and ``RosterService``, which owns every single change and its rule
checks; it MUST NOT import routers or know about HTTP.

Rules (specs/007-ux-redesign FR-034 to FR-040, research R8, R12):

- A warning can be overridden with a reason, recorded per assignment and code; hard stops (data
  that cannot be stored) have no override. Warnings never refuse a change (ADR 009).
- The publish review states exactly what a publish of a project's dates would make live: the
  drafts, the people on them, filled against required, unfilled seats and open warnings. Publishing
  records one event per affected person, which feature 010 delivers.
- Copy week and week templates plan a week of shifts in the site's local time, so a copy across a
  daylight-saving change keeps the wall-clock times. Every planned shift is created and staffed
  through ``RosterService``, so the same rules and warnings apply; ``dry_run`` answers the plan and
  changes nothing.
- Bulk edit changes the headcount of, or removes, a project's drafts in a range, with a preview.
"""

import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from typing import Any, Literal
from zoneinfo import ZoneInfo

from hds.core.authz import MANAGER_ROLES, ensure_role
from hds.core.exceptions import DomainValidationError, NotFoundError
from hds.models.roster_extras import RuleOverride, WeekTemplate
from hds.models.shift import Shift
from hds.models.user import User, UserRole
from hds.repositories.roster_extras import (
    PublishEventRepository,
    RuleOverrideRepository,
    WeekTemplateRepository,
)
from hds.repositories.shift import AssignmentRepository, ShiftRepository
from hds.repositories.user import UserRepository
from hds.services.availability_submission import AvailabilitySubmissionService
from hds.services.roster import RosterService

OnConflict = Literal["allow", "overwrite", "unfilled"]


@dataclass(frozen=True)
class PublishPreview:
    """What a publish of a project's dates would make live (007 FR-036)."""

    shifts: int
    people: int
    filled: int
    required: int
    unfilled: int
    open_warnings: int


@dataclass(frozen=True)
class PlannedPerson:
    """A person on a planned shift; their own times as minutes from the shift's start, if any."""

    user_id: uuid.UUID
    start_minutes: int | None = None
    end_minutes: int | None = None


@dataclass(frozen=True)
class PlannedShift:
    """A shift of a planned week, in the site's local time (``day`` 0 is Sunday)."""

    day: int
    start: time
    minutes: int
    shift_type_id: uuid.UUID
    headcount: int
    people: tuple[PlannedPerson, ...] = ()
    # The source shift's own pay rate, which only an Admin's copy or apply sets again (R90).
    pay_rate: Decimal | None = None


@dataclass(frozen=True)
class PlannedChange:
    """One line of a copy, template or bulk plan, as the preview lists it."""

    action: Literal["create", "replace", "skip", "change", "remove"]
    starts_at: datetime
    ends_at: datetime
    shift_type_id: uuid.UUID
    headcount: int
    people: int
    shift_id: uuid.UUID | None = None


@dataclass
class PlanResult:
    """A plan's changes, and the warnings applying it caused (empty on a dry run)."""

    changes: list[PlannedChange]
    warnings: list[dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True)
class TrackedSubmission:
    """One employee's availability for a window, as the submission tracker lists it."""

    user_id: uuid.UUID
    full_name: str
    status: str
    submitted_at: datetime | None
    counted_unavailable: bool
    changed_by: uuid.UUID | None = None
    changed_at: datetime | None = None


def _sunday(day: date) -> date:
    return day - timedelta(days=(day.weekday() + 1) % 7)


def _check_week(week: date) -> None:
    """Refuse a week that does not start on a Sunday, or whose days run off the calendar.

    Asked without date arithmetic, so a date at either end of the calendar is a 422, never a 500
    (R112, like R87 for ranges).

    Raises:
        DomainValidationError: ``roster.week_not_sunday``; ``shifts.date_range_invalid`` for a week
            within seven days of the calendar's last date (HTTP 422).
    """
    if week.weekday() != 6:
        raise DomainValidationError(
            code="roster.week_not_sunday", detail="A week starts on a Sunday."
        )
    if week > date.max - timedelta(days=7):
        raise DomainValidationError(
            code="shifts.date_range_invalid", detail="The week runs past the end of the calendar."
        )


def _local(moment: datetime, zone: str) -> datetime:
    return moment.astimezone(ZoneInfo(zone))


def to_plan(shift: Shift, zone: str, people: Sequence[Any]) -> PlannedShift:
    """The planned form of a stored shift, in its site's local time."""
    start = _local(shift.starts_at, zone)
    week = _sunday(start.date())
    minutes = int((shift.ends_at - shift.starts_at).total_seconds() // 60)
    planned: list[PlannedPerson] = []
    for a in people:
        if a.starts_at is not None and a.ends_at is not None:
            planned.append(
                PlannedPerson(
                    a.user_id,
                    int((a.starts_at - shift.starts_at).total_seconds() // 60),
                    int((a.ends_at - shift.starts_at).total_seconds() // 60),
                )
            )
        else:
            planned.append(PlannedPerson(a.user_id))
    return PlannedShift(
        (start.date() - week).days,
        start.time().replace(tzinfo=None),
        minutes,
        shift.shift_type_id,
        shift.headcount,
        tuple(planned),
        shift.pay_rate,
    )


def plan_to_json(plan: Sequence[PlannedShift]) -> list[dict[str, Any]]:
    """A plan as a week template's ``body``."""
    return [
        {
            "day": p.day,
            "start": p.start.strftime("%H:%M"),
            "minutes": p.minutes,
            "shift_type_id": str(p.shift_type_id),
            "headcount": p.headcount,
            "people": [
                {
                    "user_id": str(x.user_id),
                    "start_minutes": x.start_minutes,
                    "end_minutes": x.end_minutes,
                }
                for x in p.people
            ],
            "pay_rate": str(p.pay_rate) if p.pay_rate is not None else None,
        }
        for p in plan
    ]


def plan_from_json(body: Sequence[dict[str, Any]]) -> list[PlannedShift]:
    """The plan a week template's ``body`` holds."""
    return [
        PlannedShift(
            int(item["day"]),
            time.fromisoformat(item["start"]),
            int(item["minutes"]),
            uuid.UUID(item["shift_type_id"]),
            int(item["headcount"]),
            tuple(
                PlannedPerson(uuid.UUID(x["user_id"]), x.get("start_minutes"), x.get("end_minutes"))
                for x in item.get("people", [])
            ),
            Decimal(item["pay_rate"]) if item.get("pay_rate") else None,
        )
        for item in body
    ]


def utc_range(first: date, last: date, zone: str) -> tuple[datetime, datetime]:
    """The UTC instants from ``first``'s start to the end of ``last`` at the site (``zone``).

    Dates at the edge of the calendar have no such instants: they are refused as a bad range
    (422 ``shifts.date_range_invalid``), never a 500 (R87).

    Raises:
        DomainValidationError: ``shifts.date_range_invalid`` (HTTP 422).
    """
    try:
        begins = datetime.combine(first, time(), ZoneInfo(zone)).astimezone(UTC)
        ends = datetime.combine(last + timedelta(days=1), time(), ZoneInfo(zone)).astimezone(UTC)
    except (OverflowError, ValueError) as error:
        raise DomainValidationError(
            code="shifts.date_range_invalid", detail="The dates are outside the calendar."
        ) from error
    return begins, ends


def placed(plan: PlannedShift, week: date, zone: str) -> tuple[datetime, datetime]:
    """The UTC instants of a planned shift in the week starting on Sunday ``week``.

    The start is the local wall-clock time on that day; the length stays the plan's, so a night
    across a daylight-saving change keeps its hours.
    """
    try:
        local = datetime.combine(week + timedelta(days=plan.day), plan.start, ZoneInfo(zone))
        starts_at = local.astimezone(UTC)
        return starts_at, starts_at + timedelta(minutes=plan.minutes)
    except (OverflowError, ValueError) as error:
        raise DomainValidationError(
            code="shifts.date_range_invalid", detail="The week is outside the calendar."
        ) from error


class RosterToolsService:
    """Overrides, the publish review and events, copy week, templates, bulk edit, the tracker."""

    def __init__(
        self,
        roster: RosterService,
        shift_repo: ShiftRepository,
        assignment_repo: AssignmentRepository,
        override_repo: RuleOverrideRepository,
        template_repo: WeekTemplateRepository,
        event_repo: PublishEventRepository,
        user_repo: UserRepository,
        submissions: AvailabilitySubmissionService,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        """Wire the service to the roster service and its repositories."""
        self._roster = roster
        self._shifts = shift_repo
        self._assignments = assignment_repo
        self._overrides = override_repo
        self._templates = template_repo
        self._events = event_repo
        self._users = user_repo
        self._submissions = submissions
        self._now = now

    # ---------------------------------------------------------------- overrides

    async def override(
        self, actor: User, assignment_id: uuid.UUID, warning_code: str, reason: str
    ) -> RuleOverride:
        """Record why an assignment stays despite one of its warnings (007 FR-034, S10).

        Raises:
            ForbiddenError: ``users.manager_required`` or ``roster.project_not_managed``
                (HTTP 403).
            NotFoundError: The assignment is not in the organisation (HTTP 404).
            DomainValidationError: ``override.warning_not_found`` when the assignment does not
                have that warning now (HTTP 422).
            ConflictError: ``override.duplicate`` (HTTP 409).
        """
        ensure_role(actor, *MANAGER_ROLES)
        assignment = await self._assignments.get_by_id(assignment_id, actor.organization_id)
        if assignment is None:
            raise NotFoundError(resource="Assignment", id=assignment_id)
        shift = await self._shifts.get_by_id(assignment.shift_id, actor.organization_id)
        if shift is None:
            raise NotFoundError(resource="Assignment", id=assignment_id)
        await self._roster.require_edit(actor, shift.project_id)
        warnings = await self._roster.judge(actor, [shift.id])
        if not any(
            w["code"] == warning_code and w["employee_id"] == assignment.user_id for w in warnings
        ):
            raise DomainValidationError(
                code="override.warning_not_found",
                detail="The assignment does not have that warning now.",
            )
        return await self._overrides.create(
            actor.organization_id,
            {
                "assignment_id": assignment_id,
                "warning_code": warning_code,
                "reason": reason,
                "created_by": actor.id,
            },
        )

    async def overrides_for(
        self, actor: User, assignment_ids: Sequence[uuid.UUID]
    ) -> list[RuleOverride]:
        """The overrides on the given assignments (Manager or Admin, for the roster read)."""
        ensure_role(actor, *MANAGER_ROLES)
        return await self._overrides.for_assignments(actor.organization_id, assignment_ids)

    # ---------------------------------------------------------- publish review

    async def _drafts(
        self, actor: User, project_id: uuid.UUID, first: date, last: date
    ) -> tuple[str, list[Shift]]:
        # The project first: another organisation's, or no project at all, is a 404 (R70).
        await self._roster.require_project(actor, project_id)
        if last < first:
            raise DomainValidationError(
                code="shifts.date_range_invalid", detail="The range must not end before it starts."
            )
        zone = await self._roster.site_zone(project_id)
        begins, ends = utc_range(first, last, zone)
        return zone, await self._shifts.in_project(
            actor.organization_id, project_id, begins, ends, status="draft"
        )

    async def publish_preview(
        self, actor: User, project_id: uuid.UUID, first: date, last: date
    ) -> PublishPreview:
        """What publishing the project's drafts on ``first`` to ``last`` would make live.

        Open warnings are those without a recorded override; a warning about the headcount has no
        assignment to override, so it stays open.

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
            DomainValidationError: ``shifts.date_range_invalid`` (HTTP 422).
        """
        _, drafts = await self._drafts(actor, project_id, first, last)
        views = await self._roster.views(drafts)
        people = {a.user_id for v in views for a in v.assignments}
        filled = sum(min(len(v.assignments), v.shift.headcount) for v in views)
        required = sum(v.shift.headcount for v in views)
        warnings = await self._roster.judge(actor, [d.id for d in drafts]) if drafts else []
        by_person_shift = {(a.user_id, v.shift.id): a.id for v in views for a in v.assignments}
        overridden = {
            (o.assignment_id, o.warning_code)
            for o in await self._overrides.for_assignments(
                actor.organization_id, list(by_person_shift.values())
            )
        }
        open_count = 0
        for w in warnings:
            ids = [
                by_person_shift.get((w["employee_id"], s))
                for s in w["shift_ids"]
                if w["employee_id"] is not None
            ]
            if not any(i is not None and (i, w["code"]) in overridden for i in ids):
                open_count += 1
        return PublishPreview(
            shifts=len(drafts),
            people=len(people),
            filled=filled,
            required=required,
            unfilled=required - filled,
            open_warnings=open_count,
        )

    async def publish(
        self, actor: User, project_id: uuid.UUID, first: date, last: date, notify: bool
    ) -> int:
        """Publish the project's drafts on those dates and record one event per person on them.

        Feature 010 delivers the events; ``notify`` records the publisher's choice.
        """
        _, drafts = await self._drafts(actor, project_id, first, last)
        views = await self._roster.views(drafts)
        count = await self._roster.publish(actor, project_id, first, last)
        per_person: dict[uuid.UUID, int] = {}
        for v in views:
            for a in v.assignments:
                per_person[a.user_id] = per_person.get(a.user_id, 0) + 1
        await self._events.add_many(
            actor.organization_id,
            [
                {
                    "project_id": project_id,
                    "user_id": person,
                    "published_by": actor.id,
                    "first_day": first,
                    "last_day": last,
                    "shift_count": shifts,
                    "notify": notify,
                }
                for person, shifts in sorted(per_person.items(), key=lambda kv: str(kv[0]))
            ],
        )
        return count

    # ------------------------------------------------ copy week and templates

    async def week_plan(self, actor: User, project_id: uuid.UUID, week: date) -> list[PlannedShift]:
        """The project's shifts in the week starting on Sunday ``week``, as a plan.

        The project is looked up in the caller's organisation first, so an unknown project or
        another organisation's answers 404 before its site's time zone is read (R111).

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
            NotFoundError: The project is not in the caller's organisation (HTTP 404).
        """
        await self._roster.require_project(actor, project_id)
        return await self._plan_of(actor, project_id, week)

    async def _plan_of(self, actor: User, project_id: uuid.UUID, week: date) -> list[PlannedShift]:
        """The week as a plan, for a caller that has already checked the project."""
        zone = await self._roster.site_zone(project_id)
        begins, ends = utc_range(week, week + timedelta(days=6), zone)
        shifts = await self._shifts.in_project(actor.organization_id, project_id, begins, ends)
        views = await self._roster.views(shifts)
        return [to_plan(v.shift, zone, v.assignments) for v in views]

    async def apply_plan(
        self,
        actor: User,
        project_id: uuid.UUID,
        plan: Sequence[PlannedShift],
        week: date,
        on_conflict: OnConflict,
        dry_run: bool,
    ) -> PlanResult:
        """Create the plan's shifts in the week starting on Sunday ``week``.

        A planned shift *conflicts* with a shift of the same type already starting at the same
        time. ``allow`` adds it anyway; ``overwrite`` replaces a draft (a published shift is never
        replaced, so it is skipped); ``unfilled`` adds it without people.

        Raises:
            ForbiddenError: ``users.manager_required`` or ``roster.project_not_managed``.
        """
        ensure_role(actor, *MANAGER_ROLES)
        await self._roster.require_edit(actor, project_id)
        zone = await self._roster.site_zone(project_id)
        begins, ends = utc_range(week, week + timedelta(days=7), zone)
        existing = await self._shifts.in_project(actor.organization_id, project_id, begins, ends)
        # Each shift is matched once: a second plan item at the same type and start does not
        # replace the same draft twice (R85).
        matched: set[uuid.UUID] = set()
        # People no longer active cannot be staffed again; they are left off, with a warning, in a
        # dry run exactly as in the real one (R86).
        named = {p.user_id for item in plan for p in item.people}
        active = {
            u.id
            for u in await self._users.get_many(named, actor.organization_id)
            if u.is_active and not u.is_deleted
        }
        keep_rates = actor.role == UserRole.admin
        result = PlanResult(changes=[])
        for item in plan:
            starts_at, ends_at = placed(item, week, zone)
            clash = next(
                (
                    s
                    for s in existing
                    if s.id not in matched
                    and s.shift_type_id == item.shift_type_id
                    and s.starts_at == starts_at
                ),
                None,
            )
            if clash is not None:
                matched.add(clash.id)
            action: Literal["create", "replace", "skip"] = "create"
            people = tuple(p for p in item.people if p.user_id in active)
            for gone in (p for p in item.people if p.user_id not in active):
                result.warnings.append(
                    {
                        "code": "assignments.employee_inactive",
                        "employee_id": gone.user_id,
                        "shift_ids": [],
                        "detail": "Left off: no longer an active employee.",
                    }
                )
            if clash is not None and on_conflict == "overwrite":
                action = "replace" if clash.status == "draft" else "skip"
            elif clash is not None and on_conflict == "unfilled":
                people = ()
            result.changes.append(
                PlannedChange(
                    action,
                    starts_at,
                    ends_at,
                    item.shift_type_id,
                    item.headcount,
                    len(people),
                    clash.id if clash is not None and action != "create" else None,
                )
            )
            if dry_run or action == "skip":
                continue
            if action == "replace" and clash is not None:
                await self._roster.delete_shift(actor, clash.id)
            created = await self._roster.create_shift(
                actor,
                {
                    "project_id": project_id,
                    "shift_type_id": item.shift_type_id,
                    "date": starts_at.astimezone(ZoneInfo(zone)).date(),
                    "headcount": item.headcount,
                    "pay_rate": item.pay_rate if keep_rates else None,
                    "starts_at": starts_at,
                    "ends_at": ends_at,
                },
            )
            result.warnings.extend(created.warnings)
            existing.append(created.record)
            for person in people:
                own_start = own_end = None
                if person.start_minutes is not None and person.end_minutes is not None:
                    own_start = starts_at + timedelta(minutes=person.start_minutes)
                    own_end = starts_at + timedelta(minutes=person.end_minutes)
                judged = await self._roster.assign(
                    actor, created.record.id, person.user_id, starts_at=own_start, ends_at=own_end
                )
                result.warnings.extend(judged.warnings)
        return result

    async def copy_week(
        self,
        actor: User,
        project_id: uuid.UUID,
        from_week: date,
        to_week: date,
        on_conflict: OnConflict,
        dry_run: bool,
    ) -> PlanResult:
        """Copy the project's week starting on Sunday ``from_week`` to the one on ``to_week``.

        Raises:
            DomainValidationError: ``roster.week_not_sunday`` when a week does not start on a
                Sunday (HTTP 422).
        """
        for week in (from_week, to_week):
            _check_week(week)
        ensure_role(actor, *MANAGER_ROLES)
        plan = await self.week_plan(actor, project_id, from_week)
        return await self.apply_plan(actor, project_id, plan, to_week, on_conflict, dry_run)

    async def templates(self, actor: User, project_id: uuid.UUID) -> list[WeekTemplate]:
        """The project's saved weeks (Manager or Admin); 404 for another organisation's project."""
        await self._roster.require_project(actor, project_id)
        return await self._templates.for_project(actor.organization_id, project_id)

    async def save_template(
        self, actor: User, project_id: uuid.UUID, name: str, week: date
    ) -> WeekTemplate:
        """Save the project's week starting on Sunday ``week`` as a template."""
        _check_week(week)
        ensure_role(actor, *MANAGER_ROLES)
        await self._roster.require_edit(actor, project_id)
        # require_edit has looked the project up already (group O review).
        plan = await self._plan_of(actor, project_id, week)
        return await self._templates.create(
            actor.organization_id,
            {
                "project_id": project_id,
                "name": name,
                "body": plan_to_json(plan),
                "created_by": actor.id,
            },
        )

    async def delete_template(self, actor: User, project_id: uuid.UUID, id: uuid.UUID) -> None:
        """Remove one of the project's templates."""
        ensure_role(actor, *MANAGER_ROLES)
        await self._roster.require_edit(actor, project_id)
        if not await self._templates.delete(actor.organization_id, project_id, id):
            raise NotFoundError(resource="WeekTemplate", id=id)

    async def apply_template(
        self,
        actor: User,
        project_id: uuid.UUID,
        id: uuid.UUID,
        week: date,
        on_conflict: OnConflict,
        dry_run: bool,
    ) -> PlanResult:
        """Apply a saved week to the week starting on Sunday ``week``."""
        _check_week(week)
        ensure_role(actor, *MANAGER_ROLES)
        template = await self._templates.get(actor.organization_id, project_id, id)
        if template is None:
            raise NotFoundError(resource="WeekTemplate", id=id)
        plan = plan_from_json(template.body)
        return await self.apply_plan(actor, project_id, plan, week, on_conflict, dry_run)

    # ---------------------------------------------------------------- bulk edit

    async def bulk(
        self,
        actor: User,
        project_id: uuid.UUID,
        first: date,
        last: date,
        *,
        headcount: int | None,
        remove: bool,
        dry_run: bool,
    ) -> PlanResult:
        """Change the headcount of, or remove, every draft of the project on those dates.

        Published shifts are never touched; the preview lists exactly the drafts the change hits
        (007 FR-038, S15).

        Raises:
            DomainValidationError: ``roster.bulk_change_invalid`` unless exactly one of
                ``headcount`` and ``remove`` is given (HTTP 422).
        """
        if (headcount is None) == (not remove):
            raise DomainValidationError(
                code="roster.bulk_change_invalid",
                detail="Give either a new headcount or remove, not both or neither.",
            )
        ensure_role(actor, *MANAGER_ROLES)
        await self._roster.require_edit(actor, project_id)
        _, drafts = await self._drafts(actor, project_id, first, last)
        views = await self._roster.views(drafts)
        result = PlanResult(changes=[])
        for v in views:
            s = v.shift
            result.changes.append(
                PlannedChange(
                    "remove" if remove else "change",
                    s.starts_at,
                    s.ends_at,
                    s.shift_type_id,
                    s.headcount if remove else headcount or s.headcount,
                    len(v.assignments),
                    s.id,
                )
            )
            if dry_run:
                continue
            if remove:
                await self._roster.delete_shift(actor, s.id)
            else:
                judged = await self._roster.change_shift(actor, s.id, {"headcount": headcount})
                result.warnings.extend(judged.warnings)
        return result

    # --------------------------------------------------------- submission tracker

    async def tracker(self, actor: User, window_id: uuid.UUID) -> list[TrackedSubmission]:
        """Every active staff member's availability for a window (Manager or Admin, S21).

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
            NotFoundError: The window is not in the organisation (HTTP 404).
        """
        ensure_role(actor, *MANAGER_ROLES)
        found: list[TrackedSubmission] = []
        staff = await self._users.staff_of(actor.organization_id)
        statuses = await self._submissions.statuses_for(actor, window_id, [p.id for p in staff])
        for person in staff:
            status = statuses[person.id]
            found.append(
                TrackedSubmission(
                    person.id,
                    person.full_name,
                    status.status,
                    status.submitted_at,
                    status.counted_unavailable,
                    status.changed_by,
                    status.changed_at,
                )
            )
        return found
