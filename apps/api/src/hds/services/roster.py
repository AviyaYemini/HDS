"""The manual roster: shifts, assignments, and the warnings each change causes.

Layer: services. Depends on ``hds.core`` (errors, roles, messages), ``hds.models``, the pure
``hds.scheduling`` rules and the repositories it is given; it MUST NOT import routers or know about
HTTP.

Rules (003 FR-113 to FR-120): only an Admin creates, changes and deletes shifts and assignments;
Managers and Admins read the roster. Every change answers with the warnings it causes, worked out by
``hds.scheduling.validate`` over the employee's nearby shifts in every project, drafts included, and
is never refused for a warning (FR-116, FR-117). Only data that cannot be stored is refused: an
inactive employee, the same employee twice on a shift, times that do not end after they start.
"""

import uuid
from collections.abc import Collection
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, time, timedelta
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

from hds.core.authz import MANAGER_ROLES, STAFF_ROLES, ensure_role
from hds.core.capabilities import Capability, has_on_project
from hds.core.exceptions import (
    ConflictError,
    DomainValidationError,
    ForbiddenError,
    NotFoundError,
    PreconditionFailedError,
)
from hds.core.i18n import message
from hds.models.shift import Assignment, Shift
from hds.models.user import User, UserRole
from hds.repositories.availability import AvailabilityRepository
from hds.repositories.organisation import OrganisationRepository
from hds.repositories.project import ProjectRepository
from hds.repositories.project_manager import ProjectManagerRepository
from hds.repositories.shift import (
    AssignmentRepository,
    OwnShift,
    OwnShiftDetail,
    ShiftRepository,
    ShiftView,
)
from hds.repositories.shift_type import ShiftTypeRepository
from hds.repositories.user import UserRepository
from hds.scheduling import (
    Employee,
    Entry,
    RuleWarning,
    Settings,
    ShiftSlot,
    local_span,
    validate,
    week_start,
)
from hds.services.pay_visibility import pay_visible

if TYPE_CHECKING:
    from hds.services.availability_submission import AvailabilitySubmissionService

# The longest a shift can be, for how far around a change the rules must look (research R3).
_LONGEST_SHIFT = timedelta(hours=24)


@dataclass(frozen=True)
class Judged:
    """A stored record and the warnings its change caused."""

    record: Any
    warnings: list[dict[str, Any]]


def _refuse_bad_pay_rate(rate: Any) -> None:
    """Refuse a pay rate that is not positive; ``None`` leaves each employee's own rate."""
    if rate is not None and rate <= 0:
        raise DomainValidationError(
            code="shifts.pay_rate_invalid", detail="The pay rate must be greater than zero."
        )


def _bounds(
    zone_name: str, first: date | None, last: date | None
) -> tuple[datetime | None, datetime | None]:
    """The instants that begin ``first`` and end ``last`` in a time zone; ``None`` is an open end.

    The first and last calendar days are open ends too, as no instant lies outside them and their
    neighbours cannot be computed.

    Raises:
        DomainValidationError: ``shifts.date_range_invalid`` when ``last`` is before ``first``
            (HTTP 422).
    """
    if first is not None and last is not None and last < first:
        raise DomainValidationError(
            code="shifts.date_range_invalid", detail="The range must not end before it starts."
        )
    zone = ZoneInfo(zone_name)
    starts_from = (
        None
        if first is None or first <= date.min + timedelta(days=1)
        else datetime.combine(first, time(), zone)
    )
    starts_before = (
        None
        if last is None or last >= date.max - timedelta(days=1)
        else datetime.combine(last + timedelta(days=1), time(), zone)
    )
    return starts_from, starts_before


# How far one person's own times may reach before the shift's start and after its end (007 Q3).
OWN_TIMES_REACH = timedelta(hours=4)


def etag(shift: Shift) -> str:
    """The shift's version for ``ETag`` and ``If-Match``: its ``updated_at`` (007 FR-041)."""
    return f'"{shift.updated_at.astimezone(UTC).isoformat()}"'


def _refuse_stale(shift: Shift, expected: str | None) -> None:
    """Refuse a change made from an old copy of the shift; no ``If-Match`` skips the check."""
    if expected is not None and expected != etag(shift):
        raise PreconditionFailedError(
            code="roster.changed", detail="The roster changed since it was read; read it again."
        )


def _refuse_bad_own_times(
    shift: Shift, starts_at: datetime | None, ends_at: datetime | None
) -> None:
    """Refuse own times that are half given, end before they start, or reach past 4 hours."""
    if starts_at is None and ends_at is None:
        return
    if starts_at is None or ends_at is None or ends_at <= starts_at:
        raise DomainValidationError(
            code="assignment.times_invalid",
            detail="Give both a start and an end, the end after the start.",
        )
    if starts_at < shift.starts_at - OWN_TIMES_REACH or ends_at > shift.ends_at + OWN_TIMES_REACH:
        raise DomainValidationError(
            code="assignment.times_out_of_range",
            detail="A person's times may start at most 4 hours before the shift and end at most "
            "4 hours after it.",
        )


def _times_invalid() -> DomainValidationError:
    return DomainValidationError(
        code="shifts.times_invalid", detail="A shift must end after it starts."
    )


def _warning(found: RuleWarning) -> dict[str, Any]:
    return {
        "code": found.code,
        "employee_id": found.employee_id,
        "shift_ids": list(found.shift_ids),
        "detail": message(found.code),
        "values": dict(found.values),
    }


class RosterService:
    """Shifts and assignments, judged by the pure roster rules."""

    def __init__(
        self,
        shift_repo: ShiftRepository,
        assignment_repo: AssignmentRepository,
        shift_type_repo: ShiftTypeRepository,
        project_repo: ProjectRepository,
        user_repo: UserRepository,
        entry_repo: AvailabilityRepository,
        organisation_repo: OrganisationRepository,
        project_manager_repo: ProjectManagerRepository,
        availability: "AvailabilitySubmissionService | None" = None,
    ) -> None:
        """Wire the service to its repositories.

        Args:
            shift_repo: Shifts and their views.
            assignment_repo: Assignments.
            shift_type_repo: Shift types and the project's set.
            project_repo: Projects, to check one is in the organisation.
            user_repo: Employees to assign and to judge.
            entry_repo: Availability entries the rules weigh.
            organisation_repo: The organisation's limits and time zone.
            project_manager_repo: Who manages which project, for who sees shift rates.
            availability: When given, the entries that stand once windows close (007 FR-014), so
                assigning someone who did not submit warns ``warnings.not_submitted``; without
                it the live entries are read, as before 007.
        """
        self._shifts = shift_repo
        self._assignments = assignment_repo
        self._types = shift_type_repo
        self._projects = project_repo
        self._users = user_repo
        self._entries = entry_repo
        self._orgs = organisation_repo
        self._managers = project_manager_repo
        self._availability = availability

    # ------------------------------------------------------------------ reads

    async def list_shifts(
        self,
        actor: User,
        *,
        first: date | None,
        last: date | None,
        project_id: uuid.UUID | None,
        status: str | None,
        after: tuple[datetime, uuid.UUID] | None,
        limit: int,
    ) -> tuple[list[ShiftView], tuple[datetime, uuid.UUID] | None]:
        """A page of the roster, drafts and published, with assignments (Manager or Admin).

        ``first`` and ``last`` are dates in the organisation's time zone; a shift is in the range
        when it starts on one of them. A shift's own rate is shown only where the reader may see
        its project's pay (FR-022a): rate times hours would otherwise give the pay the reports hide.

        Returns:
            The page and the position to continue from, ``None`` on the last page.

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
            NotFoundError: ``project_id`` is not in the organisation (HTTP 404).
            DomainValidationError: ``shifts.date_range_invalid`` (HTTP 422).
        """
        ensure_role(actor, *MANAGER_ROLES)
        if (
            project_id is not None
            and await self._projects.get_by_id(project_id, actor.organization_id) is None
        ):
            raise NotFoundError(resource="Project", id=project_id)
        starts_from, starts_before = _bounds((await self._org(actor)).timezone, first, last)
        shifts = await self._shifts.page(
            actor.organization_id,
            starts_from=starts_from,
            starts_before=starts_before,
            project_id=project_id,
            status=status,
            after=after,
            limit=limit + 1,
        )
        more = len(shifts) > limit
        shifts = shifts[:limit]
        following = (shifts[-1].starts_at, shifts[-1].id) if more else None
        views = await self._shifts.views(shifts)
        if actor.role == UserRole.admin:
            return views, following
        projects = await self._projects.get_many(
            actor.organization_id, {v.shift.project_id for v in views}
        )
        shown = await pay_visible(actor, projects, self._managers)
        return [replace(v, rate_shown=v.shift.project_id in shown) for v in views], following

    async def my_shift(self, actor: User, shift_id: uuid.UUID) -> tuple[OwnShiftDetail, list[User]]:
        """One of the caller's own published shifts, and who to contact about it (007 FR-023).

        The contacts are the project's managers, standing in for a shift leader, whom HDS does
        not record (reviewer decision, 2026-11-07). Nothing about who else is on the shift.

        Raises:
            ForbiddenError: For a Client account (HTTP 403).
            NotFoundError: Not a published shift the caller is on (HTTP 404), so other shifts
                cannot be probed.
        """
        ensure_role(actor, *STAFF_ROLES)
        found = await self._shifts.own_published_one(actor.organization_id, actor.id, shift_id)
        if found is None:
            raise NotFoundError(resource="Shift", id=shift_id)
        contacts = await self._managers.members(actor.organization_id, found.project_id)
        return found, [m for m in contacts if m.is_active and not m.is_deleted]

    async def my_shifts(self, actor: User, first: date | None, last: date | None) -> list[OwnShift]:
        """The caller's own published shifts that start on ``first`` to ``last`` (FR-123).

        Never a draft, never another employee's, never a deleted project's, and nothing about
        who else is on them.

        Raises:
            ForbiddenError: For a Client account (HTTP 403).
            DomainValidationError: ``shifts.date_range_invalid`` (HTTP 422).
        """
        ensure_role(actor, *STAFF_ROLES)
        starts_from, starts_before = _bounds((await self._org(actor)).timezone, first, last)
        return await self._shifts.own_published(
            actor.organization_id, actor.id, starts_from, starts_before
        )

    async def publish(self, actor: User, project_id: uuid.UUID, first: date, last: date) -> int:
        """Publish a project's drafts that start on ``first`` to ``last`` (Manager or Admin).

        Dates are at the project's site, as its shifts' dates are. Published shifts are left as
        they are, so publishing again counts only new drafts.

        A Manager publishes only the projects they manage, an Admin any (``roster.publish:project``,
        the owner's decision of 2026-11-08). A publish names one project, so it is refused whole.

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403); ``roster.project_not_managed``
                for a Manager of other projects (HTTP 403).
            NotFoundError: The project is not in the organisation (HTTP 404).
            DomainValidationError: ``shifts.date_range_invalid`` when ``last`` is before
                ``first`` (HTTP 422).
        """
        await self.require_project(actor, project_id)
        managed = await self._managers.manages(actor.id, [project_id])
        if not has_on_project(actor.role, Capability.ROSTER_PUBLISH_PROJECT, project_id, managed):
            raise ForbiddenError(
                code="roster.project_not_managed",
                detail="Only the project's managers and Admins publish its roster.",
            )
        # The dates at the project's site, where its shifts were made from local times.
        zone = await self._shifts.site_timezone(project_id)
        starts_from, starts_before = _bounds(zone, first, last)
        return await self._shifts.publish(
            actor.organization_id, project_id, starts_from, starts_before, datetime.now(UTC)
        )

    # ------------------------------------------------------------------ shifts

    def _refuse_pay_rate(self, actor: User, data: dict[str, Any], *, change: bool = False) -> None:
        """Refuse a shift's pay rate from anyone but an Admin (original: Admins edit pay).

        On a new shift a missing or null rate is no pay edit; on a change, any ``pay_rate`` sent,
        null included, is one.
        """
        sent = "pay_rate" in data and (change or data["pay_rate"] is not None)
        if sent and actor.role != UserRole.admin:
            raise ForbiddenError(
                code="shift.pay_rate_admin_only", detail="Only an Admin sets a shift's pay rate."
            )

    async def _require_edit(self, actor: User, project_id: uuid.UUID) -> None:
        """Refuse a roster change unless ``actor`` may edit ``project_id``'s roster (007 FR-028).

        An Admin may edit every project's roster; a Manager only the projects they manage
        (``roster.edit:project``, ADR 015). This changes the original split, where only Admins
        edited by hand, and is Proposed (decision O9).

        The project is looked up in the caller's organisation first: another organisation's project
        is answered as a missing one, before any capability is asked (an Admin holds
        ``roster.edit:project`` on every project id, so the capability alone would let one through).

        Raises:
            ForbiddenError: ``users.manager_required`` for an Employee or Client;
                ``roster.project_not_managed`` for a Manager of other projects (HTTP 403).
            NotFoundError: The project is not in the caller's organisation (HTTP 404).
        """
        await self.require_project(actor, project_id)
        managed = await self._managers.manages(actor.id, [project_id])
        if not has_on_project(actor.role, Capability.ROSTER_EDIT_PROJECT, project_id, managed):
            raise ForbiddenError(
                code="roster.project_not_managed",
                detail="Only the project's managers and Admins change its roster.",
            )

    async def create_shift(self, actor: User, data: dict[str, Any]) -> Judged:
        """Create a draft shift from one of its project's shift types (its Managers, or an Admin).

        Its times are the type's local times on ``date`` at the project's site, unless
        ``starts_at`` and ``ends_at`` are both given. Only an Admin sets ``pay_rate``.

        Raises:
            ForbiddenError: ``users.manager_required``, ``roster.project_not_managed`` or
                ``shift.pay_rate_admin_only`` (HTTP 403).
            NotFoundError: The project or the shift type is not in the organisation (HTTP 404).
            DomainValidationError: ``shifts.type_not_in_project`` or ``shifts.times_invalid``
                (HTTP 422).
        """
        ensure_role(actor, *MANAGER_ROLES)
        self._refuse_pay_rate(actor, data)
        _refuse_bad_pay_rate(data.get("pay_rate"))
        org_id = actor.organization_id
        if await self._projects.get_by_id(data["project_id"], org_id) is None:
            raise NotFoundError(resource="Project", id=data["project_id"])
        await self._require_edit(actor, data["project_id"])
        shift_type = await self._types.get_by_id(data["shift_type_id"], org_id)
        if shift_type is None:
            raise NotFoundError(resource="ShiftType", id=data["shift_type_id"])
        if not await self._types.project_uses(data["project_id"], shift_type.id):
            raise DomainValidationError(
                code="shifts.type_not_in_project",
                detail="The project does not use this shift type.",
            )
        starts_at, ends_at = data.get("starts_at"), data.get("ends_at")
        if (starts_at is None) != (ends_at is None):
            raise _times_invalid()
        if starts_at is None or ends_at is None:
            zone = await self._shifts.site_timezone(data["project_id"])
            try:
                starts_at, ends_at = local_span(
                    data["date"],
                    shift_type.starts_at_local,
                    shift_type.ends_at_local,
                    shift_type.crosses_midnight,
                    zone,
                )
            except ValueError:
                raise _times_invalid() from None
        if ends_at <= starts_at:
            raise _times_invalid()
        shift = await self._shifts.create(
            org_id,
            {
                "project_id": data["project_id"],
                "shift_type_id": shift_type.id,
                "starts_at": starts_at,
                "ends_at": ends_at,
                "headcount": data["headcount"],
                "pay_rate": data.get("pay_rate"),
            },
        )
        return Judged(shift, await self._judge(actor, [shift.id]))

    async def _locked_shift(self, actor: User, id: uuid.UUID) -> Shift:
        shift = await self._shifts.get_locked(id, actor.organization_id)
        if shift is None:
            raise NotFoundError(resource="Shift", id=id)
        return shift

    async def change_shift(
        self, actor: User, id: uuid.UUID, data: dict[str, Any], expected: str | None = None
    ) -> Judged:
        """Change a shift's times, headcount or pay rate (its Managers, or an Admin).

        Its status stays. Only an Admin changes ``pay_rate``. ``expected`` is the caller's
        ``If-Match``; a stale one changes nothing.

        Raises:
            ForbiddenError: ``users.manager_required``, ``roster.project_not_managed`` or
                ``shift.pay_rate_admin_only`` (HTTP 403).
            NotFoundError: The shift is not in the organisation (HTTP 404).
            PreconditionFailedError: ``roster.changed`` (HTTP 412).
            DomainValidationError: ``shifts.times_invalid`` (HTTP 422).
        """
        ensure_role(actor, *MANAGER_ROLES)
        self._refuse_pay_rate(actor, data, change=True)
        _refuse_bad_pay_rate(data.get("pay_rate"))
        shift = await self._locked_shift(actor, id)
        await self._require_edit(actor, shift.project_id)
        _refuse_stale(shift, expected)
        shift_start, shift_end = shift.starts_at, shift.ends_at
        starts_at = data.get("starts_at", shift.starts_at)
        ends_at = data.get("ends_at", shift.ends_at)
        if ends_at <= starts_at:
            raise _times_invalid()
        updated = await self._shifts.update(id, actor.organization_id, **data)
        moved = starts_at - shift_start
        if moved or ends_at != shift_end:
            await self._follow_shift(actor, updated, moved)
        return Judged(updated, await self._judge(actor, [id]))

    async def _follow_shift(self, actor: User, shift: Shift, moved: timedelta) -> None:
        """Move each person's own times with their shift (R83).

        Own times are instants, so a shift moved by ``moved`` takes them along by the same amount.
        Where that leaves them out of the shift's reach (4 hours either side) or out of order, they
        are cleared, and the person has the shift's own times.
        """
        [view] = await self._shifts.views([shift])
        for a in view.assignments:
            if a.starts_at is None or a.ends_at is None:
                continue
            starts_at, ends_at = a.starts_at + moved, a.ends_at + moved
            within = (
                starts_at < ends_at
                and starts_at >= shift.starts_at - OWN_TIMES_REACH
                and ends_at <= shift.ends_at + OWN_TIMES_REACH
            )
            await self._assignments.update(
                a.id,
                actor.organization_id,
                starts_at=starts_at if within else None,
                ends_at=ends_at if within else None,
            )

    async def delete_shift(self, actor: User, id: uuid.UUID, expected: str | None = None) -> None:
        """Delete a shift and its assignments (its Managers, or an Admin).

        Raises:
            ForbiddenError: ``users.manager_required`` or ``roster.project_not_managed``
                (HTTP 403).
            NotFoundError: The shift is not in the organisation (HTTP 404).
            PreconditionFailedError: ``roster.changed`` (HTTP 412).
        """
        ensure_role(actor, *MANAGER_ROLES)
        shift = await self._locked_shift(actor, id)
        await self._require_edit(actor, shift.project_id)
        _refuse_stale(shift, expected)
        await self._shifts.delete(id, actor.organization_id)

    async def views(self, shifts: list[Shift]) -> list[ShiftView]:
        """The shifts with their sites' time zones and assignments."""
        return await self._shifts.views(shifts)

    # ------------------------------------------------------------- assignments

    async def assign(
        self,
        actor: User,
        shift_id: uuid.UUID,
        user_id: uuid.UUID,
        *,
        starts_at: datetime | None = None,
        ends_at: datetime | None = None,
        expected: str | None = None,
    ) -> Judged:
        """Put an active employee on a shift (its Managers, or an Admin).

        ``starts_at`` and ``ends_at``, when given, are the person's own times, within 4 hours of
        the shift's start and end (007 FR-027).

        Raises:
            ForbiddenError: ``users.manager_required`` or ``roster.project_not_managed``
                (HTTP 403).
            NotFoundError: The shift or the employee is not in the organisation, or the account is
                a Client's (HTTP 404).
            ConflictError: ``assignments.employee_inactive`` or ``assignments.duplicate``
                (HTTP 409).
            PreconditionFailedError: ``roster.changed`` (HTTP 412).
            DomainValidationError: ``assignment.times_invalid`` or
                ``assignment.times_out_of_range`` (HTTP 422).
        """
        ensure_role(actor, *MANAGER_ROLES)
        shift = await self._locked_shift(actor, shift_id)
        await self._require_edit(actor, shift.project_id)
        _refuse_stale(shift, expected)
        _refuse_bad_own_times(shift, starts_at, ends_at)
        # The employee's row too: a deactivation at the same moment waits, and two assignments of
        # one employee run one after the other, so the second's warnings see the first.
        await self._users.lock_user(user_id)
        employee = await self._users.get_by_id(user_id, actor.organization_id)
        if employee is None or employee.role == UserRole.client:
            raise NotFoundError(resource="Employee", id=user_id)
        if not employee.is_active:
            raise ConflictError(
                code="assignments.employee_inactive",
                detail="An inactive employee cannot be assigned.",
                field="user_id",
            )
        assignment = await self._assignments.create(
            actor.organization_id,
            {
                "shift_id": shift_id,
                "user_id": user_id,
                "assigned_by": actor.id,
                "starts_at": starts_at,
                "ends_at": ends_at,
            },
        )
        await self._shifts.touch(shift_id, actor.organization_id)
        return Judged(assignment, await self._judge(actor, [shift_id]))

    async def change_assignment_times(
        self,
        actor: User,
        id: uuid.UUID,
        starts_at: datetime | None,
        ends_at: datetime | None,
        expected: str | None = None,
    ) -> Judged:
        """Set or clear one person's own times in a shift (its Managers, or an Admin).

        Both ``None`` gives them the shift's times again (007 FR-027).

        Raises:
            ForbiddenError: ``users.manager_required`` or ``roster.project_not_managed``
                (HTTP 403).
            NotFoundError: The assignment is not in the organisation (HTTP 404).
            PreconditionFailedError: ``roster.changed`` (HTTP 412).
            DomainValidationError: ``assignment.times_invalid`` or
                ``assignment.times_out_of_range`` (HTTP 422).
        """
        ensure_role(actor, *MANAGER_ROLES)
        assignment: Assignment | None = await self._assignments.get_by_id(id, actor.organization_id)
        if assignment is None:
            raise NotFoundError(resource="Assignment", id=id)
        shift = await self._locked_shift(actor, assignment.shift_id)
        await self._require_edit(actor, shift.project_id)
        _refuse_stale(shift, expected)
        _refuse_bad_own_times(shift, starts_at, ends_at)
        updated = await self._assignments.update(
            id, actor.organization_id, starts_at=starts_at, ends_at=ends_at
        )
        await self._shifts.touch(shift.id, actor.organization_id)
        return Judged(updated, await self._judge(actor, [shift.id]))

    async def fresh_shift(self, actor: User, id: uuid.UUID) -> Shift:
        """The shift as stored now, for its new ``etag`` after a change (007 FR-041)."""
        shift = await self._shifts.get_locked(id, actor.organization_id)
        if shift is None:
            raise NotFoundError(resource="Shift", id=id)
        return shift

    async def unassign_returning_shift(
        self, actor: User, id: uuid.UUID, expected: str | None = None
    ) -> tuple[uuid.UUID, list[dict[str, Any]]]:
        """:meth:`unassign`, also answering the shift the assignment was on."""
        assignment = await self._assignments.get_by_id(id, actor.organization_id)
        warnings = await self.unassign(actor, id, expected)
        assert assignment is not None, "unassign found it"
        return assignment.shift_id, warnings

    async def unassign(
        self, actor: User, id: uuid.UUID, expected: str | None = None
    ) -> list[dict[str, Any]]:
        """Take an employee off a shift (its Managers, or an Admin); answer the warnings left.

        Raises:
            ForbiddenError: ``users.manager_required`` or ``roster.project_not_managed``
                (HTTP 403).
            NotFoundError: The assignment is not in the organisation (HTTP 404).
            PreconditionFailedError: ``roster.changed`` (HTTP 412).
        """
        ensure_role(actor, *MANAGER_ROLES)
        assignment: Assignment | None = await self._assignments.get_by_id(id, actor.organization_id)
        if assignment is None:
            raise NotFoundError(resource="Assignment", id=id)
        shift = await self._shifts.get_locked(assignment.shift_id, actor.organization_id)
        # Read again under the lock: another request may have removed it while this one waited.
        if shift is None or not await self._assignments.exists(id, actor.organization_id):
            raise NotFoundError(resource="Assignment", id=id)
        await self._require_edit(actor, shift.project_id)
        _refuse_stale(shift, expected)
        await self._assignments.delete(id, actor.organization_id)
        await self._shifts.touch(shift.id, actor.organization_id)
        return await self._judge(actor, [assignment.shift_id])

    # ---------------------------------------------------------------- warnings

    async def require_project(self, actor: User, project_id: uuid.UUID) -> None:
        """Refuse unless ``actor`` is a Manager or Admin and the project is in their organisation.

        For the roster's reads: another organisation's project is answered as a missing one.

        Raises:
            ForbiddenError: ``users.manager_required`` for an Employee or Client (HTTP 403).
            NotFoundError: The project is not in the caller's organisation (HTTP 404).
        """
        ensure_role(actor, *MANAGER_ROLES)
        if await self._projects.get_by_id(project_id, actor.organization_id) is None:
            raise NotFoundError(resource="Project", id=project_id)

    async def require_edit(self, actor: User, project_id: uuid.UUID) -> None:
        """Refuse unless ``actor`` may edit the project's roster; see :meth:`_require_edit`."""
        await self._require_edit(actor, project_id)

    async def judge(self, actor: User, focus: Collection[uuid.UUID]) -> list[dict[str, Any]]:
        """The warnings the shifts in ``focus`` have now (for the publish review, 007 FR-036)."""
        return await self._judge(actor, focus)

    async def site_zone(self, project_id: uuid.UUID) -> str:
        """The IANA time zone of the project's site, where its shifts' dates are."""
        return await self._shifts.site_timezone(project_id)

    async def _org(self, actor: User) -> Any:
        org = await self._orgs.get_by_id(actor.organization_id)
        assert org is not None, "the caller's organisation exists"
        return org

    async def _judge(self, actor: User, focus: Collection[uuid.UUID]) -> list[dict[str, Any]]:
        """The warnings the shifts in ``focus`` cause, from ``hds.scheduling.validate``.

        Loads every shift of the employees on them that could clash (across projects, drafts
        included): from the start of the earliest week touched, or the longest shift plus the
        minimum rest before, to the end of the latest week touched, or as far after (research R3).
        """
        org = await self._org(actor)
        settings = Settings(
            min_rest=timedelta(hours=org.min_rest_hours),
            max_weekly=timedelta(hours=org.max_weekly_hours),
            week_zone=org.timezone,
        )
        focused = await self._shifts.views(
            [
                s
                for s in [await self._shifts.get_by_id(i, actor.organization_id) for i in focus]
                if s
            ]
        )
        people = {a.user_id for view in focused for a in view.assignments}
        margin = _LONGEST_SHIFT + settings.min_rest
        week = ZoneInfo(org.timezone)
        earliest = min((v.shift.starts_at for v in focused), default=None)
        latest = max((v.shift.ends_at for v in focused), default=None)
        others: list[ShiftView] = []
        entries: list[Entry] = []
        if people and earliest is not None and latest is not None:
            week_begins = datetime.combine(week_start(earliest, org.timezone), time(), week)
            week_ends = datetime.combine(
                week_start(latest, org.timezone) + timedelta(days=7), time(), week
            )
            start, end = min(week_begins, earliest - margin), max(week_ends, latest + margin)
            nearby = await self._shifts.of_employees(actor.organization_id, people, end, start)
            others = await self._shifts.views([s for s in nearby if s.id not in set(focus)])
            entry_first, entry_last = (
                (start - timedelta(days=1)).date(),
                (end + timedelta(days=1)).date(),
            )
            if self._availability is not None:
                entries = await self._availability.scheduling_entries(
                    actor.organization_id, people, entry_first, entry_last
                )
            else:
                entries = [
                    Entry(e.user_id, e.kind, e.starts_on, e.ends_on, e.shift_type_id, e.weekday)  # type: ignore[arg-type]
                    for e in await self._entries.for_users(
                        actor.organization_id, people, entry_first, entry_last
                    )
                ]
        # A deleted employee's assignments stay, so they are judged as no longer active.
        employees = {
            u.id: Employee(u.id, u.is_active and not u.is_deleted)
            for u in await self._users.get_many(
                {a.user_id for v in [*focused, *others] for a in v.assignments},
                actor.organization_id,
            )
        }
        found = validate(
            focus=set(focus),
            shifts=[_slot(v) for v in [*focused, *others]],
            entries=entries,
            employees=employees,
            settings=settings,
        )
        return [_warning(w) for w in found]


def _slot(view: ShiftView) -> ShiftSlot:
    return ShiftSlot(
        id=view.shift.id,
        project_id=view.shift.project_id,
        shift_type_id=view.shift.shift_type_id,
        starts_at=view.shift.starts_at.astimezone(UTC),
        ends_at=view.shift.ends_at.astimezone(UTC),
        zone=view.timezone,
        headcount=view.shift.headcount,
        assigned=tuple(a.user_id for a in view.assignments),
        own_times=tuple(
            (a.user_id, a.starts_at.astimezone(UTC), a.ends_at.astimezone(UTC))
            for a in view.assignments
            if a.starts_at is not None and a.ends_at is not None
        ),
    )
