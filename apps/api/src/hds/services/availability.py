"""Availability: submission windows and employees' availability entries.

Layer: services. Depends on ``hds.core`` (errors, roles), ``hds.models`` and the repositories it
is given; it MUST NOT import routers or know about HTTP. Every rule here holds however a request
arrives (003 FR-125).

Rules: Managers and Admins open windows (FR-106). An employee changes their own blocked, open and
close entries only while every date involved lies in an open window, and their absences at any
time (FR-105, FR-105a); an Admin changes anyone's at any time (FR-107). Managers and Admins see
everyone's entries; a Manager may not change another employee's (FR-108), and an Employee neither
sees nor changes anyone else's (FR-109). Two entries of one employee may not apply to the same
shift on the same date (FR-104).
"""

import uuid
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, Any

from hds.core.authz import MANAGER_ROLES, STAFF_ROLES, ensure_role
from hds.core.exceptions import (
    ConflictError,
    DomainValidationError,
    ForbiddenError,
    NotFoundError,
)
from hds.models.availability import AvailabilityEntry
from hds.models.submission_window import SubmissionWindow
from hds.models.user import User, UserRole
from hds.repositories.availability import AvailabilityRepository
from hds.repositories.shift_type import ShiftTypeRepository
from hds.repositories.submission_window import SubmissionWindowRepository
from hds.repositories.user import UserRepository
from hds.scheduling.rules import sunday_index

if TYPE_CHECKING:
    from hds.services.availability_submission import AvailabilitySubmissionService

_ENTRY_FIELDS = ("kind", "absence_kind", "starts_on", "ends_on", "shift_type_id", "weekday")


def _applies_between(first: date, last: date, weekday: int | None) -> bool:
    """Whether an entry limited to ``weekday`` (or to none) applies to some date in the range.

    Weekdays repeat every seven days, so at most a week is looked at, however long the range.
    The days are counted rather than added up to a bound, so no date past ``last`` is built and
    the calendar's last days (up to 9999-12-31) are judged like any other.
    """
    span = min((last - first).days, 6)
    return any(weekday in (None, sunday_index(first + timedelta(days=n))) for n in range(span + 1))


def _clash(a: dict[str, Any], b: AvailabilityEntry) -> bool:
    """Whether two entries apply to the same shift on some shared date (FR-104).

    An entry without a shift type covers every shift that date, and one without a weekday covers
    every weekday in its range. An absence clashes only with another absence: it may overlap
    blocked, open and close entries (owner decision, 2026-11-03).
    """
    if (a["kind"] == "absence") != (b.kind == "absence"):
        return False
    if a["shift_type_id"] is not None and b.shift_type_id is not None:
        if a["shift_type_id"] != b.shift_type_id:
            return False
    if a["weekday"] is not None and b.weekday is not None and a["weekday"] != b.weekday:
        return False
    weekday = a["weekday"] if a["weekday"] is not None else b.weekday
    first, last = max(a["starts_on"], b.starts_on), min(a["ends_on"], b.ends_on)
    return first <= last and _applies_between(first, last, weekday)


def _no_window() -> ConflictError:
    return ConflictError(
        code="availability.no_window",
        detail="Availability can only be submitted for dates in an open window.",
    )


class AvailabilityService:
    """Submission windows and availability entries."""

    def __init__(
        self,
        window_repo: SubmissionWindowRepository,
        entry_repo: AvailabilityRepository,
        user_repo: UserRepository,
        shift_type_repo: ShiftTypeRepository,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
        submissions: "AvailabilitySubmissionService | None" = None,
    ) -> None:
        """Wire the service to its repositories.

        Args:
            window_repo: Submission windows.
            entry_repo: Availability entries.
            user_repo: Employees, for the target of an entry and its row lock.
            shift_type_repo: Shift types an entry may be limited to.
            now: The current UTC time; a window is open while it is before the deadline.
            submissions: When given, an Admin's change inside a closed window refreshes the
                employee's submitted snapshot, so scheduling sees it (007 FR-014, FR-107).
        """
        self._windows = window_repo
        self._entries = entry_repo
        self._users = user_repo
        self._shift_types = shift_type_repo
        self._now = now
        self._submissions = submissions

    # ---------------------------------------------------------------- windows

    def is_open(self, window: SubmissionWindow) -> bool:
        """Whether employees may still submit for ``window``: now is before its deadline."""
        return self._now() < window.deadline

    async def list_windows(
        self, actor: User, first: date | None, last: date | None
    ) -> list[SubmissionWindow]:
        """List the organisation's windows that share a date with ``first`` to ``last``.

        Raises:
            ForbiddenError: For a Client account (HTTP 403).
        """
        ensure_role(actor, *STAFF_ROLES)
        return await self._windows.covering(actor.organization_id, first, last)

    async def open_window(self, actor: User, data: dict[str, Any]) -> SubmissionWindow:
        """Open a window for a period of dates with a deadline (Manager or Admin).

        Args:
            actor: The caller.
            data: ``starts_on``, ``ends_on`` and ``deadline``.

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
            DomainValidationError: ``windows.date_range_invalid`` when it ends before it starts
                (HTTP 422).
            ConflictError: ``windows.overlap`` when it shares a date with another (HTTP 409).
        """
        ensure_role(actor, *MANAGER_ROLES)
        if data["ends_on"] < data["starts_on"]:
            raise DomainValidationError(
                code="windows.date_range_invalid",
                detail="The window must not end before it starts.",
            )
        return await self._windows.create(actor.organization_id, {**data, "created_by": actor.id})

    async def _window(self, actor: User, id: uuid.UUID) -> SubmissionWindow:
        window = await self._windows.get_by_id(id, actor.organization_id)
        if window is None:
            raise NotFoundError(resource="SubmissionWindow", id=id)
        return window

    async def change_window(
        self, actor: User, id: uuid.UUID, data: dict[str, Any]
    ) -> SubmissionWindow:
        """Change a window's deadline (Manager or Admin).

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
            NotFoundError: The window is not in the organisation (HTTP 404).
        """
        ensure_role(actor, *MANAGER_ROLES)
        await self._window(actor, id)
        return await self._windows.update(id, actor.organization_id, **data)

    async def remove_window(self, actor: User, id: uuid.UUID) -> None:
        """Remove a window (Manager or Admin); the entries submitted in it stay.

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
            NotFoundError: The window is not in the organisation (HTTP 404).
        """
        ensure_role(actor, *MANAGER_ROLES)
        await self._window(actor, id)
        await self._windows.delete(id, actor.organization_id)

    # ---------------------------------------------------------------- entries

    async def _target(self, actor: User, user_id: uuid.UUID, *, write: bool) -> User:
        """The employee whose entries are addressed, or the refusal the caller gets.

        An Employee addressing anyone else, or anyone addressing a Client account or a missing
        one, gets 404, so existence never leaks. A Manager may read anyone's but not write.
        """
        ensure_role(actor, *STAFF_ROLES)
        if actor.role == UserRole.employee and actor.id != user_id:
            raise NotFoundError(resource="Employee", id=user_id)
        target = await self._users.get_by_id(user_id, actor.organization_id)
        if target is None or target.role == UserRole.client:
            raise NotFoundError(resource="Employee", id=user_id)
        if write and actor.id != user_id and actor.role != UserRole.admin:
            raise ForbiddenError(
                code="availability.admin_required",
                detail="Only an Admin may change another employee's availability.",
            )
        return target

    async def list_entries(
        self, actor: User, user_id: uuid.UUID, first: date | None, last: date | None
    ) -> list[AvailabilityEntry]:
        """List an employee's entries that share a date with ``first`` to ``last``.

        Raises:
            ForbiddenError: For a Client account (HTTP 403).
            NotFoundError: Another employee's, asked by an Employee (HTTP 404).
        """
        await self._target(actor, user_id, write=False)
        return await self._entries.for_user(actor.organization_id, user_id, first, last)

    async def add_entry(
        self, actor: User, user_id: uuid.UUID, data: dict[str, Any]
    ) -> AvailabilityEntry:
        """Add an availability entry for an employee.

        Args:
            actor: The caller.
            user_id: The employee.
            data: ``kind``, ``starts_on``, ``ends_on`` and optionally ``absence_kind``,
                ``shift_type_id`` and ``weekday``.

        Raises:
            ForbiddenError: ``availability.admin_required`` for a Manager adding to someone
                else's (HTTP 403).
            NotFoundError: Another employee's for an Employee, or a shift type not in the
                organisation (HTTP 404).
            DomainValidationError: ``availability.absence_kind_invalid``,
                ``availability.absence_whole_days`` or ``availability.date_range_invalid``
                (HTTP 422).
            ConflictError: ``availability.no_window``, ``availability.window_closed`` or
                ``availability.conflict`` (HTTP 409).

        Side effects:
            Locks the employee's row, so two clashing entries at once leave one (research R6).
        """
        await self._target(actor, user_id, write=True)
        entry = {field: data.get(field) for field in _ENTRY_FIELDS}
        await self._check(actor, entry)
        await self._users.lock_user(user_id)
        await self._require_window(actor, entry)
        await self._refuse_clash(actor, user_id, entry)
        created = await self._entries.create(actor.organization_id, {**entry, "user_id": user_id})
        await self._after_admin_change(actor, user_id, None, entry)
        return created

    async def _entry(self, actor: User, id: uuid.UUID) -> AvailabilityEntry:
        ensure_role(actor, *STAFF_ROLES)
        entry = await self._entries.get_by_id(id, actor.organization_id)
        if entry is None:
            raise NotFoundError(resource="AvailabilityEntry", id=id)
        return entry

    async def change_entry(
        self, actor: User, id: uuid.UUID, data: dict[str, Any]
    ) -> AvailabilityEntry:
        """Change an entry; the window rule applies to the dates it has and the dates it gets.

        Raises:
            The errors of ``add_entry``; NotFoundError also when the entry is not in the
            organisation, or is another employee's and the caller is an Employee.
        """
        existing = await self._entry(actor, id)
        await self._target(actor, existing.user_id, write=True)
        await self._users.lock_user(existing.user_id)
        # Read again under the lock: a change committed while this one waited must be judged.
        existing = await self._fresh(actor, id)
        old = {field: getattr(existing, field) for field in _ENTRY_FIELDS}
        new = {**old, **data}
        await self._check(actor, new)
        await self._require_window(actor, old)
        await self._require_window(actor, new)
        await self._refuse_clash(actor, existing.user_id, new, exclude=id)
        updated = await self._entries.update(id, actor.organization_id, **data)
        await self._after_admin_change(actor, existing.user_id, old, new)
        return updated

    async def remove_entry(self, actor: User, id: uuid.UUID) -> None:
        """Remove an entry; the window rule applies to its dates.

        Raises:
            The errors of ``change_entry``.
        """
        existing = await self._entry(actor, id)
        await self._target(actor, existing.user_id, write=True)
        await self._users.lock_user(existing.user_id)
        existing = await self._fresh(actor, id)
        removed = {field: getattr(existing, field) for field in _ENTRY_FIELDS}
        await self._require_window(actor, removed)
        await self._entries.delete(id, actor.organization_id)
        await self._after_admin_change(actor, existing.user_id, removed, None)

    async def _after_admin_change(
        self,
        actor: User,
        user_id: uuid.UUID,
        removed: dict[str, Any] | None,
        added: dict[str, Any] | None,
    ) -> None:
        """Apply an Admin's change to the employee's submitted snapshots (007 review).

        An employee's own edits wait until they submit again; an Admin's hand edit counts at once.
        """
        if self._submissions is None or actor.role != UserRole.admin:
            return
        await self._submissions.apply_admin_edit(actor, user_id, removed, added)

    async def _fresh(self, actor: User, id: uuid.UUID) -> AvailabilityEntry:
        """Re-read an entry after its employee's row lock, or 404 if it went in the meantime."""
        entry = await self._entries.get_by_id(id, actor.organization_id, fresh=True)
        if entry is None:
            raise NotFoundError(resource="AvailabilityEntry", id=id)
        return entry

    async def _check(self, actor: User, entry: dict[str, Any]) -> None:
        """Refuse an entry whose fields contradict each other, or a foreign shift type."""
        if (entry["kind"] == "absence") != (entry["absence_kind"] is not None):
            raise DomainValidationError(
                code="availability.absence_kind_invalid",
                detail=(
                    "An absence needs its kind (leave, reserve duty or sick); "
                    "no other entry has one."
                ),
            )
        if entry["kind"] == "absence" and (
            entry["shift_type_id"] is not None or entry["weekday"] is not None
        ):
            raise DomainValidationError(
                code="availability.absence_whole_days",
                detail="An absence covers whole days, so it takes no shift type or weekday.",
            )
        if entry["ends_on"] < entry["starts_on"]:
            raise DomainValidationError(
                code="availability.date_range_invalid",
                detail="The entry must not end before it starts.",
            )
        if not _applies_between(entry["starts_on"], entry["ends_on"], entry["weekday"]):
            raise DomainValidationError(
                code="availability.no_matching_day",
                detail="The weekday never falls between the entry's first and last date.",
            )
        if entry["shift_type_id"] is not None:
            found = await self._shift_types.get_by_id(entry["shift_type_id"], actor.organization_id)
            if found is None:
                raise NotFoundError(resource="ShiftType", id=entry["shift_type_id"])
            if not found.is_active:
                raise DomainValidationError(
                    code="availability.shift_type_retired",
                    detail="That shift type has been retired.",
                )

    async def _require_window(self, actor: User, entry: dict[str, Any]) -> None:
        """Refuse a non-Admin change to a blocked, open or close entry outside an open window.

        Every date the entry applies to (only its weekday's, when it has one) must lie in a
        window, and every window it uses must be open. Absences and Admin changes skip the rule
        (FR-105a, FR-107). Windows never overlap, so they are walked in date order.
        """
        if actor.role == UserRole.admin or entry["kind"] == "absence":
            return
        first, last, weekday = entry["starts_on"], entry["ends_on"], entry["weekday"]
        used: list[SubmissionWindow] = []
        cursor = first
        reached_last = False
        for window in await self._windows.covering(actor.organization_id, first, last):
            if window.starts_on > cursor and _applies_between(
                cursor, window.starts_on - timedelta(days=1), weekday
            ):
                raise _no_window()
            start, end = max(first, window.starts_on), min(last, window.ends_on)
            if _applies_between(start, end, weekday):
                used.append(window)
            if window.ends_on >= last:
                # The windows are in date order and share no date, so none comes after this one;
                # stopping here also avoids the day after a window ending on 9999-12-31.
                reached_last = True
                break
            cursor = max(cursor, window.ends_on + timedelta(days=1))
        if not reached_last and cursor <= last and _applies_between(cursor, last, weekday):
            raise _no_window()
        if not all(self.is_open(window) for window in used):
            raise ConflictError(
                code="availability.window_closed",
                detail="The submission window for these dates has closed.",
            )

    async def _refuse_clash(
        self,
        actor: User,
        user_id: uuid.UUID,
        entry: dict[str, Any],
        exclude: uuid.UUID | None = None,
    ) -> None:
        """Refuse an entry that applies to a shift and date another of the employee's does."""
        others = await self._entries.for_user(
            actor.organization_id, user_id, entry["starts_on"], entry["ends_on"], exclude
        )
        if any(_clash(entry, other) for other in others):
            raise ConflictError(
                code="availability.conflict",
                detail="Another entry already applies to one of these shifts and dates.",
            )
