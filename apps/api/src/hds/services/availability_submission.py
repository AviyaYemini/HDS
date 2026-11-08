"""Availability submissions: Submit, the derived status, and what scheduling reads (007 FR-014).

Layer: services. Depends on ``hds.core`` (errors, roles), ``hds.models``, the pure helpers in
``hds.services.availability_effective`` and the repositories it is given; it MUST NOT import routers
or know about HTTP.

Rules (research R6, clarification Q4):

- An employee submits their own availability for a window while it is open; the submit stores a
  snapshot of their blocked, open and close entries for the window's dates.
- The status is derived, never stored: Locked once the deadline has passed; otherwise Submitted when
  the live entries still equal the snapshot, Draft when there is a snapshot or any entry, and Not
  submitted when there is neither.
- After the deadline scheduling reads the snapshot; with none, the employee counts as unavailable
  for the whole window. An Admin's hand edit after the deadline refreshes an existing snapshot, so
  it counts (003 FR-107), but never creates one: touching one day must not make an employee who
  never submitted available for every other day. To count such an employee, an Admin submits on
  their behalf, explicitly, and the submission records who did it and when.
"""

import uuid
from collections.abc import Callable, Collection, Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any, Literal

from hds.core.authz import MANAGER_ROLES, STAFF_ROLES, ensure_role
from hds.core.exceptions import ConflictError, NotFoundError
from hds.models.availability import AvailabilityEntry
from hds.models.submission_window import SubmissionWindow
from hds.models.user import User, UserRole
from hds.repositories.availability import AvailabilityRepository
from hds.repositories.availability_submission import AvailabilitySubmissionRepository
from hds.repositories.submission_window import SubmissionWindowRepository
from hds.repositories.user import UserRepository
from hds.scheduling.types import Entry
from hds.services.availability_effective import (
    ClosedWindow,
    effective_entries,
    patched,
    snapshot,
    snapshot_hash,
)

Status = Literal["not_submitted", "draft", "submitted", "locked"]


@dataclass(frozen=True)
class SubmissionStatus:
    """Where one employee's availability for one window stands."""

    window: SubmissionWindow
    status: Status
    submitted_at: datetime | None
    counted_unavailable: bool
    submitted_by: uuid.UUID | None = None


def as_entry(stored: AvailabilityEntry) -> Entry:
    """The scheduling form of a stored entry."""
    return Entry(
        stored.user_id,
        stored.kind,  # type: ignore[arg-type]
        stored.starts_on,
        stored.ends_on,
        stored.shift_type_id,
        stored.weekday,
    )


class AvailabilitySubmissionService:
    """Submit, report the status, and give scheduling the entries that stand."""

    def __init__(
        self,
        window_repo: SubmissionWindowRepository,
        entry_repo: AvailabilityRepository,
        submission_repo: AvailabilitySubmissionRepository,
        user_repo: UserRepository,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        """Wire the service to its repositories.

        Args:
            window_repo: Submission windows.
            entry_repo: Availability entries.
            submission_repo: Submissions.
            user_repo: Employees, for the row lock that orders an employee's changes.
            now: The current UTC time; a window is open while it is before the deadline.
        """
        self._windows = window_repo
        self._entries = entry_repo
        self._submissions = submission_repo
        self._users = user_repo
        self._now = now

    async def _window(self, actor: User, window_id: uuid.UUID) -> SubmissionWindow:
        window = await self._windows.get_by_id(window_id, actor.organization_id)
        if window is None:
            raise NotFoundError(resource="SubmissionWindow", id=window_id)
        return window

    async def _live_snapshot(
        self, org_id: uuid.UUID, user_id: uuid.UUID, window: SubmissionWindow
    ) -> list[dict[str, object]]:
        stored = await self._entries.for_users(org_id, [user_id], window.starts_on, window.ends_on)
        return snapshot(
            (as_entry(e) for e in sorted(stored, key=lambda e: str(e.id))),
            window.starts_on,
            window.ends_on,
        )

    async def status(self, actor: User, window_id: uuid.UUID) -> SubmissionStatus:
        """The caller's own status for one window (Employee, Manager or Admin).

        Raises:
            ForbiddenError: ``users.forbidden`` for a Client (HTTP 403).
            NotFoundError: The window is not in the organisation (HTTP 404).
        """
        ensure_role(actor, *STAFF_ROLES)
        window = await self._window(actor, window_id)
        return await self._status_of(actor.organization_id, actor.id, window)

    async def status_for(
        self, actor: User, user_id: uuid.UUID, window_id: uuid.UUID
    ) -> SubmissionStatus:
        """Another employee's status for one window (Manager or Admin, the tracker, FR-040).

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
            NotFoundError: The window is not in the organisation (HTTP 404).
        """
        ensure_role(actor, *MANAGER_ROLES)
        window = await self._window(actor, window_id)
        return await self._status_of(actor.organization_id, user_id, window)

    async def _status_of(
        self, org_id: uuid.UUID, user_id: uuid.UUID, window: SubmissionWindow
    ) -> SubmissionStatus:
        stored = await self._submissions.get(org_id, window.id, user_id)
        submitted_at = stored.submitted_at if stored else None
        by = stored.submitted_by if stored else None
        if self._now() >= window.deadline:
            unavailable = window.requires_submission and stored is None
            return SubmissionStatus(window, "locked", submitted_at, unavailable, by)
        live = await self._live_snapshot(org_id, user_id, window)
        if stored is not None and stored.snapshot_hash == snapshot_hash(live):
            return SubmissionStatus(window, "submitted", submitted_at, False, by)
        if stored is not None or live:
            return SubmissionStatus(window, "draft", submitted_at, False, by)
        return SubmissionStatus(window, "not_submitted", None, False)

    async def submit(self, actor: User, window_id: uuid.UUID) -> SubmissionStatus:
        """Submit the caller's availability for an open window; submitting again replaces it.

        Raises:
            ForbiddenError: ``users.forbidden`` for a Client (HTTP 403).
            NotFoundError: The window is not in the organisation (HTTP 404).
            ConflictError: ``availability.window_closed`` once the deadline has passed (HTTP 409).
        """
        ensure_role(actor, *STAFF_ROLES)
        window = await self._window(actor, window_id)
        # The employee's row lock orders a submit after any entry change still in flight.
        await self._users.lock_user(actor.id)
        if self._now() >= window.deadline:
            raise ConflictError(
                code="availability.window_closed",
                detail="The deadline for this window has passed.",
            )
        await self._store(actor, actor.id, window)
        return await self._status_of(actor.organization_id, actor.id, window)

    async def apply_admin_edit(
        self,
        admin: User,
        user_id: uuid.UUID,
        removed: Mapping[str, Any] | None,
        added: Mapping[str, Any] | None,
    ) -> None:
        """Apply an Admin's hand edit of one entry to the employee's existing submissions.

        An Admin edits anyone's availability by hand (003 FR-107), before the deadline or after,
        and that edit counts at once: in every window the entry touches, the submitted snapshot
        takes the employee's live entries on the days the edit touched (from the earliest start to
        the latest end of ``removed`` and ``added``), and keeps the rest as submitted
        (:func:`~hds.services.availability_effective.patched`). The employee's own edits on other
        days still count only once they submit again (clarification Q4). ``submitted_at`` and
        ``submitted_by`` stay as they were; the change is recorded as ``changed_by`` and
        ``changed_at``. A window the employee never submitted for is left alone: they stay counted
        as unavailable until an Admin submits on their behalf (:meth:`submit_on_behalf`). Absences
        are never in a snapshot.

        Args:
            admin: The Admin who made the change; recorded as ``changed_by``.
            user_id: The employee whose entry changed.
            removed: The entry as it was, for a change or a delete, else ``None``.
            added: The entry as it is now, for a create or a change, else ``None``.
        """
        changes = [e for e in (removed, added) if e is not None and e["kind"] != "absence"]
        if not changes:
            return
        first = min(e["starts_on"] for e in changes)
        last = max(e["ends_on"] for e in changes)
        org = admin.organization_id
        for window in await self._windows.covering(org, first, last):
            if not window.requires_submission:
                continue
            stored = await self._submissions.get(org, window.id, user_id)
            if stored is None:
                continue
            span_first, span_last = max(first, window.starts_on), min(last, window.ends_on)
            if span_first > span_last:
                continue
            live = await self._entries.for_users(org, [user_id], span_first, span_last)
            items = patched(
                stored.snapshot,
                user_id,
                (as_entry(e) for e in sorted(live, key=lambda e: str(e.id))),
                span_first,
                span_last,
            )
            await self._submissions.change(
                org,
                window.id,
                user_id,
                snapshot=items,
                snapshot_hash=snapshot_hash(items),
                changed_by=admin.id,
                changed_at=self._now(),
            )

    async def submit_on_behalf(
        self, admin: User, user_id: uuid.UUID, window_id: uuid.UUID
    ) -> SubmissionStatus:
        """Submit an employee's current entries for a window, as an Admin, at any time.

        The explicit way to count an employee who did not submit (003 FR-107: Admins edit anyone's
        availability by hand). The submission records the Admin as ``submitted_by`` and the time.

        Raises:
            ForbiddenError: ``users.admin_required`` (HTTP 403).
            NotFoundError: The window or the employee is not in the organisation, or the account
                is a Client's (HTTP 404).
        """
        ensure_role(admin, UserRole.admin)
        window = await self._window(admin, window_id)
        employee = await self._users.get_by_id(user_id, admin.organization_id)
        if employee is None or employee.role == UserRole.client:
            raise NotFoundError(resource="Employee", id=user_id)
        await self._users.lock_user(user_id)
        await self._store(admin, user_id, window)
        return await self._status_of(admin.organization_id, user_id, window)

    async def _store(self, by: User, user_id: uuid.UUID, window: SubmissionWindow) -> None:
        items = await self._live_snapshot(by.organization_id, user_id, window)
        await self._submissions.put(
            by.organization_id,
            window.id,
            user_id,
            submitted_at=self._now(),
            submitted_by=by.id,
            snapshot=list(items),
            snapshot_hash=snapshot_hash(items),
        )

    async def scheduling_entries(
        self, org_id: uuid.UUID, user_ids: Collection[uuid.UUID], first: date, last: date
    ) -> list[Entry]:
        """The entries scheduling judges ``user_ids`` by, over ``first`` to ``last``.

        See ``hds.services.availability_effective.effective_entries``. A closed window counts only
        if it requires submission; windows that closed before 007 keep their live entries.
        """
        stored = await self._entries.for_users(org_id, user_ids, first, last)
        live = [as_entry(e) for e in sorted(stored, key=lambda e: str(e.id))]
        now = self._now()
        closed = [
            ClosedWindow(w.id, w.starts_on, w.ends_on)
            for w in await self._windows.covering(org_id, first, last)
            if w.requires_submission and now >= w.deadline
        ]
        found = await self._submissions.for_windows(org_id, [w.id for w in closed], user_ids)
        snapshots = {(s.window_id, s.user_id): s.snapshot for s in found}
        return effective_entries(live, user_ids, closed, snapshots)
