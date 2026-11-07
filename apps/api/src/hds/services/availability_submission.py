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
  for the whole window. An Admin's hand edit after the deadline refreshes the snapshot, so it counts
  (003 FR-107).
"""

import uuid
from collections.abc import Callable, Collection
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Literal

from hds.core.authz import STAFF_ROLES, ensure_role
from hds.core.exceptions import ConflictError, NotFoundError
from hds.models.availability import AvailabilityEntry
from hds.models.submission_window import SubmissionWindow
from hds.models.user import User
from hds.repositories.availability import AvailabilityRepository
from hds.repositories.availability_submission import AvailabilitySubmissionRepository
from hds.repositories.submission_window import SubmissionWindowRepository
from hds.repositories.user import UserRepository
from hds.scheduling.types import Entry
from hds.services.availability_effective import (
    ClosedWindow,
    effective_entries,
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

    async def _status_of(
        self, org_id: uuid.UUID, user_id: uuid.UUID, window: SubmissionWindow
    ) -> SubmissionStatus:
        stored = await self._submissions.get(org_id, window.id, user_id)
        submitted_at = stored.submitted_at if stored else None
        if self._now() >= window.deadline:
            return SubmissionStatus(
                window, "locked", submitted_at, window.requires_submission and stored is None
            )
        live = await self._live_snapshot(org_id, user_id, window)
        if stored is not None and stored.snapshot_hash == snapshot_hash(live):
            return SubmissionStatus(window, "submitted", submitted_at, False)
        if stored is not None or live:
            return SubmissionStatus(window, "draft", submitted_at, False)
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
        items = await self._live_snapshot(actor.organization_id, actor.id, window)
        await self._submissions.put(
            actor.organization_id,
            window.id,
            actor.id,
            submitted_at=self._now(),
            submitted_by=actor.id,
            snapshot=list(items),
            snapshot_hash=snapshot_hash(items),
        )
        return await self._status_of(actor.organization_id, actor.id, window)

    async def refresh_after_admin_edit(
        self, admin: User, user_id: uuid.UUID, first: date, last: date
    ) -> None:
        """Re-snapshot ``user_id`` in every closed window an Admin's hand edit touched.

        After the deadline only an Admin changes an employee's entries (003 FR-107); without this,
        the change would not reach scheduling, which reads snapshots there. The Admin counts as
        the submitter, and an employee who had not submitted no longer counts as unavailable.
        """
        now = self._now()
        for window in await self._windows.covering(admin.organization_id, first, last):
            if not window.requires_submission or now < window.deadline:
                continue
            items = await self._live_snapshot(admin.organization_id, user_id, window)
            await self._submissions.put(
                admin.organization_id,
                window.id,
                user_id,
                submitted_at=now,
                submitted_by=admin.id,
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
