"""Submit, the derived status, and the Admin refresh (007 FR-014, FR-015, research R6, task T049).

Fake repositories; the clock is passed in. Covers the status table (Not submitted, Draft,
Submitted, Locked), "counted as unavailable" after a missed deadline only for windows that require
submission, the refusal to submit after the deadline, and that an Admin's hand edit after the
deadline refreshes the snapshot.
"""

import uuid
from datetime import UTC, date, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from hds.core.exceptions import ConflictError, ForbiddenError
from hds.models.user import User, UserRole
from hds.services.availability_effective import snapshot, snapshot_hash
from hds.services.availability_submission import AvailabilitySubmissionService, as_entry

NOW = datetime(2026, 11, 5, 10, tzinfo=UTC)
ORG = uuid.uuid4()


def _user(role: UserRole = UserRole.employee) -> User:
    user = MagicMock(spec=User)
    user.id, user.organization_id, user.role = uuid.uuid4(), ORG, role
    return user


def _window(deadline: datetime, requires: bool = True) -> MagicMock:
    window = MagicMock()
    window.id = uuid.uuid4()
    window.starts_on, window.ends_on = date(2026, 11, 8), date(2026, 11, 14)
    window.deadline, window.requires_submission = deadline, requires
    return window


def _stored(user_id: uuid.UUID, kind: str = "open", day: int = 9) -> MagicMock:
    e = MagicMock()
    e.id, e.user_id, e.kind = uuid.uuid4(), user_id, kind
    e.starts_on = e.ends_on = date(2026, 11, day)
    e.shift_type_id, e.weekday = None, None
    return e


def _service(
    window: MagicMock, live: list[Any], submission: Any = None, now: datetime = NOW
) -> tuple[AvailabilitySubmissionService, AsyncMock]:
    windows = AsyncMock()
    windows.get_by_id.return_value = window
    windows.covering.return_value = [window]
    entries = AsyncMock()
    entries.for_users.return_value = live
    submissions = AsyncMock()
    submissions.get.return_value = submission
    submissions.for_windows.return_value = [submission] if submission else []
    service = AvailabilitySubmissionService(windows, entries, submissions, AsyncMock(), lambda: now)
    return service, submissions


def _submitted(window: MagicMock, user: User, live: list[Any]) -> MagicMock:
    items = snapshot([as_entry(e) for e in live], window.starts_on, window.ends_on)
    row = MagicMock()
    row.window_id, row.user_id, row.submitted_at = window.id, user.id, NOW - timedelta(hours=1)
    row.snapshot, row.snapshot_hash = items, snapshot_hash(items)
    return row


@pytest.mark.asyncio
async def test_nothing_entered_and_nothing_submitted_is_not_submitted() -> None:
    user, window = _user(), _window(NOW + timedelta(days=1))
    service, _ = _service(window, [])
    found = await service.status(user, window.id)
    assert (found.status, found.counted_unavailable) == ("not_submitted", False)


@pytest.mark.asyncio
async def test_entries_without_a_submission_are_a_draft() -> None:
    user, window = _user(), _window(NOW + timedelta(days=1))
    service, _ = _service(window, [_stored(user.id)])
    assert (await service.status(user, window.id)).status == "draft"


@pytest.mark.asyncio
async def test_unchanged_since_submitting_is_submitted_and_a_change_makes_it_a_draft() -> None:
    user, window = _user(), _window(NOW + timedelta(days=1))
    live = [_stored(user.id)]
    service, _ = _service(window, live, _submitted(window, user, live))
    assert (await service.status(user, window.id)).status == "submitted"

    changed = [_stored(user.id, kind="close")]
    service, _ = _service(window, changed, _submitted(window, user, live))
    assert (await service.status(user, window.id)).status == "draft"


@pytest.mark.asyncio
async def test_after_the_deadline_it_is_locked_and_unavailable_only_without_submitting() -> None:
    user, window = _user(), _window(NOW)  # the deadline is now: closed (now >= deadline)
    service, _ = _service(window, [_stored(user.id)])
    found = await service.status(user, window.id)
    assert (found.status, found.counted_unavailable) == ("locked", True)

    live = [_stored(user.id)]
    service, _ = _service(window, live, _submitted(window, user, live))
    found = await service.status(user, window.id)
    assert (found.status, found.counted_unavailable) == ("locked", False)


@pytest.mark.asyncio
async def test_a_window_that_closed_before_007_never_counts_anyone_unavailable() -> None:
    user, window = _user(), _window(NOW - timedelta(days=3), requires=False)
    service, _ = _service(window, [])
    assert (await service.status(user, window.id)).counted_unavailable is False


@pytest.mark.asyncio
async def test_submitting_stores_the_snapshot_and_answers_submitted() -> None:
    user, window = _user(), _window(NOW + timedelta(days=1))
    live = [_stored(user.id)]
    service, submissions = _service(window, live)
    submissions.get.side_effect = [_submitted(window, user, live)]
    found = await service.submit(user, window.id)
    put = submissions.put.await_args
    assert put.kwargs["snapshot_hash"] == snapshot_hash(put.kwargs["snapshot"])
    assert put.kwargs["submitted_by"] == user.id
    assert found.status == "submitted"


@pytest.mark.asyncio
async def test_submitting_after_the_deadline_is_refused() -> None:
    user, window = _user(), _window(NOW)
    service, submissions = _service(window, [])
    with pytest.raises(ConflictError) as refused:
        await service.submit(user, window.id)
    assert refused.value.code == "availability.window_closed"
    submissions.put.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_client_cannot_submit() -> None:
    window = _window(NOW + timedelta(days=1))
    service, _ = _service(window, [])
    with pytest.raises(ForbiddenError):
        await service.submit(_user(UserRole.client), window.id)


@pytest.mark.asyncio
async def test_an_admin_edit_refreshes_an_existing_snapshot_in_a_closed_window() -> None:
    admin, employee = _user(UserRole.admin), _user()
    closed = _window(NOW - timedelta(hours=1))
    live = [_stored(employee.id)]
    service, submissions = _service(closed, live, _submitted(closed, employee, live))
    await service.refresh_after_admin_edit(admin, employee.id, date(2026, 11, 9), date(2026, 11, 9))
    assert submissions.put.await_args.kwargs["submitted_by"] == admin.id


@pytest.mark.asyncio
async def test_an_admin_edit_never_creates_a_submission_for_someone_who_did_not_submit() -> None:
    """Touching one day must not make a non-submitter available for the rest of the window."""
    admin, employee = _user(UserRole.admin), uuid.uuid4()
    closed = _window(NOW - timedelta(hours=1))
    service, submissions = _service(closed, [_stored(employee)])
    await service.refresh_after_admin_edit(admin, employee, date(2026, 11, 9), date(2026, 11, 9))
    submissions.put.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_admin_edit_in_an_open_window_changes_no_snapshot() -> None:
    admin, employee = _user(UserRole.admin), _user()
    open_ = _window(NOW + timedelta(days=1))
    live = [_stored(employee.id)]
    service, submissions = _service(open_, live, _submitted(open_, employee, live))
    await service.refresh_after_admin_edit(admin, employee.id, date(2026, 11, 9), date(2026, 11, 9))
    submissions.put.assert_not_awaited()


def _with_employee(service: AvailabilitySubmissionService, employee: User) -> None:
    service._users.get_by_id.return_value = employee  # type: ignore[attr-defined]


@pytest.mark.asyncio
async def test_an_admin_submits_on_behalf_after_the_deadline_and_is_recorded() -> None:
    admin, employee = _user(UserRole.admin), _user()
    closed = _window(NOW - timedelta(hours=1))
    service, submissions = _service(closed, [_stored(employee.id)])
    _with_employee(service, employee)
    await service.submit_on_behalf(admin, employee.id, closed.id)
    put = submissions.put.await_args
    assert put.args[2] == employee.id
    assert put.kwargs["submitted_by"] == admin.id
    assert put.kwargs["submitted_at"] == NOW


@pytest.mark.asyncio
@pytest.mark.parametrize("role", [UserRole.employee, UserRole.manager, UserRole.client])
async def test_only_an_admin_submits_on_behalf(role: UserRole) -> None:
    window = _window(NOW - timedelta(hours=1))
    service, submissions = _service(window, [])
    with pytest.raises(ForbiddenError):
        await service.submit_on_behalf(_user(role), uuid.uuid4(), window.id)
    submissions.put.assert_not_awaited()
