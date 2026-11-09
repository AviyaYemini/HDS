"""Submit, the derived status, and the Admin refresh (007 FR-014, FR-015, research R6, task T049).

Fake repositories; the clock is passed in. Covers the status table (Not submitted, Draft,
Submitted, Locked), "counted as unavailable" after a missed deadline only for windows that require
submission, the refusal to submit after the deadline, and that an Admin's hand edit patches exactly
its own change into an existing snapshot.
"""

import uuid
from datetime import UTC, date, datetime, timedelta
from typing import Any, Literal
from unittest.mock import AsyncMock, MagicMock

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from hds.core.exceptions import ConflictError, ForbiddenError
from hds.models.user import User, UserRole
from hds.scheduling.types import Entry
from hds.services.availability_effective import patched, snapshot, snapshot_hash
from hds.services.availability_submission import AvailabilitySubmissionService, as_entry

NOW = datetime(2026, 11, 5, 10, tzinfo=UTC)
ORG = uuid.uuid4()


def _user(role: UserRole = UserRole.employee) -> User:
    user = MagicMock(spec=User)
    user.id, user.organisation_id, user.role = uuid.uuid4(), ORG, role
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


def _entry(kind: str, day: int, last: int | None = None) -> dict[str, Any]:
    """An entry's fields as the availability service reports an Admin's change."""
    return {
        "kind": kind,
        "absence_kind": None,
        "starts_on": date(2026, 11, day),
        "ends_on": date(2026, 11, last or day),
        "shift_type_id": None,
        "weekday": None,
    }


def _live(user_id: uuid.UUID, kind: str, day: int, last: int | None = None) -> MagicMock:
    e = _stored(user_id, kind, day)
    e.ends_on = date(2026, 11, last or day)
    return e


def _days(submissions: AsyncMock) -> set[tuple[str, str, str]]:
    items = submissions.change.await_args.kwargs["snapshot"]
    return {(i["starts_on"], i["ends_on"], i["kind"]) for i in items}


@pytest.mark.asyncio
@pytest.mark.parametrize("deadline", [NOW - timedelta(hours=1), NOW + timedelta(days=1)])
async def test_an_admin_deleting_an_entry_the_employee_changed_since_submitting_removes_the_day(
    deadline: datetime,
) -> None:
    """An Admin's delete of a day the employee changed since submitting removes that day.

    The snapshot says Monday open; the employee blocked Monday and never submitted again; the Admin
    deletes the live Monday entry: Monday is gone from the snapshot.
    """
    admin, employee = _user(UserRole.admin), _user()
    window = _window(deadline)
    row = _submitted(
        window, employee, [_live(employee.id, "open", 9), _live(employee.id, "open", 11)]
    )
    service, submissions = _service(window, [_live(employee.id, "open", 11)], row)
    await service.apply_admin_edit(admin, employee.id, _entry("blocked", 9), None)
    assert _days(submissions) == {("2026-11-11", "2026-11-11", "open")}


@pytest.mark.asyncio
async def test_an_admin_changing_that_entry_leaves_exactly_one_item_for_the_day() -> None:
    admin, employee = _user(UserRole.admin), _user()
    window = _window(NOW - timedelta(hours=1))
    row = _submitted(window, employee, [_live(employee.id, "open", 9)])
    live = [_live(employee.id, "close", 9)]
    service, submissions = _service(window, live, row)
    await service.apply_admin_edit(admin, employee.id, _entry("blocked", 9), _entry("close", 9))
    assert _days(submissions) == {("2026-11-09", "2026-11-09", "close")}


@pytest.mark.asyncio
async def test_the_employees_unsubmitted_edit_on_another_day_stays_out() -> None:
    """Clarification Q4: only the days the Admin's edit touched come from the live entries."""
    admin, employee = _user(UserRole.admin), _user()
    window = _window(NOW - timedelta(hours=1))
    row = _submitted(
        window, employee, [_live(employee.id, "open", 9), _live(employee.id, "open", 11)]
    )
    # Since submitting, the employee blocked Wednesday the 11th and never submitted again.
    live = [
        _live(employee.id, "open", 9),
        _live(employee.id, "blocked", 11),
        _live(employee.id, "close", 12),
    ]
    service, submissions = _service(window, live, row)
    await service.apply_admin_edit(admin, employee.id, None, _entry("close", 12))
    assert _days(submissions) == {
        ("2026-11-09", "2026-11-09", "open"),
        ("2026-11-11", "2026-11-11", "open"),
        ("2026-11-12", "2026-11-12", "close"),
    }


@pytest.mark.asyncio
async def test_an_item_crossing_the_edited_days_is_split() -> None:
    admin, employee = _user(UserRole.admin), _user()
    window = _window(NOW - timedelta(hours=1))
    row = _submitted(window, employee, [_live(employee.id, "open", 8, 14)])
    live = [
        _live(employee.id, "open", 8, 9),
        _live(employee.id, "blocked", 10),
        _live(employee.id, "open", 11, 14),
    ]
    service, submissions = _service(window, live, row)
    await service.apply_admin_edit(admin, employee.id, _entry("open", 8, 14), _entry("blocked", 10))
    # The edit's days run from the earliest start to the latest end, 8th to 14th here, so the
    # whole week comes from the live entries, which split the range around the 10th.
    assert _days(submissions) == {
        ("2026-11-08", "2026-11-09", "open"),
        ("2026-11-10", "2026-11-10", "blocked"),
        ("2026-11-11", "2026-11-14", "open"),
    }
    row = _submitted(window, employee, [_live(employee.id, "open", 8, 14)])
    service, submissions = _service(window, [_live(employee.id, "blocked", 10)], row)
    await service.apply_admin_edit(admin, employee.id, None, _entry("blocked", 10))
    assert _days(submissions) == {
        ("2026-11-08", "2026-11-09", "open"),
        ("2026-11-10", "2026-11-10", "blocked"),
        ("2026-11-11", "2026-11-14", "open"),
    }


@pytest.mark.asyncio
async def test_an_on_time_submission_keeps_its_time_and_author_and_records_the_change() -> None:
    admin, employee = _user(UserRole.admin), _user()
    window = _window(NOW - timedelta(hours=1))
    row = _submitted(window, employee, [_live(employee.id, "open", 9)])
    service, submissions = _service(window, [_live(employee.id, "blocked", 9)], row)
    await service.apply_admin_edit(admin, employee.id, _entry("open", 9), _entry("blocked", 9))
    submissions.put.assert_not_awaited()
    changed = submissions.change.await_args.kwargs
    assert (changed["changed_by"], changed["changed_at"]) == (admin.id, NOW)
    assert "submitted_at" not in changed and "submitted_by" not in changed
    assert changed["snapshot_hash"] == snapshot_hash(changed["snapshot"])


@pytest.mark.asyncio
async def test_an_admin_edit_never_creates_a_submission_for_someone_who_did_not_submit() -> None:
    """Touching one day must not make a non-submitter available for the rest of the window."""
    admin, employee = _user(UserRole.admin), uuid.uuid4()
    closed = _window(NOW - timedelta(hours=1))
    service, submissions = _service(closed, [_stored(employee)])
    await service.apply_admin_edit(admin, employee, None, _entry("open", 9))
    submissions.put.assert_not_awaited()
    submissions.change.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_absence_or_a_window_without_submission_leaves_snapshots_alone() -> None:
    admin, employee = _user(UserRole.admin), _user()
    window = _window(NOW - timedelta(hours=1))
    row = _submitted(window, employee, [_stored(employee.id)])
    service, submissions = _service(window, [], row)
    await service.apply_admin_edit(admin, employee.id, None, _entry("absence", 9))
    submissions.change.assert_not_awaited()
    old = _window(NOW - timedelta(hours=1), requires=False)
    service, submissions = _service(old, [], _submitted(old, employee, [_stored(employee.id)]))
    await service.apply_admin_edit(admin, employee.id, None, _entry("blocked", 9))
    submissions.change.assert_not_awaited()


Kind = Literal["blocked", "open", "close"]
KINDS: list[Kind] = ["blocked", "open", "close"]


def _runs(draw: Any, first: int, last: int) -> list[tuple[int, int, Kind]]:
    """Consecutive day runs over ``first`` to ``last``, some left out: entries that never clash."""
    runs, day = [], first
    while day <= last:
        length = draw(st.integers(1, last - day + 1))
        if draw(st.booleans()):
            runs.append((day, day + length - 1, draw(st.sampled_from(KINDS))))
        day += length
    return runs


@settings(max_examples=200, deadline=None)
@given(data=st.data())
def test_a_patched_snapshot_never_holds_two_items_for_one_day(data: Any) -> None:
    user = uuid.uuid4()
    stored = [
        {
            "kind": k,
            "starts_on": date(2026, 11, a).isoformat(),
            "ends_on": date(2026, 11, b).isoformat(),
            "shift_type_id": None,
            "weekday": None,
        }
        for a, b, k in _runs(data.draw, 8, 14)
    ]
    first = data.draw(st.integers(8, 14))
    last = data.draw(st.integers(first, 14))
    live = [
        Entry(user, k, date(2026, 11, a), date(2026, 11, b), None, None)
        for a, b, k in _runs(data.draw, 1, 30)
    ]
    items = patched(stored, user, live, date(2026, 11, first), date(2026, 11, last))
    seen: set[date] = set()
    for item in items:
        day = date.fromisoformat(item["starts_on"])
        while day <= date.fromisoformat(item["ends_on"]):
            assert day not in seen, items
            seen.add(day)
            day += timedelta(days=1)
    # Every day in the edited span reads as the live entries do.
    inside = {
        i["starts_on"] for i in items if first <= date.fromisoformat(i["starts_on"]).day <= last
    }
    assert all(
        date(2026, 11, first).isoformat() <= d <= date(2026, 11, last).isoformat() for d in inside
    )


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


@pytest.mark.asyncio
async def test_the_status_shows_an_admins_hand_change_beside_the_submission() -> None:
    """T101: the tracker lists who changed a submission by hand, and when (migration 030)."""
    user, window = _user(), _window(NOW - timedelta(hours=1))
    row = _submitted(window, user, [_stored(user.id)])
    admin = uuid.uuid4()
    row.changed_by, row.changed_at = admin, NOW - timedelta(minutes=5)
    service, _ = _service(window, [_stored(user.id)], row)
    found = await service.status(user, window.id)
    assert (found.status, found.changed_by, found.changed_at) == (
        "locked",
        admin,
        NOW - timedelta(minutes=5),
    )
    assert found.submitted_at == row.submitted_at


@pytest.mark.asyncio
async def test_the_trackers_batch_read_derives_what_the_single_read_does() -> None:
    """R92: three reads for everyone, the same statuses as one person at a time."""
    manager, done, idle = _user(UserRole.manager), _user(), _user()
    window = _window(NOW + timedelta(days=1))
    live = [_stored(done.id)]
    row = _submitted(window, done, live)
    service, submissions = _service(window, live, row)
    submissions.for_windows.return_value = [row]
    found = await service.statuses_for(manager, window.id, [done.id, idle.id])
    statuses = {u: s.status for u, s in found.items()}
    assert statuses == {done.id: "submitted", idle.id: "not_submitted"}
    submissions.get.assert_not_awaited()
