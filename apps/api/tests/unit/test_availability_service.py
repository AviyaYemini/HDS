"""Availability service rules that need no database (003 Group G review).

Covers: 003 FR-104, FR-105, research R6.
"""

import time
import uuid
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

from hds.models.user import UserRole
from hds.services.availability import AvailabilityService, _applies_between, _clash


def entry(**values: Any) -> Any:
    base = {
        "id": uuid.uuid4(),
        "user_id": uuid.uuid4(),
        "kind": "blocked",
        "absence_kind": None,
        "starts_on": date(2027, 6, 6),
        "ends_on": date(2027, 6, 6),
        "shift_type_id": None,
        "weekday": None,
    }
    return SimpleNamespace(**{**base, **values})


def test_a_clash_check_over_centuries_is_quick() -> None:
    """Weekdays repeat every seven days, so the check never walks the whole overlap."""
    # Both on Sundays, so the check gets past the weekday test and reaches the date walk.
    a = {**vars(entry(starts_on=date(1, 1, 1), ends_on=date(9999, 12, 31), weekday=0))}
    b = entry(starts_on=date(1, 1, 1), ends_on=date(9999, 12, 31), weekday=0)

    started = time.perf_counter()
    assert _clash(a, b) is True
    assert time.perf_counter() - started < 0.05


def test_the_last_days_of_the_calendar_are_judged_without_overflow() -> None:
    """Dates up to 9999-12-31 are judged, not answered with a server error (FR-104)."""
    first, last = date(9999, 12, 30), date(9999, 12, 31)  # a Thursday and a Friday

    assert _applies_between(first, last, None) is True
    assert _applies_between(first, last, 5) is True
    assert _applies_between(first, last, 0) is False


async def test_a_window_ending_on_the_last_calendar_day_is_walked_without_overflow() -> None:
    """A window that ends on 9999-12-31 covers an entry up to that day (FR-105)."""
    actor = SimpleNamespace(id=uuid.uuid4(), organization_id=uuid.uuid4(), role=UserRole.employee)
    window = SimpleNamespace(
        starts_on=date(9999, 12, 26),
        ends_on=date(9999, 12, 31),
        deadline=datetime.now(UTC) + timedelta(days=1),
    )
    windows = AsyncMock()
    windows.covering = AsyncMock(return_value=[window])
    svc = AvailabilityService(
        window_repo=windows,
        entry_repo=AsyncMock(),
        user_repo=AsyncMock(),
        shift_type_repo=AsyncMock(),
        now=lambda: datetime.now(UTC),
    )
    late = {"kind": "blocked", "starts_on": date(9999, 12, 27), "ends_on": date(9999, 12, 31)}

    await svc._require_window(actor, {**late, "weekday": None})  # type: ignore[arg-type]


async def test_a_change_is_judged_on_the_entry_as_read_after_the_lock() -> None:
    """A concurrent change committed while this one waited for the lock must be seen."""
    actor = SimpleNamespace(id=uuid.uuid4(), organization_id=uuid.uuid4(), role=UserRole.admin)
    before = entry(user_id=actor.id, starts_on=date(2027, 6, 6), ends_on=date(2027, 6, 6))
    after = entry(
        id=before.id, user_id=actor.id, starts_on=date(2027, 6, 9), ends_on=date(2027, 6, 9)
    )
    order: list[str] = []
    entries = AsyncMock()

    async def get_by_id(id: uuid.UUID, org: uuid.UUID, **kwargs: Any) -> Any:
        order.append("read-fresh" if kwargs.get("fresh") else "read")
        return after if kwargs.get("fresh") else before

    async def lock_user(user_id: uuid.UUID) -> None:
        order.append("lock")

    entries.get_by_id = get_by_id
    entries.for_user = AsyncMock(return_value=[])
    users = AsyncMock()
    users.get_by_id = AsyncMock(return_value=SimpleNamespace(role=UserRole.employee))
    users.lock_user = lock_user
    svc = AvailabilityService(
        window_repo=AsyncMock(),
        entry_repo=entries,
        user_repo=users,
        shift_type_repo=AsyncMock(),
        now=lambda: datetime.now(UTC),
    )

    await svc.change_entry(actor, before.id, {"kind": "open"})  # type: ignore[arg-type]

    assert order.index("lock") < order.index("read-fresh")
    judged = entries.for_user.await_args.args
    assert (judged[2], judged[3]) == (date(2027, 6, 9), date(2027, 6, 9))
