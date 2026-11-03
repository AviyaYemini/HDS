"""A shift's local times on a date become real instants, correct across midnight and DST (T035).

Covers: 003 FR-118, SC-106; research R4.
"""

from datetime import UTC, date, datetime, time, timedelta

import pytest

from hds.scheduling import local_span

ZONE = "Asia/Jerusalem"


def length(day: date) -> timedelta:
    start, end = local_span(day, time(22), time(6), True, ZONE)
    return end - start


def test_a_night_shift_is_eight_hours_on_a_normal_night() -> None:
    assert length(date(2026, 6, 1)) == timedelta(hours=8)


def test_it_is_seven_hours_when_the_clocks_go_forward() -> None:
    assert length(date(2026, 3, 26)) == timedelta(hours=7)


def test_it_is_nine_hours_when_the_clocks_go_back() -> None:
    assert length(date(2026, 10, 24)) == timedelta(hours=9)


def test_the_instants_are_utc() -> None:
    start, end = local_span(date(2026, 6, 1), time(7), time(15), False, ZONE)

    assert (start, end) == (
        datetime(2026, 6, 1, 4, tzinfo=UTC),
        datetime(2026, 6, 1, 12, tzinfo=UTC),
    )


def test_a_start_inside_the_spring_gap_moves_forward_by_the_gap() -> None:
    start, _ = local_span(date(2026, 3, 27), time(2, 30), time(10), False, ZONE)

    assert start == datetime(2026, 3, 27, 0, 30, tzinfo=UTC)  # 03:30 local, after the change


def test_an_ambiguous_start_takes_the_first_occurrence() -> None:
    start, _ = local_span(date(2026, 10, 25), time(1, 30), time(9), False, ZONE)

    assert start == datetime(2026, 10, 24, 22, 30, tzinfo=UTC)  # 01:30 summer time


def test_an_end_inside_the_spring_gap_still_ends_after_the_start() -> None:
    """02:30 moves to 03:30; an end of 03:15 would come first, so the span is refused."""
    with pytest.raises(ValueError, match="end after"):
        local_span(date(2026, 3, 27), time(2, 30), time(3, 15), False, ZONE)


def test_an_end_before_the_start_that_does_not_cross_midnight_is_refused() -> None:
    with pytest.raises(ValueError, match="end after"):
        local_span(date(2026, 6, 1), time(15), time(7), False, ZONE)
