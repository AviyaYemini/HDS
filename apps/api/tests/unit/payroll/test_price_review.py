"""Pricing cases the review of PR 7 found (005 T036).

A weekly rest window whose ends differ only in seconds was priced as if it were empty, and a wall
time the clocks skip was placed up to a second after the jump.

Covers: 005 FR-009, FR-012; the review of PR 7.
"""

from dataclasses import replace
from datetime import UTC, date, datetime, time
from decimal import Decimal
from zoneinfo import ZoneInfo

from hds.payroll import DEFAULT_SETTINGS, RestWindow, price
from hds.payroll.price import _at

ZONE = "Asia/Jerusalem"
RATE = Decimal("50")


def at(year: int, month: int, day: int, hour: int, minute: int = 0, second: int = 0) -> datetime:
    """A local wall time in Israel, as a UTC instant."""
    local = datetime(year, month, day, hour, minute, second, tzinfo=ZoneInfo(ZONE))
    return local.astimezone(UTC)


def test_a_rest_window_thirty_seconds_long_pays_thirty_seconds_at_the_rest_rate() -> None:
    """Friday 18:00:00 to 18:00:30 is a real window: its 30 seconds are paid at 150%."""
    window = RestWindow(5, time(18, 0, 0), 5, time(18, 0, 30))
    settings = replace(DEFAULT_SETTINGS, rest=window)

    # Friday 2027-06-11, 17:00 to 19:00: two hours, no overtime, not a night.
    line = price(at(2027, 6, 11, 17), at(2027, 6, 11, 19), ZONE, RATE, settings, ())

    by_rate = {p.rate_pct: p.microseconds for p in line.parts}
    assert by_rate[Decimal("150.00")] == 30_000_000
    assert by_rate[Decimal(100)] == 2 * 3_600_000_000 - 30_000_000


def test_a_rest_window_ending_at_a_second_ends_at_that_second() -> None:
    """A window from Friday 17:59:59 lasts to its end second, not to the minute."""
    window = RestWindow(5, time(17, 59, 59), 5, time(18, 0, 1))
    settings = replace(DEFAULT_SETTINGS, rest=window)

    line = price(at(2027, 6, 11, 17), at(2027, 6, 11, 19), ZONE, RATE, settings, ())

    assert {p.rate_pct: p.microseconds for p in line.parts}[Decimal("150.00")] == 2_000_000


def test_a_skipped_wall_time_maps_to_the_exact_instant_of_the_jump() -> None:
    """Israel moves its clocks from 02:00 to 03:00 on Friday 2027-03-26 (00:00 UTC).

    02:10 never shows that night, so it starts exactly at the jump (02:30, halfway, hid the error).
    """
    assert _at(date(2027, 3, 26), time(2, 10), ZoneInfo(ZONE)) == datetime(
        2027, 3, 26, 0, 0, tzinfo=UTC
    )
