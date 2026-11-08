"""Worked examples of pricing one employee on one shift (005 T002).

Rate 50 per hour and the default settings: overtime after 8 hours (7 on a night shift), the first 2
overtime hours at 125% and the rest at 150%, the weekly rest from Friday 18:00 to Saturday 22:00 at
150%, and overtime increases of 25 and 50 points on a rest or special day. The site is in Israel;
2027-06-06 is a Sunday.

Covers: 005 FR-004, FR-009 to FR-015, SC-003; US3 acceptance scenarios 1 to 5 and 9.
"""

from dataclasses import replace
from datetime import UTC, date, datetime, time
from decimal import ROUND_HALF_UP, Decimal
from zoneinfo import ZoneInfo

import pytest

from hds.payroll import DEFAULT_SETTINGS, PayLine, RestWindow, SpecialDay, price
from hds.payroll.price import split_pay

ZONE = "Asia/Jerusalem"
RATE = Decimal("50")


def at(year: int, month: int, day: int, hour: int, minute: int = 0) -> datetime:
    """A local wall time in Israel, as a UTC instant."""
    local = datetime(year, month, day, hour, minute, tzinfo=ZoneInfo(ZONE))
    return local.astimezone(UTC)


def parts(line: PayLine) -> dict[str, str]:
    """The hours at each rate, as plain strings for comparison."""
    return {f"{p.rate_pct.normalize():f}": f"{p.hours.normalize():f}" for p in line.parts}


def test_a_ten_hour_weekday_day_shift_pays_two_overtime_hours_at_125() -> None:
    """Monday 06:00 to 16:00: 8 hours at 100% and 2 at 125% (FR-011)."""
    line = price(at(2027, 6, 7, 6), at(2027, 6, 7, 16), ZONE, RATE, DEFAULT_SETTINGS, ())

    assert line.pay == Decimal("525.00")
    assert parts(line) == {"100": "8", "125": "2"}
    assert line.hours == Decimal(10)


def test_a_twelve_hour_day_shift_pays_its_last_two_hours_at_150() -> None:
    """Hours 9 and 10 at 125% and hours 11 and 12 at 150% (FR-011)."""
    line = price(at(2027, 6, 7, 6), at(2027, 6, 7, 18), ZONE, RATE, DEFAULT_SETTINGS, ())

    assert line.pay == Decimal("675.00")
    assert parts(line) == {"100": "8", "125": "2", "150": "2"}


def test_a_saturday_day_shift_is_paid_at_the_rest_rate() -> None:
    """Saturday 06:00 to 14:00 is inside the weekly rest: 8 hours at 150% (FR-012)."""
    line = price(at(2027, 6, 12, 6), at(2027, 6, 12, 14), ZONE, RATE, DEFAULT_SETTINGS, ())

    assert line.pay == Decimal("600.00")
    assert parts(line) == {"150": "8"}


def test_a_weekday_night_shift_has_a_normal_length_of_seven_hours() -> None:
    """Monday 22:00 to Tuesday 06:00: a night shift, so its 8th hour is overtime (FR-010)."""
    line = price(at(2027, 6, 7, 22), at(2027, 6, 8, 6), ZONE, RATE, DEFAULT_SETTINGS, ())

    assert line.pay == Decimal("412.50")
    assert parts(line) == {"100": "7", "125": "1"}
    assert line.night is True


def test_a_friday_night_shift_is_paid_at_the_rest_rate_with_its_overtime_at_175() -> None:
    """Friday 22:00 to Saturday 06:00: 7 hours at 150% and 1 overtime hour at 175% (FR-013)."""
    line = price(at(2027, 6, 11, 22), at(2027, 6, 12, 6), ZONE, RATE, DEFAULT_SETTINGS, ())

    assert line.pay == Decimal("612.50")
    assert parts(line) == {"150": "7", "175": "1"}


def test_a_shift_into_the_rest_is_split_where_the_rest_starts() -> None:
    """Friday 16:00 to 24:00: 100% until 18:00, then 150%, its last hour overtime at 175%.

    It is a night shift (2 hours after 22:00), so its normal length is 7 hours.
    """
    line = price(at(2027, 6, 11, 16), at(2027, 6, 12, 0), ZONE, RATE, DEFAULT_SETTINGS, ())

    assert parts(line) == {"100": "2", "150": "5", "175": "1"}
    assert line.pay == Decimal("562.50")


def test_a_shift_out_of_the_rest_is_split_where_the_rest_ends() -> None:
    """Saturday 18:00 to Sunday 02:00: 150% until 22:00, then 100%, the last hour at 125%."""
    line = price(at(2027, 6, 12, 18), at(2027, 6, 13, 2), ZONE, RATE, DEFAULT_SETTINGS, ())

    assert parts(line) == {"150": "4", "100": "3", "125": "1"}
    assert line.pay == Decimal("512.50")


def test_a_special_day_at_200_adds_the_increases_to_its_overtime() -> None:
    """A 12-hour shift on a 200% day: 8 hours at 200%, 2 at 225% and 2 at 250% (FR-012, FR-013)."""
    holiday = SpecialDay(date(2027, 6, 9), Decimal("200"))

    line = price(at(2027, 6, 9, 6), at(2027, 6, 9, 18), ZONE, RATE, DEFAULT_SETTINGS, (holiday,))

    assert parts(line) == {"200": "8", "225": "2", "250": "2"}
    assert line.pay == Decimal("1275.00")


@pytest.mark.parametrize(("special", "expected"), [("200", "800.00"), ("120", "600.00")])
def test_a_special_day_inside_the_rest_takes_the_higher_rate(special: str, expected: str) -> None:
    """On a Saturday, a 200% special day wins over the 150% rest; a 120% one does not."""
    holiday = SpecialDay(date(2027, 6, 12), Decimal(special))

    line = price(at(2027, 6, 12, 6), at(2027, 6, 12, 14), ZONE, RATE, DEFAULT_SETTINGS, (holiday,))

    assert line.pay == Decimal(expected)


def test_a_special_day_covers_only_its_own_local_date() -> None:
    """A night into a 200% day: only the hours after local midnight get the day's rate."""
    holiday = SpecialDay(date(2027, 6, 9), Decimal("200"))

    line = price(at(2027, 6, 8, 22), at(2027, 6, 9, 6), ZONE, RATE, DEFAULT_SETTINGS, (holiday,))

    assert parts(line) == {"100": "2", "200": "5", "225": "1"}


def test_project_settings_replace_the_defaults() -> None:
    """A 9-hour normal day: a 10-hour shift has one overtime hour (US3 scenario 6)."""
    settings = replace(DEFAULT_SETTINGS, day_normal_minutes=540)

    line = price(at(2027, 6, 7, 6), at(2027, 6, 7, 16), ZONE, RATE, settings, ())

    assert parts(line) == {"100": "9", "125": "1"}
    assert line.pay == Decimal("512.50")


def test_a_different_rest_window_moves_the_premium() -> None:
    """A rest from Saturday 00:00 to Sunday 00:00 leaves a Friday night at 100% until midnight."""
    rest = RestWindow(start_weekday=6, start_time=time(0), end_weekday=0, end_time=time(0))
    settings = replace(DEFAULT_SETTINGS, rest=rest)

    line = price(at(2027, 6, 11, 22), at(2027, 6, 12, 6), ZONE, RATE, settings, ())

    assert parts(line) == {"100": "2", "150": "5", "175": "1"}


def test_hours_are_real_hours_when_the_clocks_go_back() -> None:
    """Saturday 2027-10-30 22:00 to Sunday 06:00 lasts 9 real hours: 2 of them overtime (FR-009)."""
    line = price(at(2027, 10, 30, 22), at(2027, 10, 31, 6), ZONE, RATE, DEFAULT_SETTINGS, ())

    assert line.hours == Decimal(9)
    assert parts(line) == {"100": "7", "125": "2"}
    assert line.pay == Decimal("475.00")


def test_hours_are_real_hours_when_the_clocks_go_forward() -> None:
    """Thursday 2027-03-25 22:00 to Friday 06:00 lasts 7 real hours: no overtime (FR-009)."""
    line = price(at(2027, 3, 25, 22), at(2027, 3, 26, 6), ZONE, RATE, DEFAULT_SETTINGS, ())

    assert line.hours == Decimal(7)
    assert line.pay == Decimal("350.00")


def test_the_rate_given_is_the_rate_used() -> None:
    """A shift's own rate of 60 is passed as the base rate: 8 weekday hours pay 480 (FR-004)."""
    line = price(at(2027, 6, 7, 7), at(2027, 6, 7, 15), ZONE, Decimal("60"), DEFAULT_SETTINGS, ())

    assert line.pay == Decimal("480.00")


def test_no_rate_gives_hours_and_parts_but_no_pay() -> None:
    """Without a rate the pay is missing, never zero (FR-004)."""
    line = price(at(2027, 6, 7, 7), at(2027, 6, 7, 15), ZONE, None, DEFAULT_SETTINGS, ())

    assert line.pay is None
    assert line.rate_missing is True
    assert line.hours == Decimal(8)


def test_pay_is_rounded_half_up_once_per_line() -> None:
    """1 hour at 10.005 is 10.005, rounded half up to 10.01 (FR-014)."""
    line = price(
        at(2027, 6, 7, 7), at(2027, 6, 7, 8), ZONE, Decimal("10.005"), DEFAULT_SETTINGS, ()
    )

    assert line.pay == Decimal("10.01")


def test_parts_are_summed_before_rounding() -> None:
    """Friday 17:00 to 19:00 at 0.003: one hour at 100% and one at 150% (FR-014).

    Rounded part by part, 0.003 and 0.0045 would each be 0.00; summed first, 0.0075 is 0.01.
    """
    line = price(
        at(2027, 6, 11, 17), at(2027, 6, 11, 19), ZONE, Decimal("0.003"), DEFAULT_SETTINGS, ()
    )

    assert parts(line) == {"100": "1", "150": "1"}
    assert line.pay == Decimal("0.01")


def test_a_rest_window_starting_at_a_skipped_time_starts_at_the_jump() -> None:
    """On 2027-03-26 the clocks go from 02:00 to 03:00; a rest from 02:30 starts at the jump."""
    rest = RestWindow(start_weekday=5, start_time=time(2, 30), end_weekday=6, end_time=time(22, 0))
    settings = replace(DEFAULT_SETTINGS, rest=rest, night_normal_minutes=1440)

    line = price(at(2027, 3, 26, 0), at(2027, 3, 26, 6), ZONE, RATE, settings, ())

    # 00:00 to 02:00 is before the rest (2 real hours); 03:00 to 06:00 is inside it (3 hours).
    assert parts(line) == {"100": "2", "150": "3"}


def test_two_special_days_on_one_date_are_refused() -> None:
    """The caller merges a project day over the organisation's before pricing (FR-008a)."""
    days = (
        SpecialDay(date(2027, 6, 9), Decimal("120")),
        SpecialDay(date(2027, 6, 9), Decimal("200")),
    )

    with pytest.raises(ValueError, match="two special days"):
        price(at(2027, 6, 9, 6), at(2027, 6, 9, 14), ZONE, RATE, DEFAULT_SETTINGS, days)


@pytest.mark.parametrize("zone", ["Mars/Olympus", ""])
def test_an_unknown_time_zone_is_a_value_error(zone: str) -> None:
    """A bad site zone is reported as documented, never as a bare lookup error."""
    with pytest.raises(ValueError, match="time zone"):
        price(at(2027, 6, 7, 6), at(2027, 6, 7, 14), zone, RATE, DEFAULT_SETTINGS, ())


@pytest.mark.parametrize(
    ("starts_at", "ends_at"),
    [
        (datetime(2027, 6, 7, 6), datetime(2027, 6, 7, 14)),
        (datetime(2027, 6, 7, 6, tzinfo=UTC), datetime(2027, 6, 7, 6, tzinfo=UTC)),
    ],
    ids=["naive", "empty"],
)
def test_a_naive_or_empty_shift_is_refused(starts_at: datetime, ends_at: datetime) -> None:
    """Shifts are UTC instants with an end after the start (Constitution VII)."""
    with pytest.raises(ValueError):
        price(starts_at, ends_at, ZONE, RATE, DEFAULT_SETTINGS, ())


def test_the_line_s_pay_is_split_by_largest_remainder() -> None:
    """Cut down to the agora; the agorot left over go to the parts that lost the most (PR 3)."""
    exacts = [Decimal("10.004"), Decimal("20.006"), Decimal("30.007")]
    pay = sum(exacts, Decimal(0)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)  # 60.02
    assert split_pay(exacts, pay) == [Decimal("10.00"), Decimal("20.01"), Decimal("30.01")]


def test_a_tie_goes_to_the_first_part_and_nothing_left_changes_nothing() -> None:
    assert split_pay([Decimal("1.005"), Decimal("1.005")], Decimal("2.01")) == [
        Decimal("1.01"),
        Decimal("1.00"),
    ]
    assert split_pay([Decimal("3.00"), Decimal("4.50")], Decimal("7.50")) == [
        Decimal("3.00"),
        Decimal("4.50"),
    ]
