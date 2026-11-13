"""The CEO demo seed's own times and rates (CodeRabbit, PR 28). Names and rates are made up."""

import datetime as dt
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

from hds.scripts.seed_demo import _own_span, _span, _stated_rates

LOCAL = ZoneInfo("Asia/Jerusalem")
DAY = dt.date(2026, 11, 13)


def local(moment: dt.datetime) -> str:
    return moment.astimezone(LOCAL).strftime("%d %H:%M")


def own(start: str | None, end: str | None) -> tuple[str, str]:
    """A person's own window on a 20:00-06:00 shift that starts on the 13th."""
    shift_start, shift_end = _span(DAY, "20:00", "06:00")
    begins, ends = _own_span(DAY, shift_start, shift_end, start, end)
    return local(begins), local(ends)


def test_night_hours_on_an_overnight_shift_are_the_next_morning() -> None:
    assert own("02:00", "06:00") == ("14 02:00", "14 06:00")


def test_a_day_s_hours_beside_a_night_task_stay_on_that_day() -> None:
    assert own("06:00", "18:00") == ("13 06:00", "13 18:00")


def test_a_window_that_overlaps_the_shift_on_both_days_stays_on_the_cell_s_day() -> None:
    # 02:00-22:00 overlaps the shift by two hours on the 13th and four on the 14th: it stays.
    assert own("02:00", "22:00") == ("13 02:00", "13 22:00")


def test_a_start_or_an_end_alone_runs_from_or_to_the_shift() -> None:
    assert own("02:00", None) == ("14 02:00", "14 06:00")
    assert own("22:00", None) == ("13 22:00", "14 06:00")
    assert own(None, "04:00") == ("13 20:00", "14 04:00")


def test_a_window_past_midnight_keeps_its_local_end_across_a_clock_change() -> None:
    # Israel moves its clocks forward early on Friday 26 March 2027.
    day = dt.date(2027, 3, 25)
    shift_start, shift_end = _span(day, "20:00", "06:00")
    begins, ends = _own_span(day, shift_start, shift_end, "22:00", "06:00")
    assert (local(begins), local(ends)) == ("25 22:00", "26 06:00")
    assert ends - begins == dt.timedelta(hours=7)


def test_each_person_keeps_their_own_stated_rate_on_a_shared_shift() -> None:
    shifts: list[dict[str, Any]] = [
        {"people": [{"name": "Person A", "hourly": 80}, {"name": "Person B", "hourly": 90}]},
        {"people": [{"name": "Person A", "hourly": 80}, {"name": "Person C"}]},
        {"people": [{"name": "Person A", "hourly": "95"}]},
    ]
    rates = _stated_rates(shifts)
    assert rates == {"Person A": Decimal("80.0000"), "Person B": Decimal("90.0000")}


def test_between_equally_frequent_rates_the_higher_is_kept() -> None:
    shifts = [
        {"people": [{"name": "Person A", "hourly": 80}]},
        {"people": [{"name": "Person A", "hourly": 85}]},
    ]
    assert _stated_rates(shifts) == {"Person A": Decimal("85.0000")}
