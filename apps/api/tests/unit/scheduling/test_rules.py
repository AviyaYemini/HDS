"""The roster rules judge a manual change and report warnings, never refusals (T031).

``validate`` is handed every shift near the ones being changed, the employees on them, their
availability entries and the organisation's settings, and reports what breaks a rule.

Covers: 003 FR-116, FR-117, SC-103; docs/hds-spec.md section 5, H1 to H4.
"""

import uuid
from datetime import UTC, date, datetime, timedelta
from typing import Any

import pytest

from hds.scheduling import Employee, Entry, RuleWarning, Settings, ShiftSlot, validate

ZONE = "Asia/Jerusalem"
SETTINGS = Settings(min_rest=timedelta(hours=11), max_weekly=timedelta(hours=48), week_zone=ZONE)
PROJECT = uuid.uuid4()
NIGHT = uuid.uuid4()
MORNING = uuid.uuid4()
ANN = uuid.uuid4()
BOB = uuid.uuid4()
PEOPLE = {ANN: Employee(id=ANN, is_active=True), BOB: Employee(id=BOB, is_active=True)}


def at(day: int, hour: int, month: int = 6) -> datetime:
    """A UTC instant in June 2026 (Israel is UTC+3 then), given in local hours."""
    return datetime(2026, month, day, hour, tzinfo=UTC) - timedelta(hours=3)


def shift(
    start: datetime,
    hours: int = 8,
    *,
    people: tuple[uuid.UUID, ...] = (ANN,),
    headcount: int | None = None,
    kind: uuid.UUID = MORNING,
) -> ShiftSlot:
    return ShiftSlot(
        id=uuid.uuid4(),
        project_id=PROJECT,
        shift_type_id=kind,
        starts_at=start,
        ends_at=start + timedelta(hours=hours),
        zone=ZONE,
        headcount=len(people) if headcount is None else headcount,
        assigned=people,
    )


def codes(warnings: list[RuleWarning]) -> list[str]:
    return sorted(w.code for w in warnings)


def run(
    focus: ShiftSlot,
    others: tuple[ShiftSlot, ...] = (),
    entries: tuple[Entry, ...] = (),
    people: dict[uuid.UUID, Employee] | None = None,
) -> list[RuleWarning]:
    return validate(
        focus={focus.id},
        shifts=(focus, *others),
        entries=entries,
        employees=PEOPLE if people is None else people,
        settings=SETTINGS,
    )


def test_a_clean_assignment_has_no_warning() -> None:
    assert run(shift(at(1, 7)), (shift(at(2, 7)),)) == []


def test_two_shifts_at_the_same_time_overlap() -> None:
    first = shift(at(1, 7))
    second = shift(at(1, 10))

    warnings = run(first, (second,))

    assert codes(warnings) == ["warnings.overlap"]
    assert warnings[0].employee_id == ANN
    assert set(warnings[0].shift_ids) == {first.id, second.id}


def test_night_then_morning_rests_one_hour() -> None:
    night = shift(at(1, 22), kind=NIGHT)  # 22:00 to 06:00
    morning = shift(at(2, 7))  # 07:00

    warnings = run(morning, (night,))

    assert codes(warnings) == ["warnings.rest_too_short"]
    assert set(warnings[0].shift_ids) == {night.id, morning.id}


def test_shifts_that_touch_rest_zero_hours_and_do_not_overlap() -> None:
    assert codes(run(shift(at(1, 15)), (shift(at(1, 7)),))) == ["warnings.rest_too_short"]


def test_exactly_the_minimum_rest_is_enough() -> None:
    assert run(shift(at(2, 2)), (shift(at(1, 7)),)) == []  # 15:00 to 02:00 is 11 hours


def test_more_than_the_weekly_maximum_in_one_sunday_to_saturday_week() -> None:
    # Sunday 7 June to Friday 12 June 2026: six 9-hour shifts, 54 hours.
    week = tuple(shift(at(day, 7), hours=9) for day in range(7, 13))

    warnings = run(week[-1], week[:-1])

    assert codes(warnings) == ["warnings.weekly_hours_exceeded"]
    assert set(warnings[0].shift_ids) == {s.id for s in week}


def test_the_week_starts_on_sunday() -> None:
    # Saturday 6 June and Sunday 7 June belong to different weeks.
    saturday = tuple(shift(at(day, 7), hours=10) for day in range(2, 7))  # Tue to Sat, 50 h
    sunday = shift(at(7, 7), hours=10)

    assert run(sunday, saturday) == []


def test_a_blocked_date_is_reported() -> None:
    blocked = Entry(ANN, "blocked", date(2026, 6, 1), date(2026, 6, 1), None, None)

    assert codes(run(shift(at(1, 7)), entries=(blocked,))) == ["warnings.blocked"]


def test_an_absence_is_reported() -> None:
    leave = Entry(ANN, "absence", date(2026, 5, 30), date(2026, 6, 3), None, None)

    assert codes(run(shift(at(1, 7)), entries=(leave,))) == ["warnings.absence"]


def test_a_close_preference_is_reported_and_an_open_one_is_not() -> None:
    close = Entry(ANN, "close", date(2026, 6, 1), date(2026, 6, 1), MORNING, None)
    open_ = Entry(ANN, "open", date(2026, 6, 2), date(2026, 6, 2), MORNING, None)

    assert codes(run(shift(at(1, 7)), entries=(close,))) == ["warnings.close_preference"]
    assert run(shift(at(2, 7)), entries=(open_,)) == []


def test_an_entry_for_another_shift_type_or_weekday_does_not_apply() -> None:
    other_type = Entry(ANN, "blocked", date(2026, 6, 1), date(2026, 6, 1), NIGHT, None)
    other_day = Entry(ANN, "blocked", date(2026, 6, 1), date(2026, 6, 30), None, 3)  # Wednesday

    assert run(shift(at(1, 7)), entries=(other_type, other_day)) == []  # 1 June is a Monday


def test_a_weekday_entry_applies_on_that_weekday_within_its_range() -> None:
    mondays = Entry(ANN, "blocked", date(2026, 6, 1), date(2026, 6, 30), None, 1)

    assert codes(run(shift(at(8, 7)), entries=(mondays,))) == ["warnings.blocked"]


def test_another_employees_entry_does_not_apply() -> None:
    bobs = Entry(BOB, "blocked", date(2026, 6, 1), date(2026, 6, 1), None, None)

    assert run(shift(at(1, 7)), entries=(bobs,)) == []


def test_a_night_shift_belongs_to_the_date_it_starts_in_its_site_zone() -> None:
    # Starts 23:00 local on 1 June, which is 20:00 UTC; blocked on 2 June only.
    night = shift(at(1, 23), kind=NIGHT)
    second = Entry(ANN, "blocked", date(2026, 6, 2), date(2026, 6, 2), None, None)
    first = Entry(ANN, "blocked", date(2026, 6, 1), date(2026, 6, 1), None, None)

    assert run(night, entries=(second,)) == []
    assert codes(run(night, entries=(first,))) == ["warnings.blocked"]


def test_more_or_fewer_people_than_the_headcount() -> None:
    over = shift(at(1, 7), people=(ANN, BOB), headcount=1)
    under = shift(at(2, 7), people=(ANN,), headcount=2)

    assert codes(run(over)) == ["warnings.over_headcount"]
    assert codes(run(under)) == ["warnings.under_headcount"]
    assert run(under)[0].employee_id is None


def test_an_assigned_employee_who_is_no_longer_active() -> None:
    gone = {ANN: Employee(id=ANN, is_active=False)}

    assert codes(run(shift(at(1, 7)), people=gone)) == ["warnings.employee_inactive"]


def test_rules_look_at_every_project() -> None:
    elsewhere = ShiftSlot(
        id=uuid.uuid4(),
        project_id=uuid.uuid4(),
        shift_type_id=MORNING,
        starts_at=at(1, 10),
        ends_at=at(1, 18),
        zone=ZONE,
        headcount=1,
        assigned=(ANN,),
    )

    assert codes(run(shift(at(1, 7)), (elsewhere,))) == ["warnings.overlap"]


def test_other_peoples_shifts_are_not_compared_with_this_employee() -> None:
    assert run(shift(at(1, 7)), (shift(at(1, 7), people=(BOB,)),)) == []


def test_a_pair_is_reported_once_when_both_shifts_are_being_changed() -> None:
    first = shift(at(1, 7))
    second = shift(at(1, 10))

    warnings = validate(
        focus={first.id, second.id},
        shifts=(first, second),
        entries=(),
        employees=PEOPLE,
        settings=SETTINGS,
    )

    assert codes(warnings) == ["warnings.overlap"]


def test_shifts_outside_the_focus_are_not_judged_against_each_other() -> None:
    focus = shift(at(20, 7))
    a, b = shift(at(1, 7)), shift(at(1, 10))  # they overlap, but neither is being changed

    assert run(focus, (a, b)) == []


def test_rest_is_judged_only_between_consecutive_shifts() -> None:
    """A rests 1 hour before B, B rests 3 hours before C: A and C are not neighbours."""
    a = shift(at(1, 7))  # to 15:00
    b = shift(at(1, 16), hours=1)  # 16:00 to 17:00
    c = shift(at(1, 20), hours=2)  # 20:00

    warnings = run(a, (b, c))

    assert [set(w.shift_ids) for w in warnings] == [{a.id, b.id}]


def test_the_same_shift_twice_is_refused() -> None:
    """A changed shift and its stored copy under one id would be counted twice."""
    stored = shift(at(1, 7))
    moved = ShiftSlot(**{**stored.__dict__, "starts_at": at(1, 8), "ends_at": at(1, 16)})

    with pytest.raises(ValueError, match="more than once"):
        run(moved, (stored,))


def test_an_assigned_employee_missing_from_the_employees_is_refused() -> None:
    with pytest.raises(KeyError):
        run(shift(at(1, 7)), people={})


def test_an_unknown_entry_kind_is_refused() -> None:
    # A caller that bypasses the types, as a value read from storage could.
    kind: Any = "closed"
    misspelt = Entry(ANN, kind, date(2026, 6, 1), date(2026, 6, 1), None, None)

    with pytest.raises(ValueError, match="closed"):
        run(shift(at(1, 7)), entries=(misspelt,))


def test_exactly_the_weekly_maximum_is_allowed() -> None:
    # Sunday 7 June to Friday 12 June 2026: six 8-hour shifts, 48 hours.
    week = tuple(shift(at(day, 7)) for day in range(7, 13))

    assert run(week[-1], week[:-1]) == []


def test_a_missed_deadline_block_reports_not_submitted_instead_of_blocked() -> None:
    """007 FR-014: a manual assignment of someone who did not submit warns, with its own code."""
    missed = Entry(
        ANN, "blocked", date(2026, 5, 31), date(2026, 6, 6), None, None, reason="not_submitted"
    )

    assert codes(run(shift(at(1, 7)), entries=(missed,))) == ["warnings.not_submitted"]
