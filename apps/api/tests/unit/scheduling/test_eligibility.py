"""Who may take a slot, against the shifts already on the roster (004 T003).

Covers: 004 FR-011 to FR-016, research G6.
"""

from datetime import date, timedelta

from hds.scheduling.eligibility import Roster

from hds.scheduling.problem import (
    MINIMUM_REST,
    NEEDED_ELSEWHERE,
    OVERLAP,
    UNAVAILABLE,
    WEEKLY_MAXIMUM,
)
from tests.unit.scheduling.factory import SETTINGS, day, entry, fixed, person, problem, slot

ANN = person(1)


def test_a_free_candidate_may_take_a_slot() -> None:
    morning = slot(day(1))
    assert Roster(problem([morning], [ANN])).refusal(ANN.id, morning) is None


def test_a_blocked_date_a_blocked_shift_type_and_an_absence_make_a_candidate_unavailable() -> None:
    morning = slot(day(1))
    for blocked in (
        entry(ANN, "blocked", day(1)),
        entry(ANN, "blocked", day(0), day(6), shift="morning"),
        entry(ANN, "blocked", day(0), day(6), weekday=1),
        entry(ANN, "absence", day(1)),
    ):
        roster = Roster(problem([morning], [ANN], entries=[blocked]))
        assert roster.refusal(ANN.id, morning) == UNAVAILABLE, blocked


def test_an_entry_for_another_type_weekday_or_date_does_not_block() -> None:
    morning = slot(day(1))
    for other in (
        entry(ANN, "blocked", day(2)),
        entry(ANN, "blocked", day(0), day(6), shift="night"),
        entry(ANN, "blocked", day(0), day(6), weekday=2),
        entry(ANN, "close", day(1)),
        entry(ANN, "open", day(1)),
    ):
        roster = Roster(problem([morning], [ANN], entries=[other]))
        assert roster.refusal(ANN.id, morning) is None, other


def test_an_overlapping_fixed_shift_refuses_with_overlap() -> None:
    morning = slot(day(1))
    roster = Roster(problem([morning], [ANN], fixed_shifts=[fixed(day(1), "morning", [ANN])]))
    assert roster.refusal(ANN.id, morning) == OVERLAP


def test_a_morning_after_a_night_is_too_little_rest() -> None:
    """Legacy flaw: rest was checked by date only, so 06:00 to 07:00 the same day was allowed."""
    morning = slot(day(2))
    night = fixed(day(1), "night", [ANN])  # ends 07:00 on day 2, when the morning starts
    roster = Roster(problem([morning], [ANN], fixed_shifts=[night]))
    assert roster.refusal(ANN.id, morning) == MINIMUM_REST


def test_shifts_that_only_touch_are_not_an_overlap_but_are_too_little_rest() -> None:
    afternoon = slot(day(1), "afternoon")
    roster = Roster(problem([afternoon], [ANN], fixed_shifts=[fixed(day(1), "morning", [ANN])]))
    assert roster.refusal(ANN.id, afternoon) == MINIMUM_REST


def test_a_week_already_at_the_maximum_refuses_with_weekly_maximum() -> None:
    six = [fixed(day(n), "morning", [ANN]) for n in range(6)]  # 48 hours, Sunday to Friday
    saturday = slot(day(6), "afternoon")
    roster = Roster(problem([saturday], [ANN], fixed_shifts=six))
    assert roster.refusal(ANN.id, saturday) == WEEKLY_MAXIMUM


def test_hours_of_another_week_do_not_count() -> None:
    six = [fixed(day(n), "morning", [ANN]) for n in range(6)]
    next_sunday = slot(day(7))
    assert (
        Roster(problem([next_sunday], [ANN], fixed_shifts=six)).refusal(ANN.id, next_sunday) is None
    )


def test_a_clash_with_a_chosen_slot_means_needed_elsewhere() -> None:
    morning, afternoon = slot(day(1)), slot(day(1), "afternoon")
    roster = Roster(problem([morning, afternoon], [ANN]))
    roster.book(ANN.id, morning)
    assert roster.refusal(ANN.id, afternoon) == NEEDED_ELSEWHERE
    roster.unbook(ANN.id, morning)
    assert roster.refusal(ANN.id, afternoon) is None


def test_the_weekly_maximum_counts_chosen_slots_as_needed_elsewhere() -> None:
    mornings = [slot(day(n)) for n in range(7)]
    roster = Roster(problem(mornings, [ANN]))
    for chosen in mornings[:6]:
        roster.book(ANN.id, chosen)
    assert roster.refusal(ANN.id, mornings[6]) == NEEDED_ELSEWHERE


def test_a_smaller_rest_setting_allows_what_the_default_refuses() -> None:
    afternoon = slot(day(1), "afternoon")
    short = SETTINGS.__class__(timedelta(0), SETTINGS.max_weekly, SETTINGS.week_zone)
    made = problem(
        [afternoon], [ANN], fixed_shifts=[fixed(day(1), "morning", [ANN])], settings=short
    )
    assert Roster(made).refusal(ANN.id, afternoon) is None


def test_the_clocks_going_forward_make_a_night_seven_hours() -> None:
    """2027-03-26 02:00 becomes 03:00 in Israel: the night from the 25th lasts 7 hours."""
    night = slot(date(2027, 3, 25), "night")
    assert night.length == timedelta(hours=7)


def test_preferences_are_read_but_never_refuse() -> None:
    morning = slot(day(1))
    roster = Roster(
        problem([morning], [ANN], entries=[entry(ANN, "close", day(1)), entry(ANN, "open", day(1))])
    )
    assert roster.refusal(ANN.id, morning) is None
    assert roster.preference(ANN.id, morning) == (True, True)  # (open, close)
