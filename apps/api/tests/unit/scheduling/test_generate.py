"""Roster generation on small, built problems, for both solvers (004 T005).

Covers: 004 FR-010 to FR-021, SC-003 to SC-007, the legacy scheduler flaws (docs/hds-spec.md
section 3).
"""

from collections import Counter
from dataclasses import replace
from datetime import timedelta
from itertools import pairwise

import pytest

from hds.scheduling.engine import generate
from hds.scheduling.problem import (
    MINIMUM_REST,
    NEEDED_ELSEWHERE,
    NO_ACTIVE_EMPLOYEE,
    OVERLAP,
    UNAVAILABLE,
    WEEKLY_MAXIMUM,
    Problem,
    Reason,
    Result,
)
from tests.unit.scheduling.factory import (
    OTHER_PROJECT,
    day,
    entry,
    fixed,
    person,
    problem,
    slot,
)

SOLVERS = ["greedy", "cpsat"]
ANN, BEN, CAL = person(1), person(2), person(3)


def run(made: Problem, solver: str = "cpsat") -> Result:
    """Generate with a fixed, single-worker search so the tests repeat."""
    return generate(made, solver=solver, time_limit=timedelta(seconds=10), workers=1)  # type: ignore[arg-type]


def reasons(result: Result, key: str) -> tuple[Reason, ...]:
    """The reasons given for the slot ``key``."""
    (found,) = [u for u in result.unfilled if u.key == key]
    return found.reasons


def hours(result: Result) -> dict[object, float]:
    """Hours per candidate in the range."""
    return {s.user_id: s.minutes / 60 for s in result.summary}


@pytest.mark.parametrize("solver", SOLVERS)
def test_a_feasible_week_is_filled_completely(solver: str) -> None:
    slots = [slot(day(n), kind) for n in range(7) for kind in ("morning", "afternoon")]
    result = run(problem(slots, [ANN, BEN, CAL]), solver)
    assert result.unfilled == ()
    assert all(len(result.assigned[s.key]) == 1 for s in slots)


@pytest.mark.parametrize("solver", SOLVERS)
def test_a_slot_everyone_blocked_is_unfilled_as_unavailable(solver: str) -> None:
    morning = slot(day(1))
    made = problem(
        [morning],
        [ANN, BEN],
        entries=[entry(ANN, "blocked", day(1)), entry(BEN, "absence", day(1))],
    )
    assert reasons(run(made, solver), morning.key) == (Reason(UNAVAILABLE, 2),)


@pytest.mark.parametrize("solver", SOLVERS)
def test_each_fixed_shift_rule_is_named(solver: str) -> None:
    morning = slot(day(2))
    made = problem(
        [morning],
        [ANN, BEN, CAL],
        fixed_shifts=[
            fixed(day(2), "morning", [ANN]),  # overlap
            fixed(day(1), "night", [BEN]),  # ends at 07:00 on day 2: rest
            *[fixed(day(n), "morning", [CAL]) for n in (0, 1, 3, 4, 5, 6)],  # 48 hours that week
        ],
    )
    assert set(reasons(run(made, solver), morning.key)) == {
        Reason(OVERLAP, 1),
        Reason(MINIMUM_REST, 1),
        Reason(WEEKLY_MAXIMUM, 1),
    }


@pytest.mark.parametrize("solver", SOLVERS)
def test_a_place_lost_to_another_slot_is_needed_elsewhere(solver: str) -> None:
    here, there = slot(day(1)), slot(day(1), project=OTHER_PROJECT)
    result = run(problem([here, there], [ANN]), solver)
    (empty,) = result.unfilled
    assert empty.reasons == (Reason(NEEDED_ELSEWHERE, 1),)


@pytest.mark.parametrize("solver", SOLVERS)
def test_with_no_active_employee_every_place_says_so(solver: str) -> None:
    morning = slot(day(1), headcount=2)
    result = run(problem([morning], [person(9, active=False)]), solver)
    assert result.unfilled[0].places == 2
    assert result.unfilled[0].reasons == (Reason(NO_ACTIVE_EMPLOYEE, 0),)


@pytest.mark.parametrize("solver", SOLVERS)
def test_a_slot_of_three_with_two_possible_has_one_place_unfilled(solver: str) -> None:
    morning = slot(day(1), headcount=3)
    made = problem([morning], [ANN, BEN, CAL], entries=[entry(CAL, "blocked", day(1))])
    result = run(made, solver)
    assert sorted(result.assigned[morning.key]) == sorted([ANN.id, BEN.id])
    assert result.unfilled[0].places == 1
    assert result.unfilled[0].reasons == (Reason(UNAVAILABLE, 1),)


@pytest.mark.parametrize("solver", SOLVERS)
def test_a_close_preference_yields_when_no_one_else_can_work(solver: str) -> None:
    """Legacy flaw: a disliked shift acted as a block and left the slot empty."""
    morning = slot(day(1))
    result = run(problem([morning], [ANN], entries=[entry(ANN, "close", day(1))]), solver)
    assert result.assigned[morning.key] == (ANN.id,)
    assert result.summary[0].close_not_met == 1


@pytest.mark.parametrize("solver", SOLVERS)
def test_open_and_close_preferences_are_honoured_when_they_cost_nothing(solver: str) -> None:
    """Ann would like the odd days and would rather not work the even ones; Ben does not mind."""
    mornings = [slot(day(n)) for n in range(1, 7)]
    wishes = [entry(ANN, "open" if n % 2 else "close", day(n)) for n in range(1, 7)]
    result = run(problem(mornings, [ANN, BEN], entries=wishes), solver)
    for s_ in mornings:
        expected = ANN if s_.day.day % 2 == day(1).day % 2 else BEN
        assert result.assigned[s_.key] == (expected.id,), s_.day


def test_hours_not_shift_counts_are_balanced() -> None:
    """Legacy flaw: fairness balanced shift counts. Ann already works 24 hours in the range."""
    mornings = [slot(day(n)) for n in range(3, 9)]
    made = problem(
        mornings,
        [ANN, BEN, CAL],
        fixed_shifts=[fixed(day(n), "afternoon", [ANN]) for n in range(3)],
    )
    made = replace(made, first_day=day(0))
    result = run(made)
    assert hours(result) == {ANN.id: 24, BEN.id: 24, CAL.id: 24}


def test_night_shifts_are_balanced() -> None:
    """Legacy flaw: nights were not balanced at all."""
    nights = [slot(day(n), "night") for n in range(4)]
    days = [slot(day(n), "afternoon") for n in range(4, 6)]
    result = run(problem(nights + days, [ANN, BEN]))
    counts = Counter(p for s in nights for p in result.assigned[s.key])
    assert counts[ANN.id] == counts[BEN.id] == 2


def test_hours_spread_is_at_most_one_shift() -> None:
    slots = [slot(day(n), kind) for n in range(5) for kind in ("morning", "afternoon")]
    result = run(problem(slots, [ANN, BEN, CAL]))
    spread = max(hours(result).values()) - min(hours(result).values())
    assert spread <= 8


@pytest.mark.parametrize("solver", SOLVERS)
def test_two_projects_in_one_run_never_double_book(solver: str) -> None:
    """Legacy flaw: generation ran one project at a time."""
    here, there = slot(day(1)), slot(day(1), project=OTHER_PROJECT)
    result = run(problem([here, there], [ANN, BEN]), solver)
    assert {*result.assigned[here.key], *result.assigned[there.key]} == {ANN.id, BEN.id}


@pytest.mark.parametrize("tired", ["ann", "ben"])
def test_a_seventh_day_in_a_row_goes_to_someone_else_when_all_else_is_equal(tired: str) -> None:
    """One of them worked the six days before the range (outside it, so those hours do not count).

    Both directions are tried, so the test cannot pass by the solver's own tie-breaking.
    """
    worn, fresh = (ANN, BEN) if tired == "ann" else (BEN, ANN)
    sunday = slot(day(7))
    before = [fixed(day(n), "morning", [worn]) for n in range(1, 7)]
    result = run(problem([sunday], [ANN, BEN], fixed_shifts=before))
    assert result.assigned[sunday.key] == (fresh.id,)


def test_a_run_longer_than_six_days_is_avoided_when_coverage_allows() -> None:
    mornings = [slot(day(n)) for n in range(14)]
    result = run(problem(mornings, [ANN, BEN]))
    assert result.unfilled == ()
    for who in (ANN, BEN):
        worked = sorted(s.day for s in mornings if who.id in result.assigned[s.key])
        longest, current = 1, 1
        for a, b in pairwise(worked):
            current = current + 1 if (b - a).days == 1 else 1
            longest = max(longest, current)
        assert longest <= 6, (who, worked)


def test_regenerating_with_nothing_changed_proposes_no_change() -> None:
    slots = [slot(day(n), kind) for n in range(7) for kind in ("morning", "night")]
    first = run(problem(slots, [ANN, BEN, CAL]))
    again = run(problem(slots, [ANN, BEN, CAL], current=first.assigned))
    assert again.changes.added == () and again.changes.removed == ()


def test_a_new_absence_changes_only_that_days_slots() -> None:
    slots = [slot(day(n)) for n in range(5)]
    first = run(problem(slots, [ANN, BEN, CAL]))
    (holder,) = first.assigned[slots[2].key]
    sick = entry(person(int(holder.int) - 1000), "absence", day(2))
    again = run(problem(slots, [ANN, BEN, CAL], entries=[sick], current=first.assigned))
    assert {key for key, _ in again.changes.removed} == {slots[2].key}
    assert {key for key, _ in again.changes.added} == {slots[2].key}


def test_a_small_problem_is_proven_optimal_and_greedy_never_is() -> None:
    made = problem([slot(day(1))], [ANN, BEN])
    assert run(made, "cpsat").optimal is True
    assert run(made, "greedy").optimal is False


def test_an_unknown_solver_is_refused() -> None:
    with pytest.raises(ValueError):
        generate(problem([], [ANN]), solver="magic", time_limit=timedelta(seconds=1))  # type: ignore[arg-type]


@pytest.mark.parametrize("budget", [0.0, 0.001])
def test_with_no_time_to_search_the_repair_pass_still_fills_and_nothing_is_optimal(
    budget: float,
) -> None:
    slots = [slot(day(n), kind) for n in range(7) for kind in ("morning", "afternoon")]
    made = problem(slots, [ANN, BEN, CAL])
    result = generate(made, solver="cpsat", time_limit=timedelta(seconds=budget), workers=1)
    assert result.unfilled == ()
    assert result.optimal is False


def test_fixed_nights_in_the_range_count_in_the_summary() -> None:
    made = problem([slot(day(3))], [ANN, BEN], fixed_shifts=[fixed(day(1), "night", [ANN])])
    made = replace(made, first_day=day(0))
    summary = {s.user_id: s for s in run(made).summary}
    assert summary[ANN.id].nights == 1
    assert summary[ANN.id].minutes >= 8 * 60


def test_a_proposal_that_breaks_a_hard_rule_is_never_returned() -> None:
    """The last line of defence: ``validate`` judges every proposal before it leaves (G3)."""
    from hds.scheduling.engine import _check
    from hds.scheduling.problem import SolverError

    morning, other = slot(day(1)), slot(day(1), project=OTHER_PROJECT)
    with pytest.raises(SolverError):
        _check(problem([morning, other], [ANN]), {morning.key: (ANN.id,), other.key: (ANN.id,)})


def test_a_stage_that_is_not_proven_makes_the_result_not_optimal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When the time limit stops a stage with a good but unproven answer, ``optimal`` is false."""
    from ortools.sat.python import cp_model

    original = cp_model.CpSolver.solve

    def feasible_only(self: cp_model.CpSolver, model: cp_model.CpModel) -> object:
        original(self, model)
        return cp_model.FEASIBLE

    monkeypatch.setattr(cp_model.CpSolver, "solve", feasible_only)
    result = run(problem([slot(day(1))], [ANN, BEN]))
    assert result.unfilled == ()
    assert result.optimal is False


def test_the_repair_pass_counts_the_hours_the_solver_already_gave() -> None:
    """Review finding: repair balanced on fixed hours only, piling places on one person."""
    from hds.scheduling.eligibility import Roster
    from hds.scheduling.engine import _repair

    given = [slot(day(n)) for n in range(3)]
    empty = slot(day(5))
    made = problem([*given, empty], [ANN, BEN])
    roster = Roster(made)
    chosen = {s.key: [ANN.id] for s in given} | {empty.key: []}
    for s in given:
        roster.book(ANN.id, s)
    _repair(made, roster, chosen, [ANN.id, BEN.id], {})
    assert chosen[empty.key] == [BEN.id]


@pytest.mark.parametrize("solver", SOLVERS)
def test_people_kept_on_a_draft_take_their_places(solver: str) -> None:
    """Review finding: a draft's non-candidates were outside the slot, so it was over-filled."""
    manager = person(50)
    kept = slot(day(1), headcount=2)
    kept = replace(kept, kept=(manager.id,))
    result = run(problem([kept], [ANN, BEN]), solver)
    assert len(result.assigned[kept.key]) == 1
    assert result.unfilled == ()


def test_the_engine_module_is_not_hidden_by_the_function() -> None:
    """Review finding: ``from .generate import generate`` replaced the module with the function."""
    import types

    import hds.scheduling.engine as engine

    assert isinstance(engine, types.ModuleType)


def test_a_slots_day_is_the_local_date_it_starts() -> None:
    """Review finding: a stored day could disagree with the start; it is now derived from it."""
    night = slot(day(1), "night")
    assert night.day == day(1)
    assert night.starts_at.date() == day(1)  # 23:00 in Israel is 20:00 UTC the same day


def test_a_part_minute_rounds_up_so_the_weekly_limit_is_never_crossed() -> None:
    """Review finding: minutes were rounded down, so seconds over the limit went unnoticed."""
    morning = slot(day(1))
    odd = replace(morning, ends_at=morning.ends_at + timedelta(seconds=30))
    assert odd.minutes == 8 * 60 + 1


def test_the_last_check_counts_kept_people_against_the_headcount() -> None:
    from hds.scheduling.engine import _check
    from hds.scheduling.problem import SolverError

    full = replace(slot(day(1), headcount=2), kept=(person(50).id,))
    with pytest.raises(SolverError):
        _check(problem([full], [ANN, BEN]), {full.key: (ANN.id, BEN.id)})


def test_a_kept_person_who_already_breaks_a_rule_does_not_fail_the_run() -> None:
    """Review finding: a draft an Admin overfilled by hand (003 allows it) failed every run."""
    crowded = replace(slot(day(1), headcount=1), kept=(person(50).id, person(51).id))
    tired = person(52)
    rested = replace(slot(day(3)), kept=(tired.id,))
    night_before = fixed(day(2), "night", [tired])  # the kept person has too little rest
    result = run(problem([crowded, rested], [ANN], fixed_shifts=[night_before]))
    assert result.assigned[crowded.key] == ()
    assert result.assigned[rested.key] == ()


def test_a_slots_own_draft_in_the_fixed_shifts_is_refused() -> None:
    """Review finding: a draft in both ``fixed`` and a slot counted its people twice."""
    from hds.scheduling.engine import slot_id

    draft = fixed(day(1), "morning", [person(50)])
    own = replace(slot(day(1)), shift_id=draft.id, kept=(person(50).id,))
    with pytest.raises(ValueError):
        run(problem([own], [ANN], fixed_shifts=[draft]))
    assert slot_id(own.key) != draft.id


def test_kept_people_are_never_reported_as_removed() -> None:
    """Review finding: kept people in ``current`` came out as removed."""
    manager = person(50)
    held = replace(slot(day(1), headcount=2), kept=(manager.id,))
    result = run(problem([held], [ANN], current={held.key: (manager.id, ANN.id)}))
    assert result.changes.removed == ()


def test_a_place_left_when_everyone_is_already_on_the_slot_still_has_a_reason() -> None:
    """Review finding: with every candidate on the slot, the empty place had no reason at all."""
    pair = slot(day(1), headcount=2)
    result = run(problem([pair], [ANN]))
    (empty,) = result.unfilled
    assert empty.places == 1
    assert empty.reasons == (Reason(NO_ACTIVE_EMPLOYEE, 0),)


def test_someone_who_cannot_work_does_not_pin_the_balance() -> None:
    """Review finding: an absent candidate held the minimum at 0, so balance ignored the rest.

    Ann already works 40 hours; Ben and Dan share two shifts; Cal is away all week. Only a balance
    among those who can work gives Ben and Dan one each.
    """
    dan, away = person(4), person(5)
    shifts = [slot(day(1)), slot(day(3))]
    made = problem(
        shifts,
        [ANN, BEN, dan, away],
        entries=[entry(away, "absence", day(0), day(6))],
        fixed_shifts=[fixed(day(n), "afternoon", [ANN]) for n in (0, 2, 4, 5, 6)],
    )
    made = replace(made, first_day=day(0), last_day=day(6))
    result = run(made)
    assert sorted(p for s in shifts for p in result.assigned[s.key]) == sorted([BEN.id, dan.id])


def test_seconds_do_not_make_the_weekly_limit_stricter_than_the_rule() -> None:
    """Review finding: CP-SAT rounded fixed shifts up to minutes, refusing what the rule allows."""
    week = [fixed(day(n), "morning", [ANN]) for n in range(5)]  # 40 hours
    week[0] = replace(week[0], ends_at=week[0].ends_at + timedelta(seconds=20))
    week[1] = replace(week[1], ends_at=week[1].ends_at - timedelta(seconds=20))
    saturday = slot(day(6))
    result = run(problem([saturday], [ANN], fixed_shifts=week))
    assert result.assigned[saturday.key] == (ANN.id,)
    assert result.optimal is True
