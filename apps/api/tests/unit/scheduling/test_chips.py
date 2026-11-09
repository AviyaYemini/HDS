"""The manager's preferences for a run, met or not, and why (008 T031).

Covers: 008 FR-020 to FR-022; research P5. Each kind met and not met, with its cause; two chips
that pull opposite ways both reported; a chip whose person is not an active candidate, or whose
project is not in the run, reported as no longer applying without failing the run. Both solvers.
"""

from dataclasses import replace
from datetime import timedelta

import pytest

from hds.scheduling.engine import generate
from hds.scheduling.problem import Chip, ChipResult, Problem, Result
from tests.unit.scheduling.factory import (
    OTHER_PROJECT,
    PROJECT,
    TYPES,
    day,
    fixed,
    person,
    problem,
    slot,
)

SOLVERS = ["greedy", "cpsat"]
ANN, BEN = person(1), person(2)
MONDAY = 1


def run(made: Problem, solver: str) -> Result:
    return generate(made, solver=solver, time_limit=timedelta(seconds=10), workers=1)  # type: ignore[arg-type]


def who(result: Result, key: str) -> tuple[object, ...]:
    return result.assigned[key]


@pytest.mark.parametrize("solver", SOLVERS)
def test_keep_off_a_weekday_is_met_when_someone_else_can_take_it(solver: str) -> None:
    monday = slot(day(MONDAY))
    made = replace(problem([monday], [ANN, BEN]), chips=(Chip("keep_off", ANN.id, weekday=MONDAY),))

    result = run(made, solver)

    assert who(result, monday.key) == (BEN.id,)
    assert result.chips == (ChipResult(0, "met"),)


@pytest.mark.parametrize("solver", SOLVERS)
def test_a_chip_never_leaves_a_place_empty_and_says_so(solver: str) -> None:
    monday = slot(day(MONDAY))
    made = replace(problem([monday], [ANN]), chips=(Chip("keep_off", ANN.id, weekday=MONDAY),))

    result = run(made, solver)

    assert who(result, monday.key) == (ANN.id,)
    assert result.chips == (ChipResult(0, "not_met", "coverage"),)


# CP-SAT only: greedy fills slots in start order and weighs chips only as tie-breakers, so it may
# give the morning away before it reaches the night (research P5).
@pytest.mark.parametrize("solver", ["cpsat"])
def test_prefer_a_shift_type_puts_them_on_it(solver: str) -> None:
    morning, night = slot(day(2)), slot(day(2), "night")
    made = replace(
        problem([morning, night], [ANN, BEN]),
        chips=(Chip("prefer", ANN.id, shift_type_id=TYPES["night"][0]),),
    )

    result = run(made, solver)

    assert who(result, night.key) == (ANN.id,)
    assert result.chips == (ChipResult(0, "met"),)


@pytest.mark.parametrize("solver", SOLVERS)
def test_a_cap_on_shifts_is_kept_when_others_can_cover(solver: str) -> None:
    slots = [slot(day(n)) for n in (0, 2, 4)]
    made = replace(problem(slots, [ANN, BEN]), chips=(Chip("max_shifts", ANN.id, count=1),))

    result = run(made, solver)

    assert sum(ANN.id in who(result, s.key) for s in slots) <= 1
    assert all(who(result, s.key) for s in slots)
    assert result.chips == (ChipResult(0, "met"),)


@pytest.mark.parametrize("solver", SOLVERS)
def test_keep_on_a_project_keeps_them_off_the_others(solver: str) -> None:
    here, there = slot(day(1)), slot(day(3), project=OTHER_PROJECT)
    made = replace(
        problem([here, there], [ANN, BEN]),
        chips=(Chip("keep_on", ANN.id, project_id=PROJECT),),
    )

    result = run(made, solver)

    assert ANN.id not in who(result, there.key)
    assert result.chips == (ChipResult(0, "met"),)


@pytest.mark.parametrize("solver", SOLVERS)
def test_two_chips_that_pull_opposite_ways_are_both_reported(solver: str) -> None:
    monday = slot(day(MONDAY))
    made = replace(
        problem([monday], [ANN, BEN]),
        chips=(
            Chip("keep_off", ANN.id, weekday=MONDAY),
            Chip("prefer", ANN.id, weekday=MONDAY),
        ),
    )

    result = run(made, solver)

    states = {r.index: (r.state, r.cause) for r in result.chips}
    assert len(states) == 2
    assert ("not_met", "conflict") in states.values()


@pytest.mark.parametrize("solver", SOLVERS)
def test_a_cap_already_passed_by_shifts_outside_the_run_names_them(solver: str) -> None:
    monday = slot(day(MONDAY))
    made = replace(
        # A shift in the run's range the run cannot change: the cap of none is already passed.
        problem([monday], [ANN, BEN], fixed_shifts=[fixed(day(MONDAY), "afternoon", [ANN])]),
        chips=(Chip("max_shifts", ANN.id, count=0),),
    )

    result = run(made, solver)

    assert who(result, monday.key) == (BEN.id,)
    assert result.chips == (ChipResult(0, "not_met", "fixed"),)


@pytest.mark.parametrize("solver", SOLVERS)
def test_chips_for_someone_inactive_or_a_project_outside_the_run_no_longer_apply(
    solver: str,
) -> None:
    monday = slot(day(MONDAY))
    gone = person(9, active=False)
    made = replace(
        problem([monday], [ANN, gone]),
        chips=(
            Chip("max_shifts", gone.id, count=0),
            Chip("keep_on", ANN.id, project_id=OTHER_PROJECT),
        ),
    )

    result = run(made, solver)

    assert who(result, monday.key) == (ANN.id,)
    assert result.chips == (ChipResult(0, "not_applying"), ChipResult(1, "not_applying"))


def test_a_cap_far_below_the_fixed_hours_still_solves() -> None:
    """The overflow is bounded by the real load: 80 fixed hours in the range against a cap of none.

    Two slots once bounded it at 48 hours, which made the model infeasible.
    """
    sunday, saturday = slot(day(0)), slot(day(6))
    week = [fixed(day(n), kind, [ANN]) for n in range(1, 6) for kind in ("morning", "afternoon")]
    made = replace(
        problem([sunday, saturday], [ANN, BEN], fixed_shifts=week),
        chips=(Chip("max_hours", ANN.id, count=0),),
    )

    result = run(made, "cpsat")

    assert ANN.id not in who(result, sunday.key) + who(result, saturday.key)
    assert result.optimal
