"""Why a place stayed empty, with names and numbers (008 T020).

Covers: 008 FR-013. Every reason keeps its count; minimum rest and the weekly maximum also name each
person they excluded, with the rest they would have and the rest needed, or the minutes already
worked that week, the slot's minutes and the limit. The counts still add up to every candidate not
on the slot. Both solvers explain the same way.
"""

from datetime import timedelta

import pytest

from hds.scheduling.engine import generate
from hds.scheduling.problem import (
    MINIMUM_REST,
    NEEDED_ELSEWHERE,
    NO_ACTIVE_EMPLOYEE,
    OVERLAP,
    UNAVAILABLE,
    WEEKLY_MAXIMUM,
    Blocked,
    Problem,
    Result,
    Unfilled,
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
ANN, BEN, CAL, DAN = person(1), person(2), person(3), person(4)
REST = 11 * 60
WEEK = 48 * 60


def run(made: Problem, solver: str) -> Result:
    return generate(made, solver=solver, time_limit=timedelta(seconds=10), workers=1)  # type: ignore[arg-type]


def empty(result: Result, key: str) -> Unfilled:
    (found,) = [u for u in result.unfilled if u.key == key]
    return found


@pytest.mark.parametrize("solver", SOLVERS)
@pytest.mark.parametrize(
    ("code", "build"),
    [
        (UNAVAILABLE, lambda s: {"entries": [entry(ANN, "blocked", s.day)]}),
        (OVERLAP, lambda s: {"fixed_shifts": [fixed(s.day, "morning", [ANN])]}),
        (MINIMUM_REST, lambda s: {"fixed_shifts": [fixed(day(1), "afternoon", [ANN])]}),
        (
            WEEKLY_MAXIMUM,
            lambda s: {
                "fixed_shifts": [fixed(day(n), "morning", [ANN]) for n in (0, 1, 3, 4, 5, 6)]
            },
        ),
    ],
)
def test_one_week_per_reason_gives_that_reason_alone(solver: str, code: str, build: object) -> None:
    morning = slot(day(2))
    made = problem([morning], [ANN], **build(morning))  # type: ignore[operator]

    (reason,) = empty(run(made, solver), morning.key).reasons

    assert (reason.code, reason.employees) == (code, 1)
    named = code in (MINIMUM_REST, WEEKLY_MAXIMUM)
    assert [p.user_id for p in reason.people] == ([ANN.id] if named else [])


@pytest.mark.parametrize("solver", SOLVERS)
def test_no_one_to_give_it_to_and_needed_elsewhere(solver: str) -> None:
    nobody = slot(day(2))
    assert [r.code for r in empty(run(problem([nobody], []), solver), nobody.key).reasons] == [
        NO_ACTIVE_EMPLOYEE
    ]
    here, there = slot(day(1)), slot(day(1), project=OTHER_PROJECT)
    (lost,) = run(problem([here, there], [ANN]), solver).unfilled
    assert [(r.code, r.employees, r.people) for r in lost.reasons] == [(NEEDED_ELSEWHERE, 1, ())]


@pytest.mark.parametrize("solver", SOLVERS)
def test_rest_names_the_person_with_the_rest_they_would_have_and_need(solver: str) -> None:
    morning = slot(day(2))  # 07:00 on day 2
    made = problem([morning], [ANN], fixed_shifts=[fixed(day(1), "afternoon", [ANN])])  # to 23:00

    (reason,) = empty(run(made, solver), morning.key).reasons

    assert reason.people == (Blocked(ANN.id, MINIMUM_REST, have=8 * 60, need=REST),)


@pytest.mark.parametrize("solver", SOLVERS)
def test_rest_gives_the_shortest_rest_when_both_sides_are_short(solver: str) -> None:
    afternoon = slot(day(2), "afternoon")  # 15:00 to 23:00 on day 2
    made = problem(
        [afternoon],
        [ANN],
        fixed_shifts=[
            fixed(day(2), "morning", [ANN], project=OTHER_PROJECT),  # ends 15:00: no rest at all
            fixed(day(3), "morning", [ANN]),  # starts 07:00: 8 h after
        ],
    )

    (reason,) = empty(run(made, solver), afternoon.key).reasons

    assert reason.people == (Blocked(ANN.id, MINIMUM_REST, have=0, need=REST),)


@pytest.mark.parametrize("solver", SOLVERS)
def test_the_weekly_maximum_names_the_minutes_worked_the_slot_and_the_limit(solver: str) -> None:
    morning = slot(day(2))
    worked = [fixed(day(n), "morning", [ANN]) for n in (0, 1, 3, 4, 5)]  # 40 h
    made = problem([morning], [ANN], fixed_shifts=[*worked, fixed(day(6), "afternoon", [ANN])])

    (reason,) = empty(run(made, solver), morning.key).reasons

    assert reason.people == (
        Blocked(ANN.id, WEEKLY_MAXIMUM, week_minutes=WEEK, slot_minutes=8 * 60, limit=WEEK),
    )


@pytest.mark.parametrize("solver", SOLVERS)
def test_the_counts_still_add_up_and_each_named_person_is_counted(solver: str) -> None:
    morning = slot(day(2))
    made = problem(
        [morning],
        [ANN, BEN, CAL, DAN],
        entries=[entry(DAN, "absence", day(2))],
        fixed_shifts=[
            fixed(day(2), "morning", [ANN]),
            fixed(day(1), "afternoon", [BEN]),
            *[fixed(day(n), "morning", [CAL]) for n in (0, 1, 3, 4, 5, 6)],
        ],
    )

    reasons = empty(run(made, solver), morning.key).reasons

    assert sum(r.employees for r in reasons) == 4
    for reason in reasons:
        if reason.code in (MINIMUM_REST, WEEKLY_MAXIMUM):
            assert len(reason.people) == reason.employees
        else:
            assert reason.people == ()
