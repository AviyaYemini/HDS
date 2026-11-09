"""Property tests: steering a run never costs a rule, a lock, or a filled place (008 T030).

On the random weeks of test_generate_properties.py, with random controls and chips: a proposal
breaks no hard rule and keeps every lock; locks come from a first proposal, so a lock is itself
within the rules, and enter the rerun as kept people (research P2). A rerun from an optimal result
with nothing changed, keeping changes small, changes nothing; and chips never cost a filled place,
since they weigh only after filling every place (FR-021).

Covers: 008 FR-017 to FR-021, SC-005, SC-006; research P2 to P6. Examples: a quarter of
``HDS_GENERATE_EXAMPLES`` (200 by default, 1,000 in CI), as each case solves two or three times.
"""

import uuid
from dataclasses import replace
from datetime import timedelta

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from hds.scheduling.engine import generate
from hds.scheduling.problem import CHIP_KINDS, Chip, Controls, Problem, Result
from tests.property.test_generate_properties import EXAMPLES, judged, problems
from tests.unit.scheduling.factory import TYPES

PROFILE = settings(
    max_examples=max(EXAMPLES // 4, 10),
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large],
)


def solve(made: Problem) -> Result:
    return generate(made, solver="cpsat", time_limit=timedelta(seconds=5), workers=1)


def filled(result: Result) -> int:
    return sum(len(on) for on in result.assigned.values())


@st.composite
def steered(draw: st.DrawFn) -> Problem:
    """A random week with random controls and chips about its people."""
    made = draw(problems())
    people = [c.id for c in made.candidates]
    chips = []
    for kind in draw(st.lists(st.sampled_from(CHIP_KINDS), max_size=4)):
        who = draw(st.sampled_from(people))
        if kind in ("max_shifts", "max_nights", "max_hours"):
            chips.append(Chip(kind, who, count=draw(st.integers(0, 16))))
        elif kind == "keep_on":
            chips.append(
                Chip(
                    kind, who, project_id=draw(st.sampled_from([s.project_id for s in made.slots]))
                )
            )
        elif draw(st.booleans()):
            chips.append(Chip(kind, who, weekday=draw(st.integers(0, 6))))
        else:
            chips.append(
                Chip(kind, who, shift_type_id=TYPES[draw(st.sampled_from(list(TYPES)))][0])
            )
    controls = Controls(keep_changes_small=draw(st.booleans()), wishes_strictly=draw(st.booleans()))
    return replace(made, chips=tuple(chips), controls=controls)


def locked(made: Problem, first: Result, keep: list[bool]) -> Problem:
    """``made`` again with some of ``first``'s people locked on their slots (kept, research P2)."""
    flags = iter(keep)
    slots = []
    for s in made.slots:
        held = tuple(p for p in first.assigned.get(s.key, ()) if next(flags, False))
        slots.append(replace(s, kept=(*s.kept, *held)))
    return replace(made, slots=tuple(slots))


@PROFILE
@given(steered(), st.lists(st.booleans(), max_size=40))
def test_steering_and_locks_never_break_a_hard_rule_and_every_lock_is_kept(
    made: Problem, keep: list[bool]
) -> None:
    first = solve(made)
    again = locked(made, first, keep)

    result = solve(again)

    # The rules judge the kept people with the proposal's: a lock counts as fixed work.
    with_kept = replace(
        result,
        assigned={s.key: (*s.kept, *result.assigned.get(s.key, ())) for s in again.slots},
    )
    assert judged(again, with_kept) == []
    for s in again.slots:
        assert not set(s.kept) & set(result.assigned.get(s.key, ())), "a lock is never placed twice"
        assert len(s.kept) + len(result.assigned.get(s.key, ())) <= max(s.headcount, len(s.kept))


@PROFILE
@given(steered())
def test_a_rerun_from_an_optimal_result_with_nothing_changed_changes_nothing(
    made: Problem,
) -> None:
    small = replace(made, controls=replace(made.controls, keep_changes_small=True))
    first = solve(small)
    if not first.optimal:
        return

    rerun = solve(replace(small, start=first.assigned))

    if rerun.optimal:
        assert rerun.assigned == first.assigned


@PROFILE
@given(steered())
def test_chips_never_cost_a_filled_place(made: Problem) -> None:
    plain = solve(replace(made, chips=()))
    steered_result = solve(made)

    if plain.optimal and steered_result.optimal:
        assert filled(steered_result) == filled(plain)


def test_the_rerun_property_is_not_vacuous() -> None:
    """At least the plain week of one slot is solved optimally, so the rerun check runs."""
    from tests.unit.scheduling.factory import day, person, problem, slot

    made = problem([slot(day(1))], [person(1), person(2)])
    first = solve(replace(made, controls=Controls(keep_changes_small=True)))
    assert first.optimal
    assert solve(replace(made, start=first.assigned)).assigned == first.assigned
    assert uuid.UUID(int=1001) in {p.id for p in made.candidates}
