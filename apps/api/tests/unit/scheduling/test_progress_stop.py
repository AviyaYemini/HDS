"""Best so far and Stop (008 T044).

Covers: 008 FR-027, FR-028, SC-007; research P7. ``progress`` is called with rising filled counts
over the places needed; ``should_stop`` ends the solve with the best result reached, still within
every rule, marked stopped and never optimal. Both solvers.
"""

from datetime import timedelta

import pytest

from hds.scheduling.engine import generate
from tests.property.test_generate_properties import judged
from tests.unit.scheduling.factory import day, person, problem, slot

SOLVERS = ["greedy", "cpsat"]
PEOPLE = [person(n) for n in range(1, 7)]


def week() -> object:
    """Every day of a week, morning and afternoon, two places each, for six people."""
    slots = [slot(day(d), kind, headcount=2) for d in range(7) for kind in ("morning", "afternoon")]
    return problem(slots, PEOPLE)


@pytest.mark.parametrize("solver", SOLVERS)
def test_progress_rises_to_the_places_filled_out_of_those_needed(solver: str) -> None:
    made = week()
    seen: list[tuple[int, int]] = []

    result = generate(
        made,  # type: ignore[arg-type]
        solver=solver,  # type: ignore[arg-type]
        time_limit=timedelta(seconds=10),
        workers=1,
        progress=lambda filled, needed: seen.append((filled, needed)),
    )

    assert seen, "progress was reported"
    needed = {n for _, n in seen}
    assert needed == {sum(s.places for s in made.slots)}  # type: ignore[attr-defined]
    filled = [f for f, _ in seen]
    assert filled == sorted(filled), "never falls"
    assert filled[-1] <= sum(len(on) for on in result.assigned.values())
    assert not result.stopped


@pytest.mark.parametrize("solver", SOLVERS)
def test_stop_ends_with_the_best_result_valid_stopped_and_not_optimal(solver: str) -> None:
    made = week()

    result = generate(
        made,  # type: ignore[arg-type]
        solver=solver,  # type: ignore[arg-type]
        time_limit=timedelta(seconds=30),
        workers=1,
        should_stop=lambda: True,
    )

    assert result.stopped
    assert not result.optimal
    assert judged(made, result) == []  # type: ignore[arg-type]


def test_a_stop_asked_mid_run_lands_well_within_five_seconds() -> None:
    """SC-007: Stop takes effect within 5 s whatever stage the solver is in."""
    import time

    made = week()
    start = time.monotonic()
    asked = start + 0.3

    result = generate(
        made,  # type: ignore[arg-type]
        solver="cpsat",
        time_limit=timedelta(seconds=60),
        workers=1,
        should_stop=lambda: time.monotonic() >= asked,
    )

    assert time.monotonic() - start < 5
    assert result.stopped or result.optimal
