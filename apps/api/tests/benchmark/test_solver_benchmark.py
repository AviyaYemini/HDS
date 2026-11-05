"""The solver benchmark's pass marks, run by hand with ``-m benchmark`` (004 T024).

Covers: 004 SC-001 (30 employees, several projects, a week, within 30 seconds); research G13.
"""

import time
from datetime import timedelta

import pytest
from benchmarks.compare_solvers import measure, week

pytestmark = pytest.mark.benchmark


def test_thirty_employees_get_a_week_within_thirty_seconds() -> None:
    started = time.perf_counter_ns()
    found = measure(week(30), "cpsat", timedelta(seconds=20))
    assert (time.perf_counter_ns() - started) < 30 * 1_000_000_000
    assert found.filled > 0


@pytest.mark.parametrize("size", [10, 30])
def test_cpsat_fills_at_least_as_many_places_as_greedy(size: int) -> None:
    problem = week(size)
    greedy = measure(problem, "greedy", timedelta(seconds=20))
    cpsat = measure(problem, "cpsat", timedelta(seconds=20))
    assert cpsat.filled >= greedy.filled
