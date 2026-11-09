"""How a solved run ended (008 FR-028, review R67): stopped, timed out or finished."""

from types import SimpleNamespace
from typing import Any

import pytest

from hds.services.schedule_job import ended_of


@pytest.mark.parametrize(
    ("solver", "stopped", "optimal", "ended"),
    [
        ("cpsat", True, False, "stopped"),
        ("cpsat", False, False, "timed_out"),
        ("cpsat", False, True, "finished"),
        ("greedy", False, False, "finished"),
        ("greedy", True, False, "stopped"),
    ],
)
def test_ended_says_how_the_run_ended(
    solver: str, stopped: bool, optimal: bool, ended: str
) -> None:
    claim: Any = SimpleNamespace(solver=solver)
    result: Any = SimpleNamespace(stopped=stopped, optimal=optimal)
    assert ended_of(claim, result) == ended
