"""The scheduling package: every roster rule, as pure functions over plain data.

Layer: scheduling (pure). Standard library only, except ``cpsat.py``, which adds the OR-Tools solver
library; MUST NOT import anything from the application, a web framework, a database driver or the
network, and MUST NOT be imported by models or repositories (Constitution III). Services call it.
Importing the package does not load ``cpsat.py``; ``generate`` loads it when that solver is asked
for (004 research G2).
"""

from .engine import generate
from .problem import (
    Changes,
    EmployeeSummary,
    Problem,
    Reason,
    Result,
    Slot,
    SolverError,
    Unfilled,
)
from .rules import local_span, validate, week_start
from .types import Employee, Entry, RuleWarning, Settings, ShiftSlot

__all__ = [
    "Changes",
    "Employee",
    "EmployeeSummary",
    "Problem",
    "Reason",
    "Result",
    "Slot",
    "SolverError",
    "Unfilled",
    "generate",
    "Entry",
    "Settings",
    "ShiftSlot",
    "RuleWarning",
    "local_span",
    "validate",
    "week_start",
]
