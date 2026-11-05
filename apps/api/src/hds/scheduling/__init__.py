"""The scheduling package: every roster rule, as pure functions over plain data.

Layer: scheduling (pure). Standard library only; MUST NOT import anything from the application, a
web framework, a database driver or the network, and MUST NOT be imported by models or repositories
(Constitution III). Services and, later, the generator call it.
"""

from .rules import local_span, validate, week_start
from .types import Employee, Entry, RuleWarning, Settings, ShiftSlot

__all__ = [
    "Employee",
    "Entry",
    "Settings",
    "ShiftSlot",
    "RuleWarning",
    "local_span",
    "validate",
    "week_start",
]
