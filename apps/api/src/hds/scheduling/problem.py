"""Inputs and outputs of roster generation (004 research G3).

Layer: scheduling (pure). Standard library only; MUST NOT import anything from the application, a
web framework, a database driver or the network (Constitution III). Services build a ``Problem``
from their records and store a ``Result``; both solvers take the one and give the other.
"""

import math
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from functools import cached_property

from .rules import local_day
from .types import Employee, Entry, Settings, ShiftSlot

UNAVAILABLE = "schedule.unavailable"
OVERLAP = "schedule.overlap"
MINIMUM_REST = "schedule.minimum_rest"
WEEKLY_MAXIMUM = "schedule.weekly_maximum"
NEEDED_ELSEWHERE = "schedule.needed_elsewhere"
NO_ACTIVE_EMPLOYEE = "schedule.no_active_employee"
REASON_CODES = (UNAVAILABLE, OVERLAP, MINIMUM_REST, WEEKLY_MAXIMUM, NEEDED_ELSEWHERE)
"""The reasons a candidate cannot take a place, in the order they are tested (research G6)."""


def minutes(length: timedelta) -> int:
    """``length`` in whole minutes, rounded up (research G4: never under-count against a limit)."""
    return math.ceil(length.total_seconds() / 60)


@dataclass(frozen=True)
class Slot:
    """A shift to staff: a new one from a staffing need, or an existing draft (``shift_id``).

    ``key`` is unique within a problem and stable for the same project, shift type and date, so a
    proposal can be compared with the drafts it would replace. ``night`` is whether its shift type
    crosses midnight (spec FR-018). ``kept`` holds people already on the draft who are not
    candidates (a Manager or Admin with no open entry, spec FR-007): they stay, take their places,
    and their rules count like a fixed shift's.
    """

    key: str
    shift_id: uuid.UUID | None
    project_id: uuid.UUID
    shift_type_id: uuid.UUID
    starts_at: datetime
    ends_at: datetime
    zone: str
    headcount: int
    night: bool
    kept: tuple[uuid.UUID, ...] = ()

    @cached_property
    def day(self) -> date:
        """The local date at the site the shift starts on, derived so it cannot disagree."""
        return local_day(self.starts_at, self.zone)

    @property
    def length(self) -> timedelta:
        """The shift's real elapsed length."""
        return self.ends_at - self.starts_at

    @cached_property
    def minutes(self) -> int:
        """The length in whole minutes, rounded up so a limit in minutes is never crossed."""
        return minutes(self.length)

    @property
    def places(self) -> int:
        """The places candidates may fill: the headcount less the people kept on it.

        Never below zero: an Admin may have overfilled the draft by hand (003 FR-116).
        """
        return max(0, self.headcount - len(self.kept))


@dataclass(frozen=True)
class Problem:
    """Everything a solver needs, and nothing it may change.

    ``fixed`` holds the shifts the proposal must respect but never changes: published shifts and
    the other shifts of the surroundings (spec FR-016), each with its employees in ``assigned``. A
    slot's own draft is never in ``fixed``: its non-candidates are the slot's ``kept`` people
    (``generate`` refuses the overlap). ``current`` maps a slot key to the candidates on that draft
    now, for "fewest changes"; kept people are ignored there. ``first_day`` and ``last_day`` bound
    the range the summary counts hours and nights in. ``night_types`` names the shift types that
    cross midnight, so fixed shifts count as nights too. ``max_run`` is the longest run of working
    days that costs nothing (spec assumption: 6).
    """

    slots: tuple[Slot, ...]
    fixed: tuple[ShiftSlot, ...]
    candidates: tuple[Employee, ...]
    entries: tuple[Entry, ...]
    settings: Settings
    first_day: date
    last_day: date
    current: Mapping[str, tuple[uuid.UUID, ...]] = field(default_factory=dict)
    night_types: frozenset[uuid.UUID] = frozenset()
    max_run: int = 6


@dataclass(frozen=True)
class Blocked:
    """One person a rule keeps off a slot, with the numbers that show it (008 FR-013, research P10).

    Minimum rest gives the rest they would have and the rest needed (``have``, ``need``); the weekly
    maximum gives the minutes they already work that week, the slot's and the limit
    (``week_minutes``, ``slot_minutes``, ``limit``). All in minutes; the rest are ``None``.
    """

    user_id: uuid.UUID
    code: str
    have: int | None = None
    need: int | None = None
    week_minutes: int | None = None
    slot_minutes: int | None = None
    limit: int | None = None


@dataclass(frozen=True)
class Reason:
    """One reason places stayed empty, and how many candidates it excluded.

    For minimum rest and the weekly maximum, ``people`` names each of them with the numbers.
    """

    code: str
    employees: int
    people: tuple[Blocked, ...] = ()


@dataclass(frozen=True)
class Unfilled:
    """A slot with ``places`` still empty, and why.

    The reasons' counts add up to every candidate not already on the slot.
    """

    key: str
    places: int
    reasons: tuple[Reason, ...]


@dataclass(frozen=True)
class EmployeeSummary:
    """One candidate's share of the range.

    Worked minutes and nights (fixed shifts included), and the open preferences met and close
    preferences not met by the proposal.
    """

    user_id: uuid.UUID
    minutes: int
    nights: int
    open_met: int
    close_not_met: int


@dataclass(frozen=True)
class Changes:
    """The proposal against the current drafts, as (slot key, user id) pairs, sorted."""

    added: tuple[tuple[str, uuid.UUID], ...]
    removed: tuple[tuple[str, uuid.UUID], ...]


@dataclass(frozen=True)
class Result:
    """A proposal.

    Who is on each slot, the empty places with reasons, the summary, the changes, and whether it is
    proven optimal (never for the greedy solver).
    """

    assigned: Mapping[str, tuple[uuid.UUID, ...]]
    unfilled: tuple[Unfilled, ...]
    summary: tuple[EmployeeSummary, ...]
    changes: Changes
    optimal: bool


class SolverError(RuntimeError):
    """A solver gave an answer that breaks a hard rule; it is a bug, never stored."""
