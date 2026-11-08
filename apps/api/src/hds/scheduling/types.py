"""Plain inputs and outputs of the roster rules.

Layer: scheduling (pure). Standard library only; MUST NOT import anything from the application, a
web framework, a database driver or the network (Constitution III). Services translate their records
into these types and back.
"""

import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Literal

EntryKind = Literal["blocked", "open", "close", "absence"]
"""The four kinds of availability entry; any other value is refused by ``validate``."""


@dataclass(frozen=True)
class ShiftSlot:
    """A shift as the rules see it.

    ``starts_at`` and ``ends_at`` are timezone-aware UTC instants, ``ends_at`` after ``starts_at``.
    ``zone`` is the site's IANA time zone, which decides the shift's date: a shift belongs to the
    local date it starts on, so a night shift is judged against the date it begins. ``assigned``
    holds the employees on it.

    ``own_times`` holds ``(employee, starts_at, ends_at)`` for the people whose work in the shift
    has its own start and end (007 FR-027: a half shift, or two extra hours); everyone else works
    the shift's times. Rest, overlap and weekly hours judge each person by :meth:`span`; the
    shift's date and its headcount stay the shift's.
    """

    id: uuid.UUID
    project_id: uuid.UUID
    shift_type_id: uuid.UUID
    starts_at: datetime
    ends_at: datetime
    zone: str
    headcount: int
    assigned: tuple[uuid.UUID, ...]
    own_times: tuple[tuple[uuid.UUID, datetime, datetime], ...] = ()

    def span(self, person: uuid.UUID) -> tuple[datetime, datetime]:
        """When ``person`` works this shift: their own times if set, otherwise the shift's."""
        for who, starts_at, ends_at in self.own_times:
            if who == person:
                return starts_at, ends_at
        return self.starts_at, self.ends_at


@dataclass(frozen=True)
class Employee:
    """An employee on a shift: only whether they are still active matters to the rules."""

    id: uuid.UUID
    is_active: bool


@dataclass(frozen=True)
class Entry:
    """An availability entry of one employee.

    ``kind`` is ``blocked``, ``open``, ``close`` or ``absence``. It covers the local dates from
    ``starts_on`` to ``ends_on`` inclusive, limited to one shift type and one weekday (0 Sunday to 6
    Saturday) when those are set.

    ``reason`` is ``None`` for an entry the employee made. ``not_submitted`` marks the block that
    stands for an availability window whose deadline passed without a submission: the employee
    counts as unavailable for every date of it (007 FR-014), and a manual assignment there warns
    ``warnings.not_submitted`` instead of ``warnings.blocked``.
    """

    employee_id: uuid.UUID
    kind: EntryKind
    starts_on: date
    ends_on: date
    shift_type_id: uuid.UUID | None
    weekday: int | None
    reason: str | None = None


@dataclass(frozen=True)
class Settings:
    """The organisation's limits: minimum rest, weekly maximum and the zone of its weeks.

    A week runs Sunday to Saturday in ``week_zone`` (research R2).
    """

    min_rest: timedelta
    max_weekly: timedelta
    week_zone: str


@dataclass(frozen=True)
class RuleWarning:
    """One broken rule.

    ``code`` is stable and has a translated text in the API's locale files. ``employee_id`` is the
    employee it concerns, ``None`` for a headcount warning, which concerns the shift.
    ``shift_ids`` holds every shift involved, sorted. ``values`` are the rule's numbers, as
    ``(name, text)`` pairs, so a screen can say them in its own language: for a short rest
    ``rest_hours`` and ``minimum_hours``, for the weekly maximum ``hours`` and ``maximum_hours``.
    """

    code: str
    employee_id: uuid.UUID | None
    shift_ids: tuple[uuid.UUID, ...]
    values: tuple[tuple[str, str], ...] = ()
