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
    """

    id: uuid.UUID
    project_id: uuid.UUID
    shift_type_id: uuid.UUID
    starts_at: datetime
    ends_at: datetime
    zone: str
    headcount: int
    assigned: tuple[uuid.UUID, ...]


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
    """

    employee_id: uuid.UUID
    kind: EntryKind
    starts_on: date
    ends_on: date
    shift_type_id: uuid.UUID | None
    weekday: int | None


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
    ``shift_ids`` holds every shift involved, sorted.
    """

    code: str
    employee_id: uuid.UUID | None
    shift_ids: tuple[uuid.UUID, ...]
