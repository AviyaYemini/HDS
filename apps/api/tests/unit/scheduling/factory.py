"""Building small roster problems for the generation tests (004 Group B).

Shifts are made from three shift types at a site in Israel, as 003's defaults: morning 07:00 to
15:00, afternoon 15:00 to 23:00 and night 23:00 to 07:00 (crossing midnight).
"""

import uuid
from collections.abc import Iterable, Mapping
from datetime import date, time, timedelta

from hds.scheduling import Employee, Entry, Settings, ShiftSlot, local_span
from hds.scheduling.problem import Problem, Slot

ZONE = "Asia/Jerusalem"
SETTINGS = Settings(min_rest=timedelta(hours=11), max_weekly=timedelta(hours=48), week_zone=ZONE)
PROJECT = uuid.UUID(int=1)
OTHER_PROJECT = uuid.UUID(int=2)
TYPES = {
    "morning": (uuid.UUID(int=11), time(7), time(15), False),
    "afternoon": (uuid.UUID(int=12), time(15), time(23), False),
    "night": (uuid.UUID(int=13), time(23), time(7), True),
}
NIGHTS = frozenset({TYPES["night"][0]})
SUNDAY = date(2027, 6, 6)


def day(n: int) -> date:
    """The ``n``-th day after Sunday 2027-06-06."""
    return SUNDAY + timedelta(days=n)


def person(n: int, active: bool = True) -> Employee:
    """Employee number ``n``."""
    return Employee(id=uuid.UUID(int=1000 + n), is_active=active)


def slot(
    when: date,
    kind: str = "morning",
    project: uuid.UUID = PROJECT,
    headcount: int = 1,
    shift_id: uuid.UUID | None = None,
) -> Slot:
    """A slot of shift type ``kind`` on ``when``."""
    type_id, start, end, crosses = TYPES[kind]
    starts_at, ends_at = local_span(when, start, end, crosses, ZONE)
    return Slot(
        key=f"{project}:{type_id}:{when.isoformat()}",
        shift_id=shift_id,
        project_id=project,
        shift_type_id=type_id,
        starts_at=starts_at,
        ends_at=ends_at,
        zone=ZONE,
        headcount=headcount,
        night=crosses,
    )


def fixed(
    when: date, kind: str, people: Iterable[Employee], project: uuid.UUID = OTHER_PROJECT
) -> ShiftSlot:
    """A fixed shift (published, or in the surroundings) with ``people`` on it."""
    made = slot(when, kind, project)
    return ShiftSlot(
        id=uuid.uuid5(uuid.NAMESPACE_URL, made.key),
        project_id=project,
        shift_type_id=made.shift_type_id,
        starts_at=made.starts_at,
        ends_at=made.ends_at,
        zone=ZONE,
        headcount=len(tuple(people)) or 1,
        assigned=tuple(p.id for p in people),
    )


def entry(
    who: Employee,
    kind: str,
    first: date,
    last: date | None = None,
    shift: str | None = None,
    weekday: int | None = None,
) -> Entry:
    """An availability entry of ``who``."""
    return Entry(
        employee_id=who.id,
        kind=kind,  # type: ignore[arg-type]
        starts_on=first,
        ends_on=last or first,
        shift_type_id=TYPES[shift][0] if shift else None,
        weekday=weekday,
    )


def problem(
    slots: Iterable[Slot],
    candidates: Iterable[Employee],
    entries: Iterable[Entry] = (),
    fixed_shifts: Iterable[ShiftSlot] = (),
    current: Mapping[str, tuple[uuid.UUID, ...]] | None = None,
    settings: Settings = SETTINGS,
) -> Problem:
    """A problem over ``slots``; the range is the slots' first to last day."""
    made = tuple(slots)
    days = [s.day for s in made] or [SUNDAY]
    return Problem(
        slots=made,
        fixed=tuple(fixed_shifts),
        candidates=tuple(candidates),
        entries=tuple(entries),
        settings=settings,
        first_day=min(days),
        last_day=max(days),
        current=dict(current or {}),
        night_types=NIGHTS,
    )
