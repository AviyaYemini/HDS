"""The roster rules that judge a change by hand, and local shift times as instants.

Layer: scheduling (pure). Standard library only; MUST NOT import anything from the application, a
web framework, a database driver or the network (Constitution III). Every rule a roster is judged by
lives here, so the future generator applies the same ones (research R1). A manual change is never
refused for a rule: callers report the warnings and keep the change (003 FR-116).
"""

import uuid
from collections.abc import Collection, Iterable, Mapping
from dataclasses import replace
from datetime import UTC, date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from .types import Employee, Entry, RuleWarning, Settings, ShiftSlot

# Entry kinds that make an assignment worth a warning, and the code each reports. ``open`` is a
# wish to work the shift, so it reports nothing.
_ENTRY_CODES = {
    "blocked": "warnings.blocked",
    "absence": "warnings.absence",
    "close": "warnings.close_preference",
}
_KINDS = frozenset(("blocked", "open", "close", "absence"))
# A block that stands for a missed availability deadline reports its own code (007 FR-014).
_NOT_SUBMITTED_CODE = "warnings.not_submitted"


def local_span(
    day: date, start: time, end: time, crosses_midnight: bool, zone: str
) -> tuple[datetime, datetime]:
    """Return the UTC instants of a shift that runs from ``start`` to ``end`` local time on ``day``.

    A shift that crosses midnight ends on the next day. A local time that does not exist (the hour
    skipped when clocks go forward) moves forward by the gap; one that happens twice (clocks going
    back) takes its first occurrence. Lengths are then real elapsed time (research R4).

    Args:
        day: The local date the shift starts on.
        start: Local start time.
        end: Local end time.
        crosses_midnight: Whether the shift ends on the next day.
        zone: The site's IANA time zone.

    Returns:
        ``(starts_at, ends_at)`` as aware UTC datetimes.

    Raises:
        ValueError: The span would not end after it starts: an end before the start without
            crossing midnight, or an end that a skipped hour leaves before the moved start.
    """
    tz = ZoneInfo(zone)
    end_day = day + timedelta(days=1) if crosses_midnight else day
    # fold=0 reads a skipped time with the offset in force before the change, which lands after
    # the gap, and an ambiguous time as its first occurrence.
    starts_at = datetime.combine(day, start, tzinfo=tz).astimezone(UTC)
    ends_at = datetime.combine(end_day, end, tzinfo=tz).astimezone(UTC)
    if ends_at <= starts_at:
        raise ValueError(
            f"a shift must end after it starts: {start} to {end} on {day} in {zone} gives "
            f"{starts_at.isoformat()} to {ends_at.isoformat()}"
        )
    return starts_at, ends_at


def validate(
    *,
    focus: Collection[uuid.UUID],
    shifts: Iterable[ShiftSlot],
    entries: Iterable[Entry],
    employees: Mapping[uuid.UUID, Employee],
    settings: Settings,
) -> list[RuleWarning]:
    """Report every rule the shifts in ``focus`` break, with the shifts around them.

    Only warnings that involve a shift in ``focus`` are reported; two other shifts that clash are
    not. For each employee on a focus shift: overlap with any of their shifts; rest below
    ``settings.min_rest`` between two consecutive shifts of theirs; hours above
    ``settings.max_weekly`` in a week that holds a focus shift; a blocked entry, absence or close
    preference covering the shift; and being no longer active. For each focus shift: more or fewer
    people than its headcount.

    Args:
        focus: Ids of the shifts being created or changed.
        shifts: Those shifts, as they will be after the change, and every other shift of their
            employees that could clash, across all projects and including drafts (003 FR-117).
            Each id appears once: pass a changed shift's new version, not its stored one too.
        entries: Availability entries of those employees for the dates involved.
        employees: Every employee assigned to any of ``shifts``, by id.
        settings: The organisation's limits.

    Returns:
        The warnings, each once, in a stable order (by code, then employee, then shifts).

    Raises:
        ValueError: A shift id appears more than once, or an entry has an unknown kind.
        KeyError: An assigned employee is missing from ``employees``, so whether they are still
            active cannot be judged.
    """
    focus = frozenset(focus)
    all_shifts = list(shifts)
    seen: set[uuid.UUID] = set()
    for slot in all_shifts:
        if slot.id in seen:
            raise ValueError(f"shift {slot.id} is given more than once")
        seen.add(slot.id)
    by_employee: dict[uuid.UUID, list[ShiftSlot]] = {}
    for slot in all_shifts:
        for person in slot.assigned:
            by_employee.setdefault(person, []).append(_as_worked_by(slot, person))
    entries_by_employee: dict[uuid.UUID, list[Entry]] = {}
    for entry in entries:
        if entry.kind not in _KINDS:
            raise ValueError(f"unknown availability entry kind {entry.kind!r}")
        entries_by_employee.setdefault(entry.employee_id, []).append(entry)
    original = {slot.id: slot for slot in all_shifts}
    # A shift counts in the week of the date it starts, whoever works it and for how long.
    week_of = {slot.id: week_start(slot.starts_at, settings.week_zone) for slot in all_shifts}

    found: set[RuleWarning] = set()
    for person, theirs in by_employee.items():
        if not any(slot.id in focus for slot in theirs):
            continue
        employee = employees[person]
        found |= _overlaps(person, theirs, focus)
        found |= _short_rests(person, theirs, focus, settings.min_rest)
        for slot in theirs:
            if slot.id not in focus:
                continue
            found |= _week(slot, person, theirs, week_of, settings.max_weekly)
            found |= _entries(original[slot.id], person, entries_by_employee.get(person, ()))
            if not employee.is_active:
                found.add(RuleWarning("warnings.employee_inactive", person, (slot.id,)))
    for slot in all_shifts:
        if slot.id in focus:
            found |= _headcount(slot)
    return sorted(found, key=lambda w: (w.code, str(w.employee_id), [str(i) for i in w.shift_ids]))


def _as_worked_by(slot: ShiftSlot, person: uuid.UUID) -> ShiftSlot:
    """``slot`` with the times ``person`` works it, for the rules judged per person.

    Only rest, overlap and weekly hours read these times; availability entries are matched by the
    shift's own start, and the week by the shift's own date.
    """
    starts_at, ends_at = slot.span(person)
    if (starts_at, ends_at) == (slot.starts_at, slot.ends_at):
        return slot
    return replace(slot, starts_at=starts_at, ends_at=ends_at, own_times=())


def _hours(span: timedelta) -> str:
    """``span`` in hours, as a screen shows it: whole hours bare, otherwise one decimal place."""
    tenths = round(span.total_seconds() / 360)
    return str(tenths // 10) if tenths % 10 == 0 else f"{tenths / 10:.1f}"


def _pair(code: str, person: uuid.UUID, a: ShiftSlot, b: ShiftSlot) -> RuleWarning:
    """A warning about two shifts, with their ids sorted so a pair is reported once."""
    return RuleWarning(code, person, tuple(sorted((a.id, b.id), key=str)))


def _overlaps(
    person: uuid.UUID, theirs: list[ShiftSlot], focus: frozenset[uuid.UUID]
) -> set[RuleWarning]:
    """Pairs of the employee's shifts, one of them in focus, that overlap.

    Two shifts overlap when each starts before the other ends; shifts that only touch do not.
    """
    found: set[RuleWarning] = set()
    for i, a in enumerate(theirs):
        for b in theirs[i + 1 :]:
            if a.id not in focus and b.id not in focus:
                continue
            if a.starts_at < b.ends_at and b.starts_at < a.ends_at:
                found.add(_pair("warnings.overlap", person, a, b))
    return found


def _short_rests(
    person: uuid.UUID, theirs: list[ShiftSlot], focus: frozenset[uuid.UUID], min_rest: timedelta
) -> set[RuleWarning]:
    """Rest below ``min_rest`` between a shift and the next one, one of them in focus.

    The next shift is the earliest that starts after this one ends, and only when no other shift
    of the employee starts in between: rest is judged between neighbours, so a shift two places
    later is not reported again.
    """
    found: set[RuleWarning] = set()
    for a in theirs:
        later = [b for b in theirs if b.starts_at >= a.ends_at]
        if not later:
            continue
        nearest = min(b.starts_at for b in later)
        if any(a.starts_at < b.starts_at < nearest for b in theirs if b is not a):
            continue
        for b in later:
            if b.starts_at != nearest or (a.id not in focus and b.id not in focus):
                continue
            if b.starts_at - a.ends_at < min_rest:
                pair = _pair("warnings.rest_too_short", person, a, b)
                values = (
                    ("rest_hours", _hours(b.starts_at - a.ends_at)),
                    ("minimum_hours", _hours(min_rest)),
                )
                found.add(RuleWarning(pair.code, pair.employee_id, pair.shift_ids, values))
    return found


def sunday_index(day: date) -> int:
    """The weekday of ``day`` counted from Sunday (0) to Saturday (6), the Israeli week."""
    return (day.weekday() + 1) % 7


def local_day(moment: datetime, zone: str) -> date:
    """The local date of ``moment`` in ``zone``: the one reading of "which day a shift is on"."""
    return moment.astimezone(ZoneInfo(zone)).date()


def week_start(moment: datetime, zone: str) -> date:
    """The Sunday that starts the week ``moment`` falls in, in ``zone`` (research R2)."""
    day = local_day(moment, zone)
    return day - timedelta(days=sunday_index(day))


def _week(
    slot: ShiftSlot,
    person: uuid.UUID,
    theirs: list[ShiftSlot],
    week_of: Mapping[uuid.UUID, date],
    max_weekly: timedelta,
) -> set[RuleWarning]:
    """Hours above the weekly maximum in the week ``slot`` starts in.

    A shift counts in full in the week it starts, as a night shift belongs to the date it starts.
    """
    same_week = [s for s in theirs if week_of[s.id] == week_of[slot.id]]
    total = sum((s.ends_at - s.starts_at for s in same_week), timedelta())
    if total <= max_weekly:
        return set()
    ids = tuple(sorted((s.id for s in same_week), key=str))
    values = (("hours", _hours(total)), ("maximum_hours", _hours(max_weekly)))
    return {RuleWarning("warnings.weekly_hours_exceeded", person, ids, values)}


def _entries(slot: ShiftSlot, person: uuid.UUID, theirs: Iterable[Entry]) -> set[RuleWarning]:
    """Blocked entries, absences and close preferences that cover ``slot``.

    An entry covers a shift when the shift's local start date is in its range, and its shift type
    and weekday, when set, match the shift's.
    """
    day = local_day(slot.starts_at, slot.zone)
    found: set[RuleWarning] = set()
    for entry in theirs:
        code = _ENTRY_CODES.get(entry.kind)
        if entry.kind == "blocked" and entry.reason == "not_submitted":
            code = _NOT_SUBMITTED_CODE
        if code is not None and entry_covers(entry, day, slot.shift_type_id):
            found.add(RuleWarning(code, person, (slot.id,)))
    return found


def entry_covers(entry: Entry, day: date, shift_type_id: uuid.UUID) -> bool:
    """Whether ``entry`` covers a shift of ``shift_type_id`` that starts on the local date ``day``.

    The date must be in the entry's range, and its shift type and weekday, when set, must match.
    Used by ``validate`` and by the generator, so both read an entry the same way.
    """
    if not entry.starts_on <= day <= entry.ends_on:
        return False
    if entry.shift_type_id is not None and entry.shift_type_id != shift_type_id:
        return False
    return entry.weekday is None or entry.weekday == sunday_index(day)


def _headcount(slot: ShiftSlot) -> set[RuleWarning]:
    """More or fewer people on ``slot`` than its headcount; it concerns no one employee."""
    if len(slot.assigned) > slot.headcount:
        return {RuleWarning("warnings.over_headcount", None, (slot.id,))}
    if len(slot.assigned) < slot.headcount:
        return {RuleWarning("warnings.under_headcount", None, (slot.id,))}
    return set()
