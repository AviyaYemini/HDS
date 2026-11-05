"""Who may take a slot, against a roster that grows as a solver chooses (004 research G4 to G6).

Layer: scheduling (pure). Standard library only; MUST NOT import anything from the application, a
web framework, a database driver or the network (Constitution III). Both solvers and the reasons for
empty places use this one test, so they cannot disagree about a rule.
"""

import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta

from .problem import (
    MINIMUM_REST,
    NEEDED_ELSEWHERE,
    OVERLAP,
    UNAVAILABLE,
    WEEKLY_MAXIMUM,
    Problem,
    Slot,
)
from .rules import entry_covers, local_day, week_start
from .types import Entry

_BLOCKING = frozenset(("blocked", "absence"))


@dataclass(frozen=True)
class Booking:
    """A shift an employee is on: fixed (never changed by the proposal) or chosen by the solver.

    ``day`` is the local date at the shift's own site it starts on; ``week`` is the Sunday of its
    week in the organisation's zone.
    """

    key: str
    starts_at: datetime
    ends_at: datetime
    day: date
    week: date
    fixed: bool

    @property
    def length(self) -> timedelta:
        """The shift's real elapsed length."""
        return self.ends_at - self.starts_at


def clash(
    a_start: datetime, a_end: datetime, b_start: datetime, b_end: datetime, min_rest: timedelta
) -> str | None:
    """How two shifts of one employee clash: ``OVERLAP``, ``MINIMUM_REST`` or ``None``.

    They overlap when each starts before the other ends; shifts that only touch do not. Otherwise
    the rest is the time from the end of the earlier to the start of the later one.
    """
    if a_start < b_end and b_start < a_end:
        return OVERLAP
    rest = b_start - a_end if b_start >= a_end else a_start - b_end
    return MINIMUM_REST if rest < min_rest else None


class Roster:
    """The fixed shifts of a problem, plus the slots chosen so far, per employee.

    Fixed are the problem's fixed shifts and the people kept on its slots. ``refusal`` says why an
    employee may not take a slot, in the order of research G6: unavailable (a blocked entry or an
    absence covers it); overlap, minimum rest or weekly maximum against a fixed shift; the same
    rules against a chosen slot, which is "needed elsewhere". ``None`` means they may take it.
    """

    def __init__(self, problem: Problem) -> None:
        """Index ``problem``'s entries, fixed shifts and kept people by employee."""
        self._settings = problem.settings
        self._entries: dict[uuid.UUID, list[Entry]] = {}
        for entry in problem.entries:
            self._entries.setdefault(entry.employee_id, []).append(entry)
        self._booked: dict[uuid.UUID, dict[str, Booking]] = {}
        self._weeks: dict[str, date] = {}
        zone = problem.settings.week_zone
        for shift in problem.fixed:
            booking = Booking(
                f"fixed:{shift.id}",
                shift.starts_at,
                shift.ends_at,
                local_day(shift.starts_at, shift.zone),
                week_start(shift.starts_at, zone),
                True,
            )
            for person in shift.assigned:
                self._booked.setdefault(person, {})[booking.key] = booking
        for slot in problem.slots:
            for person in slot.kept:
                self._booked.setdefault(person, {})[slot.key] = self._booking(slot, fixed=True)

    def _week(self, slot: Slot) -> date:
        """The Sunday of ``slot``'s week, worked out once per slot (refusal runs in hot loops)."""
        week = self._weeks.get(slot.key)
        if week is None:
            week = self._weeks[slot.key] = week_start(slot.starts_at, self._settings.week_zone)
        return week

    def _booking(self, slot: Slot, fixed: bool = False) -> Booking:
        return Booking(slot.key, slot.starts_at, slot.ends_at, slot.day, self._week(slot), fixed)

    def book(self, person: uuid.UUID, slot: Slot) -> None:
        """Put ``person`` on ``slot``."""
        self._booked.setdefault(person, {})[slot.key] = self._booking(slot)

    def unbook(self, person: uuid.UUID, slot: Slot) -> None:
        """Take ``person`` off ``slot``."""
        self._booked.get(person, {}).pop(slot.key, None)

    def holds(self, person: uuid.UUID, slot: Slot) -> bool:
        """Whether ``person`` is on ``slot``."""
        return slot.key in self._booked.get(person, {})

    def bookings(self, person: uuid.UUID) -> Iterable[Booking]:
        """Every shift ``person`` is on."""
        return self._booked.get(person, {}).values()

    def unavailable(self, person: uuid.UUID, slot: Slot) -> bool:
        """Whether a blocked entry or an absence of ``person`` covers ``slot``."""
        return any(
            e.kind in _BLOCKING and entry_covers(e, slot.day, slot.shift_type_id)
            for e in self._entries.get(person, ())
        )

    def preference(self, person: uuid.UUID, slot: Slot) -> tuple[bool, bool]:
        """Whether an open entry, and whether a close entry, of ``person`` covers ``slot``."""
        theirs = [
            e
            for e in self._entries.get(person, ())
            if entry_covers(e, slot.day, slot.shift_type_id)
        ]
        return any(e.kind == "open" for e in theirs), any(e.kind == "close" for e in theirs)

    def refusal(self, person: uuid.UUID, slot: Slot) -> str | None:
        """Why ``person`` may not take ``slot`` now, or ``None`` when they may.

        One pass over their bookings: the first rule broken against a fixed one wins, then any rule
        broken only against chosen slots is "needed elsewhere".
        """
        if self.unavailable(person, slot):
            return UNAVAILABLE
        settings = self._settings
        week = self._week(slot)
        fixed_clashes: set[str] = set()
        chosen_clash = False
        fixed_week = chosen_week = timedelta()
        for b in self.bookings(person):
            if b.key == slot.key:
                continue
            found = clash(slot.starts_at, slot.ends_at, b.starts_at, b.ends_at, settings.min_rest)
            if b.week == week:
                if b.fixed:
                    fixed_week += b.length
                else:
                    chosen_week += b.length
            if found is None:
                continue
            if b.fixed:
                fixed_clashes.add(found)
            else:
                chosen_clash = True
        for rule in (OVERLAP, MINIMUM_REST):
            if rule in fixed_clashes:
                return rule
        if fixed_week + slot.length > settings.max_weekly:
            return WEEKLY_MAXIMUM
        if chosen_clash or fixed_week + chosen_week + slot.length > settings.max_weekly:
            return NEEDED_ELSEWHERE
        return None


def fill(
    slots: Sequence[Slot],
    roster: Roster,
    people: Sequence[uuid.UUID],
    chosen: dict[str, list[uuid.UUID]],
    rank: Callable[[uuid.UUID, Slot], tuple[object, ...]],
    worked: dict[uuid.UUID, int],
) -> bool:
    """Fill free places in start order, each with the lowest-``rank`` person who may take it.

    The one place-filling loop of the greedy solver and the repair pass. Each choice is booked on
    ``roster`` and added to ``chosen``, and its minutes to ``worked``.

    Returns:
        Whether any place was filled.
    """
    added = False
    for slot in sorted(slots, key=lambda s: (s.starts_at, s.key)):
        while len(chosen[slot.key]) < slot.places:
            free = [
                p for p in people if not roster.holds(p, slot) and roster.refusal(p, slot) is None
            ]
            if not free:
                break
            pick = min(free, key=lambda p: rank(p, slot))
            roster.book(pick, slot)
            chosen[slot.key].append(pick)
            worked[pick] = worked.get(pick, 0) + slot.minutes
            added = True
    return added


def worked_minutes(
    fixed: Mapping[uuid.UUID, int], slots: Iterable[Slot], chosen: Mapping[str, Sequence[uuid.UUID]]
) -> dict[uuid.UUID, int]:
    """Each person's minutes: the fixed ones in the range plus every slot chosen for them."""
    worked = dict(fixed)
    for slot in slots:
        for person in chosen.get(slot.key, ()):
            worked[person] = worked.get(person, 0) + slot.minutes
    return worked
