"""Who else could cover a slot of a proposal (008 FR-014, research P11).

Layer: scheduling (pure). Standard library only; MUST NOT import anything from the application, a
web framework, a database driver or the network (Constitution III).

Every active candidate not already on the slot is judged against it with the rules the solvers
use (``Roster.refusal``), over all their shifts in every project: the fixed ones, the people kept
on drafts, and the working copy, which counts as fixed here because it is what would be accepted.
People free come first, ranked by the smallest effect on their wishes and on fairness: no "would
rather not" on it; then the fewest nights, then minutes, then weekend shifts they would have in the
range after taking it; then name. People who would break a rule follow, in the order the rules are
tested, each with the rule and, for rest and the weekly maximum, the numbers. Never pay (FR-016).
"""

import uuid
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime

from .eligibility import Roster
from .problem import MINIMUM_REST, REASON_CODES, WEEKLY_MAXIMUM, Blocked, Problem, Slot
from .problem import minutes as to_minutes
from .rules import local_day, sunday_index

WEEKEND = frozenset((5, 6))
"""Friday and Saturday, counted from Sunday: a weekend shift starts on one of them, at its site."""


@dataclass(frozen=True)
class Option:
    """One person who could take the slot, or the rule that keeps them off it.

    ``code`` is ``None`` when they are free; otherwise one of ``REASON_CODES``, with ``detail`` for
    rest and the weekly maximum. ``close`` and ``open`` say whether a "would rather not" or an "I
    can" covers the slot. ``minutes``, ``nights`` and ``weekends`` are what they would work in the
    range after taking it.
    """

    user_id: uuid.UUID
    code: str | None
    detail: Blocked | None
    close: bool
    open: bool
    minutes: int
    nights: int
    weekends: int


def _weekend(starts_at: datetime, zone: str) -> bool:
    """Whether a shift starting at ``starts_at`` starts on a Friday or Saturday at its site."""
    return sunday_index(local_day(starts_at, zone)) in WEEKEND


def loads(
    problem: Problem, assigned: Mapping[str, Sequence[uuid.UUID]]
) -> tuple[Counter[uuid.UUID], Counter[uuid.UUID], Counter[uuid.UUID]]:
    """Each person's minutes, nights and weekend shifts in the range, the working copy included."""
    worked: Counter[uuid.UUID] = Counter()
    nights: Counter[uuid.UUID] = Counter()
    weekends: Counter[uuid.UUID] = Counter()
    for shift in problem.fixed:
        if not problem.first_day <= local_day(shift.starts_at, shift.zone) <= problem.last_day:
            continue
        for person in shift.assigned:
            starts_at, ends_at = shift.span(person)
            worked[person] += to_minutes(ends_at - starts_at)
            nights[person] += int(shift.shift_type_id in problem.night_types)
            weekends[person] += int(_weekend(shift.starts_at, shift.zone))
    for slot in problem.slots:
        for person in (*slot.kept, *assigned.get(slot.key, ())):
            worked[person] += slot.minutes
            nights[person] += int(slot.night)
            weekends[person] += int(_weekend(slot.starts_at, slot.zone))
    return worked, nights, weekends


def options(
    problem: Problem,
    assigned: Mapping[str, Sequence[uuid.UUID]],
    key: str,
    names: Mapping[uuid.UUID, str] | None = None,
) -> tuple[Option, ...]:
    """Every active candidate not on slot ``key``, free ones first (see the module docstring).

    ``assigned`` is the working copy: the candidates on each slot. ``names`` breaks ties by name;
    without it, by id.

    Raises:
        KeyError: ``key`` is not a slot of ``problem``.
    """
    slots = {s.key: s for s in problem.slots}
    target: Slot = slots[key]
    roster = Roster(problem)
    for slot in problem.slots:
        for person in assigned.get(slot.key, ()):
            roster.book(person, slot, fixed=True)
    worked, nights, weekends = loads(problem, assigned)
    weekend = int(_weekend(target.starts_at, target.zone))
    on = {*target.kept, *assigned.get(key, ())}
    found: list[Option] = []
    for candidate in problem.candidates:
        person = candidate.id
        if not candidate.is_active or person in on:
            continue
        code = roster.refusal(person, target)
        detail = roster.detail(person, target) if code in (MINIMUM_REST, WEEKLY_MAXIMUM) else None
        wants, dislikes = roster.preference(person, target)
        found.append(
            Option(
                user_id=person,
                code=code,
                detail=detail,
                close=dislikes,
                open=wants,
                minutes=worked[person] + target.minutes,
                nights=nights[person] + int(target.night),
                weekends=weekends[person] + weekend,
            )
        )

    def name(o: Option) -> str:
        return (names or {}).get(o.user_id, "") or str(o.user_id)

    def rank(o: Option) -> tuple[object, ...]:
        if o.code is None:
            return (0, o.close, o.nights, o.minutes, o.weekends, name(o), str(o.user_id))
        return (1, REASON_CODES.index(o.code), name(o), str(o.user_id))

    return tuple(sorted(found, key=rank))


def averages(problem: Problem, assigned: Mapping[str, Sequence[uuid.UUID]]) -> tuple[float, ...]:
    """The active candidates' average minutes, nights and weekend shifts in the range, as now.

    What a person's numbers in ``options`` are set against, in words ("2 nights over the average").
    """
    people = [c.id for c in problem.candidates if c.is_active]
    if not people:
        return (0.0, 0.0, 0.0)
    worked, nights, weekends = loads(problem, assigned)
    return tuple(sum(c[p] for p in people) / len(people) for c in (worked, nights, weekends))
