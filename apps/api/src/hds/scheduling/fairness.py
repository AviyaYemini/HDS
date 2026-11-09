"""How fair a proposal is, and what it traded off, in numbers (008 FR-024, FR-025, research P12).

Layer: scheduling (pure). Standard library only; MUST NOT import anything from the application, a
web framework, a database driver or the network (Constitution III).

The server sends numbers; the client words them through the locale files. No score: a manager can
check every figure (AS10, AS11).
"""

import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from .cover import loads
from .eligibility import Roster
from .problem import ChipResult, Problem

NIGHTS_OVER = 2
WEEKENDS_OVER = 2
HOURS_OVER_PERCENT = 20
HOURS_OVER_MINUTES = 4 * 60
"""An outlier works at least 2 nights or 2 weekend shifts over the average, or 20% over the average
hours and at least 4 hours over it (research P12)."""


@dataclass(frozen=True)
class Share:
    """One person's share of the range, and which of it stands out."""

    user_id: uuid.UUID
    minutes: int
    nights: int
    weekends: int
    hours_outlier: bool
    nights_outlier: bool
    weekends_outlier: bool


@dataclass(frozen=True)
class Panel:
    """Every person in the run with their share, and the run's totals for the averages.

    The averages are each total over ``people``; whole numbers, so nothing is rounded here.
    """

    shares: tuple[Share, ...]
    people: int
    minutes: int
    nights: int
    weekends: int


@dataclass(frozen=True)
class Tradeoffs:
    """What the proposal gave up, as counts the summary words (FR-024)."""

    empty_places: int
    open_wishes_missed: int
    close_wishes_given: int
    minutes_low: int
    minutes_high: int
    nights_low: int
    nights_high: int
    chips_not_met: int


def _in_run(problem: Problem, assigned: Mapping[str, Sequence[uuid.UUID]]) -> list[uuid.UUID]:
    """The active candidates, and anyone kept or placed on a slot (locked or placed by hand)."""
    found = {c.id for c in problem.candidates if c.is_active}
    for slot in problem.slots:
        found.update(slot.kept)
        found.update(assigned.get(slot.key, ()))
    return sorted(found, key=str)


def panel(problem: Problem, assigned: Mapping[str, Sequence[uuid.UUID]]) -> Panel:
    """Each person's minutes, nights and weekend shifts in the range, against the run's averages.

    Locked (kept) and overridden assignments count like any other; so do fixed shifts in the
    range, in every project.
    """
    people = _in_run(problem, assigned)
    worked, nights, weekends = loads(problem, assigned)
    count = len(people)
    total_minutes = sum(worked[p] for p in people)
    total_nights = sum(nights[p] for p in people)
    total_weekends = sum(weekends[p] for p in people)
    shares = []
    for p in people:
        # ``x`` over the average ``total / count``, kept in whole numbers: x * count > total.
        hours_out = (
            worked[p] * count * 100 >= total_minutes * (100 + HOURS_OVER_PERCENT)
            and worked[p] * count - total_minutes >= HOURS_OVER_MINUTES * count
        )
        shares.append(
            Share(
                user_id=p,
                minutes=worked[p],
                nights=nights[p],
                weekends=weekends[p],
                hours_outlier=count > 0 and hours_out,
                nights_outlier=count > 0
                and nights[p] * count - total_nights >= NIGHTS_OVER * count,
                weekends_outlier=count > 0
                and weekends[p] * count - total_weekends >= WEEKENDS_OVER * count,
            )
        )
    return Panel(tuple(shares), count, total_minutes, total_nights, total_weekends)


def tradeoffs(
    problem: Problem,
    assigned: Mapping[str, Sequence[uuid.UUID]],
    chips: Sequence[ChipResult] = (),
    shared: Panel | None = None,
) -> Tradeoffs:
    """The counts the summary words: empty places, wishes, the spreads, and chips not met.

    ``open_wishes_missed`` counts the people whose "I can" covered a slot of the run they could
    take but who got none of them; ``close_wishes_given``, the places given on a "would rather
    not".
    """
    roster = Roster(problem)
    empty = sum(max(0, s.places - len(assigned.get(s.key, ()))) for s in problem.slots)
    given: dict[uuid.UUID, set[str]] = {}
    for slot in problem.slots:
        for p in (*slot.kept, *assigned.get(slot.key, ())):
            given.setdefault(p, set()).add(slot.key)
    missed = 0
    close_given = 0
    for c in problem.candidates:
        if not c.is_active:
            continue
        wanted = [
            s
            for s in problem.slots
            if roster.preference(c.id, s)[0] and roster.refusal(c.id, s) is None
        ]
        if wanted and not any(s.key in given.get(c.id, set()) for s in wanted):
            missed += 1
        close_given += sum(
            1
            for s in problem.slots
            if s.key in given.get(c.id, set()) and roster.preference(c.id, s)[1]
        )
    # The panel the caller has already worked out, or worked out here (R46).
    shares = (shared or panel(problem, assigned)).shares
    minutes = [s.minutes for s in shares] or [0]
    nights = [s.nights for s in shares] or [0]
    return Tradeoffs(
        empty_places=empty,
        open_wishes_missed=missed,
        close_wishes_given=close_given,
        minutes_low=min(minutes),
        minutes_high=max(minutes),
        nights_low=min(nights),
        nights_high=max(nights),
        chips_not_met=sum(1 for r in chips if r.state == "not_met"),
    )
