"""The manager's preferences for a run, judged (008 FR-020 to FR-022, research P5).

Layer: scheduling (pure). Standard library only; MUST NOT import anything from the application, a
web framework, a database driver or the network (Constitution III).

A chip is about one person. Caps count what they would work in the range: their fixed shifts there,
the places they are kept on, and the proposal's. ``keep_on`` and ``keep_off`` judge each slot they
are given; ``prefer`` is met when they get at least one slot it names, or none they may take
exists. The solvers weigh chips through ``matches`` and ``cap_of``; ``judge`` says, after solving,
whether each was met and, when not, what prevented it.
"""

import uuid
from collections.abc import Mapping, Sequence

from .eligibility import Roster
from .problem import Chip, ChipResult, Problem, Slot
from .problem import minutes as to_minutes
from .rules import local_day, sunday_index

CAPS = ("max_shifts", "max_nights", "max_hours")


def applies(chip: Chip, problem: Problem) -> bool:
    """Whether ``chip`` still applies: its person is an active candidate, its project in the run.

    A chip that no longer applies is reported as such and never fails the run (FR-022).
    """
    if not any(c.id == chip.user_id and c.is_active for c in problem.candidates):
        return False
    if chip.kind == "keep_on" and chip.project_id is not None:
        return any(s.project_id == chip.project_id for s in problem.slots)
    if chip.kind == "keep_on" and chip.site_id is not None:
        return any(s.site_id == chip.site_id for s in problem.slots)
    return True


def matches(chip: Chip, slot: Slot) -> bool:
    """Whether ``slot`` is what a ``keep_on``, ``keep_off`` or ``prefer`` chip names."""
    if chip.kind == "keep_on":
        if chip.project_id is not None:
            return slot.project_id == chip.project_id
        return chip.site_id is not None and slot.site_id == chip.site_id
    if chip.weekday is not None and sunday_index(slot.day) != chip.weekday:
        return False
    if chip.shift_type_id is not None and slot.shift_type_id != chip.shift_type_id:
        return False
    return chip.weekday is not None or chip.shift_type_id is not None


def unit(chip: Chip, slot: Slot) -> int:
    """What one ``slot`` adds toward a cap: a shift, a night, or its minutes."""
    if chip.kind == "max_shifts":
        return 1
    if chip.kind == "max_nights":
        return int(slot.night)
    return slot.minutes


def cap_of(chip: Chip) -> int:
    """A cap's limit in its own unit: shifts, nights, or minutes."""
    count = chip.count or 0
    return count * 60 if chip.kind == "max_hours" else count


def fixed_load(problem: Problem, chip: Chip) -> int:
    """What the chip's person already works toward its cap: fixed shifts in range, kept places."""
    total = 0
    for shift in problem.fixed:
        if chip.user_id not in shift.assigned:
            continue
        if not problem.first_day <= local_day(shift.starts_at, shift.zone) <= problem.last_day:
            continue
        starts_at, ends_at = shift.span(chip.user_id)
        if chip.kind == "max_shifts":
            total += 1
        elif chip.kind == "max_nights":
            total += int(shift.shift_type_id in problem.night_types)
        else:
            total += to_minutes(ends_at - starts_at)
    for slot in problem.slots:
        if chip.user_id in slot.kept:
            total += unit(chip, slot)
    return total


def _contradicts(a: Chip, b: Chip) -> bool:
    """Whether two chips about one person pull opposite ways."""
    if a.user_id != b.user_id or a is b:
        return False
    kinds = {a.kind, b.kind}
    if kinds == {"keep_off", "prefer"}:
        return (a.weekday, a.shift_type_id) == (b.weekday, b.shift_type_id)
    if kinds == {"keep_on"}:
        return (a.project_id, a.site_id) != (b.project_id, b.site_id)
    if "prefer" in kinds and kinds & set(CAPS):
        cap = a if a.kind in CAPS else b
        return cap_of(cap) == 0
    return False


def judge(
    problem: Problem, roster: Roster, assigned: Mapping[str, Sequence[uuid.UUID]]
) -> tuple[ChipResult, ...]:
    """Each chip of ``problem``: met, not met (with its cause), or no longer applying.

    ``roster`` holds the fixed shifts only, so it says which slots a person could take at all.
    """
    by_key = {s.key: s for s in problem.slots}
    theirs: dict[uuid.UUID, list[Slot]] = {}
    for key, on in assigned.items():
        for person in on:
            theirs.setdefault(person, []).append(by_key[key])
    live = [c for c in problem.chips if applies(c, problem)]
    found: list[ChipResult] = []
    for index, chip in enumerate(problem.chips):
        if chip not in live:
            found.append(ChipResult(index, "not_applying"))
            continue
        given = theirs.get(chip.user_id, [])
        if chip.kind in CAPS:
            met = fixed_load(problem, chip) + sum(unit(chip, s) for s in given) <= cap_of(chip)
        elif chip.kind == "keep_on":
            met = all(matches(chip, s) for s in given)
        elif chip.kind == "keep_off":
            met = not any(matches(chip, s) for s in given)
        else:
            possible = [
                s
                for s in problem.slots
                if matches(chip, s) and roster.refusal(chip.user_id, s) is None
            ]
            met = any(matches(chip, s) for s in given) or not possible
        if met:
            found.append(ChipResult(index, "met"))
        elif chip.kind in CAPS and fixed_load(problem, chip) > cap_of(chip):
            found.append(ChipResult(index, "not_met", "fixed"))
        elif any(_contradicts(chip, other) for other in live):
            found.append(ChipResult(index, "not_met", "conflict"))
        else:
            found.append(ChipResult(index, "not_met", "coverage"))
    return tuple(found)
