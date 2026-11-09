"""The greedy baseline: HDS 1.0's slot-by-slot assignment with its flaws fixed (004 research G5).

Layer: scheduling (pure). Standard library only; MUST NOT import anything from the application, a
web framework, a database driver or the network (Constitution III). The HDS 1.0 code is not in this
repository; this is rebuilt from the legacy audit (docs/hds-spec.md section 3). Rest is real time,
a close preference is a preference, and hours, not shift counts, are balanced. It is the benchmark
for the CP-SAT model and never claims to be optimal.
"""

import uuid
from collections.abc import Mapping, Sequence

from . import chips
from .eligibility import Roster, fill
from .problem import Problem, Slot


def solve(
    problem: Problem,
    roster: Roster,
    people: Sequence[uuid.UUID],
    minutes: Mapping[uuid.UUID, int],
    nights: Mapping[uuid.UUID, int],
) -> dict[str, list[uuid.UUID]]:
    """Fill the slots in start order, one place at a time, booking choices on ``roster``.

    Each place goes to the candidate who may take it with, in order: the manager's chips least
    against them (008 FR-021); holding it in the starting point when "Keep changes small"; then
    no close preference and an open preference, and the fewest minutes and nights so far, wishes
    first when "strictly" and balance first when "gently" (008 FR-017); holding it in the starting
    point when "Rebalance freely" (today's order); then the smallest id.

    Args:
        problem: The problem.
        roster: The fixed shifts; the choices are booked on it.
        people: The active candidates.
        minutes: Each candidate's minutes already worked in the range (fixed shifts).
        nights: Each candidate's nights already worked in the range.

    Returns:
        The people chosen for each slot, by slot key.
    """
    worked = dict(minutes)
    night_count = dict(nights)
    chosen: dict[str, list[uuid.UUID]] = {s.key: [] for s in problem.slots}

    controls = problem.controls
    live = [c for c in problem.chips if chips.applies(c, problem)]
    load = {i: chips.fixed_load(problem, c) for i, c in enumerate(live) if c.kind in chips.CAPS}

    def against(p: uuid.UUID, slot: Slot) -> int:
        """How far placing ``p`` on ``slot`` goes against their chips (a prefer counts -1)."""
        cost = 0
        for i, chip in enumerate(live):
            if chip.user_id != p:
                continue
            if chip.kind in chips.CAPS:
                cost += int(load[i] + chips.unit(chip, slot) > chips.cap_of(chip))
            elif chip.kind == "keep_on":
                cost += int(not chips.matches(chip, slot))
            elif chip.kind == "keep_off":
                cost += int(chips.matches(chip, slot))
            else:
                cost -= int(chips.matches(chip, slot))
        return cost

    def rank(p: uuid.UUID, slot: Slot) -> tuple[object, ...]:
        wants, dislikes = roster.preference(p, slot)
        holder = p in problem.starting.get(slot.key, ())
        wishes = (dislikes, not wants)
        balance = (worked.get(p, 0), night_count.get(p, 0))
        middle = wishes + balance if controls.wishes_strictly else balance + wishes
        if controls.keep_changes_small:
            return (against(p, slot), not holder, *middle, str(p))
        return (against(p, slot), *middle, not holder, str(p))

    # One slot at a time, so the night count and the caps move with each choice as minutes do.
    for slot in sorted(problem.slots, key=lambda s: (s.starts_at, s.key)):
        before = len(chosen[slot.key])
        fill([slot], roster, people, chosen, rank, worked)
        for p in chosen[slot.key][before:]:
            if slot.night:
                night_count[p] = night_count.get(p, 0) + 1
            for i, chip in enumerate(live):
                if chip.user_id == p and i in load:
                    load[i] += chips.unit(chip, slot)
    return chosen
