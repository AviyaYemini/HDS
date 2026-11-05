"""The greedy baseline: HDS 1.0's slot-by-slot assignment with its flaws fixed (004 research G5).

Layer: scheduling (pure). Standard library only; MUST NOT import anything from the application, a
web framework, a database driver or the network (Constitution III). The HDS 1.0 code is not in this
repository; this is rebuilt from the legacy audit (docs/hds-spec.md section 3). Rest is real time,
a close preference is a preference, and hours, not shift counts, are balanced. It is the benchmark
for the CP-SAT model and never claims to be optimal.
"""

import uuid
from collections.abc import Mapping, Sequence

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

    Each place goes to the candidate who may take it with, in order: no close preference, an open
    preference, the fewest minutes so far, the fewest nights so far, holding it in the current
    drafts, then the smallest id.

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

    def rank(p: uuid.UUID, slot: Slot) -> tuple[object, ...]:
        wants, dislikes = roster.preference(p, slot)
        holder = p in problem.current.get(slot.key, ())
        return (dislikes, not wants, worked.get(p, 0), night_count.get(p, 0), not holder, str(p))

    # One slot at a time, so the night count moves with each choice as the minutes do.
    for slot in sorted(problem.slots, key=lambda s: (s.starts_at, s.key)):
        before = len(chosen[slot.key])
        fill([slot], roster, people, chosen, rank, worked)
        if slot.night:
            for p in chosen[slot.key][before:]:
                night_count[p] = night_count.get(p, 0) + 1
    return chosen
