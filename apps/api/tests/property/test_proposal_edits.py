"""For any edits to a proposal, undoing them in turn gives back the solver's result (008 T006).

Edits are applied to a working copy with ``hds.services.proposal.apply``; every edit returns the
touched slots' people before it, which is what an undo restores. Valid edits are drawn from the
copy as it stands, so each one fits.

Covers: 008 FR-008, FR-011; research P8; Constitution IV.
"""

import copy
import uuid
from typing import Any

from hypothesis import given, settings
from hypothesis import strategies as st

from hds.services import proposal_copy
from hds.services.proposal import apply

PEOPLE = [str(uuid.UUID(int=i + 1)) for i in range(5)]


def result(layout: list[list[int]]) -> dict[str, Any]:
    """A solver's result with one slot per entry of ``layout``, people by index."""
    return {
        "slots": [
            {
                "key": f"need:{i}",
                "shift_id": None,
                "project_id": str(uuid.UUID(int=99)),
                "shift_type_id": str(uuid.UUID(int=98)),
                "date": f"2027-06-{6 + i:02d}",
                "starts_at": f"2027-06-{6 + i:02d}T04:00:00+00:00",
                "ends_at": f"2027-06-{6 + i:02d}T12:00:00+00:00",
                "headcount": 3,
                "assigned": [PEOPLE[p] for p in sorted(set(on))],
                "kept": [],
            }
            for i, on in enumerate(layout)
        ],
        "unfilled": [],
        "employees": [],
        "no_availability": [],
        "short_published": [],
        "changes": {"added": [], "removed": []},
    }


def edit_for(work: dict[str, Any], choice: int, person: int, other: int) -> dict[str, Any] | None:
    """A valid edit picked by the numbers drawn, or ``None`` when the copy offers none."""
    slots = work["slots"]
    slot = slots[choice % len(slots)]
    on = proposal_copy.people(slot)
    who = PEOPLE[person % len(PEOPLE)]
    kind = ("assign", "unassign", "move", "swap")[choice % 4]
    target = slots[(choice + 1) % len(slots)]
    there = proposal_copy.people(target)
    if kind == "assign" and who not in on:
        return {"op": "assign", "key": slot["key"], "user_id": who}
    if kind == "unassign" and on:
        return {"op": "unassign", "key": slot["key"], "user_id": on[person % len(on)]}
    if kind == "move" and target is not slot and on:
        mover = on[person % len(on)]
        if mover not in there:
            return {"op": "move", "key": slot["key"], "user_id": mover, "to_key": target["key"]}
    if kind == "swap" and target is not slot and on and there:
        a, b = on[person % len(on)], there[other % len(there)]
        if a not in there and b not in on:
            return {
                "op": "swap",
                "key": slot["key"],
                "user_id": a,
                "to_key": target["key"],
                "other_user_id": b,
            }
    return None


layouts = st.lists(st.lists(st.integers(0, 4), max_size=3), min_size=2, max_size=5)
edits = st.lists(st.tuples(st.integers(0, 50), st.integers(0, 50), st.integers(0, 50)), max_size=12)


@settings(max_examples=200, deadline=None)
@given(layouts, edits)
def test_undoing_every_edit_in_turn_gives_back_the_solvers_result(
    layout: list[list[int]], drawn: list[tuple[int, int, int]]
) -> None:
    start = proposal_copy.initial(result(layout))
    work = copy.deepcopy(start)
    history = []
    for choice, person, other in drawn:
        op = edit_for(work, choice, person, other)
        if op is None:
            continue
        before, touched = apply(work, op)
        history.append(before)
        # Every placed person is on the slot the edit says, once.
        for key, placed in touched.items():
            on = proposal_copy.people(proposal_copy.find(work, key) or {})
            assert placed <= set(on) and len(on) == len(set(on))

    for before in reversed(history):
        for key, people in before.items():
            slot = proposal_copy.find(work, key)
            assert slot is not None
            slot["people"] = people

    assert work == start


@settings(max_examples=200, deadline=None)
@given(layouts, edits)
def test_marks_always_account_for_every_person_before_and_after(
    layout: list[list[int]], drawn: list[tuple[int, int, int]]
) -> None:
    work = proposal_copy.initial(result(layout))
    for choice, person, other in drawn:
        op = edit_for(work, choice, person, other)
        if op is not None:
            apply(work, op)
    for slot in work["slots"]:
        marks = proposal_copy.marks(slot)
        now = set(proposal_copy.people(slot))
        before = set(slot["run_before"])
        shown_now = {m["user_id"] for m in marks if m["mark"] != "removed"}
        shown_gone = {m["user_id"] for m in marks if m["mark"] == "removed"} | {
            m["replaces"] for m in marks if m["mark"] == "changed"
        }
        assert shown_now == now and shown_gone == before - now
