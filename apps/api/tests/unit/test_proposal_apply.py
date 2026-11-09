"""A proposal edit keeps a person's lock when it moves or swaps them (008 R44).

Covers: 008 FR-018. Lock, then move or swap, in the working copy: the person keeps the lock on the
slot they reach; a person newly assigned starts unlocked.
"""

import uuid
from typing import Any

from hds.services import proposal_copy
from hds.services.proposal import apply

ANN, BEN, CAL = (str(uuid.UUID(int=n)) for n in (1, 2, 3))


def work(first: list[str], second: list[str]) -> dict[str, Any]:
    slots = [
        {
            "key": key,
            "shift_id": None,
            "project_id": str(uuid.UUID(int=99)),
            "shift_type_id": str(uuid.UUID(int=98)),
            "date": f"2027-06-0{day}",
            "starts_at": f"2027-06-0{day}T04:00:00+00:00",
            "ends_at": f"2027-06-0{day}T12:00:00+00:00",
            "headcount": 2,
            "assigned": on,
            "kept": [],
        }
        for key, day, on in (("need:a", 6, first), ("need:b", 7, second))
    ]
    return proposal_copy.initial({"slots": slots, "changes": {"added": [], "removed": []}})


def locked(copy: dict[str, Any], key: str, who: str) -> bool:
    slot = proposal_copy.find(copy, key)
    assert slot is not None
    return bool(next(p for p in slot["people"] if p["user_id"] == who).get("locked"))


def test_a_locked_person_moved_keeps_the_lock() -> None:
    copy = work([ANN], [])
    apply(copy, {"op": "lock", "key": "need:a", "user_id": ANN})

    apply(copy, {"op": "move", "key": "need:a", "user_id": ANN, "to_key": "need:b"})

    assert locked(copy, "need:b", ANN)


def test_a_swap_keeps_each_persons_own_lock() -> None:
    copy = work([ANN], [BEN])
    apply(copy, {"op": "lock", "key": "need:a", "user_id": ANN})

    apply(
        copy,
        {"op": "swap", "key": "need:a", "user_id": ANN, "to_key": "need:b", "other_user_id": BEN},
    )

    assert locked(copy, "need:b", ANN)
    assert not locked(copy, "need:a", BEN)


def test_someone_newly_assigned_starts_unlocked() -> None:
    copy = work([ANN], [])

    apply(copy, {"op": "assign", "key": "need:a", "user_id": CAL})

    assert not locked(copy, "need:a", CAL)
