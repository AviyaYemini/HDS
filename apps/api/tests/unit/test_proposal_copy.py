"""A proposal's working copy: change marks, open pairs and parts (008 T007).

Covers: 008 FR-001, FR-003 to FR-006; research P8, P9.
"""

from typing import Any

from hds.services import proposal_copy

ANN, BEN, CAT = (
    "00000000-0000-0000-0000-00000000000a",
    "00000000-0000-0000-0000-00000000000b",
    ("00000000-0000-0000-0000-00000000000c"),
)
PROJECT, OTHER = "00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002"


def slot(key: str, day: str, assigned: list[str], project: str = PROJECT) -> dict[str, Any]:
    return {
        "key": key,
        "shift_id": None,
        "project_id": project,
        "shift_type_id": "00000000-0000-0000-0000-000000000009",
        "date": day,
        "starts_at": f"{day}T04:00:00+00:00",
        "ends_at": f"{day}T12:00:00+00:00",
        "headcount": 2,
        "assigned": assigned,
        "kept": [],
    }


def result() -> dict[str, Any]:
    """Sunday: Ann stays, Ben replaces Cat. Monday: Ann added. Tuesday: Cat removed."""
    return {
        "slots": [
            slot("draft:sun", "2027-06-06", [ANN, BEN]),
            slot("need:mon", "2027-06-07", [ANN]),
            slot("draft:tue", "2027-06-08", [], OTHER),
        ],
        "changes": {
            "added": [
                {"key": "draft:sun", "user_id": BEN},
                {"key": "need:mon", "user_id": ANN},
            ],
            "removed": [
                {"key": "draft:sun", "user_id": CAT},
                {"key": "draft:tue", "user_id": CAT},
            ],
        },
    }


def by_key(work: dict[str, Any], key: str) -> dict[str, Any]:
    found = proposal_copy.find(work, key)
    assert found is not None
    return found


def test_marks_say_what_each_slot_changes() -> None:
    work = proposal_copy.initial(result())

    sunday = proposal_copy.marks(by_key(work, "draft:sun"))
    assert [(m["mark"], m["user_id"], m["replaces"]) for m in sunday] == [
        ("unchanged", ANN, None),
        ("changed", BEN, CAT),
    ]
    assert proposal_copy.counts(work) == {"added": 1, "changed": 1, "removed": 1, "unchanged": 1}
    assert by_key(work, "draft:sun")["run_before"] == sorted([ANN, CAT])


def test_a_part_by_day_project_or_pair_names_only_open_pairs() -> None:
    work = proposal_copy.initial(result())

    assert proposal_copy.selected(work) == {
        "draft:sun": {BEN, CAT},
        "need:mon": {ANN},
        "draft:tue": {CAT},
    }
    assert proposal_copy.selected(work, days=["2027-06-07"]) == {"need:mon": {ANN}}
    assert proposal_copy.selected(work, project_ids=[OTHER]) == {"draft:tue": {CAT}}
    # Ann is not open; Ben replaces Cat, so naming Ben takes Cat's removal too (R15).
    assert proposal_copy.selected(work, pairs=[("draft:sun", BEN), ("draft:sun", ANN)]) == {
        "draft:sun": {BEN, CAT}
    }


def test_accepted_pairs_are_no_longer_open_and_the_proposal_settles() -> None:
    work = proposal_copy.initial(result())
    assert not proposal_copy.any_accepted(work) and not proposal_copy.is_settled(work)

    by_key(work, "need:mon")["stored"] = [ANN]

    assert proposal_copy.any_accepted(work)
    assert proposal_copy.marks(by_key(work, "need:mon"))[0]["accepted"] is True
    for key in ("draft:sun", "draft:tue"):
        s = by_key(work, key)
        s["stored"] = sorted(proposal_copy.people(s))
    assert proposal_copy.is_settled(work)


def test_an_empty_place_is_the_headcount_less_kept_and_placed_people() -> None:
    work = proposal_copy.initial(result())

    assert proposal_copy.places(by_key(work, "need:mon")) == 1
    assert proposal_copy.places(by_key(work, "draft:sun")) == 0
