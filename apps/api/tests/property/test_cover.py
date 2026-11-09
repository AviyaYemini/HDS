"""For any roster and working copy, "who else could cover" agrees with the rules (008 T021).

Each option is judged again by ``validate`` with the person placed on the slot, over every shift:
someone marked free breaks no rule of theirs there, and someone not free breaks exactly the rule the
option names. Every active candidate not on the slot is an option once, free ones first.

Covers: 008 FR-014, SC-004; research P11; Constitution IV. Examples: ``HDS_GENERATE_EXAMPLES`` (200
by default, 1000 in CI).
"""

import os
import uuid
from typing import Any

from hypothesis import given, settings
from hypothesis import strategies as st

from hds.scheduling import Employee, ShiftSlot, validate
from hds.scheduling.cover import options
from hds.scheduling.problem import (
    MINIMUM_REST,
    OVERLAP,
    UNAVAILABLE,
    WEEKLY_MAXIMUM,
    Problem,
    Slot,
)
from tests.unit.scheduling.factory import day, entry, fixed, person, problem, slot

PEOPLE = [person(n) for n in range(1, 5)]
FIXED_PROJECT = uuid.UUID(int=3)
KINDS = ["morning", "afternoon", "night"]
# The rule each refusal stands for, as validate names it.
RULES = {
    UNAVAILABLE: {"warnings.blocked", "warnings.absence"},
    OVERLAP: {"warnings.overlap"},
    MINIMUM_REST: {"warnings.rest_too_short"},
    WEEKLY_MAXIMUM: {"warnings.weekly_hours_exceeded"},
}
HARD = set().union(*RULES.values())

EXAMPLES = int(os.environ.get("HDS_GENERATE_EXAMPLES", "200"))

places = st.tuples(st.integers(0, 6), st.sampled_from(KINDS))


@st.composite
def cases(draw: Any) -> tuple[Problem, dict[str, tuple[uuid.UUID, ...]], str]:
    """A small roster: slots, fixed shifts in another project, entries, and a working copy."""
    wanted = draw(st.lists(places, min_size=1, max_size=5, unique=True))
    slots = [slot(day(d), kind, headcount=draw(st.integers(1, 2))) for d, kind in wanted]
    taken = draw(st.lists(places, max_size=6, unique=True))
    shifts = [
        fixed(
            day(d),
            kind,
            draw(st.lists(st.sampled_from(PEOPLE), min_size=1, max_size=2, unique=True)),
            FIXED_PROJECT,
        )
        for d, kind in taken
    ]
    entries = [
        entry(who, kind, day(d), shift=draw(st.sampled_from([None, *KINDS])))
        for who, kind, d in draw(
            st.lists(
                st.tuples(
                    st.sampled_from(PEOPLE),
                    st.sampled_from(["blocked", "absence", "close", "open"]),
                    st.integers(0, 6),
                ),
                max_size=4,
            )
        )
    ]
    assigned = {
        s.key: tuple(
            p.id for p in draw(st.lists(st.sampled_from(PEOPLE), max_size=s.headcount, unique=True))
        )
        for s in slots
    }
    target = draw(st.sampled_from(slots)).key
    return problem(slots, PEOPLE, entries=entries, fixed_shifts=shifts), assigned, target


def as_shift(s: Slot, on: tuple[uuid.UUID, ...]) -> ShiftSlot:
    return ShiftSlot(
        id=uuid.uuid5(uuid.NAMESPACE_URL, s.key),
        project_id=s.project_id,
        shift_type_id=s.shift_type_id,
        starts_at=s.starts_at,
        ends_at=s.ends_at,
        zone=s.zone,
        headcount=s.headcount,
        assigned=on,
    )


def breaks(
    made: Problem, assigned: dict[str, tuple[uuid.UUID, ...]], key: str, who: uuid.UUID
) -> set[str]:
    """The hard rules ``who`` breaks on ``key`` once placed there, by ``validate``."""
    shifts = [
        as_shift(s, (*assigned.get(s.key, ()), *((who,) if s.key == key else ())))
        for s in made.slots
    ]
    found = validate(
        focus={uuid.uuid5(uuid.NAMESPACE_URL, key)},
        shifts=[*made.fixed, *shifts],
        entries=made.entries,
        employees={p.id: Employee(p.id, True) for p in PEOPLE},
        settings=made.settings,
    )
    return {w.code for w in found if w.employee_id == who and w.code in HARD}


@settings(max_examples=EXAMPLES, deadline=None)
@given(cases())
def test_free_breaks_no_rule_and_not_free_names_the_rule_it_breaks(
    case: tuple[Problem, dict[str, tuple[uuid.UUID, ...]], str],
) -> None:
    made, assigned, key = case
    found = options(made, assigned, key)

    on = set(assigned.get(key, ()))
    assert sorted(str(o.user_id) for o in found) == sorted(
        str(p.id) for p in PEOPLE if p.id not in on
    )
    frees = [o.code is None for o in found]
    assert frees == sorted(frees, reverse=True), "free people come first"
    for option in found:
        broken = breaks(made, assigned, key, option.user_id)
        if option.code is None:
            assert broken == set(), option
        else:
            assert broken & RULES[option.code], (option, broken)
