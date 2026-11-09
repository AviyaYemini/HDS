"""Who else could cover a slot: who is listed, in which order, with which numbers (008 T025).

Covers: 008 FR-014, FR-016; research P11. The rules themselves are checked against ``validate`` in
tests/property/test_cover.py.
"""

from hds.scheduling.cover import loads, options, totals
from hds.scheduling.problem import MINIMUM_REST, OVERLAP, UNAVAILABLE, Blocked
from tests.unit.scheduling.factory import OTHER_PROJECT, day, entry, fixed, person, problem, slot

ANN, BEN, CAL, DAN = person(1), person(2), person(3), person(4)


def order(found: tuple[object, ...]) -> list[object]:
    return [o.user_id for o in found]  # type: ignore[attr-defined]


def test_inactive_people_and_those_already_on_it_are_not_listed() -> None:
    morning = slot(day(2), headcount=2)
    made = problem([morning], [ANN, BEN, person(9, active=False)])

    found = options(made, {morning.key: (ANN.id,)}, morning.key)

    assert order(found) == [BEN.id]


def test_free_people_come_first_then_each_rule_in_the_order_rules_are_tested() -> None:
    morning = slot(day(2))
    made = problem(
        [morning],
        [ANN, BEN, CAL, DAN],
        entries=[entry(BEN, "blocked", day(2))],
        fixed_shifts=[fixed(day(2), "morning", [CAL]), fixed(day(1), "night", [DAN])],
    )

    found = options(made, {}, morning.key)

    assert [(o.user_id, o.code) for o in found] == [
        (ANN.id, None),
        (BEN.id, UNAVAILABLE),
        (CAL.id, OVERLAP),
        (DAN.id, MINIMUM_REST),
    ]
    assert found[-1].detail == Blocked(DAN.id, MINIMUM_REST, have=0, need=11 * 60)


def test_a_would_rather_not_ranks_last_among_the_free() -> None:
    morning = slot(day(2))
    made = problem([morning], [ANN, BEN], entries=[entry(ANN, "close", day(2))])

    found = options(made, {}, morning.key)

    assert order(found) == [BEN.id, ANN.id]
    assert found[1].close and found[1].code is None


def test_fewer_nights_then_fewer_minutes_then_fewer_weekends_come_first() -> None:
    morning = slot(day(2))
    made = problem(
        [morning, slot(day(0), "night"), slot(day(3), "afternoon"), slot(day(5), "afternoon")],
        [ANN, BEN, CAL, DAN],
    )
    night, weekday_afternoon, friday_afternoon = (s.key for s in made.slots[1:])
    working = {night: (ANN.id,), weekday_afternoon: (BEN.id,), friday_afternoon: (CAL.id,)}

    found = options(made, working, morning.key)

    # Ann has a night; Ben and Cal the same minutes, Cal's on a Friday; Dan nothing yet.
    assert order(found) == [DAN.id, BEN.id, CAL.id, ANN.id]
    by = {o.user_id: o for o in found}
    assert (by[ANN.id].nights, by[CAL.id].weekends, by[DAN.id].minutes) == (1, 1, 8 * 60)


def test_ties_go_by_name_when_names_are_given() -> None:
    morning = slot(day(2))
    made = problem([morning], [ANN, BEN])

    found = options(made, {}, morning.key, names={ANN.id: "Zoe", BEN.id: "Avi"})

    assert order(found) == [BEN.id, ANN.id]


def test_shifts_in_every_project_count() -> None:
    morning = slot(day(2))
    made = problem([morning], [ANN], fixed_shifts=[fixed(day(2), "morning", [ANN], OTHER_PROJECT)])

    (only,) = options(made, {}, morning.key)

    assert only.code == OVERLAP


def test_the_totals_count_only_the_people_named_and_their_load_in_the_range() -> None:
    morning = slot(day(2))
    night = slot(day(0), "night")
    made = problem([morning, night], [ANN, BEN, CAL])
    loaded = loads(made, {night.key: (ANN.id,)})

    assert totals(loaded, [ANN.id, BEN.id]) == (2, 8 * 60, 1, 0)
    assert totals(loaded, []) == (0, 0, 0, 0)
