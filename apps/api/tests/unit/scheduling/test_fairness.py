"""How fair a proposal is, and what it traded off (008 T032).

Covers: 008 FR-024, FR-025; research P12. The averages, the outlier thresholds exactly at their
edges, weekend shifts on Friday and Saturday at the site, locked (kept) and fixed shifts counted,
and the trade-off counts.
"""

from dataclasses import replace

from hds.scheduling.fairness import panel, tradeoffs
from hds.scheduling.problem import ChipResult
from tests.unit.scheduling.factory import day, entry, fixed, person, problem, slot

ANN, BEN, CAL = person(1), person(2), person(3)
HOUR = 60


def test_shares_and_totals_count_locked_and_fixed_shifts() -> None:
    sunday, monday = slot(day(0)), slot(day(1))
    made = problem(
        [replace(sunday, kept=(ANN.id,)), monday],
        [ANN, BEN],
        fixed_shifts=[fixed(day(1), "night", [BEN])],  # in the range: it counts
    )

    found = panel(made, {monday.key: (ANN.id,)})

    by = {s.user_id: s for s in found.shares}
    assert (by[ANN.id].minutes, by[ANN.id].nights) == (16 * HOUR, 0)
    assert (by[BEN.id].minutes, by[BEN.id].nights) == (8 * HOUR, 1)
    assert (found.people, found.minutes, found.nights) == (2, 24 * HOUR, 1)


def test_a_weekend_shift_starts_on_friday_or_saturday_at_the_site() -> None:
    thursday_night, friday, saturday = slot(day(4), "night"), slot(day(5)), slot(day(6))
    made = problem([thursday_night, friday, saturday], [ANN])

    (share,) = panel(
        made, {thursday_night.key: (ANN.id,), friday.key: (ANN.id,), saturday.key: (ANN.id,)}
    ).shares

    # Thursday's night ends on Friday but starts on Thursday: not a weekend shift.
    assert share.weekends == 2


def test_two_nights_over_the_average_is_an_outlier_and_one_less_is_not() -> None:
    nights = [slot(day(n), "night") for n in range(4)]
    made = problem(nights, [ANN, BEN, CAL])

    # Ann 4 nights, Ben 1, Cal 1: average 2, Ann exactly 2 over.
    exactly = panel(made, {s.key: (ANN.id,) for s in nights})
    # Ann 3, Ben 1: average 4/3, Ann 1.67 over.
    less = panel(made, {nights[0].key: (BEN.id,), **{s.key: (ANN.id,) for s in nights[1:]}})

    assert {s.user_id: s.nights_outlier for s in exactly.shares}[ANN.id] is True
    assert {s.user_id: s.nights_outlier for s in less.shares}[ANN.id] is False


def test_hours_need_both_twenty_percent_and_four_hours_over() -> None:
    days = [slot(day(n)) for n in range(4)]
    made = problem(days, [ANN, BEN])

    # Ann 32 h, Ben 0: average 16 h, Ann 100% and 16 h over.
    far = panel(made, {s.key: (ANN.id,) for s in days})
    # Ann 16 h, Ben 16 h: no one over.
    even = panel(
        made, {**{s.key: (ANN.id,) for s in days[:2]}, **{s.key: (BEN.id,) for s in days[2:]}}
    )

    assert {s.user_id: s.hours_outlier for s in far.shares} == {ANN.id: True, BEN.id: False}
    assert not any(s.hours_outlier for s in even.shares)


def test_trade_offs_count_empty_places_wishes_spreads_and_chips() -> None:
    sunday, monday = slot(day(0), headcount=2), slot(day(1))
    made = problem(
        [sunday, monday],
        [ANN, BEN],
        entries=[entry(ANN, "open", day(1)), entry(BEN, "close", day(0))],
    )

    found = tradeoffs(
        made,
        {sunday.key: (BEN.id,), monday.key: (BEN.id,)},
        [ChipResult(0, "met"), ChipResult(1, "not_met", "coverage")],
    )

    assert found.empty_places == 1
    assert found.open_wishes_missed == 1  # Ann wanted Monday and got nothing she wanted
    assert found.close_wishes_given == 1  # Ben would rather not work Sunday
    assert (found.minutes_low, found.minutes_high) == (0, 16 * HOUR)
    assert found.chips_not_met == 1
