"""The snapshot's comparison period: whole months back for a month range (CEO demo)."""

from datetime import date

from hds.services.overview import previous_range


def test_a_month_compares_with_the_month_before() -> None:
    assert previous_range(date(2026, 11, 1), date(2026, 11, 30)) == (
        date(2026, 10, 1),
        date(2026, 10, 31),
    )
    assert previous_range(date(2026, 3, 1), date(2026, 3, 31)) == (
        date(2026, 2, 1),
        date(2026, 2, 28),
    )


def test_a_quarter_and_a_year_compare_with_the_ones_before() -> None:
    assert previous_range(date(2026, 10, 1), date(2026, 12, 31)) == (
        date(2026, 7, 1),
        date(2026, 9, 30),
    )
    assert previous_range(date(2026, 1, 1), date(2026, 12, 31)) == (
        date(2025, 1, 1),
        date(2025, 12, 31),
    )


def test_any_other_range_compares_with_as_many_days_before() -> None:
    assert previous_range(date(2026, 11, 8), date(2026, 11, 14)) == (
        date(2026, 11, 1),
        date(2026, 11, 7),
    )
