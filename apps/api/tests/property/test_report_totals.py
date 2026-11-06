"""Property tests: report totals are sums of what is shown, however grouped (005 T024).

``aggregate`` turns priced lines (one person on one shift) into groups of rows. For any lines, the
hours by employee and by project are equal, every total is the sum of the rows under it, a shift
shared by several people counts once, pay appears exactly where some line under it may be seen and
has a rate, and missing rates are counted, once per shift, only on lines whose pay may be seen.

Covers: 005 FR-004, FR-027, FR-028, SC-002. Examples: ``HDS_GENERATE_EXAMPLES`` (200 by default,
1,000 in CI).
"""

import os
import uuid
from datetime import date
from decimal import Decimal

from hypothesis import given, settings
from hypothesis import strategies as st

from hds.services.reports import ReportLine, aggregate

EXAMPLES = int(os.environ.get("HDS_GENERATE_EXAMPLES", "200"))
PEOPLE = [(uuid.UUID(int=i), f"Person {i}") for i in range(1, 5)]
PROJECTS = [(uuid.UUID(int=100 + i), f"Project {i}") for i in range(1, 4)]
CENT = Decimal("0.01")


SHIFTS = [uuid.UUID(int=1000 + i) for i in range(12)]


@st.composite
def lines(draw: st.DrawFn) -> ReportLine:
    """One priced line: some with pay, some hidden, some missing a rate; shifts may be shared."""
    person = draw(st.sampled_from(PEOPLE))
    project = draw(st.sampled_from(PROJECTS))
    visible = draw(st.booleans())
    missing = visible and draw(st.booleans())
    pay = None if not visible or missing else Decimal(draw(st.integers(0, 10**7))) * CENT
    return ReportLine(
        shift_id=draw(st.sampled_from(SHIFTS)),
        user_id=person[0],
        user_name=person[1],
        project_id=project[0],
        project_name=project[1],
        date=date(2026, 10, draw(st.integers(1, 31))),
        hours=Decimal(draw(st.integers(1, 2400))) * CENT,
        pay_shown=visible,
        pay=pay,
        rate_missing=missing,
    )


@settings(max_examples=EXAMPLES, deadline=None)
@given(st.lists(lines(), max_size=40))
def test_totals_are_sums_of_what_is_shown_both_ways(items: list[ReportLine]) -> None:
    by_employee = aggregate(items, "employee")
    by_project = aggregate(items, "project")

    for report in (by_employee, by_project):
        assert report.hours == sum((line.hours for line in items), Decimal(0))
        assert report.shift_ids == {line.shift_id for line in items}
        shown = [line for line in items if line.pay_shown]
        assert report.shifts_missing_rate == len({x.shift_id for x in shown if x.rate_missing})
        rated = [line.pay for line in shown if line.pay is not None]
        # A pay figure only where some shown line has a rate: never zero for missing rates (FR-004).
        assert report.pay == (sum(rated, Decimal(0)) if rated else None)
        for group in report.groups:
            assert group.hours == sum((r.hours for r in group.rows), Decimal(0))
            assert group.shift_ids == set().union(*(r.shift_ids for r in group.rows))
            assert group.missing_ids == set().union(*(r.missing_ids for r in group.rows))
            row_pay = [r.pay for r in group.rows if r.pay is not None]
            assert group.pay == (sum(row_pay, Decimal(0)) if row_pay else None)
        assert report.hours == sum((g.hours for g in report.groups), Decimal(0))
    assert by_employee.hours == by_project.hours
    assert by_employee.pay == by_project.pay
    assert by_employee.shifts == by_project.shifts
