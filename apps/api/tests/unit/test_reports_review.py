"""Unit tests for what the review of PR 7 found in the reports and pay lines (005 T038, T039).

Covers: 005 FR-004, FR-014, FR-015, FR-028, FR-030; the review of PR 7.
"""

import uuid
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest

from hds.core.exceptions import DomainValidationError
from hds.models.user import UserRole
from hds.payroll import PayLine, price
from hds.repositories.pay_reports import ReportShift
from hds.services.pay import Pricing
from tests.unit.test_pay_settings_service import organisation
from tests.unit.test_reports_service import NOW, ZONE, person, project, service, shift_of

OCTOBER = (date(2026, 10, 1), date(2026, 10, 31))


async def test_lines_that_all_lack_a_rate_show_no_pay_figure_and_a_missing_count() -> None:
    """FR-004: never pay 0.00 where every shown line is missing its rate."""
    proj = project()
    svc, _ = service([proj], [shift_of(proj, 5, rate=None), shift_of(proj, 6, rate=None)])

    report = await svc.hours(person(UserRole.admin), *OCTOBER, "employee", now=NOW)

    [group] = report.groups
    [row] = group.rows
    for total in (report, group, row):
        assert total.pay is None
        assert total.shifts_missing_rate == 2


async def test_a_pay_figure_covers_the_rated_lines_and_counts_the_rest() -> None:
    proj = project()
    svc, _ = service([proj], [shift_of(proj, 5), shift_of(proj, 6, rate=None)])

    report = await svc.hours(person(UserRole.admin), *OCTOBER, "employee", now=NOW)

    assert (report.pay, report.shifts_missing_rate) == (Decimal("400.00"), 1)


async def test_the_missing_count_counts_shifts_not_people() -> None:
    """Two people on one shift with no rate are one shift missing a rate, as ``shifts`` counts."""
    proj = project()
    one = shift_of(proj, 5, rate=None)
    other = ReportShift(**{**vars(one), "user_id": uuid.uuid4(), "full_name": "Omer Cohen"})
    svc, _ = service([proj], [one, other])

    report = await svc.hours(person(UserRole.admin), *OCTOBER, "project", now=NOW)

    assert (report.shifts, report.shifts_missing_rate) == (1, 1)
    assert (report.groups[0].shifts, report.groups[0].shifts_missing_rate) == (1, 1)


async def test_lines_whose_pay_is_hidden_are_not_priced(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[Any] = []

    def counted(*args: Any) -> PayLine:
        calls.append(args)
        return price(*args)

    monkeypatch.setattr("hds.services.pay.price", counted)
    proj = project(managers_see_pay=False)
    svc, _ = service([proj], [shift_of(proj, 5), shift_of(proj, 6)])

    report = await svc.hours(person(UserRole.manager), *OCTOBER, "employee", now=NOW)

    assert calls == []
    assert (report.hours, report.pay) == (Decimal("16.00"), None)


async def test_special_days_are_read_up_to_the_end_of_the_longest_shift() -> None:
    """A shift may run past the range by more than a day; its later dates still need their days."""
    proj = project()
    long_shift = ReportShift(
        **{
            **vars(shift_of(proj, 30)),
            "ends_at": datetime(2026, 10, 30, 5, tzinfo=UTC) + timedelta(days=3),
        }
    )
    svc, _ = service([proj], [long_shift])
    days = cast(AsyncMock, svc._days)  # the fake special-day repository

    await svc.hours(person(UserRole.admin), *OCTOBER, "employee", now=NOW)

    _, _, lower, upper = days.for_projects.call_args.args
    assert lower <= date(2026, 10, 30) and upper >= date(2026, 11, 2)


def test_the_hours_at_each_rate_add_up_to_the_lines_hours() -> None:
    """1.005 hours before the weekly rest and 1.005 in it: shown 1.01 and 1.00, the line 2.01."""
    proj = project()
    rest_starts = datetime(2026, 10, 30, 15, tzinfo=UTC)  # Friday 18:00 in Israel
    half = timedelta(seconds=3618)  # 1.005 hours
    pricing = Pricing(organisation(), [])

    line = pricing.price_one(
        uuid.uuid4(),
        rest_starts - half,
        rest_starts + half,
        None,
        proj,
        ZONE,
        Decimal("50"),
        paid=True,
    )

    assert line.hours == Decimal("2.01")
    assert sum(p.hours for p in line.parts) == line.hours


async def test_an_explicit_range_over_366_days_is_still_refused() -> None:
    proj = project(start=date(2024, 1, 1))
    svc, _ = service([proj], [])

    with pytest.raises(DomainValidationError) as refused:
        await svc.work_log(
            person(UserRole.admin),
            project_id=proj.id,
            site_id=None,
            group_id=None,
            first=date(2024, 1, 1),
            last=None,
            now=NOW,
        )

    assert refused.value.code == "reports.range_invalid"
