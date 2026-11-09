"""Unit tests for pay to date, with fake repositories and a fixed clock (005 T019a).

Covers: 005 FR-004, FR-017 to FR-020, FR-029; Constitution IV.
"""

import uuid
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from hds.core.exceptions import DomainValidationError, ForbiddenError
from hds.models.project import ProjectStatus
from hds.models.user import UserRole
from hds.services.pay import PayService
from hds.services.pay_settings import COLUMNS
from tests.unit.test_pay_settings_service import organisation

ORG = uuid.uuid4()
ZONE = "Asia/Jerusalem"
NOW = datetime(2026, 11, 5, 12, 0, tzinfo=UTC)


def person(role: UserRole = UserRole.employee, rate: str | None = "50.0000") -> Any:
    return SimpleNamespace(
        id=uuid.uuid4(),
        organisation_id=ORG,
        role=role,
        default_hourly_rate=None if rate is None else Decimal(rate),
    )


def worked(start: str, end: str, *, pay_rate: str | None = None, pay_enabled: bool = True) -> Any:
    columns = {column: None for names in COLUMNS.values() for column in names}
    project = SimpleNamespace(
        id=uuid.uuid4(),
        name="Night Watch",
        status=ProjectStatus.active,
        is_deleted=False,
        pay_enabled=pay_enabled,
        **columns,
    )
    shift = SimpleNamespace(
        id=uuid.uuid4(),
        starts_at=datetime.fromisoformat(start),
        ends_at=datetime.fromisoformat(end),
        pay_rate=None if pay_rate is None else Decimal(pay_rate),
    )
    return SimpleNamespace(
        shift=shift,
        project=project,
        timezone=ZONE,
        starts_at=shift.starts_at,
        ends_at=shift.ends_at,
    )


def service(rows: list[Any]) -> tuple[PayService, Any]:
    reports, orgs, days = AsyncMock(), AsyncMock(), AsyncMock()
    reports.own_ended_shifts.return_value = rows
    orgs.get_by_id.return_value = SimpleNamespace(**vars(organisation()), timezone=ZONE)
    days.for_projects.return_value = []
    return PayService(reports, orgs, days), reports


async def test_the_month_defaults_to_the_current_one_and_only_ended_shifts_are_asked_for() -> None:
    svc, reports = service([])

    answer = await svc.my_pay(person(), None, now=NOW)

    assert answer.month == "2026-11"
    args = reports.own_ended_shifts.await_args.kwargs
    assert args["now"] == NOW
    assert args["starts_from"] <= datetime(2026, 10, 31, 22, tzinfo=UTC)  # 1 Nov 00:00 in Israel
    assert args["starts_before"] >= datetime(2026, 11, 30, 22, tzinfo=UTC)


async def test_lines_are_priced_and_totals_are_their_sums() -> None:
    rows = [
        worked("2026-10-05T05:00:00+00:00", "2026-10-05T13:00:00+00:00"),
        worked("2026-10-06T04:00:00+00:00", "2026-10-06T12:00:00+00:00", pay_rate="60.0000"),
    ]
    svc, _ = service(rows)

    answer = await svc.my_pay(person(), "2026-10", now=NOW)

    assert [line.pay for line in answer.lines] == [Decimal("400.00"), Decimal("480.00")]
    assert [line.base_rate for line in answer.lines] == [Decimal("50.0000"), Decimal("60.0000")]
    assert (answer.hours, answer.pay, answer.shifts_missing_rate) == (
        Decimal("16.00"),
        Decimal("880.00"),
        0,
    )


async def test_a_shift_outside_the_month_at_its_site_is_left_out() -> None:
    rows = [worked("2026-09-30T19:00:00+00:00", "2026-10-01T03:00:00+00:00")]  # 30 Sep 22:00
    svc, _ = service(rows)

    answer = await svc.my_pay(person(), "2026-10", now=NOW)

    assert answer.lines == []


async def test_pay_off_and_missing_rate_lines_carry_hours_but_no_pay() -> None:
    rows = [
        worked("2026-10-05T05:00:00+00:00", "2026-10-05T13:00:00+00:00", pay_enabled=False),
        worked("2026-10-06T05:00:00+00:00", "2026-10-06T13:00:00+00:00"),
    ]
    svc, _ = service(rows)

    answer = await svc.my_pay(person(rate=None), "2026-10", now=NOW)

    off, missing = answer.lines
    assert (off.pay_enabled, off.pay, off.rate_missing) == (False, None, False)
    assert (missing.pay, missing.rate_missing) == (None, True)
    # No line has a pay figure and one lacks a rate: no total at all, never 0.00 (FR-004).
    assert (answer.hours, answer.pay, answer.shifts_missing_rate) == (Decimal("16.00"), None, 1)


async def test_a_total_covers_the_lines_with_pay_and_counts_the_missing_ones() -> None:
    rows = [
        worked("2026-10-05T05:00:00+00:00", "2026-10-05T13:00:00+00:00", pay_rate="40.0000"),
        worked("2026-10-06T05:00:00+00:00", "2026-10-06T13:00:00+00:00"),
    ]
    svc, _ = service(rows)

    answer = await svc.my_pay(person(rate=None), "2026-10", now=NOW)

    assert (answer.pay, answer.shifts_missing_rate) == (Decimal("320.00"), 1)


@pytest.mark.parametrize("month", ["2026-13", "2026-1", "2026-10-01", "0000-01"])
async def test_a_bad_month_is_refused(month: str) -> None:
    svc, _ = service([])

    with pytest.raises(DomainValidationError) as refused:
        await svc.my_pay(person(), month, now=NOW)

    assert refused.value.code == "pay.month_invalid"


async def test_a_client_is_refused() -> None:
    svc, reports = service([])

    with pytest.raises(ForbiddenError):
        await svc.my_pay(person(UserRole.client), "2026-10", now=NOW)
    reports.own_ended_shifts.assert_not_awaited()
