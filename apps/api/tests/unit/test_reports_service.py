"""Unit tests for the reports service, with fake repositories and a fixed clock (005 T026a).

Covers: 005 FR-022 to FR-030; Constitution IV.
"""

import uuid
from datetime import UTC, date, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from hds.core.exceptions import DomainValidationError, ForbiddenError
from hds.models.project import ProjectStatus
from hds.models.user import UserRole
from hds.repositories.pay_reports import ReportShift
from hds.services.pay_settings import COLUMNS
from hds.services.reports import ReportsService
from tests.unit.test_pay_settings_service import organisation

ORG = uuid.uuid4()
ZONE = "Asia/Jerusalem"
NOW = datetime(2026, 11, 5, 12, tzinfo=UTC)
DANA = uuid.uuid4()


def person(role: UserRole) -> Any:
    return SimpleNamespace(id=uuid.uuid4(), organization_id=ORG, role=role)


def project(
    *, pay_enabled: bool = True, managers_see_pay: bool = False, start: date = date(2026, 10, 1)
) -> Any:
    columns = {column: None for names in COLUMNS.values() for column in names}
    return SimpleNamespace(
        id=uuid.uuid4(),
        name="Alpha",
        status=ProjectStatus.active,
        is_deleted=False,
        start_date=start,
        pay_enabled=pay_enabled,
        managers_see_pay=managers_see_pay,
        **columns,
    )


def shift_of(proj: Any, day: int, rate: str | None = "50.0000") -> ReportShift:
    return ReportShift(
        shift_id=uuid.uuid4(),
        starts_at=datetime(2026, 10, day, 5, tzinfo=UTC),
        ends_at=datetime(2026, 10, day, 13, tzinfo=UTC),
        pay_rate=None,
        project_id=proj.id,
        timezone=ZONE,
        user_id=DANA,
        full_name="Dana Levi",
        default_rate=None if rate is None else Decimal(rate),
    )


def service(
    projects: list[Any], shifts: list[ReportShift], managed: set[uuid.UUID] | None = None
) -> tuple[ReportsService, Any]:
    reports, orgs, days, managers, project_repo, sites, groups = (AsyncMock() for _ in range(7))
    reports.worked_in_range.return_value = shifts
    project_repo.get_many.return_value = projects
    orgs.get_by_id.return_value = SimpleNamespace(**vars(organisation()), timezone=ZONE)
    days.for_projects.return_value = []
    managers.manages.return_value = managed or set()
    project_repo.get_by_id.return_value = projects[0] if projects else None
    groups.member_ids.return_value = []
    svc = ReportsService(reports, orgs, days, managers, project_repo, sites, groups)
    return svc, reports


async def test_a_projects_log_starts_on_its_start_and_ends_today() -> None:
    proj = project()
    svc, reports = service([proj], [shift_of(proj, 5), shift_of(proj, 6)])

    log = await svc.work_log(
        person(UserRole.manager),
        project_id=proj.id,
        site_id=None,
        group_id=None,
        first=None,
        last=None,
        now=NOW,
    )

    assert (log.first, log.last) == (date(2026, 10, 1), date(2026, 11, 5))
    [row] = log.rows
    assert (row.shifts, row.hours, row.first_date, row.last_date) == (
        2,
        Decimal("16.00"),
        date(2026, 10, 5),
        date(2026, 10, 6),
    )


async def test_an_old_projects_default_start_is_its_start_date_with_no_limit() -> None:
    """FR-024 and FR-030 as amended after the review of PR 7: the whole project, however old."""
    proj = project(start=date(2024, 1, 1))
    svc, _ = service([proj], [])

    log = await svc.work_log(
        person(UserRole.admin),
        project_id=proj.id,
        site_id=None,
        group_id=None,
        first=None,
        last=None,
        now=NOW,
    )

    assert (log.first, log.last) == (date(2024, 1, 1), date(2026, 11, 5))


async def test_a_shift_of_two_people_counts_once_in_a_projects_total() -> None:
    proj = project()
    one = shift_of(proj, 5)
    other = ReportShift(**{**vars(one), "user_id": uuid.uuid4(), "full_name": "Omer Cohen"})
    svc, _ = service([proj], [one, other])

    report = await svc.hours(
        person(UserRole.admin), date(2026, 10, 1), date(2026, 10, 31), "project", now=NOW
    )

    assert (report.shifts, report.groups[0].shifts, report.hours) == (1, 1, Decimal("16.00"))


@pytest.mark.parametrize(
    ("scope", "first", "code"),
    [
        ({}, date(2026, 10, 1), "reports.scope_invalid"),
        ({"site_id": uuid.uuid4(), "group_id": uuid.uuid4()}, None, "reports.scope_invalid"),
        ({"site_id": uuid.uuid4()}, None, "reports.start_required"),
        ({"group_id": uuid.uuid4()}, None, "reports.start_required"),
    ],
)
async def test_scope_and_start_rules(scope: dict[str, Any], first: date | None, code: str) -> None:
    svc, _ = service([project()], [])
    args = {"project_id": None, "site_id": None, "group_id": None, **scope}

    with pytest.raises(DomainValidationError) as refused:
        await svc.work_log(person(UserRole.admin), **args, first=first, last=None, now=NOW)

    assert refused.value.code == code


@pytest.mark.parametrize(
    ("first", "last"),
    [(date(2026, 10, 10), date(2026, 10, 9)), (date(2025, 1, 1), date(2026, 1, 2))],
)
async def test_a_backwards_or_too_long_range_is_refused(first: date, last: date) -> None:
    svc, _ = service([project()], [])

    with pytest.raises(DomainValidationError) as refused:
        await svc.hours(person(UserRole.admin), first, last, "employee", now=NOW)

    assert refused.value.code == "reports.range_invalid"


@pytest.mark.parametrize(
    ("role", "pay_enabled", "open_", "assigned", "sees"),
    [
        (UserRole.admin, True, False, False, True),
        (UserRole.admin, False, True, True, False),
        (UserRole.manager, True, True, True, True),
        (UserRole.manager, True, True, False, False),
        (UserRole.manager, True, False, True, False),
        (UserRole.manager, False, True, True, False),
    ],
)
async def test_who_sees_a_projects_pay(
    role: UserRole, pay_enabled: bool, open_: bool, assigned: bool, sees: bool
) -> None:
    proj = project(pay_enabled=pay_enabled, managers_see_pay=open_)
    svc, _ = service([proj], [shift_of(proj, 5)], {proj.id} if assigned else set())

    report = await svc.hours(
        person(role), date(2026, 10, 1), date(2026, 10, 31), "project", now=NOW
    )

    assert (report.pay is not None) is sees
    assert (report.groups[0].rows[0].pay == Decimal("400.00")) is sees


@pytest.mark.parametrize("role", [UserRole.employee, UserRole.client])
async def test_employees_and_clients_are_refused(role: UserRole) -> None:
    svc, reports = service([project()], [])

    with pytest.raises(ForbiddenError):
        await svc.hours(person(role), date(2026, 10, 1), date(2026, 10, 31), "employee", now=NOW)
    reports.worked_in_range.assert_not_awaited()
