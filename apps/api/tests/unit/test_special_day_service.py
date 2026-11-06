"""Unit tests for the special days service, with fake repositories (005 T012a).

Covers: 005 FR-003, FR-008, FR-008a; Constitution IV.
"""

import uuid
from datetime import date
from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from hds.core.exceptions import ConflictError, DomainValidationError, ForbiddenError
from hds.models.project import ProjectStatus
from hds.models.user import UserRole
from hds.services.special_day import SpecialDayService, merge_days

ORG = uuid.uuid4()
DAY = date(2027, 9, 11)


def person(role: UserRole) -> Any:
    return SimpleNamespace(id=uuid.uuid4(), organization_id=ORG, role=role)


def day(project_id: uuid.UUID | None = None, on: date = DAY, rate: str = "150.00") -> Any:
    return SimpleNamespace(
        id=uuid.uuid4(),
        organization_id=ORG,
        project_id=project_id,
        day=on,
        name="Holiday",
        rate_pct=Decimal(rate),
    )


def service(
    status: ProjectStatus = ProjectStatus.active,
) -> tuple[SpecialDayService, Any, Any, Any]:
    days, orgs, projects = AsyncMock(), AsyncMock(), AsyncMock()
    found = SimpleNamespace(id=uuid.uuid4(), status=status)
    projects.lock_and_read.return_value = found
    projects.get_by_id.return_value = found
    days.exists.return_value = False
    days.create.side_effect = lambda org, project, values: day(project, values["day"])
    return SpecialDayService(days, orgs, projects), days, orgs, projects


async def test_an_organisation_day_is_added_under_the_organisations_lock() -> None:
    svc, days, orgs, _ = service()

    await svc.create_organisation_day(person(UserRole.admin), {"day": DAY, "name": "Yom Kippur"})

    orgs.lock.assert_awaited_once_with(ORG)
    days.exists.assert_awaited_once_with(ORG, None, DAY)
    days.create.assert_awaited_once()


async def test_a_second_day_on_one_date_is_a_conflict() -> None:
    svc, days, _, _ = service()
    days.exists.return_value = True

    with pytest.raises(ConflictError) as refused:
        await svc.create_organisation_day(person(UserRole.admin), {"day": DAY, "name": "Again"})

    assert refused.value.code == "special_days.duplicate"
    days.create.assert_not_awaited()


@pytest.mark.parametrize(
    "values",
    [
        {"day": DAY, "name": "   "},
        {"day": DAY, "name": "x", "rate_pct": Decimal("99.99")},
        {"day": DAY, "name": "x", "rate_pct": Decimal("300.01")},
    ],
)
async def test_a_blank_name_or_a_rate_out_of_bounds_is_refused(values: dict[str, Any]) -> None:
    svc, days, _, _ = service()

    with pytest.raises(DomainValidationError):
        await svc.create_organisation_day(person(UserRole.admin), values)
    days.create.assert_not_awaited()


@pytest.mark.parametrize("role", [UserRole.manager, UserRole.employee, UserRole.client])
async def test_only_an_admin_adds_a_day(role: UserRole) -> None:
    svc, *_ = service()

    with pytest.raises(ForbiddenError):
        await svc.create_organisation_day(person(role), {"day": DAY, "name": "x"})


async def test_an_ended_projects_days_are_refused() -> None:
    svc, days, _, _ = service(status=ProjectStatus.ended)

    with pytest.raises(ConflictError) as refused:
        await svc.create_project_day(
            person(UserRole.admin), uuid.uuid4(), {"day": DAY, "name": "x"}
        )

    assert refused.value.code == "pay.project_ended"
    days.create.assert_not_awaited()


async def test_a_projects_days_merge_over_the_organisations() -> None:
    svc, days, _, projects = service()
    project_id = uuid.uuid4()
    days.list_organisation.return_value = [day(on=date(2027, 9, 11)), day(on=date(2027, 10, 26))]
    days.list_project.return_value = [day(project_id, date(2027, 10, 26), "180.00")]

    merged = await svc.in_effect(person(UserRole.manager), project_id, year=None)

    assert [(d.day, d.rate_pct, d.source) for d in merged] == [
        (date(2027, 9, 11), Decimal("150.00"), "organisation"),
        (date(2027, 10, 26), Decimal("180.00"), "project"),
    ]


async def test_an_ended_project_uses_only_its_own_days() -> None:
    svc, days, _, _ = service(status=ProjectStatus.ended)
    project_id = uuid.uuid4()
    days.list_organisation.return_value = [day(on=date(2027, 12, 1))]
    days.list_project.return_value = [day(project_id)]

    merged = await svc.in_effect(person(UserRole.manager), project_id, year=None)

    assert [d.source for d in merged] == ["project"]


def test_pricing_takes_a_projects_day_over_the_organisations_and_ended_projects_own_only() -> None:
    project_id = uuid.uuid4()
    rows = [
        day(on=date(2027, 9, 11), rate="150.00"),
        day(on=date(2027, 10, 26), rate="200.00"),
        day(project_id, date(2027, 10, 26), "180.00"),
        day(uuid.uuid4(), date(2027, 12, 1), "250.00"),  # another project's
    ]

    assert merge_days(rows, project_id, ended=False) == {
        date(2027, 9, 11): Decimal("150.00"),
        date(2027, 10, 26): Decimal("180.00"),
    }
    assert merge_days(rows, project_id, ended=True) == {date(2027, 10, 26): Decimal("180.00")}
