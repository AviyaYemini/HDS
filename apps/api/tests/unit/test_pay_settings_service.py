"""Unit tests for the pay settings service, with fake repositories (005 T012a).

Covers: 005 FR-003, FR-005 to FR-007, FR-008a (the freeze copies the days); Constitution IV.
"""

import uuid
from datetime import time
from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from hds.core.exceptions import (
    ConflictError,
    DomainValidationError,
    ForbiddenError,
    NotFoundError,
)
from hds.models.project import ProjectStatus
from hds.models.user import UserRole
from hds.payroll import DEFAULT_SETTINGS, RestWindow
from hds.services.pay_settings import COLUMNS, PaySettingsService

ORG = uuid.uuid4()


def person(role: UserRole) -> Any:
    return SimpleNamespace(id=uuid.uuid4(), organisation_id=ORG, role=role, is_active=True)


def organisation(**values: Any) -> Any:
    d = DEFAULT_SETTINGS
    row = {
        "pay_day_normal_minutes": d.day_normal_minutes,
        "pay_night_normal_minutes": d.night_normal_minutes,
        "pay_tier1_minutes": d.tier1_minutes,
        "pay_overtime1_pct": d.overtime1_pct,
        "pay_overtime2_pct": d.overtime2_pct,
        "pay_rest_start_weekday": d.rest.start_weekday,
        "pay_rest_start_time": d.rest.start_time,
        "pay_rest_end_weekday": d.rest.end_weekday,
        "pay_rest_end_time": d.rest.end_time,
        "pay_rest_pct": d.rest_pct,
        "pay_increase1_points": d.increase1_points,
        "pay_increase2_points": d.increase2_points,
    }
    return SimpleNamespace(id=ORG, **{**row, **values})


def project(status: ProjectStatus = ProjectStatus.active, **values: Any) -> Any:
    row: dict[str, Any] = {column: None for columns in COLUMNS.values() for column in columns}
    return SimpleNamespace(
        id=uuid.uuid4(),
        organisation_id=ORG,
        status=status,
        pay_enabled=True,
        managers_see_pay=False,
        **{**row, **values},
    )


def service(org: Any = None, proj: Any = None) -> tuple[PaySettingsService, Any, Any, Any]:
    orgs, projects, days = AsyncMock(), AsyncMock(), AsyncMock()
    orgs.get_by_id.return_value = org if org is not None else organisation()
    orgs.update_pay.side_effect = lambda _id, values: organisation(**values)
    projects.get_by_id.return_value = proj
    projects.lock_and_read.return_value = proj
    return PaySettingsService(orgs, projects, days), orgs, projects, days


async def test_managers_read_the_defaults_and_employees_do_not() -> None:
    svc, *_ = service()

    settings = await svc.read_organisation(person(UserRole.manager))

    assert settings == DEFAULT_SETTINGS
    with pytest.raises(ForbiddenError):
        await svc.read_organisation(person(UserRole.employee))


async def test_only_an_admin_changes_the_defaults() -> None:
    svc, orgs, *_ = service()

    with pytest.raises(ForbiddenError):
        await svc.change_organisation(person(UserRole.manager), {"rest_pct": Decimal("200")})
    changed = await svc.change_organisation(person(UserRole.admin), {"rest_pct": Decimal("200")})

    assert changed.rest_pct == Decimal("200")
    orgs.update_pay.assert_awaited_once_with(ORG, {"pay_rest_pct": Decimal("200")})


@pytest.mark.parametrize(
    "change",
    [
        {"day_normal_minutes": 0},
        {"tier1_minutes": 1441},
        {"overtime1_pct": Decimal("99.99")},
        {"rest_pct": Decimal("300.01")},
        {"increase1_points": Decimal("0")},
        {"increase2_points": Decimal("200.01")},
        {"overtime2_pct": None},
    ],
)
async def test_values_out_of_bounds_are_refused(change: dict[str, Any]) -> None:
    svc, orgs, *_ = service()

    with pytest.raises(DomainValidationError) as refused:
        await svc.change_organisation(person(UserRole.admin), change)

    assert refused.value.code == "pay.settings_invalid"
    orgs.update_pay.assert_not_awaited()


async def test_a_rest_window_whose_ends_meet_is_refused() -> None:
    svc, *_ = service()
    window = RestWindow(5, time(18), 5, time(18))

    with pytest.raises(DomainValidationError) as refused:
        await svc.change_organisation(person(UserRole.admin), {"rest_window": window})

    assert refused.value.code == "pay.rest_window_invalid"


async def test_a_project_shows_its_own_values_over_the_defaults() -> None:
    proj = project(pay_day_normal_minutes=540, pay_rest_pct=Decimal("200.00"))
    svc, *_ = service(proj=proj)

    answer = await svc.read_project(person(UserRole.manager), proj.id)

    assert answer.settings.day_normal_minutes == 540
    assert answer.settings.rest_pct == Decimal("200.00")
    assert answer.settings.overtime1_pct == DEFAULT_SETTINGS.overtime1_pct
    assert answer.own == ["day_normal_minutes", "rest_pct"]


async def test_another_organisations_project_is_not_found() -> None:
    svc, *_ = service(proj=None)

    with pytest.raises(NotFoundError):
        await svc.read_project(person(UserRole.manager), uuid.uuid4())
    with pytest.raises(NotFoundError):
        await svc.change_project(person(UserRole.admin), uuid.uuid4(), {"pay_enabled": False})


async def test_null_returns_a_setting_to_the_default_and_the_window_is_one_setting() -> None:
    proj = project(pay_overtime1_pct=Decimal("140.00"))
    svc, _, projects, _ = service(proj=proj)

    await svc.change_project(
        person(UserRole.admin),
        proj.id,
        {"settings": {"overtime1_pct": None, "rest_window": None}, "managers_see_pay": True},
    )

    projects.update.assert_awaited_once_with(
        proj.id,
        ORG,
        pay_overtime1_pct=None,
        pay_rest_start_weekday=None,
        pay_rest_start_time=None,
        pay_rest_end_weekday=None,
        pay_rest_end_time=None,
        managers_see_pay=True,
    )


async def test_an_ended_project_cannot_be_changed() -> None:
    proj = project(status=ProjectStatus.ended)
    svc, _, projects, _ = service(proj=proj)

    with pytest.raises(ConflictError) as refused:
        await svc.change_project(person(UserRole.admin), proj.id, {"pay_enabled": False})

    assert refused.value.code == "pay.project_ended"
    projects.update.assert_not_awaited()


async def test_a_manager_cannot_change_a_project_or_send_settings_on_create() -> None:
    proj = project()
    svc, *_ = service(proj=proj)

    with pytest.raises(ForbiddenError):
        await svc.change_project(person(UserRole.manager), proj.id, {"pay_enabled": False})
    with pytest.raises(ForbiddenError) as refused:
        svc.columns_for_new_project(person(UserRole.manager), {"pay_enabled": False})
    assert refused.value.code == "projects.pay_settings_admin_only"


async def test_new_project_settings_become_columns() -> None:
    svc, *_ = service()

    columns = svc.columns_for_new_project(
        person(UserRole.admin), {"pay_enabled": False, "settings": {"rest_pct": Decimal("175")}}
    )

    assert columns == {"pay_enabled": False, "pay_rest_pct": Decimal("175")}


async def test_ending_a_project_locks_in_order_and_copies_defaults_and_days() -> None:
    proj = project(pay_rest_pct=Decimal("200.00"))
    svc, orgs, projects, days = service(proj=proj)
    order: list[str] = []

    def locked(name: str, answer: Any) -> Any:
        def lock(*_: Any) -> Any:
            order.append(name)
            return answer

        return lock

    orgs.lock.side_effect = locked("organisation", None)
    projects.lock_and_read.side_effect = locked("project", proj)

    await svc.freeze(ORG, proj.id)

    assert order == ["organisation", "project"]
    filled = projects.update.await_args.kwargs
    assert "pay_rest_pct" not in filled  # its own value stays
    assert filled["pay_overtime1_pct"] == DEFAULT_SETTINGS.overtime1_pct
    assert filled["pay_rest_start_time"] == DEFAULT_SETTINGS.rest.start_time
    days.copy_organisation_days.assert_awaited_once_with(ORG, proj.id)


async def test_ending_an_ended_project_again_copies_nothing() -> None:
    proj = project(status=ProjectStatus.ended)
    svc, _, projects, days = service(proj=proj)

    await svc.freeze(ORG, proj.id)

    projects.update.assert_not_awaited()
    days.copy_organisation_days.assert_not_awaited()
