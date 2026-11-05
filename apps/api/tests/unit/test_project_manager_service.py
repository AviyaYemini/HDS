"""Unit tests for managers on projects, with fake repositories (005 T012a).

Covers: 005 FR-021; Constitution IV.
"""

import uuid
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

from hds.core.exceptions import DomainValidationError, ForbiddenError, NotFoundError
from hds.models.user import UserRole
from hds.services.project_manager import ProjectManagerService

ORG = uuid.uuid4()


def person(role: UserRole) -> Any:
    return SimpleNamespace(id=uuid.uuid4(), organization_id=ORG, role=role)


def service(staff: set[uuid.UUID]) -> tuple[ProjectManagerService, Any, Any]:
    managers, projects = AsyncMock(), AsyncMock()
    projects.lock.return_value = True
    managers.staff_among.return_value = staff
    managers.members.return_value = []
    return ProjectManagerService(managers, projects), managers, projects


async def test_the_set_is_replaced_under_the_projects_lock_without_repeats() -> None:
    one, two = uuid.uuid4(), uuid.uuid4()
    svc, managers, projects = service({one, two})
    project_id = uuid.uuid4()

    await svc.replace(person(UserRole.admin), project_id, [one, two, one])

    projects.lock.assert_awaited_once_with(project_id, ORG)
    managers.replace.assert_awaited_once_with(ORG, project_id, [one, two])


async def test_anyone_who_is_not_an_active_manager_or_admin_is_refused() -> None:
    one, stranger = uuid.uuid4(), uuid.uuid4()
    svc, managers, _ = service({one})

    with pytest.raises(DomainValidationError) as refused:
        await svc.replace(person(UserRole.admin), uuid.uuid4(), [one, stranger])

    assert refused.value.code == "project_managers.not_staff"
    managers.replace.assert_not_awaited()


@pytest.mark.parametrize("role", [UserRole.manager, UserRole.employee, UserRole.client])
async def test_only_an_admin_replaces_the_set(role: UserRole) -> None:
    svc, managers, _ = service(set())

    with pytest.raises(ForbiddenError):
        await svc.replace(person(role), uuid.uuid4(), [])
    managers.replace.assert_not_awaited()


async def test_a_missing_project_is_not_found() -> None:
    svc, _, projects = service(set())
    projects.lock.return_value = False

    with pytest.raises(NotFoundError):
        await svc.replace(person(UserRole.admin), uuid.uuid4(), [])
