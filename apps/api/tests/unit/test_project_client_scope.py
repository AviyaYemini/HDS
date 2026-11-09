"""Unit tests for ProjectService client-scope filtering (US5, T093).

Covers: FR-010.
"""

import inspect
import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

from hds.core.exceptions import ForbiddenError
from hds.models.project import ProjectStatus
from hds.models.user import User, UserRole
from hds.repositories.project import ProjectRepository
from hds.services.project import ProjectService


def _user(role: UserRole = UserRole.client, client_id: uuid.UUID | None = None) -> User:
    u = MagicMock(spec=User)
    u.id = uuid.uuid4()
    u.organisation_id = uuid.uuid4()
    u.role = role
    u.client_id = client_id or uuid.uuid4()
    return u


def _project(client_id: uuid.UUID | None = None) -> MagicMock:
    p = MagicMock()
    p.id = uuid.uuid4()
    p.client_id = client_id or uuid.uuid4()
    return p


def _svc(repo: AsyncMock) -> ProjectService:
    return ProjectService(
        project_repo=repo, client_repo=AsyncMock(), site_repo=AsyncMock(), pay_settings=AsyncMock()
    )


@pytest.mark.asyncio
async def test_list_projects_client_scoped_to_own_client() -> None:
    client_user = _user(role=UserRole.client)
    expected_client_id = client_user.client_id

    repo = AsyncMock()
    repo.list_by_client = AsyncMock(return_value=[_project(client_id=expected_client_id)])

    svc = _svc(repo)
    await svc.list_projects(
        org_id=client_user.organisation_id,
        current_user=client_user,
    )

    # A Client reads through the per-client query, never the organisation-wide one.
    repo.list_by_client.assert_awaited_once_with(
        expected_client_id, client_user.organisation_id, status=None
    )
    repo.list_by_org.assert_not_awaited()


@pytest.mark.asyncio
async def test_list_projects_client_ignores_external_client_id_filter() -> None:
    client_user = _user(role=UserRole.client)
    other_client_id = uuid.uuid4()

    repo = AsyncMock()
    repo.list_by_client = AsyncMock(return_value=[])

    svc = _svc(repo)
    await svc.list_projects(
        org_id=client_user.organisation_id,
        current_user=client_user,
        client_id=other_client_id,
    )

    # Must use own client_id, not the filter arg
    repo.list_by_client.assert_awaited_once_with(
        client_user.client_id, client_user.organisation_id, status=None
    )
    repo.list_by_org.assert_not_awaited()


@pytest.mark.asyncio
async def test_list_projects_client_keeps_the_status_filter() -> None:
    client_user = _user(role=UserRole.client)
    repo = AsyncMock()
    repo.list_by_client = AsyncMock(return_value=[])

    await _svc(repo).list_projects(
        org_id=client_user.organisation_id, current_user=client_user, status=ProjectStatus.ended
    )

    repo.list_by_client.assert_awaited_once_with(
        client_user.client_id, client_user.organisation_id, status=ProjectStatus.ended
    )


@pytest.mark.asyncio
async def test_get_project_client_with_unowned_project_raises_forbidden() -> None:
    client_user = _user(role=UserRole.client)
    project = _project(client_id=uuid.uuid4())  # different client_id

    repo = AsyncMock()
    repo.get_by_id = AsyncMock(return_value=project)

    svc = _svc(repo)
    with pytest.raises(ForbiddenError) as exc_info:
        await svc.get_project(
            id=project.id, org_id=client_user.organisation_id, current_user=client_user
        )
    assert exc_info.value.code == "projects.access_denied"


@pytest.mark.asyncio
async def test_a_staff_listing_filtered_by_client_uses_the_per_client_read() -> None:
    staff = _user(role=UserRole.manager)
    wanted = uuid.uuid4()
    repo = AsyncMock()

    await _svc(repo).list_projects(
        org_id=staff.organisation_id,
        current_user=staff,
        client_id=wanted,
        status=ProjectStatus.ended,
    )

    repo.list_by_client.assert_awaited_once_with(
        wanted, staff.organisation_id, status=ProjectStatus.ended
    )
    repo.list_by_org.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_staff_listing_without_a_client_filter_lists_the_organisation() -> None:
    staff = _user(role=UserRole.admin)
    repo = AsyncMock()

    await _svc(repo).list_projects(org_id=staff.organisation_id, current_user=staff)

    repo.list_by_org.assert_awaited_once_with(staff.organisation_id, status=None)
    repo.list_by_client.assert_not_awaited()


def test_no_project_read_takes_an_optional_client() -> None:
    """003 FR-006: "every client" cannot be asked for by passing an empty client by mistake."""
    assert "client_id" not in inspect.signature(ProjectRepository.list_by_org).parameters
    client = inspect.signature(ProjectRepository.list_by_client).parameters["client_id"]
    assert client.default is inspect.Parameter.empty
