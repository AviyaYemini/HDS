"""Unit tests for ProjectService client-scope filtering (US5, T093).

Covers: FR-010.
"""

import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

from hds.core.exceptions import ForbiddenError
from hds.models.user import User, UserRole
from hds.services.project import ProjectService


def _user(role: UserRole = UserRole.client, client_id: uuid.UUID | None = None) -> User:
    u = MagicMock(spec=User)
    u.id = uuid.uuid4()
    u.organization_id = uuid.uuid4()
    u.role = role
    u.client_id = client_id or uuid.uuid4()
    return u


def _project(client_id: uuid.UUID | None = None) -> MagicMock:
    p = MagicMock()
    p.id = uuid.uuid4()
    p.client_id = client_id or uuid.uuid4()
    return p


def _svc(repo: AsyncMock) -> ProjectService:
    return ProjectService(project_repo=repo, client_repo=AsyncMock(), site_repo=AsyncMock())


@pytest.mark.asyncio
async def test_list_projects_client_scoped_to_own_client() -> None:
    client_user = _user(role=UserRole.client)
    expected_client_id = client_user.client_id

    repo = AsyncMock()
    repo.list_by_org = AsyncMock(return_value=[_project(client_id=expected_client_id)])

    svc = _svc(repo)
    await svc.list_projects(
        org_id=client_user.organization_id,
        current_user=client_user,
    )

    # Service must override client_id filter to client_user.client_id
    call_kwargs = repo.list_by_org.call_args[1]
    assert call_kwargs["client_id"] == expected_client_id


@pytest.mark.asyncio
async def test_list_projects_client_ignores_external_client_id_filter() -> None:
    client_user = _user(role=UserRole.client)
    other_client_id = uuid.uuid4()

    repo = AsyncMock()
    repo.list_by_org = AsyncMock(return_value=[])

    svc = _svc(repo)
    await svc.list_projects(
        org_id=client_user.organization_id,
        current_user=client_user,
        client_id=other_client_id,
    )

    call_kwargs = repo.list_by_org.call_args[1]
    # Must use own client_id, not the filter arg
    assert call_kwargs["client_id"] == client_user.client_id


@pytest.mark.asyncio
async def test_get_project_client_with_unowned_project_raises_forbidden() -> None:
    client_user = _user(role=UserRole.client)
    project = _project(client_id=uuid.uuid4())  # different client_id

    repo = AsyncMock()
    repo.get_by_id = AsyncMock(return_value=project)

    svc = _svc(repo)
    with pytest.raises(ForbiddenError) as exc_info:
        await svc.get_project(
            id=project.id, org_id=client_user.organization_id, current_user=client_user
        )
    assert exc_info.value.code == "projects.access_denied"
