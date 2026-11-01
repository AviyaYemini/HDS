"""Unit tests for SiteService."""

import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

from hds.core.exceptions import ForbiddenError, NotFoundError
from hds.models.user import User, UserRole
from hds.services.site import SiteService


def _actor(role: UserRole = UserRole.manager) -> User:
    user = MagicMock(spec=User)
    user.id = uuid.uuid4()
    user.organization_id = uuid.uuid4()
    user.role = role
    return user


def _site(**kw: object) -> MagicMock:
    s = MagicMock()
    s.id = kw.get("id", uuid.uuid4())
    s.name = kw.get("name", "Main Gate")
    return s


@pytest.mark.asyncio
async def test_create_site_success() -> None:
    actor = _actor()
    repo = AsyncMock()
    repo.create = AsyncMock(return_value=_site())
    data = {"name": "Main Gate", "address": "1 Example St", "timezone": "UTC"}

    svc = SiteService(site_repo=repo)
    await svc.create_site(actor, data)

    repo.create.assert_awaited_once_with(actor.organization_id, data)


@pytest.mark.asyncio
async def test_edit_site_success() -> None:
    repo = AsyncMock()
    existing = _site()
    repo.get_by_id = AsyncMock(return_value=existing)
    repo.update = AsyncMock(return_value=_site(name="Side Gate"))

    svc = SiteService(site_repo=repo)
    await svc.edit_site(_actor(), existing.id, {"name": "Side Gate"})

    repo.update.assert_awaited_once_with(existing.id, name="Side Gate")


@pytest.mark.asyncio
async def test_edit_site_not_found() -> None:
    repo = AsyncMock()
    repo.get_by_id = AsyncMock(return_value=None)

    svc = SiteService(site_repo=repo)
    with pytest.raises(NotFoundError):
        await svc.edit_site(_actor(), uuid.uuid4(), {"name": "X"})
    repo.update.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("role", [UserRole.employee, UserRole.client])
async def test_non_manager_is_forbidden(role: UserRole) -> None:
    repo = AsyncMock()
    svc = SiteService(site_repo=repo)
    actor = _actor(role)

    with pytest.raises(ForbiddenError):
        await svc.create_site(actor, {"name": "X"})
    with pytest.raises(ForbiddenError):
        await svc.edit_site(actor, uuid.uuid4(), {"name": "X"})

    repo.create.assert_not_awaited()
    repo.update.assert_not_awaited()
