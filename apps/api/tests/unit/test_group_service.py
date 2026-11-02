"""Unit tests for GroupService (T054).

Covers: FR-018, FR-020, NFR-001.
"""

import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

from hds.core.exceptions import ConflictError, ForbiddenError, NotFoundError
from hds.models.user import User, UserRole
from hds.services.group import GroupService


def _actor(role: UserRole = UserRole.manager) -> User:
    user = MagicMock(spec=User)
    user.id = uuid.uuid4()
    user.organization_id = uuid.uuid4()
    user.role = role
    return user


def _grp(**kw: object) -> MagicMock:
    g = MagicMock()
    g.id = kw.get("id", uuid.uuid4())
    g.name = kw.get("name", "Group A")
    return g


@pytest.mark.asyncio
async def test_create_group_success() -> None:
    actor = _actor()
    repo = AsyncMock()
    repo.get_by_name_and_org = AsyncMock(return_value=None)
    repo.create = AsyncMock(return_value=_grp(name="Alpha"))

    svc = GroupService(group_repo=repo)
    await svc.create_group(actor, "Alpha")

    repo.create.assert_awaited_once_with(actor.organization_id, "Alpha")


@pytest.mark.asyncio
async def test_create_group_duplicate_name_raises_conflict() -> None:
    repo = AsyncMock()
    repo.get_by_name_and_org = AsyncMock(return_value=_grp(name="Alpha"))

    svc = GroupService(group_repo=repo)
    with pytest.raises(ConflictError) as exc_info:
        await svc.create_group(_actor(), "Alpha")
    assert exc_info.value.code == "groups.duplicate_name"
    repo.create.assert_not_awaited()


@pytest.mark.asyncio
async def test_rename_group() -> None:
    actor = _actor()
    repo = AsyncMock()
    repo.get_by_id = AsyncMock(return_value=_grp(name="Old"))
    repo.get_by_name_and_org = AsyncMock(return_value=None)
    repo.update = AsyncMock(return_value=_grp(name="New"))

    svc = GroupService(group_repo=repo)
    group_id = uuid.uuid4()
    await svc.rename_group(actor, group_id, "New")

    repo.update.assert_awaited_once_with(group_id, actor.organization_id, name="New")


@pytest.mark.asyncio
async def test_rename_group_not_found() -> None:
    repo = AsyncMock()
    repo.get_by_id = AsyncMock(return_value=None)

    svc = GroupService(group_repo=repo)
    with pytest.raises(NotFoundError):
        await svc.rename_group(_actor(), uuid.uuid4(), "New")
    repo.update.assert_not_awaited()


@pytest.mark.asyncio
async def test_delete_group() -> None:
    actor = _actor()
    repo = AsyncMock()
    repo.get_by_id = AsyncMock(return_value=_grp())
    repo.delete = AsyncMock()

    svc = GroupService(group_repo=repo)
    group_id = uuid.uuid4()
    await svc.delete_group(actor, group_id)

    repo.delete.assert_awaited_once_with(group_id, actor.organization_id)


@pytest.mark.asyncio
async def test_replace_employee_groups() -> None:
    repo = AsyncMock()
    repo.replace_user_groups = AsyncMock()

    svc = GroupService(group_repo=repo)
    user_id = uuid.uuid4()
    group_ids = [uuid.uuid4(), uuid.uuid4()]
    await svc.replace_employee_groups(_actor(), user_id, group_ids)

    repo.replace_user_groups.assert_awaited_once_with(user_id, group_ids)


@pytest.mark.asyncio
async def test_replace_employee_groups_with_empty_list_clears_memberships() -> None:
    repo = AsyncMock()
    repo.replace_user_groups = AsyncMock()

    svc = GroupService(group_repo=repo)
    user_id = uuid.uuid4()
    await svc.replace_employee_groups(_actor(), user_id, [])

    repo.replace_user_groups.assert_awaited_once_with(user_id, [])


@pytest.mark.asyncio
@pytest.mark.parametrize("role", [UserRole.employee, UserRole.client])
async def test_non_manager_is_forbidden_everywhere(role: UserRole) -> None:
    repo = AsyncMock()
    svc = GroupService(group_repo=repo)
    actor = _actor(role)
    gid = uuid.uuid4()

    with pytest.raises(ForbiddenError):
        await svc.create_group(actor, "X")
    with pytest.raises(ForbiddenError):
        await svc.rename_group(actor, gid, "X")
    with pytest.raises(ForbiddenError):
        await svc.delete_group(actor, gid)
    with pytest.raises(ForbiddenError):
        await svc.replace_employee_groups(actor, uuid.uuid4(), [])

    repo.create.assert_not_awaited()
    repo.update.assert_not_awaited()
    repo.delete.assert_not_awaited()
    repo.replace_user_groups.assert_not_awaited()
