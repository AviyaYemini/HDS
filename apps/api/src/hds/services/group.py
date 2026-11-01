"""Group service: create, rename, delete groups; manage group memberships."""

import uuid

from hds.core.authz import MANAGER_ROLES, ensure_role
from hds.core.exceptions import ConflictError, NotFoundError
from hds.models.group import Group
from hds.models.user import User
from hds.repositories.group import GroupRepository


class GroupService:
    def __init__(self, group_repo: GroupRepository) -> None:
        self._repo = group_repo

    async def create_group(self, actor: User, name: str) -> Group:
        ensure_role(actor, *MANAGER_ROLES)
        existing = await self._repo.get_by_name_and_org(name, actor.organization_id)
        if existing is not None:
            raise ConflictError(
                code="groups.duplicate_name",
                detail=f"A group named '{name}' already exists in this organisation.",
            )
        return await self._repo.create(actor.organization_id, name)

    async def rename_group(self, actor: User, group_id: uuid.UUID, new_name: str) -> Group:
        ensure_role(actor, *MANAGER_ROLES)
        group = await self._repo.get_by_id(group_id, actor.organization_id)
        if group is None:
            raise NotFoundError(resource="Group", id=group_id)
        conflict = await self._repo.get_by_name_and_org(new_name, actor.organization_id)
        if conflict is not None and conflict.id != group_id:
            raise ConflictError(
                code="groups.duplicate_name",
                detail=f"A group named '{new_name}' already exists.",
            )
        return await self._repo.update(group_id, name=new_name)

    async def delete_group(self, actor: User, group_id: uuid.UUID) -> None:
        ensure_role(actor, *MANAGER_ROLES)
        group = await self._repo.get_by_id(group_id, actor.organization_id)
        if group is None:
            raise NotFoundError(resource="Group", id=group_id)
        await self._repo.delete(group_id)

    async def replace_employee_groups(
        self, actor: User, user_id: uuid.UUID, group_ids: list[uuid.UUID]
    ) -> None:
        ensure_role(actor, *MANAGER_ROLES)
        for group_id in group_ids:
            if await self._repo.get_by_id(group_id, actor.organization_id) is None:
                raise NotFoundError(resource="Group", id=group_id)
        await self._repo.replace_user_groups(user_id, group_ids)
