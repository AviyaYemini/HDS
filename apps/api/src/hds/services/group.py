"""Group service: create, rename, delete groups; manage group memberships.

Layer: services. May import ``hds.repositories``, ``hds.models`` and ``hds.core``; MUST NOT
import routers or build HTTP responses (Constitution II).
"""

import uuid

from hds.core.authz import MANAGER_ROLES, ensure_role
from hds.core.exceptions import ConflictError, NotFoundError
from hds.models.group import Group
from hds.models.user import User
from hds.repositories.group import GroupRepository


class GroupService:
    """Employee groups. Every operation requires Manager or Admin."""

    def __init__(self, group_repo: GroupRepository) -> None:
        """Wire the service.

        Args:
            group_repo: Group storage.
        """
        self._repo = group_repo

    async def list_groups(self, actor: User) -> list[Group]:
        """List the organisation's groups.

        Args:
            actor: The caller.

        Returns:
            The groups, ordered by name.

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
        """
        ensure_role(actor, *MANAGER_ROLES)
        return await self._repo.list_by_org(actor.organization_id)

    async def create_group(self, actor: User, name: str) -> Group:
        """Create a group with a name unique in the organisation.

        Args:
            actor: The caller.
            name: Group name.

        Returns:
            The new group.

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
            ConflictError: ``groups.duplicate_name`` (HTTP 409).
        """
        ensure_role(actor, *MANAGER_ROLES)
        existing = await self._repo.get_by_name_and_org(name, actor.organization_id)
        if existing is not None:
            raise ConflictError(
                code="groups.duplicate_name",
                detail=f"A group named '{name}' already exists in this organisation.",
            )
        return await self._repo.create(actor.organization_id, name)

    async def rename_group(self, actor: User, group_id: uuid.UUID, new_name: str) -> Group:
        """Rename a group; renaming to its current name is allowed.

        Args:
            actor: The caller.
            group_id: Group to rename.
            new_name: New name, unique in the organisation.

        Returns:
            The updated group.

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
            NotFoundError: The group is not in the organisation (HTTP 404).
            ConflictError: ``groups.duplicate_name`` (HTTP 409).
        """
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
        return await self._repo.update(group_id, actor.organization_id, name=new_name)

    async def delete_group(self, actor: User, group_id: uuid.UUID) -> None:
        """Delete a group and its memberships; the members themselves are kept.

        Args:
            actor: The caller.
            group_id: Group to delete.

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
            NotFoundError: The group is not in the organisation (HTTP 404).
        """
        ensure_role(actor, *MANAGER_ROLES)
        group = await self._repo.get_by_id(group_id, actor.organization_id)
        if group is None:
            raise NotFoundError(resource="Group", id=group_id)
        await self._repo.delete(group_id, actor.organization_id)

    async def replace_employee_groups(
        self, actor: User, user_id: uuid.UUID, group_ids: list[uuid.UUID]
    ) -> None:
        """Set an employee's groups to exactly ``group_ids``.

        Every group id is checked against the organisation first, so nothing is written if one
        is foreign (NFR-001). The caller must already have verified ``user_id`` (the employees
        router does).

        Args:
            actor: The caller.
            user_id: Employee whose membership is replaced.
            group_ids: The complete new membership; empty removes all.

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
            NotFoundError: A group id is not in the organisation (HTTP 404).
        """
        ensure_role(actor, *MANAGER_ROLES)
        for group_id in group_ids:
            if await self._repo.get_by_id(group_id, actor.organization_id) is None:
                raise NotFoundError(resource="Group", id=group_id)
        await self._repo.replace_user_groups(user_id, group_ids)
