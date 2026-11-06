"""Persistence of employee groups and their membership.

Layer: repositories. Depends on ``hds.models`` and ``hds.repositories.base`` only; it MUST NOT
import services or routers.
"""

import uuid
from typing import Any

from sqlalchemy import delete, select

from hds.models.group import Group, user_groups
from hds.repositories.base import OrgScopedRepository


class GroupRepository(OrgScopedRepository[Group]):
    """Queries and writes for ``Group`` and the ``user_groups`` link table."""

    model = Group

    async def list_by_org(self, org_id: uuid.UUID) -> list[Group]:
        """List the organisation's groups ordered by name.

        Args:
            org_id: The caller's organisation.
        """
        result = await self.session.execute(
            select(Group).where(Group.organization_id == org_id).order_by(Group.name)
        )
        return list(result.scalars().all())

    async def get_by_name_and_org(self, name: str, org_id: uuid.UUID) -> Group | None:
        """Find a group by exact name, used to detect duplicates before inserting.

        Args:
            name: Group name, compared case-sensitively.
            org_id: The caller's organisation.

        Returns:
            The group, or ``None``.
        """
        result = await self.session.execute(
            select(Group).where(Group.name == name, Group.organization_id == org_id)
        )
        return result.scalar_one_or_none()

    async def create(self, org_id: uuid.UUID, name: str) -> Group:
        """Insert a group.

        Args:
            org_id: Organisation that will own the group.
            name: Group name; unique within the organisation.

        Returns:
            The persisted group.

        Raises:
            sqlalchemy.exc.IntegrityError: If the name already exists in the organisation.
        """
        group = Group(id=uuid.uuid4(), organization_id=org_id, name=name)
        self.session.add(group)
        await self.session.flush()
        await self.session.refresh(group)
        return group

    async def update(self, id: uuid.UUID, org_id: uuid.UUID, **fields: Any) -> Group:
        """Update columns of a group of the organisation.

        Args:
            id: Group to update.
            org_id: The caller's organisation.
            **fields: Column values to set.

        Returns:
            The refreshed group.

        Raises:
            NotFoundError: When the group is not in the organisation.
        """
        return await self._update_scoped(id, org_id, **fields)

    async def delete(self, id: uuid.UUID, org_id: uuid.UUID) -> None:
        """Hard-delete a group; its membership rows go with it, the users stay.

        Args:
            id: Group to delete.
            org_id: The caller's organisation.
        """
        await self._delete_scoped(id, org_id)

    async def member_ids(self, group_id: uuid.UUID) -> list[uuid.UUID]:
        """The ids of a group's current members; callers check the group is theirs first."""
        rows = await self.session.execute(
            select(user_groups.c.user_id).where(user_groups.c.group_id == group_id)
        )
        return list(rows.scalars())

    async def get_groups_for_user(self, user_id: uuid.UUID, org_id: uuid.UUID) -> list[Group]:
        """List the groups a user belongs to within the organisation.

        Args:
            user_id: The user.
            org_id: The caller's organisation.
        """
        result = await self.session.execute(
            select(Group)
            .join(user_groups, Group.id == user_groups.c.group_id)
            .where(user_groups.c.user_id == user_id, Group.organization_id == org_id)
        )
        return list(result.scalars().all())

    async def replace_user_groups(self, user_id: uuid.UUID, group_ids: list[uuid.UUID]) -> None:
        """Make the user a member of exactly ``group_ids`` within the request's transaction.

        No organisation check happens here: the service must already have verified that the
        user and every group belong to the caller's organisation (NFR-001).

        Args:
            user_id: The verified user.
            group_ids: The complete new membership; an empty list removes the user from all.
        """
        await self.session.execute(delete(user_groups).where(user_groups.c.user_id == user_id))
        for gid in group_ids:
            await self.session.execute(user_groups.insert().values(user_id=user_id, group_id=gid))
        await self.session.flush()
