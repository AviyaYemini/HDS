import uuid
from typing import Any

from sqlalchemy import delete, select

from hds.models.group import Group, user_groups
from hds.repositories.base import OrgScopedRepository


class GroupRepository(OrgScopedRepository[Group]):
    model = Group

    async def list_by_org(self, org_id: uuid.UUID) -> list[Group]:
        result = await self.session.execute(
            select(Group).where(Group.organization_id == org_id).order_by(Group.name)
        )
        return list(result.scalars().all())

    async def get_by_name_and_org(self, name: str, org_id: uuid.UUID) -> Group | None:
        result = await self.session.execute(
            select(Group).where(Group.name == name, Group.organization_id == org_id)
        )
        return result.scalar_one_or_none()

    async def create(self, org_id: uuid.UUID, name: str) -> Group:
        group = Group(id=uuid.uuid4(), organization_id=org_id, name=name)
        self.session.add(group)
        await self.session.commit()
        await self.session.refresh(group)
        return group

    async def update(self, id: uuid.UUID, org_id: uuid.UUID, **fields: Any) -> Group:
        return await self._update_scoped(id, org_id, **fields)

    async def delete(self, id: uuid.UUID, org_id: uuid.UUID) -> None:
        await self._delete_scoped(id, org_id)

    async def get_groups_for_user(self, user_id: uuid.UUID, org_id: uuid.UUID) -> list[Group]:
        result = await self.session.execute(
            select(Group)
            .join(user_groups, Group.id == user_groups.c.group_id)
            .where(user_groups.c.user_id == user_id, Group.organization_id == org_id)
        )
        return list(result.scalars().all())

    async def replace_user_groups(self, user_id: uuid.UUID, group_ids: list[uuid.UUID]) -> None:
        await self.session.execute(delete(user_groups).where(user_groups.c.user_id == user_id))
        for gid in group_ids:
            await self.session.execute(user_groups.insert().values(user_id=user_id, group_id=gid))
        await self.session.commit()
