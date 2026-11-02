import uuid
from typing import Any

from sqlalchemy import func, select, update

from hds.models.client import Client
from hds.models.project import Project
from hds.models.user import User
from hds.repositories.base import OrgScopedRepository


class ClientRepository(OrgScopedRepository[Client]):
    model = Client

    async def list_by_org(self, org_id: uuid.UUID, is_active: bool | None = None) -> list[Client]:
        stmt = select(Client).where(
            Client.organization_id == org_id,
            Client.is_deleted.is_(False),
        )
        if is_active is not None:
            stmt = stmt.where(Client.is_active == is_active)
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def get_by_id(self, id: uuid.UUID, org_id: uuid.UUID) -> Client | None:
        result = await self.session.execute(
            select(Client).where(
                Client.id == id,
                Client.organization_id == org_id,
                Client.is_deleted.is_(False),
            )
        )
        return result.scalar_one_or_none()

    async def create(self, org_id: uuid.UUID, data: dict[str, Any]) -> Client:
        client = Client(id=uuid.uuid4(), organization_id=org_id, **data)
        self.session.add(client)
        await self.session.commit()
        await self.session.refresh(client)
        return client

    async def update(self, id: uuid.UUID, org_id: uuid.UUID, **fields: Any) -> Client:
        return await self._update_scoped(id, org_id, **fields)

    async def soft_delete(self, id: uuid.UUID, org_id: uuid.UUID) -> None:
        await self._soft_delete_scoped(id, org_id)

    async def deactivate_client_users(self, client_id: uuid.UUID) -> None:
        await self.session.execute(
            update(User)
            .where(User.client_id == client_id, User.is_deleted.is_(False))
            .values(is_active=False)
        )
        await self.session.commit()

    async def list_users(self, client_id: uuid.UUID, org_id: uuid.UUID) -> list[User]:
        result = await self.session.execute(
            select(User).where(
                User.client_id == client_id,
                User.organization_id == org_id,
                User.is_deleted.is_(False),
            )
        )
        return list(result.scalars().all())

    async def count_non_deleted_projects(self, client_id: uuid.UUID) -> int:
        result = await self.session.execute(
            select(func.count()).where(
                Project.client_id == client_id,
                Project.is_deleted.is_(False),
            )
        )
        return result.scalar_one()
