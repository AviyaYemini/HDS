"""Persistence of clients and the queries that depend on them.

Layer: repositories. Depends on ``hds.models`` and ``hds.repositories.base`` only; it MUST NOT
import services or routers.
"""

import uuid
from typing import Any

from sqlalchemy import func, select, update

from hds.models.client import Client
from hds.models.project import Project
from hds.models.user import User
from hds.repositories.base import OrgScopedRepository


class ClientRepository(OrgScopedRepository[Client]):
    """Queries and writes for ``Client``. Soft-deleted clients are invisible to lookups."""

    model = Client

    async def list_by_org(self, org_id: uuid.UUID, is_active: bool | None = None) -> list[Client]:
        """List the organisation's non-deleted clients.

        Args:
            org_id: The caller's organisation.
            is_active: Keep only active or only inactive clients; ``None`` keeps both.

        Returns:
            Matching clients in unspecified order.
        """
        stmt = select(Client).where(
            Client.organization_id == org_id,
            Client.is_deleted.is_(False),
        )
        if is_active is not None:
            stmt = stmt.where(Client.is_active == is_active)
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def get_by_id(
        self, id: uuid.UUID, org_id: uuid.UUID, *, for_update: bool = False
    ) -> Client | None:
        """Fetch a non-deleted client of the organisation, or ``None`` (also for other tenants).

        Args:
            id: Client to fetch.
            org_id: The caller's organisation.
            for_update: Lock the row until the transaction ends and read its latest committed
                values, so a rule judged on them still holds when the change is written.
        """
        stmt = select(Client).where(
            Client.id == id,
            Client.organization_id == org_id,
            Client.is_deleted.is_(False),
        )
        if for_update:
            stmt = stmt.with_for_update().execution_options(populate_existing=True)
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def create(self, org_id: uuid.UUID, data: dict[str, Any]) -> Client:
        """Insert a client in the organisation.

        Args:
            org_id: Organisation that will own the client.
            data: Column values for the new row.

        Returns:
            The persisted client.
        """
        client = Client(id=uuid.uuid4(), organization_id=org_id, **data)
        self.session.add(client)
        await self.session.flush()
        await self.session.refresh(client)
        return client

    async def update(self, id: uuid.UUID, org_id: uuid.UUID, **fields: Any) -> Client:
        """Update columns of a client of the organisation.

        Args:
            id: Client to update.
            org_id: The caller's organisation.
            **fields: Column values to set.

        Returns:
            The refreshed client.

        Raises:
            NotFoundError: When the client is not in the organisation.
        """
        return await self._update_scoped(id, org_id, **fields)

    async def soft_delete(self, id: uuid.UUID, org_id: uuid.UUID) -> None:
        """Mark a client deleted; projects and users keep their reference.

        Args:
            id: Client to delete.
            org_id: The caller's organisation.
        """
        await self._soft_delete_scoped(id, org_id)

    async def deactivate_client_users(self, client_id: uuid.UUID) -> None:
        """Deactivate every non-deleted user of a client so they can no longer sign in.

        A pending (never activated) portal user is skipped: only accepting an invitation changes
        a pending account, and ``activated_at`` is never written here (FR-226).

        Not scoped by organisation: pass a ``client_id`` the service already verified belongs
        to the caller's organisation.

        Args:
            client_id: The verified client.
        """
        await self.session.execute(
            update(User)
            .where(
                User.client_id == client_id,
                User.is_deleted.is_(False),
                User.activated_at.is_not(None),
            )
            .values(is_active=False)
        )
        await self.session.flush()

    async def list_users(self, client_id: uuid.UUID, org_id: uuid.UUID) -> list[User]:
        """List the non-deleted user accounts attached to a client.

        Args:
            client_id: The client.
            org_id: The caller's organisation.

        Returns:
            The client's portal users in ascending id order. The order is fixed so that callers
            that lock or update these users row by row always take them in the same order.
        """
        result = await self.session.execute(
            select(User)
            .where(
                User.client_id == client_id,
                User.organization_id == org_id,
                User.is_deleted.is_(False),
            )
            .order_by(User.id)
        )
        return list(result.scalars().all())

    async def count_non_deleted_projects(self, client_id: uuid.UUID) -> int:
        """Count a client's projects that are not soft-deleted.

        Used to refuse deleting a client that is still in use. Not scoped by organisation:
        pass a ``client_id`` the service already verified.

        Args:
            client_id: The verified client.

        Returns:
            The number of live projects.
        """
        result = await self.session.execute(
            select(func.count()).where(
                Project.client_id == client_id,
                Project.is_deleted.is_(False),
            )
        )
        return result.scalar_one()
