"""Persistence of projects.

Layer: repositories. Depends on ``hds.models`` and ``hds.repositories.base`` only; it MUST NOT
import services or routers.
"""

import uuid
from collections.abc import Collection
from typing import Any

from sqlalchemy import select

from hds.models.project import Project, ProjectStatus
from hds.repositories.base import OrgScopedRepository


class ProjectRepository(OrgScopedRepository[Project]):
    """Queries and writes for ``Project``. Soft-deleted projects are invisible to lookups."""

    model = Project

    async def list_by_org(
        self, org_id: uuid.UUID, status: ProjectStatus | None = None
    ) -> list[Project]:
        """List every client's non-deleted projects in the organisation (staff only).

        It takes no client on purpose: an optional client that means "every client" when empty
        let a Client account with no client see all projects (003 FR-006). One client's projects
        come from ``list_by_client``.

        Args:
            org_id: The caller's organisation.
            status: Keep only projects in this lifecycle state.

        Returns:
            Matching projects in unspecified order.
        """
        stmt = select(Project).where(
            Project.organization_id == org_id,
            Project.is_deleted.is_(False),
        )
        if status is not None:
            stmt = stmt.where(Project.status == status)
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def list_by_client(
        self, client_id: uuid.UUID, org_id: uuid.UUID, status: ProjectStatus | None = None
    ) -> list[Project]:
        """List a client's non-deleted projects; the data scope for the Client role.

        The client is required, so this read cannot mean "every client" (003 FR-006).

        Args:
            client_id: The client whose projects to return.
            org_id: The caller's organisation.
            status: Keep only projects in this lifecycle state.

        Returns:
            Matching projects in unspecified order.
        """
        stmt = select(Project).where(
            Project.client_id == client_id,
            Project.organization_id == org_id,
            Project.is_deleted.is_(False),
        )
        if status is not None:
            stmt = stmt.where(Project.status == status)
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def lock(self, id: uuid.UUID, org_id: uuid.UUID) -> bool:
        """Lock a non-deleted project of the organisation until the transaction ends.

        ``FOR NO KEY UPDATE``, so foreign-key checks of rows that point at the project do not
        wait for it. Two replacements of the project's shift types then run one after the other.

        Returns:
            Whether the project exists in the organisation.
        """
        result = await self.session.execute(
            select(Project.id)
            .where(
                Project.id == id,
                Project.organization_id == org_id,
                Project.is_deleted.is_(False),
            )
            .with_for_update(key_share=True)
        )
        return result.first() is not None

    async def lock_for_shifts(self, org_id: uuid.UUID, ids: Collection[uuid.UUID] | None) -> None:
        """Lock the organisation's projects (``ids`` only, when given) in id order, ``FOR UPDATE``.

        The full lock, unlike ``lock``: a new shift's foreign key takes a key-share lock on its
        project, so creating a shift in one of them waits too. Accepting a schedule proposal holds
        these while it checks the proposal is current and writes it (004 research G9).
        """
        query = select(Project.id).where(
            Project.organization_id == org_id, Project.is_deleted.is_(False)
        )
        if ids is not None:
            query = query.where(Project.id.in_(list(ids)))
        await self.session.execute(query.order_by(Project.id).with_for_update())

    async def lock_and_read(self, id: uuid.UUID, org_id: uuid.UUID) -> Project | None:
        """Lock a non-deleted project of the organisation as ``lock`` does, and read it afresh.

        The row is read again under the lock (``populate_existing``), so a copy the session
        loaded earlier in the request cannot hide a change another transaction committed while
        this one waited (005 Group C race tests).
        """
        result = await self.session.execute(
            select(Project)
            .where(
                Project.id == id,
                Project.organization_id == org_id,
                Project.is_deleted.is_(False),
            )
            .with_for_update(key_share=True)
            .execution_options(populate_existing=True)
        )
        return result.scalar_one_or_none()

    async def get_by_id(self, id: uuid.UUID, org_id: uuid.UUID) -> Project | None:
        """Fetch a non-deleted project of the organisation, or ``None`` (also for other tenants)."""
        result = await self.session.execute(
            select(Project).where(
                Project.id == id,
                Project.organization_id == org_id,
                Project.is_deleted.is_(False),
            )
        )
        return result.scalar_one_or_none()

    async def create(self, org_id: uuid.UUID, data: dict[str, Any]) -> Project:
        """Insert a project in the organisation.

        Args:
            org_id: Organisation that will own the project.
            data: Column values; ``billing_rate`` must be a positive ``Decimal``.

        Returns:
            The persisted project.

        Raises:
            sqlalchemy.exc.IntegrityError: If the billing rate is not positive. The error
                handler maps it to HTTP 422.
        """
        project = Project(id=uuid.uuid4(), organization_id=org_id, **data)
        self.session.add(project)
        await self.session.flush()
        await self.session.refresh(project)
        return project

    async def update(self, id: uuid.UUID, org_id: uuid.UUID, **fields: Any) -> Project:
        """Update columns of a project of the organisation.

        Args:
            id: Project to update.
            org_id: The caller's organisation.
            **fields: Column values to set.

        Returns:
            The refreshed project.

        Raises:
            NotFoundError: When the project is not in the organisation.
        """
        return await self._update_scoped(id, org_id, **fields)

    async def soft_delete(self, id: uuid.UUID, org_id: uuid.UUID) -> None:
        """Mark a project deleted.

        Args:
            id: Project to delete.
            org_id: The caller's organisation.
        """
        await self._soft_delete_scoped(id, org_id)
