"""Persistence of projects.

Layer: repositories. Depends on ``hds.models`` and ``hds.repositories.base`` only; it MUST NOT
import services or routers.
"""

import uuid
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
