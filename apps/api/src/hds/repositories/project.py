import uuid
from typing import Any

from sqlalchemy import select

from hds.models.project import Project, ProjectStatus
from hds.repositories.base import OrgScopedRepository


class ProjectRepository(OrgScopedRepository[Project]):
    model = Project

    async def list_by_org(
        self,
        org_id: uuid.UUID,
        status: ProjectStatus | None = None,
        client_id: uuid.UUID | None = None,
    ) -> list[Project]:
        stmt = select(Project).where(
            Project.organization_id == org_id,
            Project.is_deleted.is_(False),
        )
        if status is not None:
            stmt = stmt.where(Project.status == status)
        if client_id is not None:
            stmt = stmt.where(Project.client_id == client_id)
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def list_by_client(self, client_id: uuid.UUID, org_id: uuid.UUID) -> list[Project]:
        result = await self.session.execute(
            select(Project).where(
                Project.client_id == client_id,
                Project.organization_id == org_id,
                Project.is_deleted.is_(False),
            )
        )
        return list(result.scalars().all())

    async def get_by_id(self, id: uuid.UUID, org_id: uuid.UUID) -> Project | None:
        result = await self.session.execute(
            select(Project).where(
                Project.id == id,
                Project.organization_id == org_id,
                Project.is_deleted.is_(False),
            )
        )
        return result.scalar_one_or_none()

    async def create(self, org_id: uuid.UUID, data: dict[str, Any]) -> Project:
        project = Project(id=uuid.uuid4(), organization_id=org_id, **data)
        self.session.add(project)
        await self.session.commit()
        await self.session.refresh(project)
        return project

    async def update(self, id: uuid.UUID, org_id: uuid.UUID, **fields: Any) -> Project:
        return await self._update_scoped(id, org_id, **fields)

    async def soft_delete(self, id: uuid.UUID, org_id: uuid.UUID) -> None:
        await self._soft_delete_scoped(id, org_id)
