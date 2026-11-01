"""Project service: create, edit, manage status, soft-delete; enforce rate and date rules."""

import uuid
from datetime import date
from decimal import Decimal
from typing import Any

from hds.core.authz import ADMIN_ROLES, MANAGER_ROLES, ensure_role
from hds.core.exceptions import DomainValidationError, ForbiddenError, NotFoundError
from hds.models.project import Project, ProjectStatus
from hds.models.user import User, UserRole
from hds.repositories.client import ClientRepository
from hds.repositories.project import ProjectRepository
from hds.repositories.site import SiteRepository


def _validate_billing_rate(rate: Any) -> None:
    if Decimal(str(rate)) <= Decimal("0"):
        raise DomainValidationError(
            code="projects.billing_rate_invalid",
            detail="billing_rate must be greater than 0.",
        )


def _validate_date_range(start: date | None, end: date | None) -> None:
    if start is not None and end is not None and end < start:
        raise DomainValidationError(
            code="projects.date_range_invalid",
            detail="end_date must not be earlier than start_date.",
        )


class ProjectService:
    def __init__(
        self,
        project_repo: ProjectRepository,
        client_repo: ClientRepository,
        site_repo: SiteRepository,
    ) -> None:
        self._repo = project_repo
        self._client_repo = client_repo
        self._site_repo = site_repo

    async def _require_client(self, client_id: uuid.UUID, org_id: uuid.UUID) -> None:
        if await self._client_repo.get_by_id(client_id, org_id) is None:
            raise NotFoundError(resource="Client", id=client_id)

    async def _require_site(self, site_id: uuid.UUID, org_id: uuid.UUID) -> None:
        if await self._site_repo.get_by_id(site_id, org_id) is None:
            raise NotFoundError(resource="Site", id=site_id)

    async def create_project(self, actor: User, data: dict[str, Any]) -> Project:
        ensure_role(actor, *MANAGER_ROLES)
        if data.get("billing_rate") is not None:
            _validate_billing_rate(data["billing_rate"])
        _validate_date_range(data.get("start_date"), data.get("end_date"))
        await self._require_client(data["client_id"], actor.organization_id)
        await self._require_site(data["site_id"], actor.organization_id)
        return await self._repo.create(actor.organization_id, data)

    async def edit_project(self, actor: User, id: uuid.UUID, data: dict[str, Any]) -> Project:
        ensure_role(actor, *MANAGER_ROLES)
        existing = await self._repo.get_by_id(id, actor.organization_id)
        if existing is None:
            raise NotFoundError(resource="Project", id=id)
        if "billing_rate" in data:
            _validate_billing_rate(data["billing_rate"])
        _validate_date_range(existing.start_date, data.get("end_date", existing.end_date))
        return await self._repo.update(id, **data)

    async def set_status(self, actor: User, id: uuid.UUID, status: str) -> Project:
        ensure_role(actor, *MANAGER_ROLES)
        existing = await self._repo.get_by_id(id, actor.organization_id)
        if existing is None:
            raise NotFoundError(resource="Project", id=id)
        return await self._repo.update(id, status=status)

    async def soft_delete(self, actor: User, id: uuid.UUID) -> None:
        ensure_role(actor, *ADMIN_ROLES)
        existing = await self._repo.get_by_id(id, actor.organization_id)
        if existing is None:
            raise NotFoundError(resource="Project", id=id)
        await self._repo.soft_delete(id)

    async def list_projects(
        self,
        org_id: uuid.UUID,
        current_user: User,
        status: ProjectStatus | None = None,
        client_id: uuid.UUID | None = None,
    ) -> list[Project]:
        if current_user.role == UserRole.client:
            client_id = current_user.client_id
        elif client_id is not None:
            await self._require_client(client_id, org_id)
        return await self._repo.list_by_org(org_id, status=status, client_id=client_id)

    async def get_project(self, id: uuid.UUID, org_id: uuid.UUID, current_user: User) -> Project:
        project = await self._repo.get_by_id(id, org_id)
        if project is None:
            raise NotFoundError(resource="Project", id=id)
        if current_user.role == UserRole.client:
            if project.client_id != current_user.client_id:
                raise ForbiddenError(
                    code="projects.access_denied",
                    detail="Access to this project is not allowed.",
                )
        return project
