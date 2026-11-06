"""Project service: create, edit, manage status, soft-delete; enforce rate and date rules.

Layer: services. May import ``hds.repositories``, ``hds.models`` and ``hds.core``; MUST NOT
import routers or build HTTP responses (Constitution II).

Rules enforced here: billing rate above zero, end date not before start date, and client and
site ids that belong to the caller's organisation. Clients see only their own projects.
"""

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
from hds.services.pay_settings import PaySettingsService


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
    """Project administration and role-scoped project reads."""

    def __init__(
        self,
        project_repo: ProjectRepository,
        client_repo: ClientRepository,
        site_repo: SiteRepository,
        pay_settings: PaySettingsService,
    ) -> None:
        """Wire the service.

        Args:
            project_repo: Project storage.
            client_repo: Used to verify client ids.
            site_repo: Used to verify site ids.
            pay_settings: Checks the pay settings sent with a new project (Admin only) and
                freezes a project's pay when it ends (005 FR-006a, FR-007).
        """
        self._repo = project_repo
        self._client_repo = client_repo
        self._site_repo = site_repo
        self._pay = pay_settings

    async def _require_client(self, client_id: uuid.UUID, org_id: uuid.UUID) -> None:
        if await self._client_repo.get_by_id(client_id, org_id) is None:
            raise NotFoundError(resource="Client", id=client_id)

    async def _require_site(self, site_id: uuid.UUID, org_id: uuid.UUID) -> None:
        if await self._site_repo.get_by_id(site_id, org_id) is None:
            raise NotFoundError(resource="Site", id=site_id)

    async def create_project(self, actor: User, data: dict[str, Any]) -> Project:
        """Create a project for a client and site of the actor's organisation (Manager+).

        Args:
            actor: The caller.
            data: Project fields including ``client_id`` and ``site_id``.

        Returns:
            The new project.

        Raises:
            ForbiddenError: ``users.manager_required``, or ``projects.pay_settings_admin_only``
                when a Manager sends pay settings (HTTP 403).
            DomainValidationError: ``projects.billing_rate_invalid``,
                ``projects.date_range_invalid``, ``pay.settings_invalid`` or
                ``pay.rest_window_invalid`` (HTTP 422).
            NotFoundError: Client or site not in the organisation (HTTP 404).
        """
        ensure_role(actor, *MANAGER_ROLES)
        data = dict(data)
        pay = data.pop("pay_settings", None)
        if pay is not None:
            data.update(self._pay.columns_for_new_project(actor, pay))
        if data.get("billing_rate") is not None:
            _validate_billing_rate(data["billing_rate"])
        _validate_date_range(data.get("start_date"), data.get("end_date"))
        await self._require_client(data["client_id"], actor.organization_id)
        await self._require_site(data["site_id"], actor.organization_id)
        return await self._repo.create(actor.organization_id, data)

    async def edit_project(self, actor: User, id: uuid.UUID, data: dict[str, Any]) -> Project:
        """Update a project (Manager or Admin); the date rule is checked against stored dates.

        Args:
            actor: The caller.
            id: Project to edit.
            data: Fields to change.

        Returns:
            The updated project.

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
            NotFoundError: The project is not in the organisation (HTTP 404).
            DomainValidationError: ``projects.billing_rate_invalid`` or
                ``projects.date_range_invalid`` (HTTP 422).
        """
        ensure_role(actor, *MANAGER_ROLES)
        existing = await self._repo.get_by_id(id, actor.organization_id)
        if existing is None:
            raise NotFoundError(resource="Project", id=id)
        if "billing_rate" in data:
            _validate_billing_rate(data["billing_rate"])
        _validate_date_range(existing.start_date, data.get("end_date", existing.end_date))
        return await self._repo.update(id, actor.organization_id, **data)

    async def set_status(self, actor: User, id: uuid.UUID, status: str) -> Project:
        """Move a project between ``active`` and ``ended`` (Manager or Admin).

        Args:
            actor: The caller.
            id: Project to change.
            status: New status value.

        Returns:
            The updated project.

        Ending a project freezes its pay settings and special days first (005 FR-007).

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
            NotFoundError: The project is not in the organisation (HTTP 404).
        """
        ensure_role(actor, *MANAGER_ROLES)
        existing = await self._repo.get_by_id(id, actor.organization_id)
        if existing is None:
            raise NotFoundError(resource="Project", id=id)
        if status == ProjectStatus.ended:
            await self._pay.freeze(actor.organization_id, id)  # 005 FR-007, under the locks
        return await self._repo.update(id, actor.organization_id, status=status)

    async def change_project(
        self, actor: User, id: uuid.UUID, data: dict[str, Any], status: str | None
    ) -> Project:
        """Change a project's fields and, when given, its status, in a safe order (Manager+).

        The status goes first: ending a project locks the organisation and then the project
        (005 FR-007), the order schedule accept uses, so it must never follow an update that has
        already locked the project.

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
            NotFoundError: The project is not in the organisation (HTTP 404).
            DomainValidationError: As ``edit_project`` (HTTP 422).
        """
        if status is None:
            return await self.edit_project(actor, id, data)
        project = await self.set_status(actor, id, status)
        if data:
            project = await self.edit_project(actor, id, data)
        return project

    async def soft_delete(self, actor: User, id: uuid.UUID) -> None:
        """Delete a project (Admin only); the row is kept and hidden.

        Deleting freezes the project's pay first, as ending does (005 FR-007): its shifts still
        count in pay to date and the reports, so later changes to the defaults or the
        organisation's special days must not reach them.

        Args:
            actor: The caller.
            id: Project to delete.

        Raises:
            ForbiddenError: ``users.admin_required`` (HTTP 403).
            NotFoundError: The project is not in the organisation (HTTP 404).
        """
        ensure_role(actor, *ADMIN_ROLES)
        existing = await self._repo.get_by_id(id, actor.organization_id)
        if existing is None:
            raise NotFoundError(resource="Project", id=id)
        await self._pay.freeze(actor.organization_id, id)  # under the locks, org then project
        await self._repo.soft_delete(id, actor.organization_id)

    async def list_projects(
        self,
        org_id: uuid.UUID,
        current_user: User,
        status: ProjectStatus | None = None,
        client_id: uuid.UUID | None = None,
    ) -> list[Project]:
        """List projects visible to the caller.

        A Client always sees only their own client's projects, whatever filter they send, and an
        account with no client sees none (FR-237); this method, not the router, enforces that data
        scope. Employees are rejected earlier by the router dependency.

        Args:
            org_id: The caller's organisation.
            current_user: The caller.
            status: Keep only this status.
            client_id: Keep only this client's projects (ignored for a Client caller).

        Returns:
            The visible projects.

        Raises:
            NotFoundError: ``client_id`` is not in the organisation (HTTP 404).
        """
        if current_user.role == UserRole.client:
            if current_user.client_id is None:
                return []  # an account with no client sees no project
            return await self._repo.list_by_client(current_user.client_id, org_id, status=status)
        if client_id is not None:
            await self._require_client(client_id, org_id)
            return await self._repo.list_by_client(client_id, org_id, status=status)
        return await self._repo.list_by_org(org_id, status=status)

    async def get_project(self, id: uuid.UUID, org_id: uuid.UUID, current_user: User) -> Project:
        """Return one project, refusing a Client access to another client's project.

        Args:
            id: Project to load.
            org_id: The caller's organisation.
            current_user: The caller.

        Returns:
            The project.

        Raises:
            NotFoundError: Missing, in another organisation, or the caller is a Client account
                with no client (HTTP 404).
            ForbiddenError: ``projects.access_denied`` when a Client asks for a project of a
                different client (HTTP 403).
        """
        if current_user.role == UserRole.client and current_user.client_id is None:
            raise NotFoundError(resource="Project", id=id)
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
