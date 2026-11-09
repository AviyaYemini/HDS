"""Managers on projects: who may see a project's pay when it is open to Managers (005 FR-021).

Layer: services. Depends on ``hds.core`` (errors, roles), ``hds.models`` and the repositories it is
given; it MUST NOT import routers or know about HTTP.

Admins replace a project's set; Managers and Admins read it. Each member must be an active Manager
or Admin of the organisation when the set is replaced; whether a Manager sees pay is decided again
on every read from the live role (FR-022), so a demotion takes effect at once.
"""

import uuid
from collections.abc import Sequence

from hds.core.authz import MANAGER_ROLES, ensure_role
from hds.core.exceptions import DomainValidationError, NotFoundError
from hds.models.user import User, UserRole
from hds.repositories.project import ProjectRepository
from hds.repositories.project_manager import ProjectManagerRepository


class ProjectManagerService:
    """Read and replace a project's managers."""

    def __init__(
        self, project_manager_repo: ProjectManagerRepository, project_repo: ProjectRepository
    ) -> None:
        """Wire the service to the managers and the projects."""
        self._managers = project_manager_repo
        self._projects = project_repo

    async def members(self, actor: User, project_id: uuid.UUID) -> list[User]:
        """A project's managers (Manager or Admin).

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
            NotFoundError: The project is not in the organisation (HTTP 404).
        """
        ensure_role(actor, *MANAGER_ROLES)
        if await self._projects.get_by_id(project_id, actor.organisation_id) is None:
            raise NotFoundError(resource="Project", id=project_id)
        return await self._managers.members(actor.organisation_id, project_id)

    async def replace(
        self, actor: User, project_id: uuid.UUID, user_ids: Sequence[uuid.UUID]
    ) -> list[User]:
        """Make ``user_ids`` the project's managers (Admin); repeats count once.

        Raises:
            ForbiddenError: ``users.admin_required`` (HTTP 403).
            NotFoundError: The project is not in the organisation (HTTP 404).
            DomainValidationError: ``project_managers.not_staff`` when one of them is not an
                active Manager or Admin of the organisation (HTTP 422).
        """
        ensure_role(actor, UserRole.admin)
        wanted = list(dict.fromkeys(user_ids))
        if not await self._projects.lock(project_id, actor.organisation_id):
            raise NotFoundError(resource="Project", id=project_id)
        staff = await self._managers.staff_among(actor.organisation_id, wanted)
        if set(wanted) - staff:
            raise DomainValidationError(
                code="project_managers.not_staff",
                detail="Only active Managers and Admins of the organisation can be assigned.",
            )
        await self._managers.replace(actor.organisation_id, project_id, wanted)
        return await self._managers.members(actor.organisation_id, project_id)
