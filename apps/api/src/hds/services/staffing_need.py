"""Staffing needs: how many people each project needs on each shift type and weekday.

Layer: services. Depends on ``hds.core`` (errors, roles), ``hds.models`` and the repositories it is
given; it MUST NOT import routers or know about HTTP.

Rules (004 FR-001 to FR-003): Managers and Admins read and replace a project's needs as a whole.
A need's shift type must be one the project uses and not retired, and a (shift type, weekday)
appears once. The generator makes its slots from these needs.
"""

import uuid
from collections.abc import Mapping, Sequence
from typing import Any

from hds.core.authz import MANAGER_ROLES, ensure_role
from hds.core.exceptions import DomainValidationError, NotFoundError
from hds.models.staffing_need import StaffingNeed
from hds.models.user import User
from hds.repositories.project import ProjectRepository
from hds.repositories.shift_type import ShiftTypeRepository
from hds.repositories.staffing_need import StaffingNeedRepository


class StaffingNeedService:
    """Read and replace projects' staffing needs."""

    def __init__(
        self,
        need_repo: StaffingNeedRepository,
        shift_type_repo: ShiftTypeRepository,
        project_repo: ProjectRepository,
    ) -> None:
        """Wire the service to its repositories.

        Args:
            need_repo: Staffing needs.
            shift_type_repo: Shift types and project shift types.
            project_repo: Projects, to check and lock one in the organisation.
        """
        self._needs = need_repo
        self._types = shift_type_repo
        self._projects = project_repo

    async def list_needs(self, actor: User, project_id: uuid.UUID) -> list[StaffingNeed]:
        """A project's needs (Manager or Admin).

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
            NotFoundError: The project is not in the organisation (HTTP 404).
        """
        ensure_role(actor, *MANAGER_ROLES)
        if await self._projects.get_by_id(project_id, actor.organisation_id) is None:
            raise NotFoundError(resource="Project", id=project_id)
        return await self._needs.for_project(project_id)

    async def replace_needs(
        self, actor: User, project_id: uuid.UUID, items: Sequence[Mapping[str, Any]]
    ) -> list[StaffingNeed]:
        """Replace a project's needs (Manager or Admin).

        The project's row is locked first, as replacing its shift types does, so the two run one
        after the other; the named types are share-locked, so a retirement waits. A type the
        project does not use, including another organisation's, is refused rather than reported as
        missing, so another tenant's ids reveal nothing.

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
            NotFoundError: The project is not in the organisation (HTTP 404).
            DomainValidationError: ``requirements.duplicate``,
                ``requirements.shift_type_not_in_project`` or ``requirements.shift_type_retired``
                (HTTP 422); nothing changes.
        """
        ensure_role(actor, *MANAGER_ROLES)
        if not await self._projects.lock(project_id, actor.organisation_id):
            raise NotFoundError(resource="Project", id=project_id)
        pairs = [(item["shift_type_id"], item["weekday"]) for item in items]
        if len(set(pairs)) != len(pairs):
            raise DomainValidationError(
                code="requirements.duplicate",
                detail="A shift type and weekday may appear only once.",
            )
        named = list(dict.fromkeys(item["shift_type_id"] for item in items))
        # Share-lock the types named here and those the project's needs use now, before any need
        # row is touched: a retirement takes its type's row lock first too, so the two always lock
        # in the same order and cannot deadlock.
        held = await self._needs.types_of(project_id)
        found = {
            t.id: t for t in await self._types.share_lock(actor.organisation_id, [*named, *held])
        }
        used = {t.id for t in await self._types.for_project(project_id)}
        for type_id in named:  # request order, so the first problem is the one reported
            if type_id in found and not found[type_id].is_active:
                raise DomainValidationError(
                    code="requirements.shift_type_retired",
                    detail="A retired shift type cannot have staffing needs.",
                )
            if type_id not in used:
                raise DomainValidationError(
                    code="requirements.shift_type_not_in_project",
                    detail="The project does not use this shift type.",
                )
        await self._needs.replace(actor.organisation_id, project_id, items)
        return await self._needs.for_project(project_id)
