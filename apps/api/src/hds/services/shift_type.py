"""Shift types: an organisation's kinds of shift, and which of them each project uses.

Layer: services. Depends on ``hds.core`` (errors, roles), ``hds.models`` and the repositories it is
given; it MUST NOT import routers or know about HTTP.

Rules (003 FR-110 to FR-112): everyone who works shifts reads the types; Managers and Admins add,
change and retire them, and choose a project's set. A type crosses midnight exactly when its end is
not after its start, so that is computed, never sent. Changing a type does not move shifts already
made from it, which keep their own times.
"""

import uuid
from datetime import time
from typing import Any

from hds.core.authz import MANAGER_ROLES, STAFF_ROLES, ensure_role
from hds.core.exceptions import DomainValidationError, NotFoundError
from hds.models.shift_type import ShiftType
from hds.models.user import User
from hds.repositories.project import ProjectRepository
from hds.repositories.shift_type import ShiftTypeRepository


def _refuse_equal_times(starts: time, ends: time) -> None:
    """Refuse a type that starts and ends at the same time, almost always a slip, not 24 hours."""
    if starts == ends:
        raise DomainValidationError(
            code="shift_types.times_equal",
            detail="A shift type must not start and end at the same time.",
        )


class ShiftTypeService:
    """Shift types and project shift types."""

    def __init__(
        self, shift_type_repo: ShiftTypeRepository, project_repo: ProjectRepository
    ) -> None:
        """Wire the service to its repositories.

        Args:
            shift_type_repo: Shift types and project shift types.
            project_repo: Projects, to check one is in the organisation.
        """
        self._types = shift_type_repo
        self._projects = project_repo

    async def list_types(self, actor: User) -> list[ShiftType]:
        """Every shift type of the organisation, retired ones included and marked.

        Raises:
            ForbiddenError: For a Client account (HTTP 403).
        """
        ensure_role(actor, *STAFF_ROLES)
        return await self._types.list_for_org(actor.organization_id)

    async def add_type(self, actor: User, data: dict[str, Any]) -> ShiftType:
        """Add a shift type (Manager or Admin).

        Args:
            actor: The caller.
            data: ``name``, ``starts_at_local`` and ``ends_at_local``.

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
            ConflictError: ``shift_types.duplicate_name`` (HTTP 409), from the database's unique
                index on active names, so two requests at once cannot both take a name.
        """
        ensure_role(actor, *MANAGER_ROLES)
        _refuse_equal_times(data["starts_at_local"], data["ends_at_local"])
        crosses = data["ends_at_local"] <= data["starts_at_local"]
        return await self._types.create(
            actor.organization_id, {**data, "crosses_midnight": crosses}
        )

    async def change_type(self, actor: User, id: uuid.UUID, data: dict[str, Any]) -> ShiftType:
        """Rename, retime, retire or bring back a shift type (Manager or Admin).

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
            NotFoundError: The type is not in the organisation (HTTP 404).
            ConflictError: ``shift_types.duplicate_name`` when the name, or bringing a retired type
                back, clashes with an active type (HTTP 409).
        """
        ensure_role(actor, *MANAGER_ROLES)
        # Locked, so two changes at once cannot each compute crossing from stale times.
        existing = await self._types.get_locked(id, actor.organization_id)
        if existing is None:
            raise NotFoundError(resource="ShiftType", id=id)
        fields = dict(data)
        if "starts_at_local" in data or "ends_at_local" in data:
            starts = data.get("starts_at_local", existing.starts_at_local)
            ends = data.get("ends_at_local", existing.ends_at_local)
            _refuse_equal_times(starts, ends)
            fields["crosses_midnight"] = ends <= starts
        return await self._types.update(id, actor.organization_id, **fields)

    async def _project(self, actor: User, project_id: uuid.UUID) -> None:
        if await self._projects.get_by_id(project_id, actor.organization_id) is None:
            raise NotFoundError(resource="Project", id=project_id)

    async def project_types(self, actor: User, project_id: uuid.UUID) -> list[ShiftType]:
        """The shift types a project uses (Manager or Admin).

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
            NotFoundError: The project is not in the organisation (HTTP 404).
        """
        ensure_role(actor, *MANAGER_ROLES)
        await self._project(actor, project_id)
        return await self._types.for_project(project_id)

    async def set_project_types(
        self, actor: User, project_id: uuid.UUID, ids: list[uuid.UUID]
    ) -> list[ShiftType]:
        """Replace the set of shift types a project uses (Manager or Admin).

        Removing a type from a project leaves the shifts already made from it.

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
            NotFoundError: The project, or one of the types, is not in the organisation (HTTP 404).
            DomainValidationError: ``shift_types.retired`` for a retired type (HTTP 422).
        """
        ensure_role(actor, *MANAGER_ROLES)
        # The project's row first, so two replacements of its set run one after the other.
        if not await self._projects.lock(project_id, actor.organization_id):
            raise NotFoundError(resource="Project", id=project_id)
        found = {t.id: t for t in await self._types.share_lock(actor.organization_id, ids)}
        for type_id in ids:
            if type_id not in found:
                raise NotFoundError(resource="ShiftType", id=type_id)
            if not found[type_id].is_active:
                raise DomainValidationError(
                    code="shift_types.retired", detail="A retired shift type cannot be added."
                )
        await self._types.set_for_project(project_id, ids)
        return await self._types.for_project(project_id)
