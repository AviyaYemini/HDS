"""Qualification service: create, rename, delete; manage employee qualifications.

Layer: services. May import ``hds.repositories``, ``hds.models`` and ``hds.core``; MUST NOT
import routers or build HTTP responses (Constitution II).
"""

import uuid

from hds.core.authz import MANAGER_ROLES, ensure_role
from hds.core.exceptions import ConflictError, NotFoundError
from hds.models.qualification import Qualification
from hds.models.user import User
from hds.repositories.qualification import QualificationRepository


class QualificationService:
    """Qualifications. Every operation requires Manager or Admin."""

    def __init__(self, qual_repo: QualificationRepository) -> None:
        """Wire the service.

        Args:
            qual_repo: Qualification storage.
        """
        self._repo = qual_repo

    async def list_qualifications(self, actor: User) -> list[Qualification]:
        """List the organisation's qualifications.

        Args:
            actor: The caller.

        Returns:
            The qualifications, ordered by name.

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
        """
        ensure_role(actor, *MANAGER_ROLES)
        return await self._repo.list_by_org(actor.organization_id)

    async def create_qualification(self, actor: User, name: str) -> Qualification:
        """Create a qualification with a name unique in the organisation.

        Args:
            actor: The caller.
            name: Qualification name.

        Returns:
            The new qualification.

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
            ConflictError: ``qualifications.duplicate_name`` (HTTP 409).
        """
        ensure_role(actor, *MANAGER_ROLES)
        existing = await self._repo.get_by_name_and_org(name, actor.organization_id)
        if existing is not None:
            raise ConflictError(
                code="qualifications.duplicate_name",
                detail=f"A qualification named '{name}' already exists.",
            )
        return await self._repo.create(actor.organization_id, name)

    async def rename_qualification(
        self, actor: User, qual_id: uuid.UUID, new_name: str
    ) -> Qualification:
        """Rename a qualification; renaming to its current name is allowed.

        Args:
            actor: The caller.
            qual_id: Qualification to rename.
            new_name: New name, unique in the organisation.

        Returns:
            The updated qualification.

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
            NotFoundError: Not in the organisation (HTTP 404).
            ConflictError: ``qualifications.duplicate_name`` (HTTP 409).
        """
        ensure_role(actor, *MANAGER_ROLES)
        qual = await self._repo.get_by_id(qual_id, actor.organization_id)
        if qual is None:
            raise NotFoundError(resource="Qualification", id=qual_id)
        conflict = await self._repo.get_by_name_and_org(new_name, actor.organization_id)
        if conflict is not None and conflict.id != qual_id:
            raise ConflictError(
                code="qualifications.duplicate_name",
                detail=f"A qualification named '{new_name}' already exists.",
            )
        return await self._repo.update(qual_id, actor.organization_id, name=new_name)

    async def delete_qualification(self, actor: User, qual_id: uuid.UUID) -> None:
        """Delete a qualification and its assignments; the employees are kept.

        Args:
            actor: The caller.
            qual_id: Qualification to delete.

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
            NotFoundError: Not in the organisation (HTTP 404).
        """
        ensure_role(actor, *MANAGER_ROLES)
        qual = await self._repo.get_by_id(qual_id, actor.organization_id)
        if qual is None:
            raise NotFoundError(resource="Qualification", id=qual_id)
        await self._repo.delete(qual_id, actor.organization_id)

    async def replace_employee_qualifications(
        self, actor: User, user_id: uuid.UUID, qual_ids: list[uuid.UUID]
    ) -> None:
        """Set an employee's qualifications to exactly ``qual_ids``.

        Every id is checked against the organisation first, so nothing is written if one is
        foreign (NFR-001). The caller must already have verified ``user_id`` (the employees
        router does).

        Args:
            actor: The caller.
            user_id: Employee whose qualifications are replaced.
            qual_ids: The complete new set; empty removes all.

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
            NotFoundError: A qualification id is not in the organisation (HTTP 404).
        """
        ensure_role(actor, *MANAGER_ROLES)
        for qual_id in qual_ids:
            if await self._repo.get_by_id(qual_id, actor.organization_id) is None:
                raise NotFoundError(resource="Qualification", id=qual_id)
        await self._repo.replace_user_qualifications(user_id, qual_ids)
