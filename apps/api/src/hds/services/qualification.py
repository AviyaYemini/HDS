"""Qualification service: create, rename, delete; manage employee qualifications."""

import uuid

from hds.core.authz import MANAGER_ROLES, ensure_role
from hds.core.exceptions import ConflictError, NotFoundError
from hds.models.qualification import Qualification
from hds.models.user import User
from hds.repositories.qualification import QualificationRepository


class QualificationService:
    def __init__(self, qual_repo: QualificationRepository) -> None:
        self._repo = qual_repo

    async def create_qualification(self, actor: User, name: str) -> Qualification:
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
        ensure_role(actor, *MANAGER_ROLES)
        qual = await self._repo.get_by_id(qual_id)
        if qual is None:
            raise NotFoundError(resource="Qualification", id=qual_id)
        conflict = await self._repo.get_by_name_and_org(new_name, actor.organization_id)
        if conflict is not None and conflict.id != qual_id:
            raise ConflictError(
                code="qualifications.duplicate_name",
                detail=f"A qualification named '{new_name}' already exists.",
            )
        return await self._repo.update(qual_id, name=new_name)

    async def delete_qualification(self, actor: User, qual_id: uuid.UUID) -> None:
        ensure_role(actor, *MANAGER_ROLES)
        qual = await self._repo.get_by_id(qual_id)
        if qual is None:
            raise NotFoundError(resource="Qualification", id=qual_id)
        await self._repo.delete(qual_id)

    async def replace_employee_qualifications(
        self, actor: User, user_id: uuid.UUID, qual_ids: list[uuid.UUID]
    ) -> None:
        ensure_role(actor, *MANAGER_ROLES)
        await self._repo.replace_user_qualifications(user_id, qual_ids)
