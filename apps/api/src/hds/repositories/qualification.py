"""Persistence of qualifications and their assignment to employees.

Layer: repositories. Depends on ``hds.models`` and ``hds.repositories.base`` only; it MUST NOT
import services or routers.
"""

import uuid
from typing import Any

from sqlalchemy import delete, select

from hds.models.qualification import Qualification, user_qualifications
from hds.repositories.base import OrgScopedRepository


class QualificationRepository(OrgScopedRepository[Qualification]):
    """Queries and writes for ``Qualification`` and the ``user_qualifications`` link table."""

    model = Qualification

    async def list_by_org(self, org_id: uuid.UUID) -> list[Qualification]:
        """List the organisation's qualifications ordered by name.

        Args:
            org_id: The caller's organisation.
        """
        result = await self.session.execute(
            select(Qualification)
            .where(Qualification.organisation_id == org_id)
            .order_by(Qualification.name)
        )
        return list(result.scalars().all())

    async def get_by_name_and_org(self, name: str, org_id: uuid.UUID) -> Qualification | None:
        """Find a qualification by exact name, used to detect duplicates before inserting.

        Args:
            name: Qualification name, compared case-sensitively.
            org_id: The caller's organisation.

        Returns:
            The qualification, or ``None``.
        """
        result = await self.session.execute(
            select(Qualification).where(
                Qualification.name == name,
                Qualification.organisation_id == org_id,
            )
        )
        return result.scalar_one_or_none()

    async def create(self, org_id: uuid.UUID, name: str) -> Qualification:
        """Insert a qualification.

        Args:
            org_id: Organisation that will own it.
            name: Qualification name; unique within the organisation.

        Returns:
            The persisted qualification.

        Raises:
            sqlalchemy.exc.IntegrityError: If the name already exists in the organisation.
        """
        qual = Qualification(id=uuid.uuid4(), organisation_id=org_id, name=name)
        self.session.add(qual)
        await self.session.flush()
        await self.session.refresh(qual)
        return qual

    async def update(self, id: uuid.UUID, org_id: uuid.UUID, **fields: Any) -> Qualification:
        """Update columns of a qualification of the organisation.

        Args:
            id: Qualification to update.
            org_id: The caller's organisation.
            **fields: Column values to set.

        Returns:
            The refreshed qualification.

        Raises:
            NotFoundError: When it is not in the organisation.
        """
        return await self._update_scoped(id, org_id, **fields)

    async def delete(self, id: uuid.UUID, org_id: uuid.UUID) -> None:
        """Hard-delete a qualification; assignments go with it, the users stay.

        Args:
            id: Qualification to delete.
            org_id: The caller's organisation.
        """
        await self._delete_scoped(id, org_id)

    async def get_qualifications_for_user(
        self, user_id: uuid.UUID, org_id: uuid.UUID
    ) -> list[Qualification]:
        """List the qualifications a user holds within the organisation.

        Args:
            user_id: The user.
            org_id: The caller's organisation.
        """
        result = await self.session.execute(
            select(Qualification)
            .join(
                user_qualifications,
                Qualification.id == user_qualifications.c.qualification_id,
            )
            .where(
                user_qualifications.c.user_id == user_id,
                Qualification.organisation_id == org_id,
            )
        )
        return list(result.scalars().all())

    async def replace_user_qualifications(
        self, user_id: uuid.UUID, qual_ids: list[uuid.UUID]
    ) -> None:
        """Make the user hold exactly ``qual_ids`` within the request's transaction.

        No organisation check happens here: the service must already have verified that the
        user and every qualification belong to the caller's organisation (NFR-001).

        Args:
            user_id: The verified user.
            qual_ids: The complete new set; an empty list removes all.
        """
        await self.session.execute(
            delete(user_qualifications).where(user_qualifications.c.user_id == user_id)
        )
        for qid in qual_ids:
            await self.session.execute(
                user_qualifications.insert().values(user_id=user_id, qualification_id=qid)
            )
        await self.session.flush()
