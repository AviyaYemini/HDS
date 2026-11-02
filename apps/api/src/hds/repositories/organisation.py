"""Persistence of organisations.

Layer: repositories. Depends on ``hds.models`` and ``hds.repositories.base`` only.
"""

import uuid

from sqlalchemy import select

from hds.models.organisation import Organisation
from hds.repositories.base import BaseRepository


class OrganisationRepository(BaseRepository[Organisation]):
    """Read access to organisations, which are the tenants themselves and so not scoped."""

    model = Organisation

    async def get_by_id(self, id: uuid.UUID) -> Organisation | None:
        """Fetch an organisation by id.

        Args:
            id: Organisation id, taken from the authenticated user, never from a request.

        Returns:
            The organisation, or ``None``.
        """
        result = await self.session.execute(select(Organisation).where(Organisation.id == id))
        return result.scalar_one_or_none()
