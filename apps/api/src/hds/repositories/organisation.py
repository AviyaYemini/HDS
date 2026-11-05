"""Persistence of organisations.

Layer: repositories. Depends on ``hds.models`` and ``hds.repositories.base`` only.
"""

import uuid
from collections.abc import Mapping
from typing import Any

from sqlalchemy import select, update

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

    async def update_limits(self, id: uuid.UUID, limits: Mapping[str, int]) -> Organisation | None:
        """Set the organisation's rest and weekly limits from ``limits``.

        Args:
            id: Organisation id, taken from the authenticated user.
            limits: The fields to change; others stay.

        Returns:
            The organisation as stored, or ``None``.
        """
        values = {f: limits[f] for f in ("min_rest_hours", "max_weekly_hours") if f in limits}
        if not values:
            return await self.get_by_id(id)
        # One UPDATE, atomic on its own. It takes the row's lock until the transaction ends, as
        # window operations do, so the two wait on each other; both are rare and short.
        result = await self.session.execute(
            update(Organisation)
            .where(Organisation.id == id)
            .values(**values)
            .returning(Organisation)
            .execution_options(populate_existing=True)
        )
        return result.scalar_one_or_none()

    async def update_pay(self, id: uuid.UUID, values: Mapping[str, Any]) -> Organisation | None:
        """Set premium default columns (``pay_*``) of the organisation (005 FR-006).

        Args:
            id: Organisation id, taken from the authenticated user.
            values: Column names and values, already checked by the service.

        Returns:
            The organisation as stored, or ``None``.
        """
        if not values:
            return await self.get_by_id(id)
        result = await self.session.execute(
            update(Organisation)
            .where(Organisation.id == id)
            .values(**values)
            .returning(Organisation)
            .execution_options(populate_existing=True)
        )
        return result.scalar_one_or_none()

    async def lock(self, id: uuid.UUID) -> None:
        """Lock the organisation's row until the transaction ends (``FOR NO KEY UPDATE``).

        Accepting a schedule proposal holds it, so a change of the limits waits (004 research G9).
        """
        await self.session.execute(
            select(Organisation.id).where(Organisation.id == id).with_for_update(key_share=True)
        )
