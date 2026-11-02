"""Persistence of sites.

Layer: repositories. Depends on ``hds.models`` and ``hds.repositories.base`` only; it MUST NOT
import services or routers.
"""

import uuid
from typing import Any

from sqlalchemy import select

from hds.models.site import Site
from hds.repositories.base import OrgScopedRepository


class SiteRepository(OrgScopedRepository[Site]):
    """Queries and writes for ``Site``.

    The API offers no site deletion; sites are only created and edited.
    """

    model = Site

    async def list_by_org(self, org_id: uuid.UUID) -> list[Site]:
        """List the organisation's sites ordered by name.

        Args:
            org_id: The caller's organisation.
        """
        result = await self.session.execute(
            select(Site).where(Site.organization_id == org_id).order_by(Site.name)
        )
        return list(result.scalars().all())

    async def create(self, org_id: uuid.UUID, data: dict[str, Any]) -> Site:
        """Insert a site in the organisation and commit.

        Args:
            org_id: Organisation that will own the site.
            data: Column values for the new row.

        Returns:
            The persisted site.
        """
        site = Site(id=uuid.uuid4(), organization_id=org_id, **data)
        self.session.add(site)
        await self.session.commit()
        await self.session.refresh(site)
        return site

    async def update(self, id: uuid.UUID, org_id: uuid.UUID, **fields: Any) -> Site:
        """Update columns of a site of the organisation and commit.

        Args:
            id: Site to update.
            org_id: The caller's organisation.
            **fields: Column values to set.

        Returns:
            The refreshed site.

        Raises:
            NotFoundError: When the site is not in the organisation.
        """
        return await self._update_scoped(id, org_id, **fields)
