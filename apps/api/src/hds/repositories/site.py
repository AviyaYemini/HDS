import uuid
from typing import Any

from sqlalchemy import select, update

from hds.models.site import Site
from hds.repositories.base import OrgScopedRepository


class SiteRepository(OrgScopedRepository[Site]):
    model = Site

    async def list_by_org(self, org_id: uuid.UUID) -> list[Site]:
        result = await self.session.execute(
            select(Site).where(Site.organization_id == org_id).order_by(Site.name)
        )
        return list(result.scalars().all())

    async def create(self, org_id: uuid.UUID, data: dict[str, Any]) -> Site:
        site = Site(id=uuid.uuid4(), organization_id=org_id, **data)
        self.session.add(site)
        await self.session.commit()
        await self.session.refresh(site)
        return site

    async def update(self, id: uuid.UUID, **fields: Any) -> Site:
        await self.session.execute(update(Site).where(Site.id == id).values(**fields))
        await self.session.commit()
        site = await self._get_unscoped(id)
        assert site is not None
        return site
