"""Site service: create and edit sites."""

import uuid
from typing import Any

from hds.core.authz import MANAGER_ROLES, ensure_role
from hds.core.exceptions import NotFoundError
from hds.models.site import Site
from hds.models.user import User
from hds.repositories.site import SiteRepository


class SiteService:
    def __init__(self, site_repo: SiteRepository) -> None:
        self._repo = site_repo

    async def create_site(self, actor: User, data: dict[str, Any]) -> Site:
        ensure_role(actor, *MANAGER_ROLES)
        return await self._repo.create(actor.organization_id, data)

    async def edit_site(self, actor: User, id: uuid.UUID, data: dict[str, Any]) -> Site:
        ensure_role(actor, *MANAGER_ROLES)
        existing = await self._repo.get_by_id(id, actor.organization_id)
        if existing is None:
            raise NotFoundError(resource="Site", id=id)
        return await self._repo.update(id, actor.organization_id, **data)
