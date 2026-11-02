"""Site service: create and edit sites.

Layer: services. May import ``hds.repositories``, ``hds.models`` and ``hds.core``; MUST NOT
import routers or build HTTP responses (Constitution II).
"""

import uuid
from typing import Any

from hds.core.authz import MANAGER_ROLES, ensure_role
from hds.core.exceptions import NotFoundError
from hds.models.site import Site
from hds.models.user import User
from hds.repositories.site import SiteRepository


class SiteService:
    """Sites. Creating and editing require Manager or Admin; there is no delete."""

    def __init__(self, site_repo: SiteRepository) -> None:
        """Wire the service.

        Args:
            site_repo: Site storage.
        """
        self._repo = site_repo

    async def create_site(self, actor: User, data: dict[str, Any]) -> Site:
        """Create a site in the actor's organisation.

        Args:
            actor: The caller.
            data: Site fields (name, address, IANA time zone).

        Returns:
            The new site.

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
        """
        ensure_role(actor, *MANAGER_ROLES)
        return await self._repo.create(actor.organization_id, data)

    async def edit_site(self, actor: User, id: uuid.UUID, data: dict[str, Any]) -> Site:
        """Update a site of the actor's organisation.

        Args:
            actor: The caller.
            id: Site to edit.
            data: Fields to change.

        Returns:
            The updated site.

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
            NotFoundError: The site is not in the organisation (HTTP 404).
        """
        ensure_role(actor, *MANAGER_ROLES)
        existing = await self._repo.get_by_id(id, actor.organization_id)
        if existing is None:
            raise NotFoundError(resource="Site", id=id)
        return await self._repo.update(id, actor.organization_id, **data)
