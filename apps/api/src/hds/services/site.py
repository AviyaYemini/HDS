"""Site service: create and edit sites.

Layer: services. May import ``hds.repositories``, ``hds.models`` and ``hds.core``; MUST NOT
import routers or build HTTP responses (Constitution II).
"""

import uuid
from typing import Any
from zoneinfo import ZoneInfo

from hds.core.authz import MANAGER_ROLES, ensure_role
from hds.core.exceptions import DomainValidationError, NotFoundError
from hds.models.site import Site
from hds.models.user import User
from hds.repositories.site import SiteRepository


def validate_timezone(name: object) -> None:
    """Refuse anything that is not the name of a time zone the system can load.

    The check is a real load (``ZoneInfo``), not a pattern, so a well-formed but unknown name such
    as ``Mars/Base`` fails, and names with path characters (``../etc/passwd``) are refused.

    Args:
        name: The value offered for a site's time zone.

    Raises:
        DomainValidationError: ``sites.timezone_invalid`` (HTTP 422).
    """
    try:
        if not isinstance(name, str) or not name.strip():
            raise ValueError("empty")
        ZoneInfo(name)
    except (ValueError, OSError, KeyError):  # ZoneInfoNotFoundError is a KeyError
        raise DomainValidationError(
            code="sites.timezone_invalid",
            detail="The time zone must be an IANA name such as Asia/Jerusalem.",
        ) from None


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
            DomainValidationError: ``sites.timezone_invalid`` (HTTP 422) for a time zone that is
                not a real IANA name; nothing is stored.
        """
        ensure_role(actor, *MANAGER_ROLES)
        validate_timezone(data.get("timezone"))
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
            DomainValidationError: ``sites.timezone_invalid`` (HTTP 422) when ``data`` sets a time
                zone that is not a real IANA name. An edit that leaves the zone out is not
                checked, so a site that already holds a bad zone can still be renamed.
        """
        ensure_role(actor, *MANAGER_ROLES)
        if "timezone" in data:
            validate_timezone(data["timezone"])
        existing = await self._repo.get_by_id(id, actor.organization_id)
        if existing is None:
            raise NotFoundError(resource="Site", id=id)
        return await self._repo.update(id, actor.organization_id, **data)
