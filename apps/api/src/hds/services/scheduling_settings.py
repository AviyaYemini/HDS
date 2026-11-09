"""The organisation's scheduling limits: minimum rest and weekly maximum (004 FR-026).

Layer: services. Depends on ``hds.core`` (errors, roles), ``hds.models`` and the repositories it is
given; it MUST NOT import routers or know about HTTP.

Managers and Admins read the limits; only Admins change them. Generation and the warnings of manual
rostering (003) both read the stored values.
"""

from collections.abc import Mapping

from hds.core.authz import MANAGER_ROLES, ensure_role
from hds.core.exceptions import NotFoundError
from hds.models.organisation import Organisation
from hds.models.user import User, UserRole
from hds.repositories.organisation import OrganisationRepository


class SchedulingSettingsService:
    """Read and change an organisation's rest and weekly limits."""

    def __init__(self, organisation_repo: OrganisationRepository) -> None:
        """Wire the service to the organisations repository."""
        self._orgs = organisation_repo

    async def read(self, actor: User) -> Organisation:
        """The caller's organisation, with its limits (Manager or Admin).

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
        """
        ensure_role(actor, *MANAGER_ROLES)
        org = await self._orgs.get_by_id(actor.organisation_id)
        if org is None:
            raise NotFoundError(resource="Organisation", id=actor.organisation_id)
        return org

    async def change(self, actor: User, data: Mapping[str, int]) -> Organisation:
        """Change either limit (Admin); the schema bounds them to 1 to 24 and 1 to 168 hours.

        Raises:
            ForbiddenError: ``users.admin_required`` (HTTP 403).
        """
        ensure_role(actor, UserRole.admin)
        org = await self._orgs.update_limits(actor.organisation_id, data)
        if org is None:
            raise NotFoundError(resource="Organisation", id=actor.organisation_id)
        return org
