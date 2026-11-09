"""What a signed-in person may do, for ``GET /me`` (007 FR-001, research R4).

Layer: services. Depends on ``hds.core.capabilities`` (the pure map), ``hds.models`` and the
project-manager repository it is given; it MUST NOT import routers or know about HTTP. Read-only.
"""

import uuid
from dataclasses import dataclass

from hds.core.capabilities import Capability, capabilities_for
from hds.models.user import User, UserRole
from hds.repositories.project_manager import ProjectManagerRepository


@dataclass(frozen=True)
class UserCapabilities:
    """A user's capability names and the projects their ``:project`` capabilities cover."""

    capabilities: list[Capability]
    managed_project_ids: list[uuid.UUID]


class CapabilityService:
    """Resolve the capability map for one user, from their live role."""

    def __init__(self, managers: ProjectManagerRepository) -> None:
        """Wire the service to the project-manager repository."""
        self._managers = managers

    async def for_user(self, user: User) -> UserCapabilities:
        """The capabilities ``user`` holds now, and the projects they manage.

        Only a Manager's list of managed projects matters: an Admin's scoped capabilities cover
        every project, so an Admin gets an empty list, and the other roles hold no scoped
        capability at all.
        """
        managed: list[uuid.UUID] = []
        if user.role == UserRole.manager:
            managed = await self._managers.managed_by(user.organisation_id, user.id)
        return UserCapabilities(capabilities_for(user.role), managed)
