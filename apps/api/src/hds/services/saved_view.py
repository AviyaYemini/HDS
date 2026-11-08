"""Saved views of tables, one user's own (007 FR-043, A2).

Layer: services. Depends on ``hds.core`` (errors, roles), ``hds.models`` and the repository it is
given; it MUST NOT import routers or know about HTTP. Staff only: the client portal has no tables.
"""

import uuid

from hds.core.authz import STAFF_ROLES, ensure_role
from hds.core.exceptions import NotFoundError
from hds.models.saved_view import SavedView
from hds.models.user import User
from hds.repositories.saved_view import SavedViewRepository


class SavedViewService:
    """List, save and remove the caller's views."""

    def __init__(self, views: SavedViewRepository) -> None:
        """Wire the service to the saved-views repository."""
        self._views = views

    async def list(self, actor: User, screen: str) -> list[SavedView]:
        """The caller's views of ``screen``."""
        ensure_role(actor, *STAFF_ROLES)
        return await self._views.for_screen(actor.id, screen)

    async def save(self, actor: User, screen: str, name: str, query: str) -> SavedView:
        """Save a view for the caller.

        Raises:
            ConflictError: ``views.duplicate_name`` (HTTP 409).
        """
        ensure_role(actor, *STAFF_ROLES)
        return await self._views.create(
            actor.organization_id,
            {"user_id": actor.id, "screen": screen, "name": name, "query": query},
        )

    async def remove(self, actor: User, id: uuid.UUID) -> None:
        """Remove one of the caller's views; another user's answers 404.

        Raises:
            NotFoundError: Not one of the caller's views (HTTP 404).
        """
        ensure_role(actor, *STAFF_ROLES)
        if not await self._views.delete_own(actor.id, id):
            raise NotFoundError(resource="SavedView", id=id)
