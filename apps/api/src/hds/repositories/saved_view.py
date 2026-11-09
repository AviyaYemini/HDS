"""Saved views: one user's named table filters (007 FR-043).

Layer: repositories. Depends on ``hds.models``, ``hds.core.exceptions`` and
``hds.repositories.base`` only; it MUST NOT import services or routers.
"""

import uuid
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.exc import IntegrityError

from hds.core.exceptions import ConflictError
from hds.models.saved_view import SavedView
from hds.repositories.base import BaseRepository


class SavedViewRepository(BaseRepository[SavedView]):
    """Reads and writes one user's saved views; every query names the owner."""

    model = SavedView

    async def for_screen(self, user_id: uuid.UUID, screen: str) -> list[SavedView]:
        """The user's views of one screen, by name."""
        result = await self.session.execute(
            select(SavedView)
            .where(SavedView.user_id == user_id, SavedView.screen == screen)
            .order_by(SavedView.name)
        )
        return list(result.scalars())

    async def create(self, org_id: uuid.UUID, data: dict[str, Any]) -> SavedView:
        """Save a view.

        Raises:
            ConflictError: ``views.duplicate_name`` when the user has one by that name there.
        """
        row = SavedView(id=uuid.uuid4(), organisation_id=org_id, **data)
        self.session.add(row)
        try:
            async with self.session.begin_nested():
                await self.session.flush()
        except IntegrityError:
            raise ConflictError(
                code="views.duplicate_name",
                detail="You already have a view by that name here.",
                field="name",
            ) from None
        await self.session.refresh(row)
        return row

    async def delete_own(self, user_id: uuid.UUID, id: uuid.UUID) -> bool:
        """Remove one of the user's views; say whether there was one."""
        result = await self.session.execute(
            delete(SavedView)
            .where(SavedView.user_id == user_id, SavedView.id == id)
            .returning(SavedView.id)
        )
        return result.scalar_one_or_none() is not None
