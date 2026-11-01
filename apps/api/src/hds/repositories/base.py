import uuid
from typing import Generic, TypeVar

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from hds.models.base import Base

ModelT = TypeVar("ModelT", bound=Base)


class BaseRepository(Generic[ModelT]):
    model: type[ModelT]

    def __init__(self, session: AsyncSession) -> None:
        self.session = session


class OrgScopedRepository(BaseRepository[ModelT]):
    """Repository for tables with ``organization_id``: every lookup is tenant-scoped.

    ``get_by_id`` and ``list`` REQUIRE the caller's organisation; a row that belongs to
    another organisation is indistinguishable from a missing one (``None``).
    """

    async def get_by_id(self, id: uuid.UUID, org_id: uuid.UUID) -> ModelT | None:
        stmt = select(self.model).where(
            self.model.id == id,  # type: ignore[attr-defined]
            self.model.organization_id == org_id,  # type: ignore[attr-defined]
        )
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def list(self, org_id: uuid.UUID, **filters: object) -> list[ModelT]:
        stmt = select(self.model).where(
            self.model.organization_id == org_id  # type: ignore[attr-defined]
        )
        for key, value in filters.items():
            if value is not None:
                stmt = stmt.where(getattr(self.model, key) == value)
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def _get_unscoped(self, id: uuid.UUID) -> ModelT | None:
        """Re-read a row we just wrote. Callers must already have passed a scoped lookup."""
        stmt = select(self.model).where(self.model.id == id)  # type: ignore[attr-defined]
        return (await self.session.execute(stmt)).scalar_one_or_none()
