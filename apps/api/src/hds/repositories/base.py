import uuid
from datetime import UTC, datetime
from typing import Any, Generic, TypeVar

from sqlalchemy import select
from sqlalchemy import update as sa_update
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.exceptions import NotFoundError
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

    async def _update_scoped(self, id: uuid.UUID, org_id: uuid.UUID, **fields: Any) -> ModelT:
        """UPDATE ... WHERE id AND organization_id; NotFoundError when no row is in that org."""
        result = await self.session.execute(
            sa_update(self.model)
            .where(
                self.model.id == id,  # type: ignore[attr-defined]
                self.model.organization_id == org_id,  # type: ignore[attr-defined]
            )
            .values(**fields)
        )
        if result.rowcount == 0:  # type: ignore[attr-defined]
            raise NotFoundError(self.model.__name__, id)
        await self.session.commit()
        row = await self._get_unscoped(id)  # the scoped UPDATE above already proved ownership
        assert row is not None
        return row

    async def _soft_delete_scoped(self, id: uuid.UUID, org_id: uuid.UUID) -> None:
        await self.session.execute(
            sa_update(self.model)
            .where(
                self.model.id == id,  # type: ignore[attr-defined]
                self.model.organization_id == org_id,  # type: ignore[attr-defined]
            )
            .values(is_deleted=True, deleted_at=datetime.now(UTC))
        )
        await self.session.commit()

    async def _delete_scoped(self, id: uuid.UUID, org_id: uuid.UUID) -> None:
        row = await self.get_by_id(id, org_id)
        if row is not None:
            await self.session.delete(row)
            await self.session.commit()

    async def _get_unscoped(self, id: uuid.UUID) -> ModelT | None:
        """Re-read a row we just wrote. Callers must already have passed a scoped lookup."""
        stmt = select(self.model).where(self.model.id == id)  # type: ignore[attr-defined]
        return (await self.session.execute(stmt)).scalar_one_or_none()
