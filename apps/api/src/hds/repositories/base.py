"""Base classes for repositories: tenant-scoped reads and writes.

Layer: repositories. Depends on ``hds.models`` and ``hds.core.exceptions`` only; it MUST NOT
import services or routers. Repositories are the only code that talks to the database
(Constitution II). They do not commit: the request's unit of work (``hds.core.database``)
commits once at the end, so a multi-step action is all-or-nothing. Write methods only flush.
"""

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
    """Holds the session and the mapped class a repository works on.

    Subclasses set ``model``. Use this directly only for tables without ``organization_id``
    (organisations, refresh tokens); everything else extends ``OrgScopedRepository``.
    """

    model: type[ModelT]

    def __init__(self, session: AsyncSession) -> None:
        """Bind the repository to a request-scoped session.

        Args:
            session: The request's session from ``hds.core.database.get_unit_of_work``.
        """
        self.session = session

    async def commit(self) -> None:
        """Commit the current transaction now, ahead of the end of the request.

        Repositories normally only ``flush``; the request's unit of work commits once at the
        end. Use this only for an effect that must survive a refusal raised afterwards, such as
        the failed-sign-in count, because everything else is rolled back when the request fails.
        Anything written earlier in the same request is committed with it.
        """
        await self.session.commit()


class OrgScopedRepository(BaseRepository[ModelT]):
    """Repository for tables with ``organization_id``: every lookup is tenant-scoped.

    ``get_by_id`` and ``list`` REQUIRE the caller's organisation; a row that belongs to
    another organisation is indistinguishable from a missing one (``None``).
    """

    async def get_by_id(self, id: uuid.UUID, org_id: uuid.UUID) -> ModelT | None:
        """Fetch one row by id inside an organisation.

        Soft-deleted rows are returned here; subclasses whose table has ``is_deleted``
        override this to hide them.

        Args:
            id: Primary key to look up.
            org_id: The caller's organisation.

        Returns:
            The row, or ``None`` when it is missing or belongs to another organisation (the
            two cases are deliberately indistinguishable).
        """
        stmt = select(self.model).where(
            self.model.id == id,  # type: ignore[attr-defined]
            self.model.organization_id == org_id,  # type: ignore[attr-defined]
        )
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def list(self, org_id: uuid.UUID, **filters: object) -> list[ModelT]:
        """List every row of an organisation, optionally narrowed by column equality.

        Args:
            org_id: The caller's organisation.
            **filters: ``column=value`` pairs combined with AND; a ``None`` value is ignored,
                and a name that is not a column raises ``AttributeError``, so pass only
                trusted keys, never request input.

        Returns:
            Matching rows in unspecified order.
        """
        stmt = select(self.model).where(
            self.model.organization_id == org_id  # type: ignore[attr-defined]
        )
        for key, value in filters.items():
            if value is not None:
                stmt = stmt.where(getattr(self.model, key) == value)
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def _update_scoped(self, id: uuid.UUID, org_id: uuid.UUID, **fields: Any) -> ModelT:
        """Update columns of one row, matched on both id and organisation, then flush.

        The organisation is part of the ``WHERE`` clause, so a row of another tenant cannot be
        touched even if its id is guessed.

        Args:
            id: Row to update.
            org_id: The caller's organisation.
            **fields: Column values to set.

        Returns:
            The refreshed row.

        Raises:
            NotFoundError: When no row with that id exists in the organisation.
        """
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
        await self.session.flush()
        row = await self._get_unscoped(id)  # the scoped UPDATE above already proved ownership
        assert row is not None
        return row

    async def _soft_delete_scoped(self, id: uuid.UUID, org_id: uuid.UUID) -> None:
        """Flag a row as deleted (``is_deleted``, ``deleted_at`` in UTC).

        Does nothing, silently, when the row is not in the organisation.
        """
        await self.session.execute(
            sa_update(self.model)
            .where(
                self.model.id == id,  # type: ignore[attr-defined]
                self.model.organization_id == org_id,  # type: ignore[attr-defined]
            )
            .values(is_deleted=True, deleted_at=datetime.now(UTC))
        )
        await self.session.flush()

    async def _delete_scoped(self, id: uuid.UUID, org_id: uuid.UUID) -> None:
        """Hard-delete a row of the organisation and flush; no-op when it is not found."""
        row = await self.get_by_id(id, org_id)
        if row is not None:
            await self.session.delete(row)
            await self.session.flush()

    async def _get_unscoped(self, id: uuid.UUID) -> ModelT | None:
        """Re-read a row we just wrote. Callers must already have passed a scoped lookup."""
        stmt = select(self.model).where(self.model.id == id)  # type: ignore[attr-defined]
        return (await self.session.execute(stmt)).scalar_one_or_none()
