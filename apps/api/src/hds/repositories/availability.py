"""Availability entries of employees.

Layer: repositories. Depends on ``hds.models`` and ``hds.repositories.base`` only; it MUST NOT
import services or routers. Every read is scoped to the caller's organisation.
"""

import uuid
from datetime import date
from typing import Any

from sqlalchemy import select

from hds.models.availability import AvailabilityEntry
from hds.repositories.base import OrgScopedRepository


class AvailabilityRepository(OrgScopedRepository[AvailabilityEntry]):
    """An organisation's availability entries."""

    model = AvailabilityEntry

    async def get_by_id(
        self, id: uuid.UUID, org_id: uuid.UUID, *, fresh: bool = False
    ) -> AvailabilityEntry | None:
        """Fetch an entry of the organisation, or ``None`` (also for another organisation's).

        Args:
            id: The entry.
            org_id: The caller's organisation.
            fresh: Read the committed row again even if the session holds an older copy; use it
                after taking the employee's row lock.
        """
        stmt = select(AvailabilityEntry).where(
            AvailabilityEntry.id == id, AvailabilityEntry.organization_id == org_id
        )
        if fresh:
            stmt = stmt.execution_options(populate_existing=True)
        return (await self.session.execute(stmt)).scalar_one_or_none()

    async def for_user(
        self,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        first: date | None = None,
        last: date | None = None,
        exclude: uuid.UUID | None = None,
    ) -> list[AvailabilityEntry]:
        """One employee's entries that share a date with ``first`` to ``last``.

        Args:
            org_id: The caller's organisation.
            user_id: The employee.
            first: First date, inclusive; no lower bound when ``None``.
            last: Last date, inclusive; no upper bound when ``None``.
            exclude: An entry to leave out, for example the one being changed.

        Returns:
            The entries ordered by start date.
        """
        stmt = select(AvailabilityEntry).where(
            AvailabilityEntry.organization_id == org_id, AvailabilityEntry.user_id == user_id
        )
        if first is not None:
            stmt = stmt.where(AvailabilityEntry.ends_on >= first)
        if last is not None:
            stmt = stmt.where(AvailabilityEntry.starts_on <= last)
        if exclude is not None:
            stmt = stmt.where(AvailabilityEntry.id != exclude)
        result = await self.session.execute(
            stmt.order_by(AvailabilityEntry.starts_on, AvailabilityEntry.id)
        )
        return list(result.scalars().all())

    async def create(self, org_id: uuid.UUID, data: dict[str, Any]) -> AvailabilityEntry:
        """Insert an entry and flush.

        Args:
            org_id: Organisation that owns it.
            data: Column values, including ``user_id``.

        Returns:
            The stored entry.
        """
        entry = AvailabilityEntry(id=uuid.uuid4(), organization_id=org_id, **data)
        self.session.add(entry)
        await self.session.flush()
        return entry

    async def update(self, id: uuid.UUID, org_id: uuid.UUID, **fields: Any) -> AvailabilityEntry:
        """Change an entry of the organisation.

        Raises:
            NotFoundError: The entry is not in the organisation.
        """
        return await self._update_scoped(id, org_id, **fields)

    async def delete(self, id: uuid.UUID, org_id: uuid.UUID) -> None:
        """Remove an entry of the organisation; nothing happens when it is not there."""
        await self._delete_scoped(id, org_id)
