"""Shift types and the shift types a project uses.

Layer: repositories. Depends on ``hds.models``, ``hds.core.exceptions`` and
``hds.repositories.base`` only; it MUST NOT import services or routers. Every read is scoped to the
caller's organisation.
"""

import uuid
from collections.abc import Sequence
from typing import Any

from sqlalchemy import delete, insert, select
from sqlalchemy.exc import IntegrityError

from hds.core.exceptions import ConflictError
from hds.models.shift_type import ShiftType, project_shift_types
from hds.repositories.base import OrgScopedRepository

_UNIQUE_NAME = "uq_shift_types_org_name"


def duplicate_name() -> ConflictError:
    """The refusal for an active name already in use in the organisation."""
    return ConflictError(
        code="shift_types.duplicate_name",
        detail="Another active shift type has this name.",
        field="name",
    )


class ShiftTypeRepository(OrgScopedRepository[ShiftType]):
    """An organisation's shift types; ``get_by_id`` of another organisation's type is ``None``."""

    model = ShiftType

    async def list_for_org(self, org_id: uuid.UUID) -> list[ShiftType]:
        """Every shift type of the organisation, active first, then by start time and name."""
        result = await self.session.execute(
            select(ShiftType)
            .where(ShiftType.organisation_id == org_id)
            .order_by(ShiftType.is_active.desc(), ShiftType.starts_at_local, ShiftType.name)
        )
        return list(result.scalars().all())

    async def create(self, org_id: uuid.UUID, data: dict[str, Any]) -> ShiftType:
        """Insert a shift type and flush.

        Raises:
            ConflictError: ``shift_types.duplicate_name`` (HTTP 409), also when another request
                took the name at the same moment.
        """
        shift_type = ShiftType(id=uuid.uuid4(), organisation_id=org_id, **data)
        try:
            async with self.session.begin_nested():
                self.session.add(shift_type)
                await self.session.flush()
        except IntegrityError as exc:
            if _UNIQUE_NAME in str(exc.orig):
                raise duplicate_name() from None
            raise
        return shift_type

    async def update(self, id: uuid.UUID, org_id: uuid.UUID, **fields: Any) -> ShiftType:
        """Change a shift type of the organisation.

        Raises:
            NotFoundError: The type is not in the organisation.
            ConflictError: ``shift_types.duplicate_name`` (HTTP 409).
        """
        try:
            async with self.session.begin_nested():
                return await self._update_scoped(id, org_id, **fields)
        except IntegrityError as exc:
            if _UNIQUE_NAME in str(exc.orig):
                raise duplicate_name() from None
            raise

    async def get_locked(self, id: uuid.UUID, org_id: uuid.UUID) -> ShiftType | None:
        """Fetch a type of the organisation and lock its row until the transaction ends.

        Reads the committed row even if the session holds an older copy, so a change computed
        from it (for example whether it crosses midnight) cannot be based on stale times.
        """
        result = await self.session.execute(
            select(ShiftType)
            .where(ShiftType.id == id, ShiftType.organisation_id == org_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        return result.scalar_one_or_none()

    async def share_lock_all(self, org_id: uuid.UUID) -> None:
        """Share-lock every shift type of the organisation, so a retirement or change waits."""
        await self.session.execute(
            select(ShiftType.id)
            .where(ShiftType.organisation_id == org_id)
            .order_by(ShiftType.id)
            .with_for_update(read=True)
        )

    async def share_lock(self, org_id: uuid.UUID, ids: Sequence[uuid.UUID]) -> list[ShiftType]:
        """The organisation's types among ``ids``, locked against changes until the end.

        ``FOR SHARE`` lets other readers in but makes a concurrent retirement wait, so a type
        checked as active stays active until this transaction commits.
        """
        if not ids:
            return []
        result = await self.session.execute(
            select(ShiftType)
            .where(ShiftType.organisation_id == org_id, ShiftType.id.in_(set(ids)))
            .with_for_update(read=True)
            .execution_options(populate_existing=True)
        )
        return list(result.scalars().all())

    async def for_project(self, project_id: uuid.UUID) -> list[ShiftType]:
        """The active shift types a project uses, by start time and name.

        A retired type leaves every project's set, so a set read here can always be sent back.
        """
        result = await self.session.execute(
            select(ShiftType)
            .join(project_shift_types, project_shift_types.c.shift_type_id == ShiftType.id)
            .where(project_shift_types.c.project_id == project_id, ShiftType.is_active.is_(True))
            .order_by(ShiftType.starts_at_local, ShiftType.name)
        )
        return list(result.scalars().all())

    async def set_for_project(self, project_id: uuid.UUID, ids: Sequence[uuid.UUID]) -> None:
        """Replace the set of shift types a project uses with ``ids``; callers check the ids."""
        await self.session.execute(
            delete(project_shift_types).where(project_shift_types.c.project_id == project_id)
        )
        if ids:
            await self.session.execute(
                insert(project_shift_types),
                [{"project_id": project_id, "shift_type_id": i} for i in dict.fromkeys(ids)],
            )
        await self.session.flush()

    async def project_uses(self, project_id: uuid.UUID, shift_type_id: uuid.UUID) -> bool:
        """Whether the project lists the shift type and it is still active (003 FR-111)."""
        found = await self.session.execute(
            select(project_shift_types.c.project_id)
            .join(ShiftType, ShiftType.id == project_shift_types.c.shift_type_id)
            .where(
                project_shift_types.c.project_id == project_id,
                project_shift_types.c.shift_type_id == shift_type_id,
                ShiftType.is_active.is_(True),
            )
        )
        return found.first() is not None
