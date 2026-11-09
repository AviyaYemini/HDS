"""Persistence of projects' staffing needs (004 FR-001 to FR-003).

Layer: repositories. Depends on ``hds.models`` and ``hds.repositories.base`` only.
"""

import uuid
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from sqlalchemy import delete, insert, select

from hds.models.project import Project
from hds.models.shift_type import ShiftType
from hds.models.staffing_need import StaffingNeed
from hds.repositories.base import OrgScopedRepository


class StaffingNeedRepository(OrgScopedRepository[StaffingNeed]):
    """Staffing needs, always read and written per project; callers check the project's tenant."""

    model = StaffingNeed

    async def for_project(self, project_id: uuid.UUID) -> list[StaffingNeed]:
        """A project's needs, by weekday then shift type."""
        result = await self.session.execute(
            select(StaffingNeed)
            .where(StaffingNeed.project_id == project_id)
            .order_by(StaffingNeed.weekday, StaffingNeed.shift_type_id)
        )
        return list(result.scalars().all())

    async def for_projects(self, project_ids: Iterable[uuid.UUID]) -> list[StaffingNeed]:
        """The needs of several projects, for building a roster problem.

        Needs of a deleted project, or on a retired type, are left out here too, whatever ids the
        caller passes.
        """
        ids = list(project_ids)
        if not ids:
            return []
        result = await self.session.execute(
            select(StaffingNeed)
            .join(Project, Project.id == StaffingNeed.project_id)
            .join(ShiftType, ShiftType.id == StaffingNeed.shift_type_id)
            .where(
                StaffingNeed.project_id.in_(ids),
                Project.is_deleted.is_(False),
                ShiftType.is_active.is_(True),
            )
        )
        return list(result.scalars().all())

    async def types_of(self, project_id: uuid.UUID) -> list[uuid.UUID]:
        """The shift types a project's needs use now."""
        result = await self.session.execute(
            select(StaffingNeed.shift_type_id)
            .where(StaffingNeed.project_id == project_id)
            .distinct()
        )
        return list(result.scalars().all())

    async def replace(
        self, org_id: uuid.UUID, project_id: uuid.UUID, items: Sequence[Mapping[str, Any]]
    ) -> None:
        """Replace a project's needs with ``items``; callers lock the project and check them."""
        await self.session.execute(
            delete(StaffingNeed).where(StaffingNeed.project_id == project_id)
        )
        if items:
            await self.session.execute(
                insert(StaffingNeed),
                [{**item, "organisation_id": org_id, "project_id": project_id} for item in items],
            )
        await self.session.flush()

    async def keep_types(self, project_id: uuid.UUID, shift_type_ids: Iterable[uuid.UUID]) -> None:
        """Remove a project's needs on any shift type outside ``shift_type_ids`` (FR-002)."""
        await self.session.execute(
            delete(StaffingNeed).where(
                StaffingNeed.project_id == project_id,
                StaffingNeed.shift_type_id.not_in(list(shift_type_ids)),
            )
        )
        await self.session.flush()

    async def remove_type(self, shift_type_id: uuid.UUID) -> None:
        """Remove every need on a shift type, in every project, when it is retired (FR-002)."""
        await self.session.execute(
            delete(StaffingNeed).where(StaffingNeed.shift_type_id == shift_type_id)
        )
        await self.session.flush()
