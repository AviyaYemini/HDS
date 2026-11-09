"""Persistence of special days (005 FR-008, FR-008a).

Layer: repositories. Depends on ``hds.models`` and ``hds.repositories.base`` only; it MUST NOT
import services or routers. Callers lock the organisation (for its days) or the project (for a
project's days) before they write, so the duplicate check and the insert happen under one lock.
"""

import uuid
from collections.abc import Collection, Mapping
from datetime import date
from typing import Any

from sqlalchemy import delete, exists, extract, insert, literal, select

from hds.models.special_day import SpecialDay
from hds.repositories.base import OrgScopedRepository


class SpecialDayRepository(OrgScopedRepository[SpecialDay]):
    """Special days of an organisation and of its projects."""

    model = SpecialDay

    async def list_organisation(self, org_id: uuid.UUID, year: int | None) -> list[SpecialDay]:
        """The organisation's own days (no project), by date; ``year`` narrows them."""
        query = select(SpecialDay).where(
            SpecialDay.organisation_id == org_id, SpecialDay.project_id.is_(None)
        )
        if year is not None:
            query = query.where(extract("year", SpecialDay.day) == year)
        return list((await self.session.execute(query.order_by(SpecialDay.day))).scalars())

    async def list_project(self, project_id: uuid.UUID, year: int | None) -> list[SpecialDay]:
        """One project's own days, by date; ``year`` narrows them."""
        query = select(SpecialDay).where(SpecialDay.project_id == project_id)
        if year is not None:
            query = query.where(extract("year", SpecialDay.day) == year)
        return list((await self.session.execute(query.order_by(SpecialDay.day))).scalars())

    async def for_projects(
        self, org_id: uuid.UUID, project_ids: Collection[uuid.UUID], first: date, last: date
    ) -> list[SpecialDay]:
        """Every organisation day, and every day of ``project_ids``, from ``first`` to ``last``."""
        query = select(SpecialDay).where(
            SpecialDay.organisation_id == org_id,
            SpecialDay.day.between(first, last),
            SpecialDay.project_id.is_(None) | SpecialDay.project_id.in_(list(project_ids)),
        )
        return list((await self.session.execute(query)).scalars())

    async def reread(self, id: uuid.UUID, org_id: uuid.UUID) -> SpecialDay | None:
        """A day of the organisation, read afresh even if the session holds an older copy."""
        query = (
            select(SpecialDay)
            .where(SpecialDay.id == id, SpecialDay.organisation_id == org_id)
            .execution_options(populate_existing=True)
        )
        return (await self.session.execute(query)).scalar_one_or_none()

    async def exists(self, org_id: uuid.UUID, project_id: uuid.UUID | None, day: date) -> bool:
        """Whether the organisation (``project_id`` None) or the project already has ``day``."""
        owner = (
            SpecialDay.project_id.is_(None)
            if project_id is None
            else SpecialDay.project_id == project_id
        )
        query = select(
            exists().where(SpecialDay.organisation_id == org_id, owner, SpecialDay.day == day)
        )
        return bool((await self.session.execute(query)).scalar())

    async def create(
        self, org_id: uuid.UUID, project_id: uuid.UUID | None, values: Mapping[str, Any]
    ) -> SpecialDay:
        """Add a day of the organisation or of one project."""
        row = SpecialDay(organisation_id=org_id, project_id=project_id, **values)
        self.session.add(row)
        await self.session.flush()
        await self.session.refresh(row)
        return row

    async def change(self, row: SpecialDay, values: Mapping[str, Any]) -> SpecialDay:
        """Change a day's date, name or rate."""
        for key, value in values.items():
            setattr(row, key, value)
        await self.session.flush()
        await self.session.refresh(row)
        return row

    async def remove(self, row: SpecialDay) -> None:
        """Delete a day."""
        await self.session.execute(delete(SpecialDay).where(SpecialDay.id == row.id))
        await self.session.flush()

    async def copy_organisation_days(self, org_id: uuid.UUID, project_id: uuid.UUID) -> None:
        """Copy every organisation day the project does not override into the project's own list.

        Used when a project ends (FR-007), so later changes to the organisation's days do not reach
        it.
        """
        own = select(SpecialDay.day).where(SpecialDay.project_id == project_id)
        source = select(
            SpecialDay.organisation_id,
            literal(project_id, type_=SpecialDay.project_id.type),
            SpecialDay.day,
            SpecialDay.name,
            SpecialDay.rate_pct,
        ).where(
            SpecialDay.organisation_id == org_id,
            SpecialDay.project_id.is_(None),
            SpecialDay.day.not_in(own),
        )
        await self.session.execute(
            # Without the model's Python defaults, which SQLAlchemy would evaluate once for every
            # copied row: each copy takes its own id from the database's gen_random_uuid().
            insert(SpecialDay).from_select(
                ["organisation_id", "project_id", "day", "name", "rate_pct"],
                source,
                include_defaults=False,
            )
        )
        await self.session.flush()
