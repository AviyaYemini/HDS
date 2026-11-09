"""Overrides, week templates and publish events (007 data-model 032).

Layer: repositories. Depends on ``hds.models`` and ``hds.repositories.base`` only; it MUST NOT
import services or routers.
"""

import uuid
from collections.abc import Collection, Sequence
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.exc import IntegrityError

from hds.core.exceptions import ConflictError
from hds.models.roster_extras import PublishEvent, RosterEditEvent, RuleOverride, WeekTemplate
from hds.repositories.base import BaseRepository


class RuleOverrideRepository(BaseRepository[RuleOverride]):
    """Recorded overrides of roster warnings."""

    model = RuleOverride

    async def for_assignments(
        self, org_id: uuid.UUID, assignment_ids: Collection[uuid.UUID]
    ) -> list[RuleOverride]:
        """The overrides recorded on any of ``assignment_ids``, oldest first."""
        if not assignment_ids:
            return []
        result = await self.session.execute(
            select(RuleOverride)
            .where(
                RuleOverride.organization_id == org_id,
                RuleOverride.assignment_id.in_(list(assignment_ids)),
            )
            .order_by(RuleOverride.created_at, RuleOverride.id)
        )
        return list(result.scalars())

    async def delete_for_assignments(
        self, org_id: uuid.UUID, assignment_ids: Collection[uuid.UUID]
    ) -> None:
        """Remove the overrides of ``assignment_ids``: they excused rules on a shift they left."""
        if assignment_ids:
            await self.session.execute(
                delete(RuleOverride).where(
                    RuleOverride.organization_id == org_id,
                    RuleOverride.assignment_id.in_(list(assignment_ids)),
                )
            )

    async def create(self, org_id: uuid.UUID, data: dict[str, Any]) -> RuleOverride:
        """Record an override.

        Raises:
            ConflictError: ``override.duplicate`` when that warning already has one (HTTP 409).
        """
        row = RuleOverride(id=uuid.uuid4(), organization_id=org_id, **data)
        self.session.add(row)
        try:
            async with self.session.begin_nested():
                await self.session.flush()
        except IntegrityError:
            raise ConflictError(
                code="override.duplicate", detail="This warning already has an override."
            ) from None
        await self.session.refresh(row)
        return row


class WeekTemplateRepository(BaseRepository[WeekTemplate]):
    """A project's saved weeks."""

    model = WeekTemplate

    async def for_project(self, org_id: uuid.UUID, project_id: uuid.UUID) -> list[WeekTemplate]:
        """The project's templates by name."""
        result = await self.session.execute(
            select(WeekTemplate)
            .where(WeekTemplate.organization_id == org_id, WeekTemplate.project_id == project_id)
            .order_by(WeekTemplate.name)
        )
        return list(result.scalars())

    async def get(
        self, org_id: uuid.UUID, project_id: uuid.UUID, id: uuid.UUID
    ) -> WeekTemplate | None:
        """One of the project's templates."""
        result = await self.session.execute(
            select(WeekTemplate).where(
                WeekTemplate.organization_id == org_id,
                WeekTemplate.project_id == project_id,
                WeekTemplate.id == id,
            )
        )
        return result.scalar_one_or_none()

    async def create(self, org_id: uuid.UUID, data: dict[str, Any]) -> WeekTemplate:
        """Save a template.

        Raises:
            ConflictError: ``templates.duplicate_name`` when the project has one by that name.
        """
        row = WeekTemplate(id=uuid.uuid4(), organization_id=org_id, **data)
        self.session.add(row)
        try:
            async with self.session.begin_nested():
                await self.session.flush()
        except IntegrityError:
            raise ConflictError(
                code="templates.duplicate_name",
                detail="The project already has a template by that name.",
                field="name",
            ) from None
        await self.session.refresh(row)
        return row

    async def delete(self, org_id: uuid.UUID, project_id: uuid.UUID, id: uuid.UUID) -> bool:
        """Remove a template; say whether one was there."""
        result = await self.session.execute(
            delete(WeekTemplate)
            .where(
                WeekTemplate.organization_id == org_id,
                WeekTemplate.project_id == project_id,
                WeekTemplate.id == id,
            )
            .returning(WeekTemplate.id)
        )
        return result.scalar_one_or_none() is not None


class PublishEventRepository(BaseRepository[PublishEvent]):
    """One row per person affected by a publish, for feature 010 to deliver."""

    model = PublishEvent

    async def add_many(self, org_id: uuid.UUID, rows: Sequence[dict[str, Any]]) -> None:
        """Record the events of one publish."""
        for data in rows:
            self.session.add(PublishEvent(id=uuid.uuid4(), organization_id=org_id, **data))
        await self.session.flush()

    async def for_project(self, org_id: uuid.UUID, project_id: uuid.UUID) -> list[PublishEvent]:
        """A project's events, oldest first (for tests and feature 010)."""
        result = await self.session.execute(
            select(PublishEvent)
            .where(PublishEvent.organization_id == org_id, PublishEvent.project_id == project_id)
            .order_by(PublishEvent.created_at, PublishEvent.id)
        )
        return list(result.scalars())


class RosterEditEventRepository(BaseRepository[RosterEditEvent]):
    """One row per manual roster edit, for feature 010 to deliver (008 T042g)."""

    model = RosterEditEvent

    async def add(self, org_id: uuid.UUID, data: dict[str, Any]) -> RosterEditEvent:
        """Record one edit."""
        row = RosterEditEvent(id=uuid.uuid4(), organization_id=org_id, **data)
        self.session.add(row)
        await self.session.flush()
        return row

    async def for_project(self, org_id: uuid.UUID, project_id: uuid.UUID) -> list[RosterEditEvent]:
        """A project's edits, oldest first (for tests and feature 010)."""
        result = await self.session.execute(
            select(RosterEditEvent)
            .where(
                RosterEditEvent.organization_id == org_id,
                RosterEditEvent.project_id == project_id,
            )
            .order_by(RosterEditEvent.created_at, RosterEditEvent.id)
        )
        return list(result.scalars())
