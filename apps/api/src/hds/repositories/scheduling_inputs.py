"""Reads that a roster problem is built from (004 research G9).

Layer: repositories. Depends on ``hds.models`` and ``hds.repositories.base`` only. Every query is
scoped to the organisation passed in.
"""

import uuid
from collections.abc import Collection
from dataclasses import dataclass
from datetime import date, datetime

from sqlalchemy import select

from hds.models.availability import AvailabilityEntry
from hds.models.project import Project, ProjectStatus
from hds.models.shift import Assignment, Shift
from hds.models.shift_type import ShiftType, project_shift_types
from hds.models.site import Site
from hds.models.user import User, UserRole
from hds.repositories.base import BaseRepository


@dataclass(frozen=True)
class ActiveProject:
    """An active project with the dates it runs and its site's time zone."""

    id: uuid.UUID
    start_date: date
    end_date: date | None
    zone: str
    site_id: uuid.UUID | None = None


@dataclass(frozen=True)
class StoredShift:
    """A stored shift, its site's zone and the people on it.

    ``own_times`` holds ``(person, starts_at, ends_at)`` for the people whose assignment has its
    own times (007 FR-027).
    """

    id: uuid.UUID
    project_id: uuid.UUID
    shift_type_id: uuid.UUID
    starts_at: datetime
    ends_at: datetime
    headcount: int
    status: str
    zone: str
    people: tuple[uuid.UUID, ...]
    own_times: tuple[tuple[uuid.UUID, datetime, datetime], ...] = ()
    # The people locked on it (008 FR-018): a later run keeps them.
    locked: tuple[uuid.UUID, ...] = ()


class SchedulingInputRepository(BaseRepository[Shift]):
    """The projects, shifts, people and entries a run reads."""

    model = Shift

    async def active_projects(
        self, org_id: uuid.UUID, ids: Collection[uuid.UUID] | None
    ) -> list[ActiveProject]:
        """The organisation's active, not deleted projects; only ``ids`` when given."""
        query = (
            select(Project.id, Project.start_date, Project.end_date, Site.timezone, Project.site_id)
            .join(Site, Site.id == Project.site_id)
            .where(
                Project.organization_id == org_id,
                Project.status == ProjectStatus.active,
                Project.is_deleted.is_(False),
            )
        )
        if ids is not None:
            query = query.where(Project.id.in_(list(ids)))
        result = await self.session.execute(query.order_by(Project.id))
        return [ActiveProject(r[0], r[1], r[2], r[3], r[4]) for r in result.all()]

    async def project_types(
        self, project_ids: Collection[uuid.UUID]
    ) -> set[tuple[uuid.UUID, uuid.UUID]]:
        """The (project, shift type) pairs in use, active types only."""
        if not project_ids:
            return set()
        result = await self.session.execute(
            select(project_shift_types.c.project_id, project_shift_types.c.shift_type_id)
            .join(ShiftType, ShiftType.id == project_shift_types.c.shift_type_id)
            .where(
                project_shift_types.c.project_id.in_(list(project_ids)),
                ShiftType.is_active.is_(True),
            )
        )
        return {(r[0], r[1]) for r in result.all()}

    async def shifts_between(
        self, org_id: uuid.UUID, start: datetime, end: datetime
    ) -> list[StoredShift]:
        """Every shift of the organisation, draft or published, overlapping ``start`` to ``end``."""
        rows = (
            await self.session.execute(
                select(Shift, Site.timezone)
                .join(Project, Project.id == Shift.project_id)
                .join(Site, Site.id == Project.site_id)
                .where(
                    Shift.organization_id == org_id,
                    Shift.starts_at < end,
                    Shift.ends_at > start,
                )
                .order_by(Shift.starts_at, Shift.id)
            )
        ).all()
        ids = [shift.id for shift, _ in rows]
        people: dict[uuid.UUID, list[uuid.UUID]] = {i: [] for i in ids}
        own: dict[uuid.UUID, list[tuple[uuid.UUID, datetime, datetime]]] = {i: [] for i in ids}
        locked: dict[uuid.UUID, list[uuid.UUID]] = {i: [] for i in ids}
        if ids:
            assigned = await self.session.execute(
                select(
                    Assignment.shift_id,
                    Assignment.user_id,
                    Assignment.starts_at,
                    Assignment.ends_at,
                    Assignment.locked_at,
                )
                .where(Assignment.shift_id.in_(ids))
                .order_by(Assignment.user_id)
            )
            for shift_id, user_id, starts_at, ends_at, locked_at in assigned.all():
                people[shift_id].append(user_id)
                if starts_at is not None and ends_at is not None:
                    own[shift_id].append((user_id, starts_at, ends_at))
                if locked_at is not None:
                    locked[shift_id].append(user_id)
        return [
            StoredShift(
                s.id,
                s.project_id,
                s.shift_type_id,
                s.starts_at,
                s.ends_at,
                s.headcount,
                s.status,
                zone,
                tuple(people[s.id]),
                tuple(own[s.id]),
                tuple(locked[s.id]),
            )
            for s, zone in rows
        ]

    async def candidates(self, org_id: uuid.UUID, first: date, last: date) -> list[User]:
        """Who generation may assign (004 FR-007).

        Every active Employee; a Manager or Admin only with an open entry sharing a date with
        ``first`` to ``last``. Never a Client or an inactive or deleted account.
        """
        offered = (
            select(AvailabilityEntry.user_id)
            .where(
                AvailabilityEntry.organization_id == org_id,
                AvailabilityEntry.kind == "open",
                AvailabilityEntry.starts_on <= last,
                AvailabilityEntry.ends_on >= first,
            )
            .scalar_subquery()
        )
        result = await self.session.execute(
            select(User)
            .where(
                User.organization_id == org_id,
                User.is_active.is_(True),
                User.is_deleted.is_(False),
                (User.role == UserRole.employee)
                | (User.role.in_([UserRole.manager, UserRole.admin]) & User.id.in_(offered)),
            )
            .order_by(User.id)
        )
        return list(result.scalars().all())
