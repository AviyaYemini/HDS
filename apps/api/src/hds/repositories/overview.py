"""Read-only queries behind the snapshot dashboard (CEO demo).

Layer: repositories. Depends on ``hds.models`` and ``hds.repositories.base`` only; it MUST NOT
import services or routers.
"""

import uuid
from collections.abc import Collection
from dataclasses import dataclass
from datetime import date, datetime

from sqlalchemy import func, or_, select

from hds.models.project import Project
from hds.models.project_member import ProjectMember
from hds.models.shift import Assignment, Shift
from hds.models.site import Site
from hds.models.user import User, UserRole
from hds.repositories.base import BaseRepository

_STARTS = func.coalesce(Assignment.starts_at, Shift.starts_at)
_ENDS = func.coalesce(Assignment.ends_at, Shift.ends_at)


@dataclass(frozen=True)
class ScheduledLine:
    """One person on one shift, worked or still to come, draft or published."""

    shift_id: uuid.UUID
    starts_at: datetime
    ends_at: datetime
    pay_rate: object
    project_id: uuid.UUID
    timezone: str
    user_id: uuid.UUID
    full_name: str
    default_rate: object
    status: str


@dataclass(frozen=True)
class ShiftCover:
    """A shift's places and how many people are on it."""

    shift_id: uuid.UUID
    project_id: uuid.UUID
    starts_at: datetime
    timezone: str
    headcount: int
    on_it: int


class OverviewRepository(BaseRepository[Shift]):
    """Everything on the roster in a range, its cover, and the staff counts."""

    model = Shift

    async def lines(
        self,
        org_id: uuid.UUID,
        starts_from: datetime,
        starts_before: datetime,
        project_ids: Collection[uuid.UUID] | None,
    ) -> list[ScheduledLine]:
        """Every person on every shift starting in the window, by their own times."""
        query = (
            select(
                Shift.id,
                _STARTS,
                _ENDS,
                Shift.pay_rate,
                Shift.project_id,
                Site.timezone,
                User.id,
                User.full_name,
                User.default_hourly_rate,
                Shift.status,
            )
            .join(Assignment, Assignment.shift_id == Shift.id)
            .join(User, User.id == Assignment.user_id)
            .join(Project, Project.id == Shift.project_id)
            .join(Site, Site.id == Project.site_id)
            .where(
                Shift.organisation_id == org_id,
                Shift.starts_at >= starts_from,
                Shift.starts_at < starts_before,
            )
            .order_by(Shift.starts_at, Shift.id, User.id)
        )
        if project_ids is not None:
            query = query.where(Shift.project_id.in_(list(project_ids)))
        return [ScheduledLine(*row) for row in (await self.session.execute(query)).all()]

    async def cover(
        self,
        org_id: uuid.UUID,
        starts_from: datetime,
        starts_before: datetime,
        project_ids: Collection[uuid.UUID] | None,
    ) -> list[ShiftCover]:
        """Each shift starting in the window with its headcount and how many are on it."""
        on_it = (
            select(func.count(Assignment.id))
            .where(Assignment.shift_id == Shift.id)
            .correlate(Shift)
            .scalar_subquery()
        )
        query = (
            select(
                Shift.id, Shift.project_id, Shift.starts_at, Site.timezone, Shift.headcount, on_it
            )
            .join(Project, Project.id == Shift.project_id)
            .join(Site, Site.id == Project.site_id)
            .where(
                Shift.organisation_id == org_id,
                Shift.starts_at >= starts_from,
                Shift.starts_at < starts_before,
            )
        )
        if project_ids is not None:
            query = query.where(Shift.project_id.in_(list(project_ids)))
        return [ShiftCover(*row) for row in (await self.session.execute(query)).all()]

    async def staff(
        self,
        org_id: uuid.UUID,
        project_ids: Collection[uuid.UUID] | None = None,
        starts_from: datetime | None = None,
        starts_before: datetime | None = None,
    ) -> list[tuple[uuid.UUID, str, date | None]]:
        """The active, non-deleted Employees and Managers: id, employment type, last day.

        With ``project_ids``, only those on one of the projects' teams or on one of their shifts
        starting in the window (a Manager's own people).
        """
        query = select(User.id, User.employment_type, User.engaged_until).where(
            User.organisation_id == org_id,
            User.is_active.is_(True),
            User.is_deleted.is_(False),
            User.role.in_([UserRole.employee, UserRole.manager]),
        )
        if project_ids is not None:
            ids = list(project_ids)
            members = select(ProjectMember.user_id).where(ProjectMember.project_id.in_(ids))
            worked = (
                select(Assignment.user_id)
                .join(Shift, Shift.id == Assignment.shift_id)
                .where(Shift.project_id.in_(ids))
            )
            if starts_from is not None and starts_before is not None:
                worked = worked.where(
                    Shift.starts_at >= starts_from, Shift.starts_at < starts_before
                )
            query = query.where(or_(User.id.in_(members), User.id.in_(worked)))
        return [(r[0], r[1], r[2]) for r in (await self.session.execute(query)).all()]
