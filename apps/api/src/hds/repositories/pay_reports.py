"""Read-only queries behind pay to date and the reports (005 research R8, R9).

Layer: repositories. Depends on ``hds.models`` and ``hds.repositories.base`` only; it MUST NOT
import services or routers, and it never writes. Every query is scoped to one organisation, reads
published shifts only, and selects by UTC start through ``idx_shifts_org_published_start``; the
service narrows the result to the local dates it was asked for.
"""

import uuid
from collections.abc import Collection
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from sqlalchemy import func, select

from hds.models.project import Project
from hds.models.shift import Assignment, Shift
from hds.models.site import Site
from hds.models.user import User
from hds.repositories.base import BaseRepository


@dataclass(frozen=True)
class WorkedShift:
    """One published shift, its project, its site's time zone, and when the person worked it.

    ``starts_at`` and ``ends_at`` are the person's own times when the assignment has them,
    otherwise the shift's (007 FR-027); pay prices those.
    """

    shift: Shift
    project: Project
    timezone: str
    starts_at: datetime
    ends_at: datetime


# When the person on an assignment worked: their own times, otherwise the shift's (007 FR-027).
# Which shifts count is still decided by the shift's own start, the date it belongs to.
_STARTS = func.coalesce(Assignment.starts_at, Shift.starts_at)
_ENDS = func.coalesce(Assignment.ends_at, Shift.ends_at)


class PayReportRepository(BaseRepository[Shift]):
    """Published shifts, read for pay and reports."""

    model = Shift

    async def own_ended_shifts(
        self,
        *,
        org_id: uuid.UUID,
        user_id: uuid.UUID,
        starts_from: datetime,
        starts_before: datetime,
        now: datetime,
    ) -> list[WorkedShift]:
        """One person's published shifts that start in ``[starts_from, starts_before)`` and ended.

        Ordered by start. Projects that were deleted or have ended are included: the work was done.
        """
        query = (
            select(Shift, Project, Site.timezone, _STARTS, _ENDS)
            .join(Assignment, Assignment.shift_id == Shift.id)
            .join(Project, Project.id == Shift.project_id)
            .join(Site, Site.id == Project.site_id)
            .where(
                Shift.organization_id == org_id,
                Shift.status == "published",
                Shift.starts_at >= starts_from,
                Shift.starts_at < starts_before,
                _ENDS <= now,
                Assignment.user_id == user_id,
            )
            .order_by(Shift.starts_at, Shift.id)
        )
        rows = await self.session.execute(query)
        return [WorkedShift(*row) for row in rows.all()]


@dataclass(frozen=True)
class ReportShift:
    """One person on one published shift, with what pricing needs, read as plain columns."""

    shift_id: uuid.UUID
    starts_at: datetime
    ends_at: datetime
    pay_rate: Decimal | None
    project_id: uuid.UUID
    timezone: str
    user_id: uuid.UUID
    full_name: str
    default_rate: Decimal | None


class ReportShiftRepository(BaseRepository[Shift]):
    """Everyone's published, ended shifts in a range, for the reports."""

    model = Shift

    async def worked_in_range(
        self,
        *,
        org_id: uuid.UUID,
        starts_from: datetime,
        starts_before: datetime,
        now: datetime,
        project_ids: Collection[uuid.UUID] | None = None,
        site_id: uuid.UUID | None = None,
        user_ids: Collection[uuid.UUID] | None = None,
    ) -> list[ReportShift]:
        """Published shifts that start in the window and have ended, one row per person on them.

        ``project_ids``, ``site_id`` and ``user_ids`` narrow the rows when given. Deactivated and
        deleted people and ended or deleted projects are included: the work was done (FR-026).
        """
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
            )
            .join(Assignment, Assignment.shift_id == Shift.id)
            .join(User, User.id == Assignment.user_id)
            .join(Project, Project.id == Shift.project_id)
            .join(Site, Site.id == Project.site_id)
            .where(
                Shift.organization_id == org_id,
                Shift.status == "published",
                Shift.starts_at >= starts_from,
                Shift.starts_at < starts_before,
                _ENDS <= now,
            )
            .order_by(Shift.starts_at, Shift.id, User.id)
        )
        if project_ids is not None:
            query = query.where(Shift.project_id.in_(list(project_ids)))
        if site_id is not None:
            query = query.where(Project.site_id == site_id)
        if user_ids is not None:
            query = query.where(User.id.in_(list(user_ids)))
        rows = await self.session.execute(query)
        return [ReportShift(*row) for row in rows.all()]
