"""Reports: who worked where (work-log) and hours and pay per employee and project (005).

Layer: services. Depends on ``hds.core`` (errors, roles), ``hds.models``, ``hds.payroll`` (through
``hds.services.pay``) and the repositories it is given; it MUST NOT import routers or know about
HTTP. Read-only: these are read-only service methods over read-only repositories (plan prompt).

Managers and Admins read both reports (FR-023, FR-027). A shift counts when published and ended,
in the range of its local start date at its site (FR-029). The work-log shows no pay (FR-025). The
hours report shows pay only for projects the reader may see (FR-022, FR-028): an Admin every
project with pay on; a Manager only a project with pay on, open to Managers, and that they are
assigned to, read from the live role on every request. Where the reader may not see pay there is
no pay figure at all, and every total covers only the lines shown with pay.
"""

import uuid
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Literal
from zoneinfo import ZoneInfo

from hds.core.authz import MANAGER_ROLES, ensure_role
from hds.core.exceptions import DomainValidationError, NotFoundError
from hds.models.organisation import Organisation
from hds.models.project import Project
from hds.models.user import User, UserRole
from hds.repositories.group import GroupRepository
from hds.repositories.organisation import OrganisationRepository
from hds.repositories.pay_reports import ReportShift, ReportShiftRepository
from hds.repositories.project import ProjectRepository
from hds.repositories.project_manager import ProjectManagerRepository
from hds.repositories.site import SiteRepository
from hds.repositories.special_day import SpecialDayRepository
from hds.services.pay import Pricing, shown, utc_window, widen

MAX_DAYS = 366
GroupBy = Literal["employee", "project"]
_ZERO = Decimal("0.00")


@dataclass(frozen=True)
class Scope:
    """What a work-log is narrowed to: some projects, one site, or some people."""

    project_ids: list[uuid.UUID] | None = None
    site_id: uuid.UUID | None = None
    user_ids: list[uuid.UUID] | None = None


@dataclass(frozen=True)
class ReportLine:
    """One person on one shift, priced as the reader may see it.

    ``pay_shown`` says whether the reader may see this project's pay; ``pay`` is then the line's
    pay, or ``None`` when the rate is missing (``rate_missing``). When not shown, ``pay`` is
    ``None`` and nothing about the rate is told.
    """

    shift_id: uuid.UUID
    user_id: uuid.UUID
    user_name: str
    project_id: uuid.UUID
    project_name: str
    date: date
    hours: Decimal
    pay_shown: bool
    pay: Decimal | None
    rate_missing: bool


@dataclass
class _Totals:
    """What every level of the report adds up: person-hours, distinct shifts, pay where shown."""

    hours: Decimal = _ZERO
    pay: Decimal | None = None
    shifts_missing_rate: int = 0
    shift_ids: set[uuid.UUID] = field(default_factory=set, repr=False)

    @property
    def shifts(self) -> int:
        """How many different shifts are under this total (a shift of three people counts once)."""
        return len(self.shift_ids)

    def add(self, line: ReportLine) -> None:
        """Add one line: its hours and shift; its pay, from zero, when the reader may see it."""
        self.hours += line.hours
        self.shift_ids.add(line.shift_id)
        if line.pay_shown:
            self.pay = (self.pay or _ZERO) + (line.pay or _ZERO)
            self.shifts_missing_rate += 1 if line.rate_missing else 0


@dataclass
class Row(_Totals):
    """Totals of one employee at one project."""

    id: uuid.UUID = field(default_factory=uuid.uuid4)
    name: str = ""


@dataclass
class Group(_Totals):
    """Totals of one employee or project, with a row per project or employee."""

    id: uuid.UUID = field(default_factory=uuid.uuid4)
    name: str = ""
    rows: list[Row] = field(default_factory=list)


@dataclass
class HoursReport(_Totals):
    """The hours report: groups and the grand totals."""

    first: date = date.min
    last: date = date.min
    group_by: GroupBy = "employee"
    groups: list[Group] = field(default_factory=list)


@dataclass(frozen=True)
class WorkLogRow:
    """One person at one project within the range."""

    user_id: uuid.UUID
    full_name: str
    project_id: uuid.UUID
    project_name: str
    shifts: int
    hours: Decimal
    first_date: date
    last_date: date


@dataclass(frozen=True)
class WorkLog:
    """Who worked where in the range."""

    first: date
    last: date
    rows: list[WorkLogRow]


def aggregate(lines: Iterable[ReportLine], group_by: GroupBy) -> HoursReport:
    """Group priced lines by employee (rows per project) or by project (rows per employee).

    Hours are person-hours, summed; shifts are distinct shifts. A pay total exists exactly where
    some line under it is one whose pay the reader may see; it is the sum of their pay, and lines
    missing a rate add nothing and are counted in ``shifts_missing_rate``. Where no line's pay may
    be seen there is no pay figure at all (FR-028). The dates are left to the caller.
    """
    groups: dict[uuid.UUID, Group] = {}
    rows: dict[tuple[uuid.UUID, uuid.UUID], Row] = {}
    report = HoursReport(group_by=group_by)
    for line in lines:
        if group_by == "employee":
            outer, inner = (line.user_id, line.user_name), (line.project_id, line.project_name)
        else:
            outer, inner = (line.project_id, line.project_name), (line.user_id, line.user_name)
        group = groups.setdefault(outer[0], Group(id=outer[0], name=outer[1]))
        row = rows.get((outer[0], inner[0]))
        if row is None:
            row = rows[(outer[0], inner[0])] = Row(id=inner[0], name=inner[1])
            group.rows.append(row)
        for total in (row, group, report):
            total.add(line)
    report.groups = sorted(groups.values(), key=lambda g: (g.name, str(g.id)))
    for group in report.groups:
        group.rows.sort(key=lambda r: (r.name, str(r.id)))
    return report


def check_range(first: date, last: date) -> None:
    """Refuse a range that ends before it starts or covers more than 366 days (FR-030).

    Raises:
        DomainValidationError: ``reports.range_invalid`` (HTTP 422).
    """
    if last < first or (last - first).days >= MAX_DAYS:
        raise DomainValidationError(
            code="reports.range_invalid",
            detail="A report covers 1 to 366 days and ends on or after its start.",
        )


def local_date(shift: ReportShift) -> date:
    """The date a shift starts on at its site."""
    return shift.starts_at.astimezone(ZoneInfo(shift.timezone)).date()


class ReportsService:
    """The work-log and the hours report."""

    def __init__(
        self,
        report_shift_repo: ReportShiftRepository,
        organisation_repo: OrganisationRepository,
        special_day_repo: SpecialDayRepository,
        project_manager_repo: ProjectManagerRepository,
        project_repo: ProjectRepository,
        site_repo: SiteRepository,
        group_repo: GroupRepository,
    ) -> None:
        """Wire the service to its read-only queries."""
        self._shifts = report_shift_repo
        self._orgs = organisation_repo
        self._days = special_day_repo
        self._managers = project_manager_repo
        self._projects = project_repo
        self._sites = site_repo
        self._groups = group_repo

    async def _organisation(self, actor: User) -> Organisation:
        org = await self._orgs.get_by_id(actor.organization_id)
        if org is None:
            raise NotFoundError(resource="Organisation", id=actor.organization_id)
        return org

    async def _in_range(
        self,
        actor: User,
        first: date,
        last: date,
        now: datetime,
        scope: Scope | None = None,
    ) -> list[ReportShift]:
        starts_from, starts_before = utc_window(first, last)
        scope = scope or Scope()
        found = await self._shifts.worked_in_range(
            org_id=actor.organization_id,
            starts_from=starts_from,
            starts_before=starts_before,
            now=now,
            project_ids=scope.project_ids,
            site_id=scope.site_id,
            user_ids=scope.user_ids,
        )
        return [s for s in found if first <= local_date(s) <= last]

    async def work_log(
        self,
        actor: User,
        *,
        project_id: uuid.UUID | None,
        site_id: uuid.UUID | None,
        group_id: uuid.UUID | None,
        first: date | None,
        last: date | None,
        now: datetime | None = None,
    ) -> WorkLog:
        """Who worked at a project, a site or a group in a range (Manager or Admin).

        Args:
            actor: The caller.
            project_id: A project; ``first`` then defaults to its start date, or to 366 days
                before ``last`` for a project older than that, so the default is always a range
                the limit allows.
            site_id: A site: every project at it.
            group_id: A group: its current members, at every project.
            first: The first date; required for a site or group.
            last: The last date; today in the organisation's zone by default.
            now: The current instant; tests pass a fixed one.

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
            DomainValidationError: ``reports.scope_invalid``, ``reports.start_required`` or
                ``reports.range_invalid`` (HTTP 422).
            NotFoundError: The project, site or group is not the organisation's (HTTP 404).
        """
        ensure_role(actor, *MANAGER_ROLES)
        if sum(x is not None for x in (project_id, site_id, group_id)) != 1:
            raise DomainValidationError(
                code="reports.scope_invalid",
                detail="Give exactly one of project_id, site_id and group_id.",
            )
        now = now or datetime.now(UTC)
        org = await self._organisation(actor)
        last = last or now.astimezone(ZoneInfo(org.timezone)).date()
        scope = Scope()
        if project_id is not None:
            project = await self._projects.get_by_id(project_id, actor.organization_id)
            if project is None:
                raise NotFoundError(resource="Project", id=project_id)
            if first is None:
                # Since the project started, but at most the last 366 days (FR-024, FR-030).
                first = max(project.start_date, last - timedelta(days=MAX_DAYS - 1))
            scope = Scope(project_ids=[project_id])
        elif site_id is not None:
            if await self._sites.get_by_id(site_id, actor.organization_id) is None:
                raise NotFoundError(resource="Site", id=site_id)
            scope = Scope(site_id=site_id)
        elif group_id is not None:
            if await self._groups.get_by_id(group_id, actor.organization_id) is None:
                raise NotFoundError(resource="Group", id=group_id)
            scope = Scope(user_ids=await self._groups.member_ids(group_id))
        if first is None:  # only a project has a start of its own
            raise DomainValidationError(
                code="reports.start_required",
                detail="A work-log of a site or a group needs a start date.",
            )
        check_range(first, last)
        shifts = await self._in_range(actor, first, last, now, scope)
        names = {
            p.id: p.name
            for p in await self._shifts.projects(
                actor.organization_id, {s.project_id for s in shifts}
            )
        }
        totals: dict[tuple[uuid.UUID, uuid.UUID], list[ReportShift]] = defaultdict(list)
        for s in shifts:
            totals[(s.user_id, s.project_id)].append(s)
        rows = [
            WorkLogRow(
                user_id=user_id,
                full_name=items[0].full_name,
                project_id=project_id_,
                project_name=names.get(project_id_, ""),
                shifts=len(items),
                hours=sum(
                    (_hours(s) for s in items),
                    _ZERO,
                ),
                first_date=min(local_date(s) for s in items),
                last_date=max(local_date(s) for s in items),
            )
            for (user_id, project_id_), items in totals.items()
        ]
        rows.sort(key=lambda r: (r.full_name, r.project_name, str(r.user_id)))
        return WorkLog(first=first, last=last, rows=rows)

    async def _visible(self, actor: User, projects: Iterable[Project]) -> set[uuid.UUID]:
        """The projects whose pay the reader may see (FR-022, FR-028)."""
        with_pay = [p for p in projects if p.pay_enabled]
        if actor.role == UserRole.admin:
            return {p.id for p in with_pay}
        open_ = {p.id for p in with_pay if p.managers_see_pay}
        if not open_:
            return set()
        return await self._managers.manages(actor.id, open_)

    async def hours(
        self,
        actor: User,
        first: date,
        last: date,
        group_by: GroupBy,
        now: datetime | None = None,
    ) -> HoursReport:
        """Hours, shifts and, where the reader may see it, pay, by employee or by project.

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
            DomainValidationError: ``reports.range_invalid`` (HTTP 422).
        """
        ensure_role(actor, *MANAGER_ROLES)
        check_range(first, last)
        now = now or datetime.now(UTC)
        org = await self._organisation(actor)
        shifts = await self._in_range(actor, first, last, now)
        project_ids = {s.project_id for s in shifts}
        projects = {
            p.id: p for p in await self._shifts.projects(actor.organization_id, project_ids)
        }
        visible = await self._visible(actor, projects.values())
        days = await self._days.for_projects(actor.organization_id, visible, *widen(first, last))
        pricing = Pricing(org, days)
        lines = []
        for s in shifts:
            project = projects[s.project_id]
            paid = s.project_id in visible
            priced = pricing.price_one(
                s.shift_id,
                s.starts_at,
                s.ends_at,
                s.pay_rate,
                project,
                s.timezone,
                s.default_rate,
                paid=paid,
            )
            lines.append(
                ReportLine(
                    shift_id=s.shift_id,
                    user_id=s.user_id,
                    user_name=s.full_name,
                    project_id=project.id,
                    project_name=project.name,
                    date=priced.date,
                    hours=priced.hours,
                    pay_shown=paid,
                    pay=priced.pay,
                    rate_missing=priced.rate_missing,
                )
            )
        report = aggregate(lines, group_by)
        report.first, report.last = first, last
        return report


def _hours(shift: ReportShift) -> Decimal:
    """A shift's real length in hours, as shown (two decimals, half up, as in pay to date)."""
    micros = Decimal((shift.ends_at - shift.starts_at) // timedelta(microseconds=1))
    return shown(micros / 3_600_000_000)
