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
from hds.models.user import User
from hds.payroll import MICROS_PER_HOUR
from hds.repositories.client import ClientRepository
from hds.repositories.group import GroupRepository
from hds.repositories.organisation import OrganisationRepository
from hds.repositories.pay_reports import ReportShift, ReportShiftRepository
from hds.repositories.project import ProjectRepository
from hds.repositories.project_manager import ProjectManagerRepository
from hds.repositories.site import SiteRepository
from hds.repositories.special_day import SpecialDayRepository
from hds.services.pay import Pricing, shown, spanned, utc_window
from hds.services.pay_visibility import pay_visible

MAX_DAYS = 366
GroupBy = Literal["employee", "project", "client"]
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
    client_id: uuid.UUID | None = None
    client_name: str = ""


@dataclass
class _Totals:
    """What every level of the report adds up: person-hours, distinct shifts, pay where shown."""

    hours: Decimal = _ZERO
    pay: Decimal | None = None
    shift_ids: set[uuid.UUID] = field(default_factory=set, repr=False)
    missing_ids: set[uuid.UUID] = field(default_factory=set, repr=False)

    @property
    def shifts(self) -> int:
        """How many different shifts are under this total (a shift of three people counts once)."""
        return len(self.shift_ids)

    @property
    def shifts_missing_rate(self) -> int:
        """How many different shifts shown with pay have a line missing its rate, as ``shifts``."""
        return len(self.missing_ids)

    def add(self, line: ReportLine) -> None:
        """Add one line: its hours and shift; where the reader may see pay, its pay or its gap.

        A pay figure exists only once a line with a rate is added: lines missing a rate are
        counted, never added as zero (FR-004).
        """
        self.hours += line.hours
        self.shift_ids.add(line.shift_id)
        if not line.pay_shown:
            return
        if line.pay is None:
            self.missing_ids.add(line.shift_id)
        else:
            self.pay = (self.pay or _ZERO) + line.pay


@dataclass
class Row(_Totals):
    """Totals of one employee at one project."""

    id: uuid.UUID = field(kw_only=True)
    name: str = field(kw_only=True)


@dataclass
class Group(_Totals):
    """Totals of one employee or project, with a row per project or employee."""

    id: uuid.UUID = field(kw_only=True)
    name: str = field(kw_only=True)
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
    """Who worked where in the range, with the key figures the report opens with (007 FR-050)."""

    first: date
    last: date
    rows: list[WorkLogRow]

    @property
    def people(self) -> int:
        """How many different people worked in the range."""
        return len({row.user_id for row in self.rows})

    @property
    def shifts(self) -> int:
        """Person-shifts: each person on each shift counted once, as the rows count them."""
        return sum(row.shifts for row in self.rows)

    @property
    def hours(self) -> Decimal:
        """Person-hours, the exact sum of the rows' hours."""
        return sum((row.hours for row in self.rows), Decimal(0))


def aggregate(lines: Iterable[ReportLine], group_by: GroupBy) -> HoursReport:
    """Group priced lines by employee (rows per project), or by project or client (rows per person).

    Hours are person-hours, summed; shifts are distinct shifts. A pay total exists exactly where
    some line under it is one whose pay the reader may see and that has a rate; it is the sum of
    their pay. Shown lines missing a rate add nothing and their shifts are counted in
    ``shifts_missing_rate``, so a total of only such lines has a count and no pay figure, never
    zero (FR-004). Where no line's pay may be seen there is no pay figure at all (FR-028). The
    dates are left to the caller.
    """
    groups: dict[uuid.UUID, Group] = {}
    rows: dict[tuple[uuid.UUID, uuid.UUID], Row] = {}
    report = HoursReport(group_by=group_by)
    for line in lines:
        if group_by == "employee":
            outer, inner = (line.user_id, line.user_name), (line.project_id, line.project_name)
        elif group_by == "client":
            # A client's hours, one row per employee across its projects (007, PR 5).
            client = line.client_id or line.project_id  # every project has its client
            outer, inner = (client, line.client_name), (line.user_id, line.user_name)
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


def check_range(first: date, last: date, *, limited: bool = True) -> None:
    """Refuse a range that ends before it starts or covers more than 366 days (FR-030).

    ``limited`` is false only for a project's work-log with no start date, which covers the whole
    project (FR-024).

    Raises:
        DomainValidationError: ``reports.range_invalid`` (HTTP 422).
    """
    if last < first or (limited and (last - first).days >= MAX_DAYS):
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
        client_repo: ClientRepository,
    ) -> None:
        """Wire the service to its read-only queries."""
        self._shifts = report_shift_repo
        self._orgs = organisation_repo
        self._days = special_day_repo
        self._managers = project_manager_repo
        self._projects = project_repo
        self._sites = site_repo
        self._groups = group_repo
        self._clients = client_repo

    async def _organisation(self, actor: User) -> Organisation:
        org = await self._orgs.get_by_id(actor.organisation_id)
        if org is None:
            raise NotFoundError(resource="Organisation", id=actor.organisation_id)
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
            org_id=actor.organisation_id,
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
        """Who worked at a project, a site, a group or the whole organisation (Manager or Admin).

        No project, site or group means the whole organisation, read under the same rule as any one
        project's log: every Manager and Admin (007, the owner's PR 5 addition); it needs a start.

        Args:
            actor: The caller.
            project_id: A project; ``first`` then defaults to its start date, and the range then
                has no 366-day limit, so the log covers the whole project (FR-024, FR-030).
            site_id: A site: every project at it.
            group_id: A group: its current members, at every project.
            first: The first date; required for a site or group.
            last: The last date; today in the organisation's zone by default.
            now: The current instant; tests pass a fixed one.

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
            DomainValidationError: ``reports.scope_invalid`` (more than one scope),
                ``reports.start_required`` or ``reports.range_invalid`` (HTTP 422).
            NotFoundError: The project, site or group is not the organisation's (HTTP 404).
        """
        ensure_role(actor, *MANAGER_ROLES)
        if sum(x is not None for x in (project_id, site_id, group_id)) > 1:
            raise DomainValidationError(
                code="reports.scope_invalid",
                detail="Give at most one of project_id, site_id and group_id.",
            )
        now = now or datetime.now(UTC)
        org = await self._organisation(actor)
        last = last or now.astimezone(ZoneInfo(org.timezone)).date()
        scope = Scope()
        whole_project = False
        if project_id is not None:
            project = await self._projects.get_by_id(project_id, actor.organisation_id)
            if project is None:
                raise NotFoundError(resource="Project", id=project_id)
            if first is None:
                # Since the project started, however long ago (FR-024, FR-030).
                first, whole_project = project.start_date, True
            scope = Scope(project_ids=[project_id])
        elif site_id is not None:
            if await self._sites.get_by_id(site_id, actor.organisation_id) is None:
                raise NotFoundError(resource="Site", id=site_id)
            scope = Scope(site_id=site_id)
        elif group_id is not None:
            if await self._groups.get_by_id(group_id, actor.organisation_id) is None:
                raise NotFoundError(resource="Group", id=group_id)
            scope = Scope(user_ids=await self._groups.member_ids(group_id))
        if first is None:  # only a project has a start of its own
            raise DomainValidationError(
                code="reports.start_required",
                detail="A work-log of a site, a group or the organisation needs a start date.",
            )
        check_range(first, last, limited=not whole_project)
        shifts = await self._in_range(actor, first, last, now, scope)
        names = {
            p.id: p.name
            for p in await self._projects.get_many(
                actor.organisation_id, {s.project_id for s in shifts}
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
        return await pay_visible(actor, projects, self._managers)

    async def hours(
        self,
        actor: User,
        first: date,
        last: date,
        group_by: GroupBy,
        now: datetime | None = None,
    ) -> HoursReport:
        """Hours, shifts and, where the reader may see it, pay, by employee, project or client.

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
            p.id: p for p in await self._projects.get_many(actor.organisation_id, project_ids)
        }
        visible = await self._visible(actor, projects.values())
        # Only the report's own clients, deleted ones too: the work was done (code review, PR 5).
        clients = (
            {
                c.id: c.name
                for c in await self._clients.get_many(
                    actor.organisation_id, {p.client_id for p in projects.values()}
                )
            }
            if group_by == "client"
            else {}
        )
        paid_shifts = [s for s in shifts if s.project_id in visible]
        days = (
            await self._days.for_projects(actor.organisation_id, visible, *spanned(paid_shifts))
            if paid_shifts
            else []
        )
        pricing = Pricing(org, days)
        lines = []
        for s in shifts:
            project = projects[s.project_id]
            if s.project_id not in visible:
                # Hours only: a line whose pay the reader may not see is never priced.
                lines.append(
                    ReportLine(
                        shift_id=s.shift_id,
                        user_id=s.user_id,
                        user_name=s.full_name,
                        project_id=project.id,
                        project_name=project.name,
                        date=local_date(s),
                        hours=_hours(s),
                        pay_shown=False,
                        pay=None,
                        rate_missing=False,
                        client_id=project.client_id,
                        client_name=clients.get(project.client_id, ""),
                    )
                )
                continue
            priced = pricing.price_one(
                s.shift_id,
                s.starts_at,
                s.ends_at,
                s.pay_rate,
                project,
                s.timezone,
                s.default_rate,
                paid=True,
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
                    pay_shown=True,
                    pay=priced.pay,
                    rate_missing=priced.rate_missing,
                    client_id=project.client_id,
                    client_name=clients.get(project.client_id, ""),
                )
            )
        report = aggregate(lines, group_by)
        report.first, report.last = first, last
        return report


def _hours(shift: ReportShift) -> Decimal:
    """A shift's real length in hours, as shown (two decimals, half up, as in pay to date)."""
    micros = Decimal((shift.ends_at - shift.starts_at) // timedelta(microseconds=1))
    return shown(micros / MICROS_PER_HOUR)
