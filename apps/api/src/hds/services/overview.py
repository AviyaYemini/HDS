"""The snapshot dashboard: money, cover and people in one read (CEO demo).

Layer: services. Billing is person-hours × the project's billing rate; pay is priced as the hours
report prices it (premiums, special days, a shift's own rate over the person's). "Actual" counts
published shifts that have ended; the forecast adds every shift, draft or published, still to come
in the range. An Admin sees every project; a Manager only those they manage. Money is shown only
where the reader may see the project's pay, as in the hours report.
"""

import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

from hds.core.authz import MANAGER_ROLES, ensure_role
from hds.core.exceptions import NotFoundError
from hds.models.project import Project
from hds.models.user import User, UserRole
from hds.repositories.client import ClientRepository
from hds.repositories.organisation import OrganisationRepository
from hds.repositories.overview import OverviewRepository
from hds.repositories.project import ProjectRepository
from hds.repositories.project_manager import ProjectManagerRepository
from hds.repositories.project_member import ProjectMemberRepository
from hds.repositories.special_day import SpecialDayRepository
from hds.services.pay import Pricing, shown, spanned, utc_window
from hds.services.pay_visibility import pay_visible
from hds.services.reports import billed, check_range

ZERO = Decimal(0)
HOUR = timedelta(hours=1)
NEAR_LIMIT = timedelta(hours=4)
ENDING_SOON = timedelta(days=14)


def sunday(day: date) -> date:
    """The Sunday that starts ``day``'s week (weeks run Sunday to Saturday)."""
    return day - timedelta(days=(day.weekday() + 1) % 7)


def pct(part: Decimal, whole: Decimal) -> Decimal | None:
    """``part`` as a percentage of ``whole``, two decimals; ``None`` when ``whole`` is zero."""
    return shown(part * 100 / whole) if whole else None


@dataclass
class Money:
    """Hours, billing and pay, actual and forecast."""

    hours_actual: Decimal = ZERO
    hours_forecast: Decimal = ZERO
    billing_actual: Decimal = ZERO
    billing_forecast: Decimal = ZERO
    pay_actual: Decimal = ZERO
    pay_forecast: Decimal = ZERO
    money_shown: bool = False

    def add(self, hours: Decimal, billing: Decimal | None, pay: Decimal | None, done: bool) -> None:
        """One priced line: into the forecast always, into the actual when it is done."""
        self.hours_forecast += hours
        if done:
            self.hours_actual += hours
        if billing is None:
            return
        self.money_shown = True
        self.billing_forecast += billing
        self.pay_forecast += pay or ZERO
        if done:
            self.billing_actual += billing
            self.pay_actual += pay or ZERO


@dataclass
class ProjectLine:
    """One project's row in the table."""

    project: Project
    client_name: str
    managers: list[str]
    money: Money = field(default_factory=Money)
    needed: int = 0
    filled: int = 0
    needed_next: int = 0
    filled_next: int = 0
    temporary_workers: int = 0
    weeks: dict[date, Money] = field(default_factory=lambda: defaultdict(Money))

    @property
    def margin_pct(self) -> Decimal | None:
        """The forecast margin as a percentage of billing; ``None`` where money is not shown."""
        m = self.money
        if not m.money_shown:
            return None
        return pct(m.billing_forecast - m.pay_forecast, m.billing_forecast)

    @property
    def status(self) -> str:
        """``no_rate``, ``loss`` (negative margin), ``gaps`` (empty places this week) or ``ok``."""
        if not self.project.billing_rate:
            return "no_rate"
        margin = self.margin_pct
        if margin is not None and margin < 0:
            return "loss"
        if self.needed > self.filled:
            return "gaps"
        return "ok"


@dataclass
class Overview:
    """Everything the snapshot dashboard shows."""

    first: date
    last: date
    today: date
    money: Money
    week_start: date
    needed: int
    filled: int
    projects_with_gaps: int
    needed_next: int
    filled_next: int
    projects_with_gaps_next: int
    active: int
    working_this_week: int
    temporary: int
    temporary_ending_soon: int
    near_weekly_limit: int
    weeks: list[tuple[date, Money]]
    days: list[tuple[date, Money]]
    projects: list[ProjectLine]
    terms_estimated: bool
    previous: "Previous | None" = None
    rostered_until: date | None = None


@dataclass
class Previous:
    """The period before, for "▲ 6% from October": its range, totals and the same days so far."""

    first: date
    last: date
    money: Money
    billing_to_date: Decimal | None
    hours_to_date: Decimal
    billing_to_rostered: Decimal | None = None
    hours_to_rostered: Decimal = ZERO


def previous_range(first: date, last: date) -> tuple[date, date]:
    """The period just before ``first``..``last``.

    Whole calendar months back when the range is whole months, otherwise the same number of days.
    """
    next_day = last + timedelta(days=1)
    if first.day == 1 and next_day.day == 1:
        months = (next_day.year - first.year) * 12 + next_day.month - first.month
        year, month = divmod(first.month - 1 - months, 12)
        start = date(first.year + year, month + 1, 1)
        return start, first - timedelta(days=1)
    length = (last - first).days + 1
    return first - timedelta(days=length), first - timedelta(days=1)


class OverviewService:
    """Builds the snapshot dashboard."""

    def __init__(
        self,
        overview_repo: OverviewRepository,
        organisation_repo: OrganisationRepository,
        special_day_repo: SpecialDayRepository,
        project_manager_repo: ProjectManagerRepository,
        project_repo: ProjectRepository,
        client_repo: ClientRepository,
        member_repo: ProjectMemberRepository,
    ) -> None:
        """Wire the service to its read-only queries."""
        self._rows = overview_repo
        self._orgs = organisation_repo
        self._days = special_day_repo
        self._managers = project_manager_repo
        self._projects = project_repo
        self._clients = client_repo
        self._members = member_repo

    async def overview(
        self,
        actor: User,
        first: date,
        last: date,
        now: datetime | None = None,
        *,
        compare: bool = True,
    ) -> Overview:
        """The snapshot from ``first`` to ``last`` for ``actor`` (Manager or Admin).

        Raises:
            ForbiddenError: ``users.manager_required`` (HTTP 403).
            DomainValidationError: ``reports.range_invalid`` (HTTP 422).
        """
        ensure_role(actor, *MANAGER_ROLES)
        check_range(first, last)
        now = now or datetime.now(UTC)
        org = await self._orgs.get_by_id(actor.organisation_id)
        if org is None:
            raise NotFoundError(resource="Organisation", id=actor.organisation_id)
        zone = ZoneInfo(org.timezone)
        today = now.astimezone(zone).date()
        org_id = actor.organisation_id

        projects = {p.id: p for p in await self._projects.list_by_org(org_id)}
        if actor.role != UserRole.admin:
            managed = await self._managers.manages(actor.id, projects.keys())
            projects = {k: v for k, v in projects.items() if k in managed}
        scope = set(projects)
        visible = await pay_visible(actor, projects.values(), self._managers)

        week_start = sunday(today)
        week_end = week_start + timedelta(days=6)
        next_start, next_end = week_start + timedelta(days=7), week_end + timedelta(days=7)
        range_from, range_before = utc_window(min(first, week_start), max(last, week_end))
        lines = await self._rows.lines(org_id, range_from, range_before, scope)

        def local(moment: datetime, tz: str) -> date:
            """The calendar day of ``moment`` at the site."""
            return moment.astimezone(ZoneInfo(tz)).date()

        paid = [s for s in lines if s.project_id in visible]
        special = await self._days.for_projects(org_id, visible, *spanned(paid)) if paid else []
        pricing = Pricing(org, special)
        clients = {
            c.id: c.name
            for c in await self._clients.get_many(org_id, {p.client_id for p in projects.values()})
        }
        temporary_on = await self._members.temporary_counts(org_id)
        table = {
            p.id: ProjectLine(
                project=p,
                client_name=clients.get(p.client_id, ""),
                managers=[u.full_name for u in await self._managers.members(org_id, p.id)],
                temporary_workers=temporary_on.get(p.id, 0),
            )
            for p in projects.values()
        }

        # Money is shown when the reader may see any project's pay, even with no work yet.
        total = Money(money_shown=bool(visible))
        weeks: dict[date, Money] = defaultdict(Money)
        days: dict[date, Money] = defaultdict(Money)
        week_hours: dict[uuid.UUID, timedelta] = defaultdict(timedelta)
        working: set[uuid.UUID] = set()
        for s in lines:
            day = local(s.starts_at, s.timezone)
            if week_start <= day <= week_end:
                week_hours[s.user_id] += s.ends_at - s.starts_at
                working.add(s.user_id)
            if not first <= day <= last:
                continue
            project = projects[s.project_id]
            done = s.status == "published" and s.ends_at <= now
            if s.project_id in visible:
                priced = pricing.price_one(
                    s.shift_id,
                    s.starts_at,
                    s.ends_at,
                    s.pay_rate,  # type: ignore[arg-type]
                    project,
                    s.timezone,
                    s.default_rate,  # type: ignore[arg-type]
                    paid=True,
                )
                hours, billing, pay = priced.hours, billed(priced.hours, project), priced.pay
            else:
                hours = shown(Decimal((s.ends_at - s.starts_at) / HOUR))
                billing = pay = None
            total.add(hours, billing, pay, done)
            weeks[sunday(day)].add(hours, billing, pay, done)
            days[day].add(hours, billing, pay, done)
            table[s.project_id].money.add(hours, billing, pay, done)
            table[s.project_id].weeks[sunday(day)].add(hours, billing, pay, done)

        needed = filled = needed_next = filled_next = 0
        for c in await self._rows.cover(org_id, *utc_window(week_start, next_end), scope):
            day = local(c.starts_at, c.timezone)
            on = min(c.on_it, c.headcount)
            if week_start <= day <= week_end:
                needed += c.headcount
                filled += on
                table[c.project_id].needed += c.headcount
                table[c.project_id].filled += on
            elif next_start <= day <= next_end:
                needed_next += c.headcount
                filled_next += on
                table[c.project_id].needed_next += c.headcount
                table[c.project_id].filled_next += on

        # The last day in the range with work on the roster: the forecast stops there.
        rostered_until = max((d for d in days if first <= d <= last), default=None)
        # An Admin sees the whole organisation; a Manager the people of their own projects.
        staff = await self._rows.staff(
            org_id,
            None if actor.role == UserRole.admin else scope,
            range_from,
            range_before,
        )
        limit = timedelta(hours=org.max_weekly_hours) - NEAR_LIMIT
        rows = sorted(
            table.values(),
            key=lambda r: (-r.money.billing_forecast, -r.money.hours_forecast, r.project.name),
        )
        return Overview(
            first=first,
            last=last,
            today=today,
            money=total,
            week_start=week_start,
            needed=needed,
            filled=filled,
            projects_with_gaps=sum(1 for r in table.values() if r.needed > r.filled),
            needed_next=needed_next,
            filled_next=filled_next,
            projects_with_gaps_next=sum(1 for r in table.values() if r.needed_next > r.filled_next),
            active=len(staff),
            working_this_week=len(working),
            temporary=sum(1 for _, kind, _ in staff if kind == "temporary"),
            temporary_ending_soon=sum(
                1
                for _, kind, until in staff
                if kind == "temporary" and until and today <= until <= today + ENDING_SOON
            ),
            near_weekly_limit=sum(1 for h in week_hours.values() if h >= limit),
            weeks=sorted(weeks.items()),
            days=sorted(days.items()),
            projects=[r for r in rows if r.money.hours_forecast or r.needed or r.needed_next],
            terms_estimated=any(
                r.project.terms_estimated for r in rows if r.money.hours_forecast or r.needed
            ),
            previous=(
                await self._previous(actor, first, last, today, now, rostered_until)
                if compare
                else None
            ),
            rostered_until=rostered_until,
        )

    async def _previous(
        self,
        actor: User,
        first: date,
        last: date,
        today: date,
        now: datetime,
        rostered_until: date | None,
    ) -> Previous:
        """The period before, and how far it had got by the same day (for "so far" deltas)."""
        prev_first, prev_last = previous_range(first, last)
        before = await self.overview(actor, prev_first, prev_last, now, compare=False)
        cutoff = prev_first + (today - first)
        so_far = [m for day, m in before.days if day <= cutoff]
        # The same span the forecast covers: up to the last rostered day, as far into the period.
        reach = prev_first + ((rostered_until or last) - first)
        rostered = [m for day, m in before.days if day <= reach]
        shown = before.money.money_shown
        return Previous(
            first=prev_first,
            last=prev_last,
            money=before.money,
            billing_to_date=(
                sum((m.billing_forecast for m in so_far), ZERO)
                if before.money.money_shown
                else None
            ),
            hours_to_date=sum((m.hours_forecast for m in so_far), ZERO),
            billing_to_rostered=(
                sum((m.billing_forecast for m in rostered), ZERO) if shown else None
            ),
            hours_to_rostered=sum((m.hours_forecast for m in rostered), ZERO),
        )
