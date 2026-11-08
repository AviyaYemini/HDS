"""Pay to date: the gross pay a person has earned so far in a month (005 FR-017 to FR-020).

Layer: services. Depends on ``hds.core`` (errors, roles), ``hds.models``, ``hds.payroll`` (the only
place pay is worked out) and the repositories it is given; it MUST NOT import routers or know about
HTTP. Read-only.

A shift counts when it is published and has ended (FR-029), in the month of its local start date at
its site. Each line is priced by ``hds.payroll.price`` with the project's settings and special days
as they stand now (FR-016); hours and pay are shown to two decimals, and every total is the sum of
the lines shown. A project with pay off shows hours and no pay; a shift with no rate shows that the
rate is missing, never zero (FR-004, FR-019).
"""

import re
import uuid
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import ROUND_DOWN, ROUND_HALF_UP, Decimal
from typing import Any
from zoneinfo import ZoneInfo

from hds.core.authz import STAFF_ROLES, ensure_role
from hds.core.exceptions import DomainValidationError, NotFoundError
from hds.models.organisation import Organisation
from hds.models.project import Project, ProjectStatus
from hds.models.special_day import SpecialDay
from hds.models.user import User
from hds.payroll import CENT, PayPart, PaySettings, price
from hds.payroll import SpecialDay as PaidDay
from hds.repositories.organisation import OrganisationRepository
from hds.repositories.pay_reports import PayReportRepository, WorkedShift
from hds.repositories.special_day import SpecialDayRepository
from hds.services.pay_settings import project_settings
from hds.services.special_day import merge_days

MARGIN = timedelta(days=1)
"""No zone is more than 14 hours from UTC, so a day either side holds every local date."""
_MONTH = re.compile(r"^([0-9]{4})-(0[1-9]|1[0-2])$")


def shown(value: Decimal) -> Decimal:
    """A number as shown: two decimals, half up."""
    return value.quantize(CENT, rounding=ROUND_HALF_UP)


@dataclass(frozen=True)
class PartView:
    """Hours paid at one rate, as shown, and their share of the line's pay (to the agora)."""

    rate_pct: Decimal
    hours: Decimal
    pay: Decimal | None = None


@dataclass(frozen=True)
class LineView:
    """One person on one ended shift, priced."""

    shift_id: uuid.UUID
    project_id: uuid.UUID
    project_name: str
    date: date
    starts_at: datetime
    ends_at: datetime
    timezone: str
    hours: Decimal
    base_rate: Decimal | None
    rate_missing: bool
    pay_enabled: bool
    parts: list[PartView]
    pay: Decimal | None


@dataclass(frozen=True)
class PayToDate:
    """One person's lines for a month and their totals."""

    month: str
    lines: list[LineView] = field(default_factory=list)
    hours: Decimal = Decimal("0.00")
    pay: Decimal | None = Decimal("0.00")
    """The sum of the lines' pay; ``None`` when no line has pay and some line lacks a rate, so a
    month whose rates are all missing is never shown as zero (FR-004)."""
    shifts_missing_rate: int = 0


class Pricing:
    """Prices worked shifts with each project's settings and days as they stand now."""

    def __init__(self, org: Organisation, days: Iterable[SpecialDay]) -> None:
        """Hold the organisation's defaults and every special day that may apply."""
        self._org = org
        self._days = list(days)
        self._settings: dict[uuid.UUID, PaySettings] = {}
        self._paid_days: dict[uuid.UUID, list[PaidDay]] = {}

    def _for(self, project: Project) -> tuple[PaySettings, list[PaidDay]]:
        if project.id not in self._settings:
            self._settings[project.id] = project_settings(self._org, project)[0]
            fixed = project.status == ProjectStatus.ended or project.is_deleted
            rates = merge_days(self._days, project.id, fixed)
            self._paid_days[project.id] = [PaidDay(d, r) for d, r in rates.items()]
        return self._settings[project.id], self._paid_days[project.id]

    def line(self, worked: WorkedShift, default_rate: Decimal | None) -> LineView:
        """Price one person on one shift; ``default_rate`` is the person's own rate."""
        shift = worked.shift
        return self.price_one(
            shift.id,
            worked.starts_at,
            worked.ends_at,
            shift.pay_rate,
            worked.project,
            worked.timezone,
            default_rate,
            paid=worked.project.pay_enabled,
        )

    def price_one(
        self,
        shift_id: uuid.UUID,
        starts_at: datetime,
        ends_at: datetime,
        shift_rate: Decimal | None,
        project: Project,
        timezone: str,
        default_rate: Decimal | None,
        *,
        paid: bool,
    ) -> LineView:
        """Price one person on one shift of ``project``.

        Args:
            shift_id: The shift.
            starts_at: Its start.
            ends_at: Its end.
            shift_rate: The shift's own rate, which wins over ``default_rate`` (FR-004).
            project: Its project, for the settings and special days.
            timezone: Its site's zone.
            default_rate: The person's own rate.
            paid: Whether pay is worked out at all: the project has pay on and the reader may
                see it. Without pay, the line keeps its hours and parts and shows no rate.

        Returns:
            The line, with ``pay`` ``None`` when not paid or when there is no rate.
        """
        settings, days = self._for(project)
        base = shift_rate if shift_rate is not None else default_rate
        priced = price(starts_at, ends_at, timezone, base if paid else None, settings, days)
        hours = shown(priced.hours)
        return LineView(
            shift_id=shift_id,
            project_id=project.id,
            project_name=project.name,
            date=starts_at.astimezone(ZoneInfo(timezone)).date(),
            starts_at=starts_at,
            ends_at=ends_at,
            timezone=timezone,
            hours=hours,
            base_rate=base if paid else None,
            rate_missing=paid and base is None,
            pay_enabled=project.pay_enabled,
            parts=shown_parts(priced.parts, hours),
            pay=priced.pay,
        )


def month_bounds(text: str | None, zone: str, now: datetime) -> tuple[str, date, date]:
    """The month asked for (the current one in ``zone`` when none), and its first and last day.

    Raises:
        DomainValidationError: ``pay.month_invalid`` for anything but ``YYYY-MM`` (HTTP 422).
    """
    if text is None:
        local = now.astimezone(ZoneInfo(zone))
        text = f"{local.year:04d}-{local.month:02d}"
    found = _MONTH.match(text)
    if found is None or int(found.group(1)) < 1:
        raise DomainValidationError(
            code="pay.month_invalid", detail="month must be written YYYY-MM, such as 2026-10."
        )
    year, month = int(found.group(1)), int(found.group(2))
    first = date(year, month, 1)
    following = date(year + month // 12, month % 12 + 1, 1) if year < 9999 or month < 12 else None
    last = (following - timedelta(days=1)) if following else date(9999, 12, 31)
    return text, first, last


def spanned(shifts: Iterable[Any]) -> tuple[date, date]:
    """The dates, widened by a day, from the earliest start to the latest end of ``shifts``.

    Special days are read over this span, not the range asked for: a shift that starts in the
    range may run on for days, and every local date it touches needs its special day. The shifts
    carry ``starts_at`` and ``ends_at``; there must be at least one.
    """
    items = list(shifts)
    first = min(s.starts_at for s in items).astimezone(UTC).date()
    last = max(s.ends_at for s in items).astimezone(UTC).date()
    return widen(first, last)


def shown_parts(parts: Iterable[PayPart], hours: Decimal) -> list[PartView]:
    """The hours at each rate, rounded so that they add up to the line's shown ``hours``.

    Each part is cut down to the hundredth and the missing hundredths go to the parts that lost
    the most, the first of them on a tie (largest remainder), so no part moves by more than 0.01
    and the parts never disagree with the line (FR-015).
    """
    items = list(parts)
    floors = [p.hours.quantize(CENT, rounding=ROUND_DOWN) for p in items]
    left = int((hours - sum(floors, Decimal(0))) / CENT)
    order = sorted(range(len(items)), key=lambda i: floors[i] - items[i].hours)
    for i in order[: max(left, 0)]:
        floors[i] += CENT
    return [PartView(p.rate_pct, f, p.pay) for p, f in zip(items, floors, strict=True)]


def widen(first: date, last: date) -> tuple[date, date]:
    """``first`` and ``last`` a day further apart, kept inside the calendar."""
    lower = first - MARGIN if first > date.min + MARGIN else date.min
    upper = last + MARGIN if last < date.max - MARGIN else date.max
    return lower, upper


def utc_window(first: date, last: date) -> tuple[datetime, datetime]:
    """A UTC window holding every instant whose local date, anywhere, is ``first`` to ``last``.

    Kept inside the calendar, so a month at either end of it (0001-01, 9999-12) is no error.
    """
    lower, upper = widen(first, last)
    start = datetime.combine(lower, datetime.min.time(), UTC)
    end = (
        datetime.combine(upper + timedelta(days=1), datetime.min.time(), UTC)
        if upper < date.max
        else datetime.max.replace(tzinfo=UTC)
    )
    return start, end


class PayService:
    """A person's gross pay to date."""

    def __init__(
        self,
        pay_report_repo: PayReportRepository,
        organisation_repo: OrganisationRepository,
        special_day_repo: SpecialDayRepository,
    ) -> None:
        """Wire the service to the read-only queries, organisations and special days."""
        self._reports = pay_report_repo
        self._orgs = organisation_repo
        self._days = special_day_repo

    async def my_pay(
        self, actor: User, month: str | None, now: datetime | None = None
    ) -> PayToDate:
        """The caller's pay for ``month`` (``YYYY-MM``), from shifts that have ended.

        Args:
            actor: Any Employee, Manager or Admin; each sees only their own shifts.
            month: The month, or ``None`` for the current one in the organisation's time zone.
            now: The current instant; tests pass a fixed one.

        Raises:
            ForbiddenError: ``users.forbidden`` for a Client (HTTP 403).
            DomainValidationError: ``pay.month_invalid`` (HTTP 422).
        """
        ensure_role(actor, *STAFF_ROLES)
        now = now or datetime.now(UTC)
        org = await self._orgs.get_by_id(actor.organization_id)
        if org is None:
            raise NotFoundError(resource="Organisation", id=actor.organization_id)
        text, first, last = month_bounds(month, org.timezone, now)
        starts_from, starts_before = utc_window(first, last)
        rows = await self._reports.own_ended_shifts(
            org_id=actor.organization_id,
            user_id=actor.id,
            starts_from=starts_from,
            starts_before=starts_before,
            now=now,
        )
        rows = [
            r
            for r in rows
            if first <= r.shift.starts_at.astimezone(ZoneInfo(r.timezone)).date() <= last
        ]
        days = (
            await self._days.for_projects(
                actor.organization_id, {r.project.id for r in rows}, *spanned(r.shift for r in rows)
            )
            if rows
            else []
        )
        pricing = Pricing(org, days)
        lines = [pricing.line(r, actor.default_hourly_rate) for r in rows]
        paid = [line.pay for line in lines if line.pay is not None]
        missing = sum(1 for line in lines if line.rate_missing)
        return PayToDate(
            month=text,
            lines=lines,
            hours=sum((line.hours for line in lines), Decimal("0.00")),
            pay=None if missing and not paid else sum(paid, Decimal("0.00")),
            shifts_missing_rate=missing,
        )
