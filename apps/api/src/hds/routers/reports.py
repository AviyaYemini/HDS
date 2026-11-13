"""Reports router: who worked where, and hours and pay per employee and project.

Layer: routers. Delegates to ``ReportsService``, which ``hds.wiring`` builds; HTTP only. Managers
and Admins only (005 FR-023 to FR-028).
"""

import uuid
from datetime import date, timedelta
from typing import Any, Literal

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field

from hds.core.deps import require_manager
from hds.core.types import Amount, HoursOut, IsoDate, ShareOut
from hds.models.user import User
from hds.services.overview import Money, Overview, OverviewService, Previous
from hds.services.reports import Group, HoursReport, ReportsService, Row, WorkLog
from hds.wiring import overview, reports

router = APIRouter(prefix="/api/v1/reports", tags=["reports"])


class WorkLogRowRead(BaseModel):
    """One person at one project within the range."""

    user_id: uuid.UUID
    full_name: str
    project_id: uuid.UUID
    project_name: str
    shifts: int
    hours: HoursOut
    first_date: date
    last_date: date


class WorkLogRead(BaseModel):
    """Who worked in the range, once per person and project, by name; never any pay.

    ``people``, ``shifts`` (person-shifts) and ``hours`` (person-hours) are the key figures the
    report opens with, summed by the server so the client never adds them up (007 FR-050).
    """

    first: date = Field(serialization_alias="from")
    last: date = Field(serialization_alias="to")
    rows: list[WorkLogRowRead]
    people: int
    shifts: int
    hours: HoursOut


class HoursRowRead(BaseModel):
    """One employee at one project; ``pay`` only where the reader may see it."""

    id: uuid.UUID
    name: str
    hours: HoursOut
    shifts: int
    shifts_missing_rate: int = Field(
        description="Shifts shown with pay that have a line missing a rate; 0 where pay is hidden."
    )
    pay: Amount | None = Field(
        default=None,
        description=(
            "Left out where pay is not shown, and where every line shown with pay lacks a rate "
            "(then shifts_missing_rate is above 0): a missing rate is never shown as zero."
        ),
    )
    billing: Amount | None = Field(
        default=None,
        description="Hours × the project's billing rate, where pay is shown (CEO demo).",
    )


class HoursGroupRead(HoursRowRead):
    """One employee (rows per project) or one project (rows per employee)."""

    rows: list[HoursRowRead]


class HoursReportRead(BaseModel):
    """Hours and shifts by employee or project; pay totals cover only the lines shown with pay."""

    first: date = Field(serialization_alias="from")
    last: date = Field(serialization_alias="to")
    group_by: Literal["employee", "project", "client"]
    groups: list[HoursGroupRead]
    hours: HoursOut
    shifts: int
    shifts_missing_rate: int
    pay: Amount | None = Field(
        default=None, description="Left out when no pay is shown or no line shown has a rate."
    )
    billing: Amount | None = Field(
        default=None,
        description="Hours × each project's billing rate, where pay is shown (CEO demo).",
    )


def _totals(item: Row | Group | HoursReport) -> dict[str, Any]:
    fields: dict[str, Any] = {
        "hours": item.hours,
        "shifts": item.shifts,
        "shifts_missing_rate": item.shifts_missing_rate,
    }
    if item.pay is not None:
        fields["pay"] = item.pay
    if item.billing is not None:
        fields["billing"] = item.billing
    return fields


def read_hours(report: HoursReport) -> HoursReportRead:
    """The response form of the hours report, with pay only where it is shown."""
    groups = [
        HoursGroupRead.model_validate(
            {
                "id": g.id,
                "name": g.name,
                **_totals(g),
                "rows": [
                    HoursRowRead.model_validate({"id": r.id, "name": r.name, **_totals(r)})
                    for r in g.rows
                ],
            }
        )
        for g in report.groups
    ]
    return HoursReportRead.model_validate(
        {
            "first": report.first,
            "last": report.last,
            "group_by": report.group_by,
            "groups": groups,
            **_totals(report),
        }
    )


def read_log(log: WorkLog) -> WorkLogRead:
    """The response form of the work-log."""
    return WorkLogRead(
        first=log.first,
        last=log.last,
        rows=[WorkLogRowRead.model_validate(row, from_attributes=True) for row in log.rows],
        people=log.people,
        shifts=log.shifts,
        hours=log.hours,
    )


@router.get(
    "/work-log",
    response_model=WorkLogRead,
    response_model_by_alias=True,
    summary="Who worked at a project, site, group or the whole organisation",
    description=(
        "Manager or Admin. Each person once per project with their shift count, hours and first "
        "and last date in the range, for at most one of a project, a site or a group (two or more "
        "422 `reports.scope_invalid`); none means the whole organisation, which any Manager or "
        "Admin may read, as they may any project's log. For a project the range starts on its "
        "start date unless `from` is given, and then covers the whole project with no length "
        "limit; a site, a group or the whole organisation needs `from` (422 "
        "`reports.start_required`). `to` is today by default. A range "
        "with `from` covers at most 366 days (422 `reports.range_invalid`). Only published "
        "shifts that have ended count; no pay is shown."
    ),
)
async def work_log(
    project_id: uuid.UUID | None = None,
    site_id: uuid.UUID | None = None,
    group_id: uuid.UUID | None = None,
    first: IsoDate | None = Query(default=None, alias="from"),
    last: IsoDate | None = Query(default=None, alias="to"),
    current_user: User = Depends(require_manager),
    svc: ReportsService = Depends(reports),
) -> WorkLogRead:
    """Who worked at a project, site or group.

    Manager or Admin. Once per person and project, with no pay.
    """
    log = await svc.work_log(
        current_user,
        project_id=project_id,
        site_id=site_id,
        group_id=group_id,
        first=first,
        last=last,
    )
    return read_log(log)


@router.get(
    "/hours",
    response_model=HoursReportRead,
    response_model_by_alias=True,
    response_model_exclude_unset=True,
    summary="Hours and pay per employee, project or client",
    description=(
        "Manager or Admin. Hours and shift counts from `from` to `to` (at most 366 days, 422 "
        "`reports.range_invalid`), grouped by employee, by project or by client (a client's rows "
        "are its employees), with the grand totals. An "
        "Admin sees pay for every project with pay on; a Manager only for projects they are "
        "assigned to whose pay is open to Managers. Elsewhere there is no pay figure at all, "
        "and pay totals cover only the lines shown with pay."
    ),
)
async def hours(
    first: IsoDate = Query(alias="from"),
    last: IsoDate = Query(alias="to"),
    group_by: Literal["employee", "project", "client"] = Query(),
    current_user: User = Depends(require_manager),
    svc: ReportsService = Depends(reports),
) -> HoursReportRead:
    """Hours and pay per employee or project.

    Manager or Admin. Pay only where the reader may see it.
    """
    return read_hours(await svc.hours(current_user, first, last, group_by))


# ── The snapshot dashboard (CEO demo) ──────────────────────────────────────────────────────────


class CoverageRead(BaseModel):
    """This week's places: how many the shifts need and how many are filled."""

    week_start: date
    filled: int
    needed: int
    empty_places: int
    projects_with_gaps: int


class PeopleRead(BaseModel):
    """Active Employees and Managers, who works this week, and the temporary workers."""

    active: int
    working_this_week: int
    temporary: int
    temporary_ending_soon: int = Field(
        description="Temporary workers whose last day is within 14 days."
    )
    near_weekly_limit: int = Field(
        description="People scheduled this week within 4 hours of the most hours in a week."
    )


class OverviewWeekRead(BaseModel):
    """One week of the range: hours, billing and pay, and whether it is still a forecast."""

    week_start: date
    hours: HoursOut
    billing: Amount | None = None
    pay: Amount | None = None
    forecast: bool


class OverviewDayRead(BaseModel):
    """One day of the range with work on it, for the daily candles (CEO demo)."""

    date: date
    hours: HoursOut
    billing: Amount | None = None
    pay: Amount | None = None
    forecast: bool = Field(description="True from today on: still a forecast from the roster.")


class OverviewProjectWeekRead(BaseModel):
    """One week of one project, for its trend line."""

    week_start: date
    hours: HoursOut
    billing: Amount | None = None


class OverviewPreviousRead(BaseModel):
    """The period before, for the key figures' change: whole months back for a month range."""

    first: date = Field(serialization_alias="from")
    last: date = Field(serialization_alias="to")
    hours: HoursOut
    billing: Amount | None = None
    pay: Amount | None = None
    margin_pct: ShareOut | None = None
    hours_to_date: HoursOut = Field(
        description="Its hours up to the same day as today in this range."
    )
    billing_to_date: Amount | None = Field(
        default=None, description="Its billing up to the same day as today in this range."
    )
    hours_to_rostered: HoursOut = Field(
        description="Its hours up to the same day as this range's last rostered day."
    )
    billing_to_rostered: Amount | None = Field(
        default=None,
        description="Its billing up to the same day as this range's last rostered day: compare "
        "the forecast with this, as the forecast stops where the roster does.",
    )


class OverviewProjectRead(BaseModel):
    """One project's row: its money in the range and this week's cover."""

    id: uuid.UUID
    name: str
    client_name: str
    managers: list[str]
    type: Literal["security", "training"]
    hours: HoursOut
    billing: Amount | None = None
    pay: Amount | None = None
    margin_pct: ShareOut | None = Field(default=None, description="Negative for a loss.")
    coverage_pct: ShareOut | None = Field(
        default=None, description="Null when no places this week."
    )
    empty_places: int
    empty_places_next: int = Field(description="Empty places next week.")
    temporary_workers: int
    terms_estimated: bool
    status: Literal["ok", "gaps", "loss", "no_rate"]
    weeks: list[OverviewProjectWeekRead]


class OverviewRead(BaseModel):
    """The snapshot: money, cover and people, by week and by project (CEO demo)."""

    first: date = Field(serialization_alias="from")
    last: date = Field(serialization_alias="to")
    today: date
    billing_actual: Amount | None = None
    billing_forecast: Amount | None = None
    pay_actual: Amount | None = None
    pay_forecast: Amount | None = None
    margin_pct: ShareOut | None = None
    hours_actual: HoursOut
    hours_forecast: HoursOut
    coverage: CoverageRead
    coverage_next: CoverageRead = Field(description="Next week's places, the week being prepared.")
    people: PeopleRead
    weeks: list[OverviewWeekRead]
    days: list[OverviewDayRead]
    projects: list[OverviewProjectRead]
    terms_estimated: bool
    previous: OverviewPreviousRead | None = None
    rostered_until: date | None = Field(
        default=None, description="The last day in the range with work on the roster."
    )


def _money(m: Money) -> dict[str, Any]:
    if not m.money_shown:
        return {}
    margin = m.billing_forecast - m.pay_forecast
    return {
        "billing_actual": m.billing_actual,
        "billing_forecast": m.billing_forecast,
        "pay_actual": m.pay_actual,
        "pay_forecast": m.pay_forecast,
        "margin_pct": (margin * 100 / m.billing_forecast) if m.billing_forecast else None,
    }


def read_overview(o: Overview) -> OverviewRead:
    """The response form of the snapshot."""
    return OverviewRead(
        first=o.first,
        last=o.last,
        today=o.today,
        hours_actual=o.money.hours_actual,
        hours_forecast=o.money.hours_forecast,
        coverage=CoverageRead(
            week_start=o.week_start,
            filled=o.filled,
            needed=o.needed,
            empty_places=o.needed - o.filled,
            projects_with_gaps=o.projects_with_gaps,
        ),
        coverage_next=CoverageRead(
            week_start=o.week_start + timedelta(days=7),
            filled=o.filled_next,
            needed=o.needed_next,
            empty_places=o.needed_next - o.filled_next,
            projects_with_gaps=o.projects_with_gaps_next,
        ),
        people=PeopleRead(
            active=o.active,
            working_this_week=o.working_this_week,
            temporary=o.temporary,
            temporary_ending_soon=o.temporary_ending_soon,
            near_weekly_limit=o.near_weekly_limit,
        ),
        weeks=[
            OverviewWeekRead(
                week_start=start,
                hours=m.hours_forecast,
                billing=m.billing_forecast if m.money_shown else None,
                pay=m.pay_forecast if m.money_shown else None,
                forecast=start + timedelta(days=6) >= o.today,
            )
            for start, m in o.weeks
        ],
        days=[
            OverviewDayRead(
                date=day,
                hours=m.hours_forecast,
                billing=m.billing_forecast if m.money_shown else None,
                pay=m.pay_forecast if m.money_shown else None,
                forecast=day >= o.today,
            )
            for day, m in o.days
        ],
        projects=[
            OverviewProjectRead(
                id=r.project.id,
                name=r.project.name,
                client_name=r.client_name,
                managers=r.managers,
                type=r.project.type.value,
                hours=r.money.hours_forecast,
                billing=r.money.billing_forecast if r.money.money_shown else None,
                pay=r.money.pay_forecast if r.money.money_shown else None,
                margin_pct=r.margin_pct,
                coverage_pct=(r.filled * 100 / r.needed) if r.needed else None,
                empty_places=r.needed - r.filled,
                empty_places_next=r.needed_next - r.filled_next,
                temporary_workers=r.temporary_workers,
                terms_estimated=r.project.terms_estimated,
                status=r.status,  # type: ignore[arg-type]
                weeks=[
                    OverviewProjectWeekRead(
                        week_start=start,
                        hours=m.hours_forecast,
                        billing=m.billing_forecast if m.money_shown else None,
                    )
                    for start, m in sorted(r.weeks.items())
                ],
            )
            for r in o.projects
        ],
        terms_estimated=o.terms_estimated,
        previous=_previous(o.previous) if o.previous else None,
        rostered_until=o.rostered_until,
        **_money(o.money),
    )


def _previous(prev: Previous) -> OverviewPreviousRead:
    m = prev.money
    shown = m.money_shown
    return OverviewPreviousRead(
        first=prev.first,
        last=prev.last,
        hours=m.hours_forecast,
        billing=m.billing_forecast if shown else None,
        pay=m.pay_forecast if shown else None,
        margin_pct=(
            (m.billing_forecast - m.pay_forecast) * 100 / m.billing_forecast
            if shown and m.billing_forecast
            else None
        ),
        hours_to_date=prev.hours_to_date,
        billing_to_date=prev.billing_to_date,
        hours_to_rostered=prev.hours_to_rostered,
        billing_to_rostered=prev.billing_to_rostered,
    )


@router.get(
    "/overview",
    response_model=OverviewRead,
    response_model_by_alias=True,
    summary="The snapshot: money, cover and people (CEO demo)",
    description=(
        "Manager or Admin; a Manager sees only the projects they manage. From `from` to `to` (at "
        "most 366 days, 422 `reports.range_invalid`): billing (person-hours × the project's "
        "billing rate) and gross pay, actual (published shifts that have ended) and forecast (plus "
        "every shift, draft or published, still to come), by week and by project; this week's "
        "cover; and the people, temporary workers included. Money appears only where the reader "
        "may see the project's pay."
    ),
)
async def get_overview(
    first: IsoDate = Query(alias="from"),
    last: IsoDate = Query(alias="to"),
    current_user: User = Depends(require_manager),
    svc: OverviewService = Depends(overview),
) -> OverviewRead:
    """The snapshot dashboard (CEO demo)."""
    return read_overview(await svc.overview(current_user, first, last))
