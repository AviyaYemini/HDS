"""Reports router: who worked where, and hours and pay per employee and project.

Layer: routers. Delegates to ``ReportsService``, which ``hds.wiring`` builds; HTTP only. Managers
and Admins only (005 FR-023 to FR-028).
"""

import uuid
from datetime import date
from typing import Any, Literal

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field

from hds.core.deps import require_manager
from hds.core.types import Amount, HoursOut, IsoDate
from hds.models.user import User
from hds.services.reports import Group, HoursReport, ReportsService, Row, WorkLog
from hds.wiring import reports

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
    """Who worked in the range, once per person and project, by name; never any pay."""

    first: date = Field(serialization_alias="from")
    last: date = Field(serialization_alias="to")
    rows: list[WorkLogRowRead]


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


class HoursGroupRead(HoursRowRead):
    """One employee (rows per project) or one project (rows per employee)."""

    rows: list[HoursRowRead]


class HoursReportRead(BaseModel):
    """Hours and shifts by employee or project; pay totals cover only the lines shown with pay."""

    first: date = Field(serialization_alias="from")
    last: date = Field(serialization_alias="to")
    group_by: Literal["employee", "project"]
    groups: list[HoursGroupRead]
    hours: HoursOut
    shifts: int
    shifts_missing_rate: int
    pay: Amount | None = Field(
        default=None, description="Left out when no pay is shown or no line shown has a rate."
    )


def _totals(item: Row | Group | HoursReport) -> dict[str, Any]:
    fields: dict[str, Any] = {
        "hours": item.hours,
        "shifts": item.shifts,
        "shifts_missing_rate": item.shifts_missing_rate,
    }
    if item.pay is not None:
        fields["pay"] = item.pay
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
    )


@router.get(
    "/work-log",
    response_model=WorkLogRead,
    response_model_by_alias=True,
    summary="Who worked at a project, site or group",
    description=(
        "Manager or Admin. Each person once per project with their shift count, hours and first "
        "and last date in the range, for exactly one of a project, a site or a group (422 "
        "`reports.scope_invalid`). For a project the range starts on its start date unless "
        "`from` is given, and then covers the whole project with no length limit; a site or a "
        "group needs `from` (422 `reports.start_required`). `to` is today by default. A range "
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
    summary="Hours and pay per employee or project",
    description=(
        "Manager or Admin. Hours and shift counts from `from` to `to` (at most 366 days, 422 "
        "`reports.range_invalid`), grouped by employee or by project, with the grand totals. An "
        "Admin sees pay for every project with pay on; a Manager only for projects they are "
        "assigned to whose pay is open to Managers. Elsewhere there is no pay figure at all, "
        "and pay totals cover only the lines shown with pay."
    ),
)
async def hours(
    first: IsoDate = Query(alias="from"),
    last: IsoDate = Query(alias="to"),
    group_by: Literal["employee", "project"] = Query(),
    current_user: User = Depends(require_manager),
    svc: ReportsService = Depends(reports),
) -> HoursReportRead:
    """Hours and pay per employee or project.

    Manager or Admin. Pay only where the reader may see it.
    """
    return read_hours(await svc.hours(current_user, first, last, group_by))
