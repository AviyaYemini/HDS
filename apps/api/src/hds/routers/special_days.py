"""Special days router: dates paid at their own rate, for the organisation or one project.

Layer: routers. Delegates to ``SpecialDayService``, which ``hds.wiring`` builds; HTTP only.
Managers and Admins read; Admins add, change and delete (005 FR-008, FR-008a).
"""

import uuid
from datetime import date

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field

from hds.core.deps import require_admin, require_manager
from hds.core.types import ChangeRequest, Name, Percent, PercentOut, StrictDate, omittable
from hds.models.special_day import SpecialDay
from hds.models.user import User
from hds.services.special_day import DayInEffect, SpecialDayService
from hds.wiring import special_days

router = APIRouter(prefix="/api/v1", tags=["special days"])


class SpecialDayCreate(BaseModel):
    """A date paid at its own rate (150.00 percent unless given), with a name."""

    day: StrictDate
    name: Name
    rate_pct: Percent | None = None


class SpecialDayChange(ChangeRequest):
    """A day's date, name or rate; omitted fields stay, ``null`` and unknown fields are refused."""

    day: StrictDate = omittable()
    name: Name = omittable()
    rate_pct: Percent = omittable()


class SpecialDayRead(BaseModel):
    """A special day; ``project_id`` is set for a project's own day."""

    id: uuid.UUID
    project_id: uuid.UUID | None
    day: date
    name: str
    rate_pct: PercentOut


class SpecialDayList(BaseModel):
    """The organisation's days, by date."""

    items: list[SpecialDayRead]


class DayInEffectRead(SpecialDayRead):
    """A day as a project's shifts are paid by it: the project's own, or the organisation's."""

    source: str = Field(description="`project` or `organisation`.")


class DaysInEffect(BaseModel):
    """The days a project's shifts are paid by, by date."""

    items: list[DayInEffectRead]


Year = Query(default=None, ge=1, le=9999, description="Only the days of this year.")


def read(row: SpecialDay) -> SpecialDayRead:
    """The response form of a day."""
    return SpecialDayRead(
        id=row.id, project_id=row.project_id, day=row.day, name=row.name, rate_pct=row.rate_pct
    )


def read_in_effect(row: DayInEffect) -> DayInEffectRead:
    """The response form of a day in effect for a project."""
    return DayInEffectRead(
        id=row.id,
        project_id=row.project_id,
        day=row.day,
        name=row.name,
        rate_pct=row.rate_pct,
        source=row.source,
    )


def values_of(body: BaseModel) -> dict[str, object]:
    """The fields a body sent, without an unset rate (the database default applies)."""
    return {k: v for k, v in body.model_dump(exclude_unset=True).items() if v is not None}


@router.get(
    "/special-days",
    response_model=SpecialDayList,
    summary="List the organisation's special days",
    description="Manager or Admin. The organisation's special days by date, of one year if given.",
)
async def list_days(
    year: int | None = Year,
    current_user: User = Depends(require_manager),
    svc: SpecialDayService = Depends(special_days),
) -> SpecialDayList:
    """List the organisation's special days.

    Manager or Admin. By date, of one year if given.
    """
    return SpecialDayList(items=[read(d) for d in await svc.list_organisation(current_user, year)])


@router.post(
    "/special-days",
    response_model=SpecialDayRead,
    status_code=201,
    summary="Add a special day",
    description=(
        "Admin. A date paid at its own rate (100 to 300 percent, 150.00 by default) for every "
        "project that has not ended. One day per date (409 `special_days.duplicate`)."
    ),
)
async def add_day(
    body: SpecialDayCreate,
    current_user: User = Depends(require_admin),
    svc: SpecialDayService = Depends(special_days),
) -> SpecialDayRead:
    """Add a special day.

    Admin. For every project that has not ended; one day per date.
    """
    return read(await svc.create_organisation_day(current_user, values_of(body)))


@router.patch(
    "/special-days/{id}",
    response_model=SpecialDayRead,
    summary="Change a special day",
    description=(
        "Admin. Change an organisation or project day's date, name or rate. A day of an ended "
        "project is fixed (409 `pay.project_ended`); a date already taken is refused (409 "
        "`special_days.duplicate`)."
    ),
)
async def change_day(
    id: uuid.UUID,
    body: SpecialDayChange,
    current_user: User = Depends(require_admin),
    svc: SpecialDayService = Depends(special_days),
) -> SpecialDayRead:
    """Change a special day.

    Admin. Date, name or rate; an ended project's day is fixed.
    """
    return read(await svc.change_day(current_user, id, values_of(body)))


@router.delete(
    "/special-days/{id}",
    status_code=204,
    summary="Delete a special day",
    description=(
        "Admin. Delete an organisation or project day; a project's own day on the same date stays. "
        "A day of an ended project is fixed (409 `pay.project_ended`)."
    ),
)
async def delete_day(
    id: uuid.UUID,
    current_user: User = Depends(require_admin),
    svc: SpecialDayService = Depends(special_days),
) -> None:
    """Delete a special day.

    Admin. A project's own day on the same date stays; an ended project's day is fixed.
    """
    await svc.delete_day(current_user, id)


@router.get(
    "/projects/{id}/special-days",
    response_model=DaysInEffect,
    summary="List the special days a project is paid by",
    description=(
        "Manager or Admin. The organisation's days and the project's own, by date; on a date both "
        "list, the project's wins. An ended project has only its own, copied when it ended."
    ),
)
async def list_project_days(
    id: uuid.UUID,
    year: int | None = Year,
    current_user: User = Depends(require_manager),
    svc: SpecialDayService = Depends(special_days),
) -> DaysInEffect:
    """List the special days a project is paid by.

    Manager or Admin. The project's own days win over the organisation's on the same date.
    """
    days = await svc.in_effect(current_user, id, year)
    return DaysInEffect(items=[read_in_effect(d) for d in days])


@router.post(
    "/projects/{id}/special-days",
    response_model=SpecialDayRead,
    status_code=201,
    summary="Add a project's special day",
    description=(
        "Admin. A day for this project only, or this project's rate for an organisation day. One "
        "per project and date (409 `special_days.duplicate`); an ended project is fixed (409 "
        "`pay.project_ended`)."
    ),
)
async def add_project_day(
    id: uuid.UUID,
    body: SpecialDayCreate,
    current_user: User = Depends(require_admin),
    svc: SpecialDayService = Depends(special_days),
) -> SpecialDayRead:
    """Add a project's special day.

    Admin. For this project only; it overrides an organisation day on the same date.
    """
    return read(await svc.create_project_day(current_user, id, values_of(body)))
