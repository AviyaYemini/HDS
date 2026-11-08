"""Availability router: submission windows and availability entries.

Layer: routers. Delegates to ``AvailabilityService``; HTTP only. The role rules live in the
service: Managers and Admins open windows; employees change their own entries inside an open
window and their absences any time; an Admin changes anyone's; Managers read everyone's.
"""

import uuid
from datetime import date, datetime
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, Query, Response
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.database import get_unit_of_work
from hds.core.deps import require_manager, require_staff
from hds.core.types import WHOLE_NUMBER, ChangeRequest, IsoDate, IsoDateTime, StrictDate, omittable
from hds.models.submission_window import SubmissionWindow
from hds.models.user import User
from hds.repositories.availability import AvailabilityRepository
from hds.repositories.shift_type import ShiftTypeRepository
from hds.repositories.submission_window import SubmissionWindowRepository
from hds.repositories.user import UserRepository
from hds.services.availability import AvailabilityService
from hds.wiring import submission_service

router = APIRouter(prefix="/api/v1", tags=["availability"])

Kind = Literal["blocked", "open", "close", "absence"]
AbsenceKind = Literal["leave", "reserve_duty", "sick"]
# A JSON whole number only: never true or "3" (R165).
WeekdayNumber = Annotated[int, Field(ge=0, le=6), WHOLE_NUMBER]


def _svc(db: AsyncSession) -> AvailabilityService:
    return AvailabilityService(
        window_repo=SubmissionWindowRepository(db),
        entry_repo=AvailabilityRepository(db),
        user_repo=UserRepository(db),
        shift_type_repo=ShiftTypeRepository(db),
        submissions=submission_service(db),
    )


class WindowRead(BaseModel):
    """A submission window; ``is_open`` is true while now is before ``deadline``."""

    id: uuid.UUID
    starts_on: date
    ends_on: date
    deadline: datetime
    is_open: bool


class WindowListResponse(BaseModel):
    """Windows in date order."""

    items: list[WindowRead]


class WindowCreateRequest(BaseModel):
    """A period of dates (inclusive) and the deadline for submitting availability for it."""

    starts_on: StrictDate
    ends_on: StrictDate
    deadline: IsoDateTime


class WindowUpdateRequest(ChangeRequest):
    """A new deadline."""

    deadline: IsoDateTime = omittable()


class EntryRead(BaseModel):
    """An availability entry."""

    id: uuid.UUID
    user_id: uuid.UUID
    kind: Kind
    absence_kind: AbsenceKind | None
    starts_on: date
    ends_on: date
    shift_type_id: uuid.UUID | None
    weekday: int | None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class EntryListResponse(BaseModel):
    """Entries in date order."""

    items: list[EntryRead]


class EntryCreateRequest(BaseModel):
    """A new entry over ``starts_on`` to ``ends_on`` (inclusive).

    ``absence_kind`` is set for an absence and only then. Blocked, open and close entries may be
    limited to one ``shift_type_id`` and one ``weekday``; an absence takes neither.
    """

    kind: Kind
    absence_kind: AbsenceKind | None = None
    starts_on: StrictDate
    ends_on: StrictDate
    shift_type_id: uuid.UUID | None = None
    weekday: WeekdayNumber | None = Field(default=None, description="0 Sunday to 6 Saturday.")


class EntryUpdateRequest(ChangeRequest):
    """Entry fields that may change; null clears an optional one, an omitted field stays."""

    kind: Kind = omittable()
    absence_kind: AbsenceKind | None = None
    starts_on: StrictDate = omittable()
    ends_on: StrictDate = omittable()
    shift_type_id: uuid.UUID | None = None
    weekday: WeekdayNumber | None = Field(default=None, description="0 Sunday to 6 Saturday.")


def _window(svc: AvailabilityService, window: SubmissionWindow) -> WindowRead:
    return WindowRead(
        id=window.id,
        starts_on=window.starts_on,
        ends_on=window.ends_on,
        deadline=window.deadline,
        is_open=svc.is_open(window),
    )


@router.get(
    "/availability-windows",
    response_model=WindowListResponse,
    summary="List submission windows",
    description=(
        "Employee, Manager or Admin. Return the windows that share a date with `from` to `to` "
        "(both optional, inclusive), in date order, each with whether it is still open."
    ),
)
async def list_windows(
    first: IsoDate | None = Query(default=None, alias="from"),
    last: IsoDate | None = Query(default=None, alias="to"),
    current_user: User = Depends(require_staff),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> WindowListResponse:
    """List submission windows.

    Employee, Manager or Admin. Return the windows that share a date with `from` to `to`, in date
    order, each with whether it is still open.
    """
    svc = _svc(db)
    windows = await svc.list_windows(current_user, first, last)
    return WindowListResponse(items=[_window(svc, w) for w in windows])


@router.post(
    "/availability-windows",
    response_model=WindowRead,
    status_code=201,
    summary="Open a submission window",
    description=(
        "Manager or Admin. Open a window for a period of dates with a deadline. A period that "
        "ends before it starts is refused with 422 (`windows.date_range_invalid`); one that "
        "shares a date with another window with 409 (`windows.overlap`)."
    ),
)
async def open_window(
    body: WindowCreateRequest,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> WindowRead:
    """Open a submission window.

    Manager or Admin. A period that ends before it starts is refused with 422
    (`windows.date_range_invalid`); one that shares a date with another window with 409
    (`windows.overlap`).
    """
    svc = _svc(db)
    return _window(svc, await svc.open_window(current_user, body.model_dump()))


@router.patch(
    "/availability-windows/{id}",
    response_model=WindowRead,
    summary="Change a window's deadline",
    description="Manager or Admin. Move the deadline of a window.",
)
async def change_window(
    id: uuid.UUID,
    body: WindowUpdateRequest,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> WindowRead:
    """Change a window's deadline.

    Manager or Admin. Move the deadline of a window.
    """
    svc = _svc(db)
    changed = await svc.change_window(current_user, id, body.model_dump(exclude_unset=True))
    return _window(svc, changed)


@router.delete(
    "/availability-windows/{id}",
    status_code=204,
    summary="Remove a window",
    description="Manager or Admin. Remove a window; the entries submitted in it stay.",
)
async def remove_window(
    id: uuid.UUID,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> Response:
    """Remove a window.

    Manager or Admin. The entries submitted in it stay.
    """
    await _svc(db).remove_window(current_user, id)
    return Response(status_code=204)


@router.get(
    "/employees/{id}/constraints",
    response_model=EntryListResponse,
    summary="List an employee's availability",
    description=(
        "The employee themself, a Manager or an Admin. Return the entries that share a date "
        "with `from` to `to` (both optional). Another employee's, asked by an Employee, is 404."
    ),
)
async def list_entries(
    id: uuid.UUID,
    first: IsoDate | None = Query(default=None, alias="from"),
    last: IsoDate | None = Query(default=None, alias="to"),
    current_user: User = Depends(require_staff),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> EntryListResponse:
    """List an employee's availability.

    The employee themself, a Manager or an Admin. Another employee's, asked by an Employee, is
    404.
    """
    entries = await _svc(db).list_entries(current_user, id, first, last)
    return EntryListResponse(items=entries)  # type: ignore[arg-type]


@router.post(
    "/employees/{id}/constraints",
    response_model=EntryRead,
    status_code=201,
    summary="Add an availability entry",
    description=(
        "The employee themself inside an open window (absences any time), or an Admin at any "
        "time. A Manager adding to another employee's is refused with 403 "
        "(`availability.admin_required`). Dates outside a window answer 409 "
        "(`availability.no_window`), a passed deadline 409 (`availability.window_closed`), and "
        "an entry for a shift and date another entry already covers 409 "
        "(`availability.conflict`)."
    ),
)
async def add_entry(
    id: uuid.UUID,
    body: EntryCreateRequest,
    current_user: User = Depends(require_staff),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> object:
    """Add an availability entry.

    The employee themself inside an open window (absences any time), or an Admin at any time.
    The refusals are described in the operation's `description`.
    """
    return await _svc(db).add_entry(current_user, id, body.model_dump())


@router.patch(
    "/constraints/{id}",
    response_model=EntryRead,
    summary="Change an availability entry",
    description=(
        "As adding one: the window rule applies to the dates the entry has and the dates it "
        "gets. An explicit null clears `absence_kind`, `shift_type_id` or `weekday`."
    ),
)
async def change_entry(
    id: uuid.UUID,
    body: EntryUpdateRequest,
    current_user: User = Depends(require_staff),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> object:
    """Change an availability entry.

    As adding one: the window rule applies to the dates the entry has and the dates it gets.
    """
    return await _svc(db).change_entry(current_user, id, body.model_dump(exclude_unset=True))


@router.delete(
    "/constraints/{id}",
    status_code=204,
    summary="Remove an availability entry",
    description="As adding one: the window rule applies to the entry's dates.",
)
async def remove_entry(
    id: uuid.UUID,
    current_user: User = Depends(require_staff),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> Response:
    """Remove an availability entry.

    As adding one: the window rule applies to the entry's dates.
    """
    await _svc(db).remove_entry(current_user, id)
    return Response(status_code=204)
