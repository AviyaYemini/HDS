"""The caller's own roster: their published shifts, with nothing about anyone else.

Layer: routers. Delegates to ``RosterService``; HTTP only. Its own response schema has no field for
other people, so it cannot leak them (research R7).
"""

import uuid
from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.database import get_unit_of_work
from hds.core.deps import require_staff
from hds.core.types import IsoDate
from hds.models.user import User
from hds.routers.roster_wiring import roster_service

router = APIRouter(prefix="/me", tags=["my roster"])


class NamedRef(BaseModel):
    """A record's id and name."""

    id: uuid.UUID
    name: str


class MyShift(BaseModel):
    """One of the caller's published shifts: UTC instants and the site's IANA time zone."""

    id: uuid.UUID
    starts_at: datetime
    ends_at: datetime
    timezone: str
    shift_type: str
    project: NamedRef
    site: NamedRef


class MyShiftList(BaseModel):
    """The caller's published shifts, by start time."""

    items: list[MyShift]


@router.get(
    "/shifts",
    response_model=MyShiftList,
    summary="List my shifts",
    description=(
        "Employee, Manager or Admin. The caller's own published shifts that start on `from` to "
        "`to` (optional dates in the organisation's time zone), with the project, the site and "
        "its time zone. Never a draft, never anyone else's, and nothing about who else is on "
        "them."
    ),
)
async def my_shifts(
    first: Annotated[IsoDate | None, Query(alias="from")] = None,
    last: Annotated[IsoDate | None, Query(alias="to")] = None,
    current_user: User = Depends(require_staff),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> MyShiftList:
    """List my shifts.

    Employee, Manager or Admin. The caller's own published shifts, never a draft and never
    anyone else's.
    """
    rows = await roster_service(db).my_shifts(current_user, first, last)
    return MyShiftList(
        items=[
            MyShift(
                id=r.id,
                starts_at=r.starts_at,
                ends_at=r.ends_at,
                timezone=r.timezone,
                shift_type=r.shift_type,
                project=NamedRef(id=r.project_id, name=r.project_name),
                site=NamedRef(id=r.site_id, name=r.site_name),
            )
            for r in rows
        ]
    )


class ShiftContact(BaseModel):
    """Who to contact about a shift: one of its project's managers."""

    full_name: str
    phone: str | None
    email: str


class MyShiftDetail(MyShift):
    """One of the caller's published shifts, with the site's address and who to contact.

    ``note`` is a word from whoever placed or moved them there, if any (008 T042g).
    """

    site_address: str | None
    contacts: list[ShiftContact]
    note: str | None = None


@router.get(
    "/shifts/{id}",
    response_model=MyShiftDetail,
    summary="Read one of my shifts",
    description=(
        "Employee, Manager or Admin. One of the caller's own published shifts with the site's "
        "address and the project's managers to contact, who stand in for a shift leader. "
        "Answers 404 for any shift the caller is not on, a draft included, and says nothing "
        "about who else is on it."
    ),
)
async def my_shift(
    id: uuid.UUID,
    current_user: User = Depends(require_staff),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> MyShiftDetail:
    """Read one of my shifts.

    Employee, Manager or Admin, for a published shift they are on.
    """
    found, managers = await roster_service(db).my_shift(current_user, id)
    return MyShiftDetail(
        id=found.id,
        starts_at=found.starts_at,
        ends_at=found.ends_at,
        timezone=found.timezone,
        shift_type=found.shift_type,
        project=NamedRef(id=found.project_id, name=found.project_name),
        site=NamedRef(id=found.site_id, name=found.site_name),
        site_address=found.site_address,
        note=found.note,
        contacts=[
            ShiftContact(full_name=m.full_name, phone=m.phone, email=m.email) for m in managers
        ],
    )
