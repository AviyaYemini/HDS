"""Shift types router: an organisation's kinds of shift and each project's set.

Layer: routers. Delegates to ``ShiftTypeService``; HTTP only. Everyone who works shifts reads the
types; Managers and Admins change them.
"""

import uuid
from datetime import datetime

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.database import get_unit_of_work
from hds.core.deps import require_manager, require_staff
from hds.core.types import ChangeRequest, IsoTime, LocalTimeOut, Name, omittable
from hds.models.user import User
from hds.repositories.project import ProjectRepository
from hds.repositories.shift_type import ShiftTypeRepository
from hds.services.shift_type import ShiftTypeService

router = APIRouter(prefix="/api/v1", tags=["shift types"])


def _svc(db: AsyncSession) -> ShiftTypeService:
    return ShiftTypeService(
        shift_type_repo=ShiftTypeRepository(db), project_repo=ProjectRepository(db)
    )


class ShiftTypeRead(BaseModel):
    """A shift type; its times are local to the site a shift is at."""

    id: uuid.UUID
    name: str
    starts_at_local: LocalTimeOut
    ends_at_local: LocalTimeOut
    crosses_midnight: bool
    is_active: bool
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class ShiftTypeListResponse(BaseModel):
    """Shift types."""

    items: list[ShiftTypeRead]


class ShiftTypeCreateRequest(BaseModel):
    """A new shift type; it crosses midnight when the end is not after the start."""

    name: Name
    starts_at_local: IsoTime
    ends_at_local: IsoTime


class ShiftTypeUpdateRequest(ChangeRequest):
    """Shift type fields that may change; ``is_active`` false retires it."""

    name: Name = omittable()
    starts_at_local: IsoTime = omittable()
    ends_at_local: IsoTime = omittable()
    is_active: bool = omittable()


class ShiftTypeIdsRequest(BaseModel):
    """The complete set of shift type ids a project uses; an empty list clears it."""

    ids: list[uuid.UUID]


@router.get(
    "/shift-types",
    response_model=ShiftTypeListResponse,
    summary="List shift types",
    description=(
        "Employee, Manager or Admin. Every shift type of the organisation, active first; a "
        "retired one has `is_active` false."
    ),
)
async def list_types(
    current_user: User = Depends(require_staff),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> ShiftTypeListResponse:
    """List shift types.

    Employee, Manager or Admin. Every shift type of the organisation, active first.
    """
    return ShiftTypeListResponse(items=await _svc(db).list_types(current_user))  # type: ignore[arg-type]


@router.post(
    "/shift-types",
    response_model=ShiftTypeRead,
    status_code=201,
    summary="Add a shift type",
    description=(
        "Manager or Admin. It crosses midnight when its end is not after its start. An active "
        "name already in use, in any letter case, is refused with 409 "
        "(`shift_types.duplicate_name`)."
    ),
)
async def add_type(
    body: ShiftTypeCreateRequest,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> object:
    """Add a shift type.

    Manager or Admin. An active name already in use is refused with 409
    (`shift_types.duplicate_name`).
    """
    return await _svc(db).add_type(current_user, body.model_dump())


@router.patch(
    "/shift-types/{id}",
    response_model=ShiftTypeRead,
    summary="Change or retire a shift type",
    description=(
        "Manager or Admin. Rename, retime, retire (`is_active` false) or bring back a type. "
        "Shifts already made from it keep their times. A name clash with an active type is "
        "refused with 409 (`shift_types.duplicate_name`)."
    ),
)
async def change_type(
    id: uuid.UUID,
    body: ShiftTypeUpdateRequest,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> object:
    """Change or retire a shift type.

    Manager or Admin. Shifts already made from it keep their times.
    """
    return await _svc(db).change_type(current_user, id, body.model_dump(exclude_unset=True))


@router.get(
    "/projects/{id}/shift-types",
    response_model=ShiftTypeListResponse,
    summary="List a project's shift types",
    description="Manager or Admin. The shift types the project uses.",
)
async def project_types(
    id: uuid.UUID,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> ShiftTypeListResponse:
    """List a project's shift types.

    Manager or Admin. The shift types the project uses.
    """
    items = await _svc(db).project_types(current_user, id)
    return ShiftTypeListResponse(items=items)  # type: ignore[arg-type]


@router.put(
    "/projects/{id}/shift-types",
    response_model=ShiftTypeListResponse,
    summary="Set a project's shift types",
    description=(
        "Manager or Admin. Replace the set of shift types the project uses. Another "
        "organisation's type is 404; a retired one is refused with 422 (`shift_types.retired`). "
        "Removing a type leaves the shifts already made from it."
    ),
)
async def set_project_types(
    id: uuid.UUID,
    body: ShiftTypeIdsRequest,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> ShiftTypeListResponse:
    """Set a project's shift types.

    Manager or Admin. Replace the set; a retired type is refused with 422 (`shift_types.retired`).
    """
    items = await _svc(db).set_project_types(current_user, id, body.ids)
    return ShiftTypeListResponse(items=items)  # type: ignore[arg-type]
