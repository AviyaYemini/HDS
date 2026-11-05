"""Qualifications router: CRUD for org qualifications.

Layer: routers. Delegates to ``QualificationService``; all endpoints require Manager or Admin.
"""

import uuid
from datetime import datetime

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.database import get_unit_of_work
from hds.core.deps import require_manager
from hds.core.types import ChangeRequest, Name
from hds.models.user import User
from hds.repositories.qualification import QualificationRepository
from hds.services.qualification import QualificationService

router = APIRouter(prefix="/api/v1/qualifications", tags=["qualifications"])


class QualificationRead(BaseModel):
    """A qualification with its audit timestamps."""

    id: uuid.UUID
    organization_id: uuid.UUID
    name: str
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class QualificationWriteRequest(BaseModel):
    """Qualification name for create; unique within the organisation."""

    name: Name


class QualificationRenameRequest(ChangeRequest):
    """The new name; unique within the organisation. Any other field is refused with 422."""

    name: Name


class QualificationListResponse(BaseModel):
    """The organisation's qualifications."""

    items: list[QualificationRead]


@router.get(
    "",
    response_model=QualificationListResponse,
    summary="List qualifications",
    description=("Manager or Admin. Return the organisation's qualifications ordered by name."),
)
async def list_qualifications(
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> QualificationListResponse:
    """List qualifications.

    Manager or Admin. Return the organisation's qualifications ordered by name.
    """
    svc = QualificationService(qual_repo=QualificationRepository(db))
    quals = await svc.list_qualifications(current_user)
    return QualificationListResponse(items=quals)  # type: ignore[arg-type]


@router.post(
    "",
    response_model=QualificationRead,
    status_code=201,
    summary="Create a qualification",
    description=(
        "Manager or Admin. Create a qualification. Names are unique within the "
        "organisation (409 `qualifications.duplicate_name`)."
    ),
)
async def create_qualification(
    body: QualificationWriteRequest,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> object:
    """Create a qualification.

    Manager or Admin. Create a qualification. Names are unique within the organisation (409
    `qualifications.duplicate_name`).
    """
    svc = QualificationService(qual_repo=QualificationRepository(db))
    return await svc.create_qualification(actor=current_user, name=body.name)


@router.patch(
    "/{id}",
    response_model=QualificationRead,
    summary="Rename a qualification",
    description=(
        "Manager or Admin. Change a qualification's name. A name already used by "
        "another qualification returns 409 (`qualifications.duplicate_name`)."
    ),
)
async def rename_qualification(
    id: uuid.UUID,
    body: QualificationRenameRequest,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> object:
    """Rename a qualification.

    Manager or Admin. Change a qualification's name. A name already used by another
    qualification returns 409 (`qualifications.duplicate_name`).
    """
    svc = QualificationService(qual_repo=QualificationRepository(db))
    return await svc.rename_qualification(actor=current_user, qual_id=id, new_name=body.name)


@router.delete(
    "/{id}",
    status_code=204,
    summary="Delete a qualification",
    description=(
        "Manager or Admin. Delete the qualification and its assignments. The employees "
        "who held it are kept."
    ),
)
async def delete_qualification(
    id: uuid.UUID,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> None:
    """Delete a qualification.

    Manager or Admin. Delete the qualification and its assignments. The employees who held it
    are kept.
    """
    svc = QualificationService(qual_repo=QualificationRepository(db))
    await svc.delete_qualification(actor=current_user, qual_id=id)
