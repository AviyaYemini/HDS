"""Qualifications router: CRUD for org qualifications."""

import uuid
from datetime import datetime

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.database import get_db
from hds.core.deps import require_manager
from hds.core.types import Name
from hds.models.user import User
from hds.repositories.qualification import QualificationRepository
from hds.services.qualification import QualificationService

router = APIRouter(prefix="/api/v1/qualifications", tags=["qualifications"])


class QualificationRead(BaseModel):
    id: uuid.UUID
    organization_id: uuid.UUID
    name: str
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class QualificationWriteRequest(BaseModel):
    name: Name


class QualificationListResponse(BaseModel):
    items: list[QualificationRead]


@router.get("", response_model=QualificationListResponse)
async def list_qualifications(
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_db),
) -> QualificationListResponse:
    repo = QualificationRepository(db)
    quals = await repo.list_by_org(current_user.organization_id)
    return QualificationListResponse(items=quals)  # type: ignore[arg-type]


@router.post("", response_model=QualificationRead, status_code=201)
async def create_qualification(
    body: QualificationWriteRequest,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_db),
) -> object:
    svc = QualificationService(qual_repo=QualificationRepository(db))
    return await svc.create_qualification(actor=current_user, name=body.name)


@router.patch("/{id}", response_model=QualificationRead)
async def rename_qualification(
    id: uuid.UUID,
    body: QualificationWriteRequest,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_db),
) -> object:
    svc = QualificationService(qual_repo=QualificationRepository(db))
    return await svc.rename_qualification(actor=current_user, qual_id=id, new_name=body.name)


@router.delete("/{id}", status_code=204)
async def delete_qualification(
    id: uuid.UUID,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_db),
) -> None:
    svc = QualificationService(qual_repo=QualificationRepository(db))
    await svc.delete_qualification(actor=current_user, qual_id=id)
