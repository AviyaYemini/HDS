"""Groups router: CRUD for org groups."""

import uuid
from datetime import datetime

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.database import get_db
from hds.core.deps import require_manager
from hds.core.types import Name
from hds.models.user import User
from hds.repositories.group import GroupRepository
from hds.services.group import GroupService

router = APIRouter(prefix="/api/v1/groups", tags=["groups"])


class GroupRead(BaseModel):
    id: uuid.UUID
    organization_id: uuid.UUID
    name: str
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class GroupWriteRequest(BaseModel):
    name: Name


class GroupListResponse(BaseModel):
    items: list[GroupRead]


@router.get("", response_model=GroupListResponse)
async def list_groups(
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_db),
) -> GroupListResponse:
    repo = GroupRepository(db)
    groups = await repo.list_by_org(current_user.organization_id)
    return GroupListResponse(items=groups)  # type: ignore[arg-type]


@router.post("", response_model=GroupRead, status_code=201)
async def create_group(
    body: GroupWriteRequest,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_db),
) -> object:
    svc = GroupService(group_repo=GroupRepository(db))
    return await svc.create_group(actor=current_user, name=body.name)


@router.patch("/{id}", response_model=GroupRead)
async def rename_group(
    id: uuid.UUID,
    body: GroupWriteRequest,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_db),
) -> object:
    svc = GroupService(group_repo=GroupRepository(db))
    return await svc.rename_group(actor=current_user, group_id=id, new_name=body.name)


@router.delete("/{id}", status_code=204)
async def delete_group(
    id: uuid.UUID,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_db),
) -> None:
    svc = GroupService(group_repo=GroupRepository(db))
    await svc.delete_group(actor=current_user, group_id=id)
