"""Groups router: CRUD for org groups.

Layer: routers. Delegates to ``GroupService``; all endpoints require Manager or Admin.
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
from hds.repositories.group import GroupRepository
from hds.services.group import GroupService

router = APIRouter(prefix="/api/v1/groups", tags=["groups"])


class GroupRead(BaseModel):
    """A group with its audit timestamps."""

    id: uuid.UUID
    organization_id: uuid.UUID
    name: str
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class GroupWriteRequest(BaseModel):
    """Group name for create; unique within the organisation."""

    name: Name


class GroupRenameRequest(ChangeRequest):
    """The new name; unique within the organisation. Any other field is refused with 422."""

    name: Name


class GroupListResponse(BaseModel):
    """The organisation's groups."""

    items: list[GroupRead]


@router.get(
    "",
    response_model=GroupListResponse,
    summary="List groups",
    description=("Manager or Admin. Return the organisation's groups ordered by name."),
)
async def list_groups(
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> GroupListResponse:
    """List groups.

    Manager or Admin. Return the organisation's groups ordered by name.
    """
    groups = await GroupService(group_repo=GroupRepository(db)).list_groups(current_user)
    return GroupListResponse(items=groups)  # type: ignore[arg-type]


@router.post(
    "",
    response_model=GroupRead,
    status_code=201,
    summary="Create a group",
    description=(
        "Manager or Admin. Create a group. Names are unique within the organisation "
        "(409 `groups.duplicate_name`)."
    ),
)
async def create_group(
    body: GroupWriteRequest,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> object:
    """Create a group.

    Manager or Admin. Create a group. Names are unique within the organisation (409
    `groups.duplicate_name`).
    """
    svc = GroupService(group_repo=GroupRepository(db))
    return await svc.create_group(actor=current_user, name=body.name)


@router.patch(
    "/{id}",
    response_model=GroupRead,
    summary="Rename a group",
    description=(
        "Manager or Admin. Change a group's name. A name already used by another group "
        "returns 409 (`groups.duplicate_name`)."
    ),
)
async def rename_group(
    id: uuid.UUID,
    body: GroupRenameRequest,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> object:
    """Rename a group.

    Manager or Admin. Change a group's name. A name already used by another group returns 409
    (`groups.duplicate_name`).
    """
    svc = GroupService(group_repo=GroupRepository(db))
    return await svc.rename_group(actor=current_user, group_id=id, new_name=body.name)


@router.delete(
    "/{id}",
    status_code=204,
    summary="Delete a group",
    description=(
        "Manager or Admin. Delete the group and its memberships. The employees in it are kept."
    ),
)
async def delete_group(
    id: uuid.UUID,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> None:
    """Delete a group.

    Manager or Admin. Delete the group and its memberships. The employees in it are kept.
    """
    svc = GroupService(group_repo=GroupRepository(db))
    await svc.delete_group(actor=current_user, group_id=id)
