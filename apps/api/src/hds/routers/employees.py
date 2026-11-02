"""Employees router: CRUD, deactivation, groups, qualifications."""

import uuid
from datetime import datetime
from decimal import Decimal

from fastapi import APIRouter, Depends
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.database import get_db
from hds.core.deps import get_current_user_or_self, require_admin, require_manager
from hds.core.types import AssignableRole, Money, Name, Password, Phone
from hds.models.group import Group
from hds.models.qualification import Qualification
from hds.models.user import User, UserRole
from hds.repositories.group import GroupRepository
from hds.repositories.qualification import QualificationRepository
from hds.repositories.refresh_token import RefreshTokenRepository
from hds.repositories.user import UserRepository
from hds.services.employee import EmployeeService
from hds.services.group import GroupService
from hds.services.qualification import QualificationService

router = APIRouter(prefix="/api/v1/employees", tags=["employees"])


class UserRead(BaseModel):
    id: uuid.UUID
    organization_id: uuid.UUID
    email: EmailStr
    full_name: str
    phone: str | None = None
    role: UserRole
    is_active: bool
    default_hourly_rate: Decimal | None = None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class UserCreateRequest(BaseModel):
    email: EmailStr
    full_name: Name
    phone: Phone | None = None
    role: AssignableRole = "employee"
    initial_password: Password = Field(min_length=12)
    default_hourly_rate: Money | None = None


class UserUpdateRequest(BaseModel):
    full_name: Name | None = None
    phone: Phone | None = None
    role: AssignableRole | None = None
    default_hourly_rate: Money | None = None


class IDListRequest(BaseModel):
    ids: list[uuid.UUID]


class UserListResponse(BaseModel):
    items: list[UserRead]
    total: int


class GroupRead(BaseModel):
    id: uuid.UUID
    name: str
    model_config = {"from_attributes": True}


class QualificationRead(BaseModel):
    id: uuid.UUID
    name: str
    model_config = {"from_attributes": True}


def _employee_svc(db: AsyncSession) -> EmployeeService:
    return EmployeeService(user_repo=UserRepository(db), token_repo=RefreshTokenRepository(db))


async def _require_user_in_org(db: AsyncSession, user_id: uuid.UUID, caller: User) -> None:
    from hds.core.exceptions import NotFoundError

    if await UserRepository(db).get_by_id(user_id, caller.organization_id) is None:
        raise NotFoundError(resource="User", id=user_id)


@router.get("", response_model=UserListResponse)
async def list_employees(
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_db),
) -> UserListResponse:
    users = await UserRepository(db).list(org_id=current_user.organization_id)
    return UserListResponse(items=users, total=len(users))  # type: ignore[arg-type]


@router.post("", response_model=UserRead, status_code=201)
async def create_employee(
    body: UserCreateRequest,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_db),
) -> User:
    svc = _employee_svc(db)
    return await svc.create_employee(actor=current_user, data=body.model_dump())


@router.get("/{id}", response_model=UserRead)
async def get_employee(
    id: uuid.UUID,
    current_user: User = Depends(get_current_user_or_self),
    db: AsyncSession = Depends(get_db),
) -> User:
    from hds.core.exceptions import NotFoundError

    user = await UserRepository(db).get_by_id(id, current_user.organization_id)
    if user is None:
        raise NotFoundError(resource="User", id=id)
    return user


@router.patch("/{id}", response_model=UserRead)
async def edit_employee(
    id: uuid.UUID,
    body: UserUpdateRequest,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_db),
) -> User:
    svc = _employee_svc(db)
    data = body.model_dump(exclude_none=True)
    return await svc.edit_employee(actor=current_user, target_id=id, data=data)


@router.delete("/{id}", status_code=204)
async def delete_employee(
    id: uuid.UUID,
    current_user: User = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
) -> None:
    svc = _employee_svc(db)
    await svc.soft_delete(actor=current_user, target_id=id)


@router.post("/{id}/deactivate", response_model=UserRead)
async def deactivate_employee(
    id: uuid.UUID,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_db),
) -> User:
    svc = _employee_svc(db)
    return await svc.deactivate(actor=current_user, target_id=id)


@router.post("/{id}/reactivate", response_model=UserRead)
async def reactivate_employee(
    id: uuid.UUID,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_db),
) -> User:
    svc = _employee_svc(db)
    return await svc.reactivate(actor=current_user, target_id=id)


@router.get("/{id}/groups", response_model=list[GroupRead])
async def get_employee_groups(
    id: uuid.UUID,
    current_user: User = Depends(get_current_user_or_self),
    db: AsyncSession = Depends(get_db),
) -> list[Group]:
    await _require_user_in_org(db, id, current_user)
    return await GroupRepository(db).get_groups_for_user(id, current_user.organization_id)


@router.put("/{id}/groups", status_code=204)
async def replace_employee_groups(
    id: uuid.UUID,
    body: IDListRequest,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_db),
) -> None:
    await _require_user_in_org(db, id, current_user)
    svc = GroupService(group_repo=GroupRepository(db))
    await svc.replace_employee_groups(actor=current_user, user_id=id, group_ids=body.ids)


@router.get("/{id}/qualifications", response_model=list[QualificationRead])
async def get_employee_qualifications(
    id: uuid.UUID,
    current_user: User = Depends(get_current_user_or_self),
    db: AsyncSession = Depends(get_db),
) -> list[Qualification]:
    await _require_user_in_org(db, id, current_user)
    return await QualificationRepository(db).get_qualifications_for_user(
        id, current_user.organization_id
    )


@router.put("/{id}/qualifications", status_code=204)
async def replace_employee_qualifications(
    id: uuid.UUID,
    body: IDListRequest,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_db),
) -> None:
    await _require_user_in_org(db, id, current_user)
    svc = QualificationService(qual_repo=QualificationRepository(db))
    await svc.replace_employee_qualifications(actor=current_user, user_id=id, qual_ids=body.ids)
