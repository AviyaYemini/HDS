"""Employees router: CRUD, deactivation, groups, qualifications.

Layer: routers. Delegates to ``EmployeeService``, ``GroupService`` and ``QualificationService``.
The group and qualification sub-resources first check that the addressed user belongs to the
caller's organisation, because the services take that user id as already verified.
"""

import uuid
from datetime import datetime
from decimal import Decimal

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, EmailStr, Field
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.database import get_unit_of_work
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
    """A user account as returned by the API.

    ``default_hourly_rate`` is a decimal string and is view-only for the user it belongs to
    (FR-017). Keep this docstring identical in ``me.py`` and ``employees.py``: the two classes
    share one OpenAPI component only while their schemas, descriptions included, are equal.
    """

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
    """New employee.

    The initial password needs at least 12 characters; the role defaults to employee and the
    hourly rate is optional.
    """

    email: EmailStr
    full_name: Name
    phone: Phone | None = None
    role: AssignableRole = "employee"
    initial_password: Password = Field(min_length=12)
    default_hourly_rate: Money | None = None


class UserUpdateRequest(BaseModel):
    """Fields a Manager or Admin may change; omitted fields are left as they are."""

    full_name: Name | None = None
    phone: Phone | None = None
    role: AssignableRole | None = None
    default_hourly_rate: Money | None = None


class IDListRequest(BaseModel):
    """The complete new set of ids; an empty list clears the set."""

    ids: list[uuid.UUID]


class UserListResponse(BaseModel):
    """Employees matching a search; ``total`` is the number of items returned."""

    items: list[UserRead]
    total: int


class GroupRead(BaseModel):
    """A group a user belongs to."""

    id: uuid.UUID
    name: str
    model_config = {"from_attributes": True}


class QualificationRead(BaseModel):
    """A qualification a user holds."""

    id: uuid.UUID
    name: str
    model_config = {"from_attributes": True}


def _employee_svc(db: AsyncSession) -> EmployeeService:
    return EmployeeService(user_repo=UserRepository(db), token_repo=RefreshTokenRepository(db))


async def _require_user_in_org(db: AsyncSession, user_id: uuid.UUID, caller: User) -> None:
    from hds.core.exceptions import NotFoundError

    if await UserRepository(db).get_by_id(user_id, caller.organization_id) is None:
        raise NotFoundError(resource="User", id=user_id)


@router.get(
    "",
    response_model=UserListResponse,
    summary="List employees",
    description=(
        "Manager or Admin. Search the organisation's non-deleted users by name or email "
        "and filter by role, active state, group or qualification. All filters combine "
        "with AND and the result is ordered by name."
    ),
)
async def list_employees(
    search: str | None = Query(default=None, max_length=255),
    role: UserRole | None = None,
    is_active: bool | None = None,
    group_id: uuid.UUID | None = None,
    qualification_id: uuid.UUID | None = None,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> UserListResponse:
    """List employees.

    Manager or Admin. Search the organisation's non-deleted users by name or email and filter by
    role, active state, group or qualification. All filters combine with AND and the result is
    ordered by name.
    """
    users = await _employee_svc(db).list_employees(
        current_user,
        search=search,
        role=role,
        is_active=is_active,
        group_id=group_id,
        qualification_id=qualification_id,
    )
    return UserListResponse(items=users, total=len(users))  # type: ignore[arg-type]


@router.post(
    "",
    response_model=UserRead,
    status_code=201,
    summary="Create an employee",
    description=(
        "Manager or Admin. Create an account with an initial password of at least 12 "
        "characters. The role defaults to employee; only an Admin may create an Admin, "
        "and Client users have their own endpoints (`users.role_invalid`). A duplicate "
        "email returns 409 (`users.duplicate_email`)."
    ),
)
async def create_employee(
    body: UserCreateRequest,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> User:
    """Create an employee.

    Manager or Admin. Create an account with an initial password of at least 12 characters. The
    role defaults to employee; only an Admin may create an Admin, and Client users have their
    own endpoints (`users.role_invalid`). A duplicate email returns 409
    (`users.duplicate_email`).
    """
    svc = _employee_svc(db)
    return await svc.create_employee(actor=current_user, data=body.model_dump())


@router.get(
    "/{id}",
    response_model=UserRead,
    summary="Get an employee",
    description=(
        "Manager or Admin for any user in the organisation; every other role only for "
        "their own id (otherwise 403 `users.forbidden`)."
    ),
)
async def get_employee(
    id: uuid.UUID,
    current_user: User = Depends(get_current_user_or_self),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> User:
    """Get an employee.

    Manager or Admin for any user in the organisation; every other role only for their own id
    (otherwise 403 `users.forbidden`).
    """
    from hds.core.exceptions import NotFoundError

    user = await UserRepository(db).get_by_id(id, current_user.organization_id)
    if user is None:
        raise NotFoundError(resource="User", id=id)
    return user


@router.patch(
    "/{id}",
    response_model=UserRead,
    summary="Edit an employee",
    description=(
        "Manager or Admin. Update name, phone, hourly rate or role. Only an Admin may "
        "change an Admin or assign the Admin role. Changing a role ends the user's "
        "sessions. Demoting the last active Admin returns 409 (`users.last_admin`)."
    ),
)
async def edit_employee(
    id: uuid.UUID,
    body: UserUpdateRequest,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> User:
    """Edit an employee.

    Manager or Admin. Update name, phone, hourly rate or role. Only an Admin may change an Admin
    or assign the Admin role. Changing a role ends the user's sessions. Demoting the last active
    Admin returns 409 (`users.last_admin`).
    """
    svc = _employee_svc(db)
    data = body.model_dump(exclude_none=True)
    return await svc.edit_employee(actor=current_user, target_id=id, data=data)


@router.delete(
    "/{id}",
    status_code=204,
    summary="Delete an employee",
    description=(
        "Admin only. Soft-delete the user: history is kept and the email can be reused. "
        "Ends the user's sessions. The last active Admin cannot be deleted (409 "
        "`users.last_admin`)."
    ),
)
async def delete_employee(
    id: uuid.UUID,
    current_user: User = Depends(require_admin),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> None:
    """Delete an employee.

    Admin only. Soft-delete the user: history is kept and the email can be reused. Ends the
    user's sessions. The last active Admin cannot be deleted (409 `users.last_admin`).
    """
    svc = _employee_svc(db)
    await svc.soft_delete(actor=current_user, target_id=id)


@router.post(
    "/{id}/deactivate",
    response_model=UserRead,
    summary="Deactivate an employee",
    description=(
        "Manager or Admin. Block the user from signing in and end their sessions. Only "
        "an Admin may deactivate an Admin, and never the last active one (409 "
        "`users.last_admin`)."
    ),
)
async def deactivate_employee(
    id: uuid.UUID,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> User:
    """Deactivate an employee.

    Manager or Admin. Block the user from signing in and end their sessions. Only an Admin may
    deactivate an Admin, and never the last active one (409 `users.last_admin`).
    """
    svc = _employee_svc(db)
    return await svc.deactivate(actor=current_user, target_id=id)


@router.post(
    "/{id}/reactivate",
    response_model=UserRead,
    summary="Reactivate an employee",
    description=(
        "Manager or Admin. Allow a deactivated user to sign in again. Only an Admin may "
        "reactivate an Admin."
    ),
)
async def reactivate_employee(
    id: uuid.UUID,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> User:
    """Reactivate an employee.

    Manager or Admin. Allow a deactivated user to sign in again. Only an Admin may reactivate an
    Admin.
    """
    svc = _employee_svc(db)
    return await svc.reactivate(actor=current_user, target_id=id)


@router.get(
    "/{id}/groups",
    response_model=list[GroupRead],
    summary="List an employee's groups",
    description=(
        "Manager or Admin for any user in the organisation; every other role only for their own id."
    ),
)
async def get_employee_groups(
    id: uuid.UUID,
    current_user: User = Depends(get_current_user_or_self),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> list[Group]:
    """List an employee's groups.

    Manager or Admin for any user in the organisation; every other role only for their own id.
    """
    await _require_user_in_org(db, id, current_user)
    return await GroupRepository(db).get_groups_for_user(id, current_user.organization_id)


@router.put(
    "/{id}/groups",
    status_code=204,
    summary="Replace an employee's groups",
    description=(
        "Manager or Admin. Set the user's groups to exactly the given ids (an empty "
        "list removes all). Every id must belong to the organisation, otherwise 404 and "
        "nothing changes."
    ),
)
async def replace_employee_groups(
    id: uuid.UUID,
    body: IDListRequest,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> None:
    """Replace an employee's groups.

    Manager or Admin. Set the user's groups to exactly the given ids (an empty list removes
    all). Every id must belong to the organisation, otherwise 404 and nothing changes.
    """
    await _require_user_in_org(db, id, current_user)
    svc = GroupService(group_repo=GroupRepository(db))
    await svc.replace_employee_groups(actor=current_user, user_id=id, group_ids=body.ids)


@router.get(
    "/{id}/qualifications",
    response_model=list[QualificationRead],
    summary="List an employee's qualifications",
    description=(
        "Manager or Admin for any user in the organisation; every other role only for their own id."
    ),
)
async def get_employee_qualifications(
    id: uuid.UUID,
    current_user: User = Depends(get_current_user_or_self),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> list[Qualification]:
    """List an employee's qualifications.

    Manager or Admin for any user in the organisation; every other role only for their own id.
    """
    await _require_user_in_org(db, id, current_user)
    return await QualificationRepository(db).get_qualifications_for_user(
        id, current_user.organization_id
    )


@router.put(
    "/{id}/qualifications",
    status_code=204,
    summary="Replace an employee's qualifications",
    description=(
        "Manager or Admin. Set the user's qualifications to exactly the given ids (an "
        "empty list removes all). Every id must belong to the organisation, otherwise "
        "404 and nothing changes."
    ),
)
async def replace_employee_qualifications(
    id: uuid.UUID,
    body: IDListRequest,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> None:
    """Replace an employee's qualifications.

    Manager or Admin. Set the user's qualifications to exactly the given ids (an empty list
    removes all). Every id must belong to the organisation, otherwise 404 and nothing changes.
    """
    await _require_user_in_org(db, id, current_user)
    svc = QualificationService(qual_repo=QualificationRepository(db))
    await svc.replace_employee_qualifications(actor=current_user, user_id=id, qual_ids=body.ids)
