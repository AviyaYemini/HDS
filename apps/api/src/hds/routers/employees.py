"""Employees router: CRUD, deactivation, groups, qualifications.

Layer: routers. Delegates to ``EmployeeService``, ``GroupService`` and ``QualificationService``.
The group and qualification sub-resources first check that the addressed user belongs to the
caller's organisation, because the services take that user id as already verified.
"""

import uuid

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.database import get_unit_of_work
from hds.core.deps import get_current_user_or_self, require_admin, require_manager
from hds.core.types import (
    AssignableRole,
    ChangeRequest,
    Email,
    Money,
    Name,
    Password,
    Phone,
    omittable,
)
from hds.models.group import Group
from hds.models.qualification import Qualification
from hds.models.user import User, UserRole
from hds.repositories.client import ClientRepository
from hds.repositories.group import GroupRepository
from hds.repositories.invitation_token import InvitationTokenRepository
from hds.repositories.project_manager import ProjectManagerRepository
from hds.repositories.qualification import QualificationRepository
from hds.repositories.refresh_token import RefreshTokenRepository
from hds.repositories.user import UserRepository
from hds.routers.user_schemas import UserRead
from hds.services.employee import EmployeeService, shown_rate
from hds.services.group import GroupService
from hds.services.qualification import QualificationService

router = APIRouter(prefix="/api/v1/employees", tags=["employees"])


class UserCreateRequest(BaseModel):
    """New employee.

    The initial password needs at least 12 characters; the role defaults to employee and the
    hourly rate is optional.
    """

    email: Email
    full_name: Name
    phone: Phone | None = None
    role: AssignableRole = "employee"
    initial_password: Password = Field(min_length=12)
    default_hourly_rate: Money | None = None


class UserUpdateRequest(ChangeRequest):
    """Fields a Manager or Admin may change; omitted fields are left as they are, null clears."""

    full_name: Name = omittable()
    phone: Phone | None = None
    role: AssignableRole = omittable()
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


def _read(actor: User, user: User) -> UserRead:
    """``user`` as ``actor`` may see them: the default rate only where the service allows it."""
    return UserRead.model_validate(user).model_copy(
        update={"default_hourly_rate": shown_rate(actor, user)}
    )


def _employee_svc(db: AsyncSession) -> EmployeeService:
    return EmployeeService(
        user_repo=UserRepository(db),
        token_repo=RefreshTokenRepository(db),
        link_repo=InvitationTokenRepository(db),
        group_repo=GroupRepository(db),
        qualification_repo=QualificationRepository(db),
        client_repo=ClientRepository(db),
        project_manager_repo=ProjectManagerRepository(db),
    )


@router.get(
    "",
    response_model=UserListResponse,
    summary="List employees",
    description=(
        "Manager or Admin. Search the organisation's non-deleted users by name or email "
        "and filter by role, active state, group or qualification. All filters combine "
        "with AND and the result is ordered by name. A group or qualification from "
        "another organisation, or one that does not exist, returns 404."
    ),
)
async def list_employees(
    search: Name | None = Query(default=None),
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
    ordered by name. A group or qualification from another organisation, or one that does not
    exist, returns 404.
    """
    users = await _employee_svc(db).list_employees(
        current_user,
        search=search,
        role=role,
        is_active=is_active,
        group_id=group_id,
        qualification_id=qualification_id,
    )
    return UserListResponse(items=[_read(current_user, u) for u in users], total=len(users))


@router.post(
    "",
    response_model=UserRead,
    status_code=201,
    summary="Create an employee",
    description=(
        "Manager or Admin. Create an account with an initial password of at least 12 "
        "characters. The role defaults to employee; only an Admin may create an Admin, "
        "and Client users have their own endpoints (`users.role_invalid`). A duplicate "
        "email returns 409 (`users.duplicate_email`). "
        "Answers 503 (`service.busy`) with `Retry-After` when the password-hashing queue "
        "is full; nothing is changed."
    ),
)
async def create_employee(
    body: UserCreateRequest,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> UserRead:
    """Create an employee.

    Manager or Admin. Create an account with an initial password of at least 12 characters. The
    role defaults to employee; only an Admin may create an Admin, and Client users have their
    own endpoints (`users.role_invalid`). A duplicate email returns 409
    (`users.duplicate_email`). Answers 503 (`service.busy`) with `Retry-After` when the
    password-hashing queue is full; nothing is changed.
    """
    svc = _employee_svc(db)
    return _read(
        current_user, await svc.create_employee(actor=current_user, data=body.model_dump())
    )


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
) -> UserRead:
    """Get an employee.

    Manager or Admin for any user in the organisation; every other role only for their own id
    (otherwise 403 `users.forbidden`).
    """
    return _read(current_user, await _employee_svc(db).get_employee(current_user, id))


@router.patch(
    "/{id}",
    response_model=UserRead,
    summary="Edit an employee",
    description=(
        "Manager or Admin. Update name, phone, hourly rate or role. Only an Admin may "
        "change an Admin or assign the Admin role. Changing a role ends the user's "
        "sessions. Demoting the last active Admin returns 409 (`users.last_admin`). "
        "An explicit null clears a field that allows it and is refused with 422 on a "
        "required one; an omitted field is left alone. "
        "Changing the role of a Client account returns 409 "
        "(`users.client_role_locked`)."
    ),
)
async def edit_employee(
    id: uuid.UUID,
    body: UserUpdateRequest,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> UserRead:
    """Edit an employee.

    Manager or Admin. Update name, phone, hourly rate or role. Only an Admin may change an Admin
    or assign the Admin role. Changing a role ends the user's sessions. Demoting the last active
    Admin returns 409 (`users.last_admin`).
    An explicit null clears a field that allows it and is refused with 422 on a required one; an
    omitted field is left alone.
    Changing the role of a Client account returns 409 (`users.client_role_locked`).
    """
    svc = _employee_svc(db)
    data = body.model_dump(exclude_unset=True)
    return _read(current_user, await svc.edit_employee(actor=current_user, target_id=id, data=data))


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
        "`users.last_admin`). An account that was invited but never accepted cannot be "
        "deactivated: it returns 409 (`users.pending_cannot_deactivate`) and stays pending."
    ),
)
async def deactivate_employee(
    id: uuid.UUID,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> UserRead:
    """Deactivate an employee.

    Manager or Admin. Block the user from signing in and end their sessions. Only an Admin may
    deactivate an Admin, and never the last active one (409 `users.last_admin`). An account that
    was invited but never accepted cannot be deactivated: it returns 409
    (`users.pending_cannot_deactivate`) and stays pending.
    """
    svc = _employee_svc(db)
    return _read(current_user, await svc.deactivate(actor=current_user, target_id=id))


@router.post(
    "/{id}/reactivate",
    response_model=UserRead,
    summary="Reactivate an employee",
    description=(
        "Manager or Admin. Allow a deactivated user to sign in again. Only an Admin may "
        "reactivate an Admin. An account that was invited but never accepted cannot be "
        "reactivated: it returns 409 (`users.pending_cannot_reactivate`) and stays pending, "
        "because only accepting the invitation activates it. A portal user whose client "
        "is inactive or deleted returns 409 (`users.client_inactive`)."
    ),
)
async def reactivate_employee(
    id: uuid.UUID,
    current_user: User = Depends(require_manager),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> UserRead:
    """Reactivate an employee.

    Manager or Admin. Allow a deactivated user to sign in again. Only an Admin may reactivate an
    Admin. An account that was invited but never accepted cannot be reactivated: it returns 409
    (`users.pending_cannot_reactivate`) and stays pending, because only accepting the invitation
    activates it. A portal user whose client is inactive or deleted returns 409
    (`users.client_inactive`).
    """
    svc = _employee_svc(db)
    return _read(current_user, await svc.reactivate(actor=current_user, target_id=id))


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
    return await _employee_svc(db).list_groups_of(current_user, id)


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
    await _employee_svc(db).ensure_manageable(current_user, id)
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
    return await _employee_svc(db).list_qualifications_of(current_user, id)


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
    await _employee_svc(db).ensure_manageable(current_user, id)
    svc = QualificationService(qual_repo=QualificationRepository(db))
    await svc.replace_employee_qualifications(actor=current_user, user_id=id, qual_ids=body.ids)
