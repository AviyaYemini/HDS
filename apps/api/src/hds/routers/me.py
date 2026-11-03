"""Me router: current-user profile endpoints.

Layer: routers. Open to every authenticated role; it only ever touches the caller's own row.
"""

import uuid
from datetime import datetime
from decimal import Decimal

from fastapi import APIRouter, Depends
from pydantic import BaseModel, EmailStr
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.database import get_unit_of_work
from hds.core.deps import get_current_user
from hds.core.types import Name, Phone, omittable
from hds.models.user import User, UserRole
from hds.repositories.client import ClientRepository
from hds.repositories.group import GroupRepository
from hds.repositories.invitation_token import InvitationTokenRepository
from hds.repositories.qualification import QualificationRepository
from hds.repositories.refresh_token import RefreshTokenRepository
from hds.repositories.user import UserRepository
from hds.services.employee import EmployeeService

router = APIRouter(prefix="/me", tags=["me"])


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
    default_hourly_rate: Decimal | None = None  # FR-017: viewable by employee, not editable
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class ProfileUpdate(BaseModel):
    """Self-service profile changes.

    Unknown fields are accepted by the schema and refused with 403 by the service, so ``role`` is
    never silently dropped.
    """

    # Extra keys are kept so the service can reject them with 403 (profile.field_not_allowed)
    # instead of the schema silently dropping e.g. ``role``.
    model_config = {"extra": "allow"}

    full_name: Name = omittable()
    phone: Phone | None = None


@router.get(
    "",
    response_model=UserRead,
    summary="Get my profile",
    description=(
        "Return the signed-in user's own profile, including their default hourly rate "
        "(view only). Available to every role."
    ),
)
async def get_me(current_user: User = Depends(get_current_user)) -> User:
    """Get my profile.

    Return the signed-in user's own profile, including their default hourly rate (view only).
    Available to every role.
    """
    return current_user


@router.patch(
    "",
    response_model=UserRead,
    summary="Update my profile",
    description=(
        "Change the signed-in user's own full name or phone. Any other field is refused "
        "with 403 (`profile.field_not_allowed`), so role, rate, email and activity "
        "cannot be changed here. "
        "An explicit null clears a field that allows it and is refused with 422 on a "
        "required one; an omitted field is left alone."
    ),
)
async def patch_me(
    body: ProfileUpdate,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> User:
    """Update my profile.

    Change the signed-in user's own full name or phone. Any other field is refused with 403
    (`profile.field_not_allowed`), so role, rate, email and activity cannot be changed here.
    An explicit null clears a field that allows it and is refused with 422 on a required one; an
    omitted field is left alone.
    """
    data = body.model_dump(exclude_unset=True)
    svc = EmployeeService(
        user_repo=UserRepository(db),
        token_repo=RefreshTokenRepository(db),
        link_repo=InvitationTokenRepository(db),
        group_repo=GroupRepository(db),
        qualification_repo=QualificationRepository(db),
        client_repo=ClientRepository(db),
    )
    return await svc.update_own_profile(current_user, data)
