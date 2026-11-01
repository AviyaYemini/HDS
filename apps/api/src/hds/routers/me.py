"""Me router: current-user profile endpoints."""

import uuid
from datetime import datetime
from decimal import Decimal

from fastapi import APIRouter, Depends
from pydantic import BaseModel, EmailStr
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.database import get_db
from hds.core.deps import get_current_user
from hds.models.user import User, UserRole
from hds.repositories.refresh_token import RefreshTokenRepository
from hds.repositories.user import UserRepository
from hds.services.employee import EmployeeService

router = APIRouter(prefix="/me", tags=["me"])


class UserRead(BaseModel):
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
    # Extra keys are kept so the service can reject them with 403 (profile.field_not_allowed)
    # instead of the schema silently dropping e.g. ``role``.
    model_config = {"extra": "allow"}

    full_name: str | None = None
    phone: str | None = None


@router.get("", response_model=UserRead)
async def get_me(current_user: User = Depends(get_current_user)) -> User:
    return current_user


@router.patch("", response_model=UserRead)
async def patch_me(
    body: ProfileUpdate,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> User:
    data = body.model_dump(exclude_none=True)
    svc = EmployeeService(user_repo=UserRepository(db), token_repo=RefreshTokenRepository(db))
    return await svc.update_own_profile(current_user, data)
