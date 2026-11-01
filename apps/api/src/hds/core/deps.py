"""FastAPI dependency functions for authentication and authorisation."""

import uuid

from fastapi import Depends
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.config import get_settings
from hds.core.database import get_db
from hds.core.exceptions import AuthError, ForbiddenError
from hds.core.security import decode_access_token
from hds.models.user import User, UserRole
from hds.repositories.user import UserRepository

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login", auto_error=False)


async def get_current_user(
    token: str | None = Depends(oauth2_scheme),
    db: AsyncSession = Depends(get_db),
) -> User:
    if token is None:
        raise AuthError(code="auth.missing_token", detail="Authentication required")

    settings = get_settings()
    try:
        payload = decode_access_token(
            token,
            secret=settings.JWT_SECRET_KEY.get_secret_value(),
            algorithm=settings.JWT_ALGORITHM,
        )
    except ValueError as exc:
        raise AuthError(code="auth.invalid_token", detail=str(exc)) from exc

    sub = payload.get("sub")
    if sub is None:
        raise AuthError(code="auth.invalid_token", detail="Token missing subject")

    try:
        user_id = uuid.UUID(sub)
    except ValueError as exc:
        raise AuthError(code="auth.invalid_token", detail="Invalid subject") from exc

    # Constitution Principle II: repos are the only code that touches the DB
    user = await UserRepository(db).get_for_auth(user_id)
    if user is None:
        raise AuthError(code="auth.user_not_found", detail="User not found")
    if not user.is_active:
        raise AuthError(code="auth.account_inactive", detail="Account inactive")

    return user


async def require_manager(
    user: User = Depends(get_current_user),
) -> User:
    if user.role not in {UserRole.manager, UserRole.admin}:
        raise ForbiddenError(
            code="users.manager_required",
            detail="Manager or Admin role required",
        )
    return user


async def require_admin(
    user: User = Depends(get_current_user),
) -> User:
    if user.role != UserRole.admin:
        raise ForbiddenError(
            code="users.admin_required",
            detail="Admin role required",
        )
    return user


async def get_current_user_or_self(
    id: uuid.UUID,
    user: User = Depends(get_current_user),
) -> User:
    if user.role in {UserRole.manager, UserRole.admin}:
        return user
    if user.id != id:
        raise ForbiddenError(
            code="users.forbidden",
            detail="Access to this resource is not allowed",
        )
    return user
