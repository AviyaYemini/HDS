"""FastAPI dependencies that authenticate the caller and gate routes by role.

Layer: core (consumed by routers). It may read users through ``UserRepository`` but MUST NOT
import services or routers. The checks here are a first gate only: services re-check roles
with ``hds.core.authz.ensure_role`` so the matrix holds without the HTTP layer.
"""

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
    """Resolve the bearer token to an active user.

    The user row is reloaded on every request, so deactivation, role changes and deletion
    apply immediately even though the access token is still unexpired.

    Args:
        token: Bearer token from the ``Authorization`` header, if any.
        db: Request-scoped database session.

    Returns:
        The active ``User`` the token was issued to.

    Raises:
        AuthError: ``auth.missing_token`` (no token), ``auth.invalid_token`` (bad signature,
            expired, or malformed subject), ``auth.user_not_found`` or
            ``auth.account_inactive``. All map to HTTP 401.
    """
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
    """Allow Manager and Admin; reject Employee and Client.

    Args:
        user: The authenticated caller.

    Returns:
        The same user, so routes can use it as the actor.

    Raises:
        ForbiddenError: ``users.manager_required`` (HTTP 403).
    """
    if user.role not in {UserRole.manager, UserRole.admin}:
        raise ForbiddenError(
            code="users.manager_required",
            detail="Manager or Admin role required",
        )
    return user


async def require_manager_or_client(
    user: User = Depends(get_current_user),
) -> User:
    """Allow Manager, Admin and Client; reject Employee.

    Used for project reads. A Client passes this gate but the service narrows the result to
    that client's own projects, so this dependency alone is not sufficient for data scope.

    Args:
        user: The authenticated caller.

    Returns:
        The same user, so routes can use it as the actor.

    Raises:
        ForbiddenError: ``users.manager_required`` (HTTP 403) for an Employee.
    """
    if user.role not in {UserRole.manager, UserRole.admin, UserRole.client}:
        raise ForbiddenError(
            code="users.manager_required",
            detail="Manager or Admin role required",
        )
    return user


async def require_admin(
    user: User = Depends(get_current_user),
) -> User:
    """Allow Admin only.

    Args:
        user: The authenticated caller.

    Returns:
        The same user, so routes can use it as the actor.

    Raises:
        ForbiddenError: ``users.admin_required`` (HTTP 403).
    """
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
    """Allow Manager and Admin for any ``{id}``, everyone else only for their own id.

    Args:
        id: The user id taken from the route path.
        user: The authenticated caller.

    Returns:
        The caller (not the user addressed by ``id``).

    Raises:
        ForbiddenError: ``users.forbidden`` (HTTP 403) when a non-manager addresses another
            user.
    """
    if user.role in {UserRole.manager, UserRole.admin}:
        return user
    if user.id != id:
        raise ForbiddenError(
            code="users.forbidden",
            detail="Access to this resource is not allowed",
        )
    return user
