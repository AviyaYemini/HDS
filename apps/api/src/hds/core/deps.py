"""FastAPI dependencies that authenticate the caller and gate routes by role.

Layer: core (consumed by routers). It may read users through ``UserRepository`` but MUST NOT
import services or routers. The checks here are a first gate only: services re-check roles
with ``hds.core.authz.ensure_role`` so the matrix holds without the HTTP layer.
"""

import uuid
from collections.abc import Callable, Coroutine, Mapping
from typing import Any

from fastapi import Depends, Request
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.authz import ensure_self_or_manager
from hds.core.capabilities import Capability, has
from hds.core.config import get_settings
from hds.core.database import get_unit_of_work
from hds.core.exceptions import AuthError, ForbiddenError
from hds.core.security import decode_access_token
from hds.models.user import User
from hds.repositories.user import UserRepository

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login", auto_error=False)


async def get_current_user(
    request: Request,
    token: str | None = Depends(oauth2_scheme),
    db: AsyncSession = Depends(get_unit_of_work, scope="function"),
) -> User:
    """Resolve the bearer token to an active user.

    The user row is reloaded on every request, so deactivation, role changes and deletion
    apply immediately even though the access token is still unexpired.

    Records the token's session on ``request.state.session_id`` (see :func:`current_session_id`),
    so the token is decoded once per request.

    Args:
        request: The request, which carries the session id onward.
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

    request.state.session_id = session_id_from_claims(payload)
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
    # roster.read: Manager and Admin.
    if not has(user.role, Capability.ROSTER_READ):
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
    # projects.read: Manager, Admin and Client.
    if not has(user.role, Capability.PROJECTS_READ):
        raise ForbiddenError(
            code="users.manager_required",
            detail="Manager or Admin role required",
        )
    return user


async def require_staff(
    user: User = Depends(get_current_user),
) -> User:
    """Allow everyone who works shifts (Employee, Manager, Admin); reject a Client account.

    Used for availability. The service still decides whose records the caller may read or
    change; this gate only keeps Client accounts out, before the body is validated.

    Args:
        user: The authenticated caller.

    Returns:
        The same user, so routes can use it as the actor.

    Raises:
        ForbiddenError: ``users.forbidden`` (HTTP 403) for a Client account.
    """
    # my_work.read: Employee, Manager and Admin.
    if not has(user.role, Capability.MY_WORK_READ):
        raise ForbiddenError(
            code="users.forbidden", detail="Access to this resource is not allowed"
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
    # staff.delete: Admin only.
    if not has(user.role, Capability.STAFF_DELETE):
        raise ForbiddenError(
            code="users.admin_required",
            detail="Admin role required",
        )
    return user


def require_capability(
    capability: Capability, code: str = "users.forbidden"
) -> Callable[..., Coroutine[Any, Any, User]]:
    """A route gate for one capability of the map (007 FR-001, research R4).

    The 007 routes gate on the capability they mean, so granting it to a role opens the route and
    its navigation together. The older routes keep ``require_manager`` and ``require_admin``.

    Args:
        capability: The capability the route needs.
        code: The refusal's code, ``users.admin_required`` where an admin-only answer is kept.

    Returns:
        A dependency that answers the caller, or refuses with 403 and ``code``.
    """

    async def gate(user: User = Depends(get_current_user)) -> User:
        """Answer the caller when their role holds ``capability``; refuse with 403 otherwise."""
        if not has(user.role, capability):
            raise ForbiddenError(code=code, detail=f"Needs {capability.value}")
        return user

    return gate


def session_id_from_claims(claims: Mapping[str, Any]) -> uuid.UUID | None:
    """The session an access token belongs to, from its ``sid`` claim (007 T039).

    Args:
        claims: The token's verified claims.

    Returns:
        The id of the refresh token issued with the access token, or ``None`` for a token issued
        before sessions were marked, or one whose ``sid`` is not a UUID.
    """
    try:
        return uuid.UUID(str(claims["sid"]))
    except (KeyError, ValueError):
        return None


def current_session_id(request: Request, _: User = Depends(get_current_user)) -> uuid.UUID | None:
    """The caller's session, as :func:`get_current_user` read it from the token.

    FastAPI runs ``get_current_user`` once per request, so the token is not decoded again here.

    Args:
        request: The request; ``get_current_user`` left the session id on its state.

    Returns:
        The session's id, or ``None`` when the token carries none.
    """
    found: uuid.UUID | None = getattr(request.state, "session_id", None)
    return found


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
    ensure_self_or_manager(user, id)
    return user
