"""Service-layer role enforcement (Constitution VI).

Layer: core. Depends on ``hds.models.user`` and ``hds.core.exceptions`` only; it MUST NOT
import repositories, services or routers.

Every service method that is role-restricted calls ``ensure_role`` itself, so the role matrix
holds even if a router forgets its dependency (defence in depth; the UI is never the gate).
"""

import uuid

from hds.core.exceptions import ForbiddenError
from hds.models.user import User, UserRole

# Role sets that map to a dedicated error code in ``ensure_role``.
MANAGER_ROLES: tuple[UserRole, ...] = (UserRole.manager, UserRole.admin)
ADMIN_ROLES: tuple[UserRole, ...] = (UserRole.admin,)


def ensure_role(actor: User, *roles: UserRole) -> None:
    """Raise ``ForbiddenError`` unless the actor's role is one of ``roles``.

    The role is read from the ``User`` row loaded for this request, not from the token, so a
    demotion takes effect immediately. The error code depends on the allowed set so clients can
    show a precise message.

    Args:
        actor: The authenticated user performing the action.
        *roles: Roles that may perform it.

    Raises:
        ForbiddenError: ``users.admin_required`` when only Admin is allowed,
            ``users.manager_required`` when only Manager and Admin are allowed, otherwise
            ``users.forbidden``. Maps to HTTP 403.
    """
    if actor.role in roles:
        return
    if set(roles) == set(ADMIN_ROLES):
        raise ForbiddenError(code="users.admin_required", detail="Admin role required")
    if set(roles) == set(MANAGER_ROLES):
        raise ForbiddenError(code="users.manager_required", detail="Manager or Admin role required")
    raise ForbiddenError(code="users.forbidden", detail="Access to this resource is not allowed")


def ensure_self_or_manager(actor: User, user_id: uuid.UUID) -> None:
    """Raise ``ForbiddenError`` unless the actor is a Manager or Admin, or is that user.

    Args:
        actor: The authenticated user performing the action.
        user_id: The user addressed.

    Raises:
        ForbiddenError: ``users.forbidden`` (HTTP 403).
    """
    if actor.role not in MANAGER_ROLES and actor.id != user_id:
        raise ForbiddenError(
            code="users.forbidden", detail="Access to this resource is not allowed"
        )
