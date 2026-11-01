"""Service-layer role enforcement (Constitution VI)."""

from hds.core.exceptions import ForbiddenError
from hds.models.user import User, UserRole

MANAGER_ROLES: tuple[UserRole, ...] = (UserRole.manager, UserRole.admin)
ADMIN_ROLES: tuple[UserRole, ...] = (UserRole.admin,)


def ensure_role(actor: User, *roles: UserRole) -> None:
    """Raise ForbiddenError unless the actor's live role is one of ``roles``."""
    if actor.role in roles:
        return
    if set(roles) == set(ADMIN_ROLES):
        raise ForbiddenError(code="users.admin_required", detail="Admin role required")
    if set(roles) == set(MANAGER_ROLES):
        raise ForbiddenError(code="users.manager_required", detail="Manager or Admin role required")
    raise ForbiddenError(code="users.forbidden", detail="Access to this resource is not allowed")
