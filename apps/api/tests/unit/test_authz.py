"""Unit tests for core.authz.ensure_role (Constitution VI).

Covers: FR-004, FR-008, FR-009.
"""

from unittest.mock import MagicMock

import pytest

from hds.core.authz import ADMIN_ROLES, MANAGER_ROLES, ensure_role
from hds.core.exceptions import ForbiddenError
from hds.models.user import User, UserRole


def _actor(role: UserRole) -> User:
    user = MagicMock(spec=User)
    user.role = role
    return user


def test_manager_roles_allow_manager_and_admin_only() -> None:
    for role in UserRole:
        actor = _actor(role)
        if role in (UserRole.manager, UserRole.admin):
            ensure_role(actor, *MANAGER_ROLES)
        else:
            with pytest.raises(ForbiddenError) as exc_info:
                ensure_role(actor, *MANAGER_ROLES)
            assert exc_info.value.code == "users.manager_required"


def test_admin_roles_allow_admin_only() -> None:
    for role in UserRole:
        actor = _actor(role)
        if role == UserRole.admin:
            ensure_role(actor, *ADMIN_ROLES)
        else:
            with pytest.raises(ForbiddenError) as exc_info:
                ensure_role(actor, *ADMIN_ROLES)
            assert exc_info.value.code == "users.admin_required"


def test_other_role_sets_use_generic_code() -> None:
    with pytest.raises(ForbiddenError) as exc_info:
        ensure_role(_actor(UserRole.employee), UserRole.client)
    assert exc_info.value.code == "users.forbidden"
