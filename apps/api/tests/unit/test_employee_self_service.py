"""Unit tests for EmployeeService self-service methods (T046, T047).

Covers: FR-017.
"""

import uuid
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from hds.core.exceptions import ForbiddenError
from hds.models.user import User, UserRole
from hds.services.employee import EmployeeService


def _make_user(**kwargs: object) -> User:
    user = MagicMock(spec=User)
    user.id = kwargs.get("id", uuid.uuid4())
    user.organization_id = kwargs.get("org_id", uuid.uuid4())
    user.email = kwargs.get("email", "emp@hds.test")
    user.full_name = kwargs.get("full_name", "Test Employee")
    user.phone = kwargs.get("phone", None)
    user.role = kwargs.get("role", UserRole.employee)
    user.is_active = kwargs.get("is_active", True)
    user.is_deleted = kwargs.get("is_deleted", False)
    user.default_hourly_rate = kwargs.get("default_hourly_rate", Decimal("50.0000"))
    return user


@pytest.mark.asyncio
async def test_update_own_profile_success() -> None:
    user = _make_user()
    user_repo = AsyncMock()
    updated = _make_user(full_name="New Name", phone="+972501234567")
    user_repo.update = AsyncMock(return_value=updated)

    svc = EmployeeService(
        user_repo=user_repo,
        token_repo=AsyncMock(),
        link_repo=AsyncMock(),
        group_repo=AsyncMock(),
        qualification_repo=AsyncMock(),
        client_repo=AsyncMock(),
    )
    result = await svc.update_own_profile(user, {"full_name": "New Name", "phone": "+972501234567"})

    user_repo.update.assert_awaited_once_with(
        user.id, user.organization_id, full_name="New Name", phone="+972501234567"
    )
    assert result.full_name == "New Name"


@pytest.mark.asyncio
async def test_update_own_profile_disallows_role_change() -> None:
    user = _make_user()
    user_repo = AsyncMock()
    svc = EmployeeService(
        user_repo=user_repo,
        token_repo=AsyncMock(),
        link_repo=AsyncMock(),
        group_repo=AsyncMock(),
        qualification_repo=AsyncMock(),
        client_repo=AsyncMock(),
    )

    with pytest.raises(ForbiddenError) as exc_info:
        await svc.update_own_profile(user, {"role": "manager"})

    assert exc_info.value.code == "profile.field_not_allowed"
    user_repo.update.assert_not_awaited()


@pytest.mark.asyncio
async def test_update_own_profile_disallows_is_active_change() -> None:
    user = _make_user()
    user_repo = AsyncMock()
    svc = EmployeeService(
        user_repo=user_repo,
        token_repo=AsyncMock(),
        link_repo=AsyncMock(),
        group_repo=AsyncMock(),
        qualification_repo=AsyncMock(),
        client_repo=AsyncMock(),
    )

    with pytest.raises(ForbiddenError) as exc_info:
        await svc.update_own_profile(user, {"is_active": False})

    assert exc_info.value.code == "profile.field_not_allowed"


@pytest.mark.asyncio
async def test_get_profile_returns_user_with_hourly_rate() -> None:
    user = _make_user(default_hourly_rate=Decimal("75.0000"))
    user_repo = AsyncMock()
    user_repo.get_by_id = AsyncMock(return_value=user)
    svc = EmployeeService(
        user_repo=user_repo,
        token_repo=AsyncMock(),
        link_repo=AsyncMock(),
        group_repo=AsyncMock(),
        qualification_repo=AsyncMock(),
        client_repo=AsyncMock(),
    )

    result = await svc.get_profile(user.id, user.organization_id)

    assert result.default_hourly_rate == Decimal("75.0000")
    user_repo.get_by_id.assert_awaited_once_with(user.id, user.organization_id)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field", ["role", "is_active", "organization_id", "default_hourly_rate", "email", "bogus"]
)
async def test_update_own_profile_never_passes_forbidden_fields_to_repo(field: str) -> None:
    user = _make_user()
    user_repo = AsyncMock()
    svc = EmployeeService(
        user_repo=user_repo,
        token_repo=AsyncMock(),
        link_repo=AsyncMock(),
        group_repo=AsyncMock(),
        qualification_repo=AsyncMock(),
        client_repo=AsyncMock(),
    )

    with pytest.raises(ForbiddenError):
        await svc.update_own_profile(user, {"full_name": "Ok", field: "x"})

    user_repo.update.assert_not_awaited()


@pytest.mark.asyncio
async def test_update_own_profile_passes_only_whitelisted_fields() -> None:
    user = _make_user()
    user_repo = AsyncMock()
    svc = EmployeeService(
        user_repo=user_repo,
        token_repo=AsyncMock(),
        link_repo=AsyncMock(),
        group_repo=AsyncMock(),
        qualification_repo=AsyncMock(),
        client_repo=AsyncMock(),
    )

    await svc.update_own_profile(user, {"full_name": "New", "phone": "123"})

    user_repo.update.assert_awaited_once_with(
        user.id, user.organization_id, full_name="New", phone="123"
    )
