"""Unit tests for AuthService using fake repositories (T033, T120).

Covers: FR-001, FR-002, FR-003, FR-005, FR-006b, FR-015, NFR-005.
"""

import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest

from hds.core.exceptions import AuthError
from hds.models.user import User, UserRole
from hds.services.auth import AuthService, TokenResponse


def _make_user(**kwargs: object) -> User:
    user = MagicMock(spec=User)
    user.id = kwargs.get("id", uuid.uuid4())
    user.organization_id = kwargs.get("org_id", uuid.uuid4())
    user.email = kwargs.get("email", "test@hds.test")
    user.password_hash = kwargs.get("password_hash", "$argon2id$fake")
    user.role = kwargs.get("role", UserRole.employee)
    user.is_active = kwargs.get("is_active", True)
    user.is_deleted = kwargs.get("is_deleted", False)
    user.failed_login_attempts = kwargs.get("failed_login_attempts", 0)
    user.locked_until = kwargs.get("locked_until", None)
    return user


def _make_config() -> MagicMock:
    cfg = MagicMock()
    cfg.JWT_SECRET_KEY.get_secret_value.return_value = "test-only-jwt-secret-0123456789-abcdef"
    cfg.JWT_ALGORITHM = "HS256"
    cfg.ACCESS_TOKEN_TTL_SECONDS = 900
    cfg.REFRESH_TOKEN_TTL_DAYS = 7
    cfg.LOGIN_MAX_FAILURES = 5
    cfg.LOGIN_LOCKOUT_MINUTES = 15
    return cfg


@pytest.mark.asyncio
async def test_login_correct_credentials_returns_token_response() -> None:
    user = _make_user()
    user_repo = AsyncMock()
    user_repo.get_by_email.return_value = user
    user_repo.reset_failed_attempts = AsyncMock()
    token_repo = AsyncMock()
    token_repo.create = AsyncMock()

    service = AuthService(user_repo, token_repo, _make_config())
    # patch verify at module level
    import hds.services.auth as auth_module

    orig = auth_module._verify_password
    auth_module._verify_password = lambda p, h: True  # type: ignore[assignment]

    result = await service.login("test@hds.test", "password")

    assert isinstance(result, TokenResponse)
    assert result.access_token
    assert result.refresh_token

    auth_module._verify_password = orig


@pytest.mark.asyncio
async def test_wrong_password_increments_failed_attempts() -> None:
    user = _make_user(failed_login_attempts=0)
    user_repo = AsyncMock()
    user_repo.get_by_email.return_value = user
    user_repo.increment_failed_attempts = AsyncMock()
    token_repo = AsyncMock()

    service = AuthService(user_repo, token_repo, _make_config())
    import hds.services.auth as auth_module

    orig = auth_module._verify_password
    auth_module._verify_password = lambda p, h: False  # type: ignore[assignment]

    with pytest.raises(AuthError):
        await service.login("test@hds.test", "wrong")

    user_repo.increment_failed_attempts.assert_awaited()
    auth_module._verify_password = orig


@pytest.mark.asyncio
async def test_fifth_failure_sets_locked_until() -> None:
    user = _make_user(failed_login_attempts=4)
    user_repo = AsyncMock()
    user_repo.get_by_email.return_value = user
    user_repo.increment_failed_attempts = AsyncMock()
    user_repo.set_locked_until = AsyncMock()
    token_repo = AsyncMock()

    service = AuthService(user_repo, token_repo, _make_config())
    import hds.services.auth as auth_module

    orig = auth_module._verify_password
    auth_module._verify_password = lambda p, h: False  # type: ignore[assignment]

    with pytest.raises(AuthError):
        await service.login("test@hds.test", "wrong")

    user_repo.set_locked_until.assert_awaited()
    auth_module._verify_password = orig


@pytest.mark.asyncio
async def test_login_while_locked_raises_account_locked() -> None:
    future = datetime.now(UTC) + timedelta(minutes=10)
    user = _make_user(locked_until=future)
    user_repo = AsyncMock()
    user_repo.get_by_email.return_value = user
    token_repo = AsyncMock()

    service = AuthService(user_repo, token_repo, _make_config())

    with pytest.raises(AuthError) as exc_info:
        await service.login("test@hds.test", "any")

    assert exc_info.value.code == "auth.account_locked"


@pytest.mark.asyncio
async def test_successful_login_resets_counter() -> None:
    user = _make_user(failed_login_attempts=2)
    user_repo = AsyncMock()
    user_repo.get_by_email.return_value = user
    user_repo.reset_failed_attempts = AsyncMock()
    token_repo = AsyncMock()
    token_repo.create = AsyncMock()

    service = AuthService(user_repo, token_repo, _make_config())
    import hds.services.auth as auth_module

    orig = auth_module._verify_password
    auth_module._verify_password = lambda p, h: True  # type: ignore[assignment]

    await service.login("test@hds.test", "correct")

    user_repo.reset_failed_attempts.assert_awaited_with(user.id)
    auth_module._verify_password = orig


@pytest.mark.asyncio
async def test_logout_revokes_token() -> None:
    token_repo = AsyncMock()
    token_repo.get_by_hash = AsyncMock(return_value=MagicMock(id=uuid.uuid4(), revoked_at=None))
    token_repo.revoke = AsyncMock()

    service = AuthService(AsyncMock(), token_repo, _make_config())
    await service.logout("raw-token-value")

    token_repo.revoke.assert_awaited()


@pytest.mark.asyncio
async def test_logout_all_revokes_all_user_tokens() -> None:
    token_repo = AsyncMock()
    token_repo.revoke_all_for_user = AsyncMock()
    user_id = uuid.uuid4()

    service = AuthService(AsyncMock(), token_repo, _make_config())
    await service.logout_all(user_id)

    token_repo.revoke_all_for_user.assert_awaited_with(user_id)


@pytest.mark.asyncio
async def test_refresh_rotates_token() -> None:
    user_id = uuid.uuid4()
    old_token = MagicMock()
    old_token.id = uuid.uuid4()
    old_token.user_id = user_id
    old_token.revoked_at = None
    old_token.expires_at = datetime.now(UTC) + timedelta(days=1)

    user = _make_user(id=user_id)
    user_repo = AsyncMock()
    user_repo.get_for_auth = AsyncMock(return_value=user)

    token_repo = AsyncMock()
    token_repo.get_by_hash = AsyncMock(return_value=old_token)
    token_repo.revoke = AsyncMock()
    token_repo.create = AsyncMock()

    service = AuthService(user_repo, token_repo, _make_config())
    result = await service.refresh("raw-token")

    token_repo.revoke.assert_awaited_with(old_token.id)
    assert isinstance(result, TokenResponse)


def _refresh_fixture(user: User | None) -> tuple[AuthService, AsyncMock]:
    old_token = MagicMock()
    old_token.id = uuid.uuid4()
    old_token.user_id = uuid.uuid4()
    old_token.revoked_at = None
    old_token.expires_at = datetime.now(UTC) + timedelta(days=1)
    user_repo = AsyncMock()
    user_repo.get_for_auth = AsyncMock(return_value=user)
    token_repo = AsyncMock()
    token_repo.get_by_hash = AsyncMock(return_value=old_token)
    return AuthService(user_repo, token_repo, _make_config()), token_repo


@pytest.mark.asyncio
async def test_refresh_rejects_inactive_user_and_issues_nothing() -> None:
    service, token_repo = _refresh_fixture(_make_user(is_active=False))

    with pytest.raises(AuthError) as exc_info:
        await service.refresh("raw-token")

    assert exc_info.value.code == "auth.invalid_token"
    token_repo.create.assert_not_awaited()


@pytest.mark.asyncio
async def test_refresh_rejects_deleted_user_and_issues_nothing() -> None:
    # Soft-deleted users are never returned by the repository.
    service, token_repo = _refresh_fixture(None)

    with pytest.raises(AuthError) as exc_info:
        await service.refresh("raw-token")

    assert exc_info.value.code == "auth.invalid_token"
    token_repo.create.assert_not_awaited()
