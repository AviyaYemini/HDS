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
    cfg.REFRESH_REUSE_GRACE_SECONDS = 10
    return cfg


@pytest.mark.asyncio
async def test_login_correct_credentials_returns_token_response() -> None:
    user = _make_user()
    user_repo = AsyncMock()
    user_repo.get_by_email.return_value = user
    user_repo.get_for_auth.return_value = user
    user_repo.reset_failed_attempts = AsyncMock()
    token_repo = AsyncMock()
    token_repo.create = AsyncMock()

    service = AuthService(user_repo, token_repo, _make_config(), AsyncMock())
    # patch verify at module level
    import hds.services.auth as auth_module

    orig = auth_module._verify_password
    auth_module._verify_password = lambda p, h: True  # type: ignore[assignment]

    result = await service.login("test@hds.test", "password")

    assert isinstance(result, TokenResponse)
    assert result.access_token
    assert result.refresh_token

    auth_module._verify_password = orig


def _wrong_password(monkeypatch: pytest.MonkeyPatch) -> None:
    import hds.services.auth as auth_module

    monkeypatch.setattr(auth_module, "_verify_password", lambda p, h: False)


@pytest.mark.asyncio
async def test_wrong_password_is_counted_by_the_repository_with_the_configured_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    user = _make_user(failed_login_attempts=3)  # the value from before this failure
    user_repo = AsyncMock()
    user_repo.get_by_email.return_value = user
    user_repo.clear_expired_lock.return_value = False
    user_repo.register_failed_login.return_value = (4, None)
    _wrong_password(monkeypatch)
    service = AuthService(user_repo, AsyncMock(), _make_config(), AsyncMock())

    with pytest.raises(AuthError) as refused:
        await service.login("test@hds.test", "wrong")

    assert refused.value.code == "auth.invalid_credentials"
    user_repo.register_failed_login.assert_awaited_once()
    args = user_repo.register_failed_login.await_args.args
    assert args[0] == user.id and args[1] == 5  # LOGIN_MAX_FAILURES


@pytest.mark.asyncio
@pytest.mark.parametrize("stored_count", [0, 3, 4])
async def test_the_service_does_not_add_one_to_the_count_itself(
    stored_count: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The old code computed ``user.failed_login_attempts + 1`` and locked one failure early."""
    user = _make_user(failed_login_attempts=stored_count)
    user_repo = AsyncMock()
    user_repo.get_by_email.return_value = user
    user_repo.clear_expired_lock.return_value = False
    user_repo.register_failed_login.return_value = (stored_count + 1, None)
    _wrong_password(monkeypatch)
    service = AuthService(user_repo, AsyncMock(), _make_config(), AsyncMock())

    with pytest.raises(AuthError):
        await service.login("test@hds.test", "wrong")

    # Whatever the stale value on the user object, the only write is the repository's single
    # statement; nothing else touches the counter or the lock.
    assert not user_repo.method_calls or {c[0] for c in user_repo.method_calls} <= {
        "get_by_email",
        "clear_expired_lock",
        "register_failed_login",
    }


@pytest.mark.asyncio
async def test_an_expired_lock_is_cleared_before_the_password_is_checked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    order: list[str] = []
    expired = datetime.now(UTC) - timedelta(minutes=1)
    user = _make_user(locked_until=expired, failed_login_attempts=5)
    user_repo = AsyncMock()
    user_repo.get_by_email.return_value = user

    async def clear(*_a: object) -> bool:
        order.append("clear")
        return True

    async def register(*_a: object) -> tuple[int, None]:
        order.append("register")
        return 1, None

    user_repo.clear_expired_lock.side_effect = clear
    user_repo.register_failed_login.side_effect = register
    import hds.services.auth as auth_module

    def verify(p: str, h: str) -> bool:
        order.append("verify")
        return False

    monkeypatch.setattr(auth_module, "_verify_password", verify)
    service = AuthService(user_repo, AsyncMock(), _make_config(), AsyncMock())

    with pytest.raises(AuthError) as refused:
        await service.login("test@hds.test", "wrong")

    assert refused.value.code == "auth.invalid_credentials"  # not "locked"
    assert order == ["clear", "verify", "register"]


@pytest.mark.asyncio
async def test_a_live_lock_is_not_cleared_by_waiting_and_refuses_before_any_password_check(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    user = _make_user(locked_until=datetime.now(UTC) + timedelta(minutes=5))
    user_repo = AsyncMock()
    user_repo.get_by_email.return_value = user
    user_repo.clear_expired_lock.return_value = False
    import hds.services.auth as auth_module

    monkeypatch.setattr(
        auth_module, "_verify_password", lambda p, h: pytest.fail("checked a locked account")
    )
    service = AuthService(user_repo, AsyncMock(), _make_config(), AsyncMock())

    with pytest.raises(AuthError) as refused:
        await service.login("test@hds.test", "anything")

    assert refused.value.code == "auth.account_locked"
    user_repo.register_failed_login.assert_not_awaited()


@pytest.mark.asyncio
async def test_login_while_locked_raises_account_locked() -> None:
    future = datetime.now(UTC) + timedelta(minutes=10)
    user = _make_user(locked_until=future)
    user_repo = AsyncMock()
    user_repo.get_by_email.return_value = user
    user_repo.clear_expired_lock.return_value = False
    token_repo = AsyncMock()

    service = AuthService(user_repo, token_repo, _make_config(), AsyncMock())

    with pytest.raises(AuthError) as exc_info:
        await service.login("test@hds.test", "any")

    assert exc_info.value.code == "auth.account_locked"


@pytest.mark.asyncio
async def test_successful_login_resets_counter() -> None:
    user = _make_user(failed_login_attempts=2)
    user_repo = AsyncMock()
    user_repo.get_by_email.return_value = user
    user_repo.get_for_auth.return_value = user
    user_repo.reset_failed_attempts = AsyncMock()
    token_repo = AsyncMock()
    token_repo.create = AsyncMock()

    service = AuthService(user_repo, token_repo, _make_config(), AsyncMock())
    import hds.services.auth as auth_module

    orig = auth_module._verify_password
    auth_module._verify_password = lambda p, h: True  # type: ignore[assignment]

    await service.login("test@hds.test", "correct")

    user_repo.reset_failed_attempts.assert_awaited_with(user.id)
    auth_module._verify_password = orig


@pytest.mark.asyncio
async def test_logout_revokes_token() -> None:
    token_repo = AsyncMock()
    stored = MagicMock(id=uuid.uuid4(), revoked_at=None)
    token_repo.get_by_hash = AsyncMock(return_value=stored)
    token_repo.revoke_if_live = AsyncMock(return_value=True)

    service = AuthService(AsyncMock(), token_repo, _make_config(), AsyncMock())
    await service.logout("raw-token-value")

    token_repo.revoke_if_live.assert_awaited_once_with(stored.id, "logout")


@pytest.mark.asyncio
async def test_logout_all_revokes_all_user_tokens() -> None:
    token_repo = AsyncMock()
    token_repo.revoke_all_for_user = AsyncMock()
    user_id = uuid.uuid4()

    service = AuthService(AsyncMock(), token_repo, _make_config(), AsyncMock())
    await service.logout_all(user_id)

    token_repo.revoke_all_for_user.assert_awaited_with(user_id, "all")


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
    token_repo.revoke_if_live = AsyncMock(return_value=True)
    token_repo.create = AsyncMock()

    service = AuthService(user_repo, token_repo, _make_config(), AsyncMock())
    result = await service.refresh("raw-token")

    token_repo.revoke_if_live.assert_awaited_once_with(old_token.id, "rotated")
    token_repo.create.assert_awaited_once()
    token_repo.get_by_hash.assert_awaited_once()  # the winner never re-reads the token
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
    return AuthService(user_repo, token_repo, _make_config(), AsyncMock()), token_repo


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


# ── Reuse of a spent refresh token: the decision table (T253, research D3) ─────────────────────


def _stored_token(
    *, reason: str | None, revoked_ago: float | None, user_id: uuid.UUID
) -> MagicMock:
    """A stored token; ``revoked_ago`` seconds ago it was revoked, or live when ``None``."""
    token = MagicMock()
    token.id = uuid.uuid4()
    token.user_id = user_id
    token.expires_at = datetime.now(UTC) + timedelta(days=1)
    token.revoked_reason = reason
    token.revoked_at = (
        None if revoked_ago is None else datetime.now(UTC) - timedelta(seconds=revoked_ago)
    )
    return token


def _reuse_service(*stored: MagicMock) -> tuple[AuthService, AsyncMock, list[str]]:
    """A service whose token repository returns ``stored`` in turn; records revoke/commit order."""
    calls: list[str] = []
    user_repo = AsyncMock()
    user_repo.get_for_auth = AsyncMock(return_value=_make_user(id=stored[0].user_id))
    token_repo = AsyncMock()
    token_repo.get_by_hash = AsyncMock(side_effect=list(stored))
    token_repo.revoke_if_live = AsyncMock(return_value=False)  # the guarded update changed no row
    token_repo.revoke_all_for_user = AsyncMock(
        side_effect=lambda *a, **k: calls.append("revoke_all")
    )
    token_repo.commit = AsyncMock(side_effect=lambda: calls.append("commit"))
    return AuthService(user_repo, token_repo, _make_config(), AsyncMock()), token_repo, calls


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("reason", "age", "ends_all_sessions"),
    [
        ("rotated", 3, False),  # inside the 10 s window: an honest retry
        ("rotated", 11, True),  # outside it: treated as a stolen copy
        ("logout", 3600, False),
        ("all", 3600, False),
        ("account", 3600, False),
        (None, 3600, False),  # revoked before reasons were recorded: unknown
    ],
)
async def test_presenting_an_already_revoked_token_follows_the_decision_table(
    reason: str | None, age: float, ends_all_sessions: bool
) -> None:
    user_id = uuid.uuid4()
    service, token_repo, calls = _reuse_service(
        _stored_token(reason=reason, revoked_ago=age, user_id=user_id)
    )

    with pytest.raises(AuthError) as exc_info:
        await service.refresh("raw-token")

    assert exc_info.value.code == "auth.token_revoked"
    token_repo.create.assert_not_awaited()  # a refused token never yields a new one
    token_repo.revoke_if_live.assert_not_awaited()
    if ends_all_sessions:
        token_repo.revoke_all_for_user.assert_awaited_once_with(user_id, "all")
        # The revocation must be committed BEFORE the refusal is raised: the request's own
        # rollback on an error would otherwise undo the one effect that matters here.
        assert calls == ["revoke_all", "commit"]
    else:
        token_repo.revoke_all_for_user.assert_not_awaited()
        token_repo.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_losing_the_race_within_the_window_is_refused_and_ends_nothing() -> None:
    user_id = uuid.uuid4()
    live = _stored_token(reason=None, revoked_ago=None, user_id=user_id)
    just_rotated = _stored_token(reason="rotated", revoked_ago=0.2, user_id=user_id)
    service, token_repo, _ = _reuse_service(live, just_rotated)  # read live, then re-read revoked

    with pytest.raises(AuthError) as exc_info:
        await service.refresh("raw-token")

    assert exc_info.value.code == "auth.token_revoked"
    token_repo.revoke_if_live.assert_awaited_once_with(live.id, "rotated")
    token_repo.create.assert_not_awaited()
    token_repo.revoke_all_for_user.assert_not_awaited()


@pytest.mark.asyncio
async def test_losing_the_race_outside_the_window_ends_every_session() -> None:
    """A slow loser (the winner rotated long ago) is indistinguishable from a replay."""
    user_id = uuid.uuid4()
    live = _stored_token(reason=None, revoked_ago=None, user_id=user_id)
    long_ago = _stored_token(reason="rotated", revoked_ago=30, user_id=user_id)
    service, token_repo, calls = _reuse_service(live, long_ago)

    with pytest.raises(AuthError):
        await service.refresh("raw-token")

    assert calls == ["revoke_all", "commit"]


@pytest.mark.asyncio
async def test_losing_the_race_when_the_token_vanished_is_an_invalid_token() -> None:
    user_id = uuid.uuid4()
    live = _stored_token(reason=None, revoked_ago=None, user_id=user_id)
    service, token_repo, _ = _reuse_service(live)
    token_repo.get_by_hash = AsyncMock(side_effect=[live, None])

    with pytest.raises(AuthError) as exc_info:
        await service.refresh("raw-token")

    assert exc_info.value.code == "auth.invalid_token"
    token_repo.revoke_all_for_user.assert_not_awaited()


@pytest.mark.asyncio
async def test_the_grace_window_comes_from_the_configuration() -> None:
    user_id = uuid.uuid4()
    stored = _stored_token(reason="rotated", revoked_ago=5, user_id=user_id)
    service, token_repo, _ = _reuse_service(stored)
    service._config.REFRESH_REUSE_GRACE_SECONDS = 3  # 5 s ago is now outside the window

    with pytest.raises(AuthError):
        await service.refresh("raw-token")

    token_repo.revoke_all_for_user.assert_awaited_once()


@pytest.mark.asyncio
async def test_logout_of_an_already_revoked_token_does_nothing() -> None:
    token_repo = AsyncMock()
    token_repo.get_by_hash = AsyncMock(
        return_value=_stored_token(reason="rotated", revoked_ago=1, user_id=uuid.uuid4())
    )
    service = AuthService(AsyncMock(), token_repo, _make_config(), AsyncMock())

    await service.logout("raw-token")

    token_repo.revoke_if_live.assert_not_awaited()
    token_repo.revoke_all_for_user.assert_not_awaited()


@pytest.mark.asyncio
async def test_expiry_is_checked_before_the_revoked_state() -> None:
    """F08: an expired rotated token is refused as expired, so it cannot end any session."""
    stored = MagicMock()
    stored.revoked_at = datetime.now(UTC) - timedelta(days=30)
    stored.revoked_reason = "rotated"
    stored.expires_at = datetime.now(UTC) - timedelta(days=23)
    token_repo = AsyncMock()
    token_repo.get_by_hash.return_value = stored
    service = AuthService(AsyncMock(), token_repo, _make_config(), AsyncMock())

    with pytest.raises(AuthError) as refused:
        await service.refresh("old-token")

    assert refused.value.code == "auth.token_expired"
    token_repo.revoke_all_for_user.assert_not_awaited()
