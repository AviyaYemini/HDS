"""Phone sessions last 30 days and web sessions 7, and a refresh keeps the session's kind.

Covers: 007 FR-010 (research R10, ADR 016, task T027). ``client_kind`` is self-declared; ADR 016
records why that is acceptable.
"""

import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from hds.core import passwords
from hds.core.deps import current_session_id
from hds.core.exceptions import ConflictError, NotFoundError
from hds.core.security import create_access_token, decode_access_token
from hds.models.user import UserRole
from hds.services.auth import AuthService
from tests.unit.test_auth_service import _make_config, _make_user, _no_counts


def _config() -> MagicMock:
    cfg = _make_config()
    cfg.REFRESH_TOKEN_TTL_DAYS_NATIVE = 30
    return cfg


def _signed_in_service() -> tuple[AuthService, AsyncMock]:
    user = _make_user()
    user_repo = AsyncMock()
    user_repo.get_by_email.return_value = user
    user_repo.get_for_auth.return_value = user
    token_repo = AsyncMock()
    return AuthService(user_repo, token_repo, _config(), AsyncMock(), _no_counts()), token_repo


def _lifetime(token_repo: AsyncMock) -> tuple[timedelta, str]:
    args = token_repo.create.await_args.args
    return args[2] - datetime.now(UTC), args[3]


@pytest.mark.asyncio
async def test_a_web_sign_in_without_client_kind_gets_seven_days() -> None:
    service, tokens = _signed_in_service()
    with patch.object(passwords, "verify_password", AsyncMock(return_value=True)):
        await service.login("test@hds.test", "password")
    left, kind = _lifetime(tokens)
    assert kind == "web"
    assert timedelta(days=6, hours=23) < left <= timedelta(days=7)


@pytest.mark.asyncio
async def test_a_native_sign_in_gets_thirty_days() -> None:
    service, tokens = _signed_in_service()
    with patch.object(passwords, "verify_password", AsyncMock(return_value=True)):
        await service.login("test@hds.test", "password", client_kind="native")
    left, kind = _lifetime(tokens)
    assert kind == "native"
    assert timedelta(days=29, hours=23) < left <= timedelta(days=30)


@pytest.mark.asyncio
@pytest.mark.parametrize(("kind", "days"), [("native", 30), ("web", 7)])
async def test_a_refresh_keeps_the_sessions_kind_and_renews_its_lifetime(
    kind: str, days: int
) -> None:
    user_id = uuid.uuid4()
    old = MagicMock()
    old.id, old.user_id, old.revoked_at, old.client_kind = uuid.uuid4(), user_id, None, kind
    old.expires_at = datetime.now(UTC) + timedelta(hours=1)
    user_repo = AsyncMock()
    user_repo.get_for_auth = AsyncMock(return_value=_make_user(id=user_id))
    tokens = AsyncMock()
    tokens.get_by_hash = AsyncMock(return_value=old)
    tokens.revoke_if_live = AsyncMock(return_value=True)
    service = AuthService(user_repo, tokens, _config(), AsyncMock(), _no_counts())

    await service.refresh("raw-token")

    left, new_kind = _lifetime(tokens)
    assert new_kind == kind
    assert timedelta(days=days) - timedelta(minutes=1) < left <= timedelta(days=days)


@pytest.mark.asyncio
async def test_ending_a_session_that_is_not_the_callers_is_not_found() -> None:
    tokens = AsyncMock()
    tokens.revoke_for_user = AsyncMock(return_value=False)
    service = AuthService(AsyncMock(), tokens, _config(), AsyncMock(), _no_counts())
    with pytest.raises(NotFoundError):
        await service.end_session(uuid.uuid4(), uuid.uuid4())


@pytest.mark.asyncio
async def test_ending_ones_own_session_revokes_it_as_a_sign_out() -> None:
    tokens = AsyncMock()
    tokens.revoke_for_user = AsyncMock(return_value=True)
    service = AuthService(AsyncMock(), tokens, _config(), AsyncMock(), _no_counts())
    user_id, session_id = uuid.uuid4(), uuid.uuid4()
    await service.end_session(user_id, session_id)
    tokens.revoke_for_user.assert_awaited_once_with(session_id, user_id, "logout")


def _sid(access_token: str) -> str:
    secret = _config().JWT_SECRET_KEY.get_secret_value()
    return str(decode_access_token(access_token, secret=secret)["sid"])


@pytest.mark.asyncio
async def test_the_access_token_names_the_session_it_was_issued_with() -> None:
    """007 T039: the sid claim is the id of the refresh token stored with it."""
    service, tokens = _signed_in_service()
    row = MagicMock(id=uuid.uuid4())
    tokens.create.return_value = row
    with patch.object(passwords, "verify_password", AsyncMock(return_value=True)):
        answer = await service.login("test@hds.test", "password")
    assert _sid(answer.access_token) == str(row.id)


def test_the_session_id_is_read_from_the_token_and_absent_without_one() -> None:
    secret = _config().JWT_SECRET_KEY.get_secret_value()
    sid = uuid.uuid4()
    with patch("hds.core.deps.get_settings") as settings:
        settings.return_value.JWT_SECRET_KEY.get_secret_value.return_value = secret
        settings.return_value.JWT_ALGORITHM = "HS256"
        user, org = uuid.uuid4(), uuid.uuid4()
        marked = create_access_token(user, org, UserRole.employee, 60, secret, sid=sid)
        unmarked = create_access_token(user, org, UserRole.employee, 60, secret)
        assert current_session_id(marked) == sid
        assert current_session_id(unmarked) is None
        assert current_session_id(None) is None
        assert current_session_id("not a token") is None


def _live(*ids: uuid.UUID) -> list[MagicMock]:
    return [MagicMock(id=i) for i in ids]


@pytest.mark.asyncio
async def test_signing_out_of_other_devices_keeps_the_callers_session() -> None:
    """007 T039: every other live session ends; the caller's own stays."""
    service, tokens = _signed_in_service()
    mine, other = uuid.uuid4(), uuid.uuid4()
    tokens.live_for_user.return_value = _live(mine, other)
    tokens.revoke_others_for_user.return_value = 1
    assert await service.end_other_sessions(uuid.uuid4(), mine) == 1
    args = tokens.revoke_others_for_user.await_args.args
    assert args[1] == mine


@pytest.mark.asyncio
@pytest.mark.parametrize("current", [None, "revoked"])
async def test_signing_out_of_other_devices_ends_nothing_when_the_session_is_unknown(
    current: str | None,
) -> None:
    """No sid (a token from before the marker), or a session no longer live: 409, nothing ends."""
    service, tokens = _signed_in_service()
    tokens.live_for_user.return_value = _live(uuid.uuid4(), uuid.uuid4())
    sid = None if current is None else uuid.uuid4()
    with pytest.raises(ConflictError) as refused:
        await service.end_other_sessions(uuid.uuid4(), sid)
    assert refused.value.code == "session.current_unknown"
    tokens.revoke_others_for_user.assert_not_called()
    tokens.revoke_all_for_user.assert_not_called()
