"""Unit tests for InvitationService (T099).

Covers: FR-006, NFR-005.
"""

import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest

from hds.core.exceptions import AuthError, ConflictError, ForbiddenError
from hds.models.user import User, UserRole
from hds.services.invitation import InvitationService


def _make_user(**kw: object) -> MagicMock:
    u = MagicMock(spec=User)
    u.id = kw.get("id", uuid.uuid4())
    u.organization_id = kw.get("org_id", uuid.uuid4())
    u.email = kw.get("email", "new@hds.test")
    u.role = kw.get("role", UserRole.employee)
    u.is_active = kw.get("is_active", False)
    return u


def _make_token(**kw: object) -> MagicMock:
    t = MagicMock()
    t.id = kw.get("id", uuid.uuid4())
    t.user_id = kw.get("user_id", uuid.uuid4())
    t.purpose = kw.get("purpose", "invite")
    t.used_at = kw.get("used_at", None)
    t.expires_at = kw.get("expires_at", datetime.now(UTC) + timedelta(days=1))
    return t


def _svc() -> tuple[InvitationService, AsyncMock, AsyncMock, AsyncMock]:
    user_repo = AsyncMock()
    token_repo = AsyncMock()
    mailer = AsyncMock()
    refresh_repo = AsyncMock()
    svc = InvitationService(user_repo, token_repo, mailer, refresh_repo)
    return svc, user_repo, token_repo, mailer


@pytest.mark.asyncio
async def test_create_invitation_mails_token_and_returns_nothing() -> None:
    svc, user_repo, token_repo, mailer = _svc()
    manager = _make_user(role=UserRole.manager, is_active=True)
    user_repo.get_by_email = AsyncMock(return_value=None)
    user_repo.create = AsyncMock(return_value=_make_user(is_active=False))

    result = await svc.create_invitation(manager, "new@hds.test", UserRole.employee)

    assert result is None
    token_repo.create.assert_awaited_once()
    assert token_repo.create.await_args.kwargs["purpose"] == "invite"
    mailer.send.assert_awaited_once()
    assert mailer.send.await_args.kwargs["to"] == "new@hds.test"
    created = user_repo.create.await_args.args[1]
    assert created["is_active"] is False


@pytest.mark.asyncio
async def test_create_invitation_for_active_user_conflicts() -> None:
    svc, user_repo, token_repo, mailer = _svc()
    user_repo.get_by_email = AsyncMock(return_value=_make_user(is_active=True))

    with pytest.raises(ConflictError) as exc_info:
        await svc.create_invitation(
            _make_user(role=UserRole.manager), "a@hds.test", UserRole.employee
        )

    assert exc_info.value.code == "users.duplicate_email"
    mailer.send.assert_not_awaited()
    token_repo.create.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_invitation_reuses_pending_user() -> None:
    svc, user_repo, token_repo, _ = _svc()
    pending = _make_user(is_active=False)
    user_repo.get_by_email = AsyncMock(return_value=pending)

    await svc.create_invitation(_make_user(role=UserRole.manager), "a@hds.test", UserRole.employee)

    user_repo.create.assert_not_awaited()
    assert token_repo.create.await_args.kwargs["user_id"] == pending.id


@pytest.mark.asyncio
async def test_create_invitation_client_role_rejected() -> None:
    svc, user_repo, _, _ = _svc()

    with pytest.raises(ForbiddenError) as exc_info:
        await svc.create_invitation(_make_user(role=UserRole.admin), "a@hds.test", UserRole.client)

    assert exc_info.value.code == "users.role_invalid"
    user_repo.create.assert_not_awaited()


@pytest.mark.asyncio
async def test_manager_cannot_invite_admin() -> None:
    svc, user_repo, _, _ = _svc()

    with pytest.raises(ForbiddenError) as exc_info:
        await svc.create_invitation(_make_user(role=UserRole.manager), "a@hds.test", UserRole.admin)

    assert exc_info.value.code == "users.admin_required"
    user_repo.create.assert_not_awaited()


@pytest.mark.asyncio
async def test_admin_can_invite_admin() -> None:
    svc, user_repo, _, mailer = _svc()
    user_repo.get_by_email = AsyncMock(return_value=None)
    user_repo.create = AsyncMock(return_value=_make_user())

    await svc.create_invitation(_make_user(role=UserRole.admin), "a@hds.test", UserRole.admin)

    mailer.send.assert_awaited_once()


@pytest.mark.asyncio
async def test_employee_cannot_invite() -> None:
    svc, user_repo, _, _ = _svc()

    with pytest.raises(ForbiddenError):
        await svc.create_invitation(
            _make_user(role=UserRole.employee), "a@hds.test", UserRole.employee
        )

    user_repo.create.assert_not_awaited()


@pytest.mark.asyncio
async def test_accept_invitation_within_window_activates_user() -> None:
    svc, user_repo, token_repo, _ = _svc()
    user = _make_user(is_active=False)
    token = _make_token(user_id=user.id)
    token_repo.get_by_hash = AsyncMock(return_value=token)
    user_repo.update = AsyncMock(return_value=_make_user(is_active=True))

    await svc.accept_invitation("valid-raw-token", "NewPass123456!")

    kwargs = user_repo.update.await_args.kwargs
    assert kwargs["is_active"] is True
    assert kwargs["password_hash"].startswith("$argon2id$")
    token_repo.mark_used.assert_awaited_once_with(token.id)


@pytest.mark.asyncio
async def test_accept_invitation_after_expiry_raises_auth_error() -> None:
    svc, user_repo, token_repo, _ = _svc()
    token_repo.get_by_hash = AsyncMock(
        return_value=_make_token(expires_at=datetime.now(UTC) - timedelta(hours=1))
    )

    with pytest.raises(AuthError) as exc_info:
        await svc.accept_invitation("expired", "Pass123456789!")

    assert exc_info.value.code == "auth.token_expired"
    user_repo.update.assert_not_awaited()


@pytest.mark.asyncio
async def test_accept_invitation_second_use_raises_token_used() -> None:
    svc, _, token_repo, _ = _svc()
    token_repo.get_by_hash = AsyncMock(return_value=_make_token(used_at=datetime.now(UTC)))

    with pytest.raises(AuthError) as exc_info:
        await svc.accept_invitation("used", "Pass123456789!")

    assert exc_info.value.code == "auth.token_used"


@pytest.mark.asyncio
async def test_reset_token_cannot_be_used_as_invitation() -> None:
    svc, user_repo, token_repo, _ = _svc()
    token_repo.get_by_hash = AsyncMock(return_value=_make_token(purpose="reset"))

    with pytest.raises(AuthError):
        await svc.accept_invitation("raw", "Pass123456789!")

    user_repo.update.assert_not_awaited()


@pytest.mark.asyncio
async def test_request_password_reset_unknown_email_is_silent() -> None:
    svc, user_repo, token_repo, mailer = _svc()
    user_repo.get_by_email = AsyncMock(return_value=None)

    result = await svc.request_password_reset("unknown@hds.test")

    assert result is None
    token_repo.create.assert_not_awaited()
    mailer.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_request_password_reset_inactive_user_is_silent() -> None:
    svc, user_repo, token_repo, mailer = _svc()
    user_repo.get_by_email = AsyncMock(return_value=_make_user(is_active=False))

    await svc.request_password_reset("pending@hds.test")

    token_repo.create.assert_not_awaited()
    mailer.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_request_password_reset_known_user_mails_token() -> None:
    svc, user_repo, token_repo, mailer = _svc()
    user = _make_user(is_active=True)
    user_repo.get_by_email = AsyncMock(return_value=user)

    await svc.request_password_reset(user.email)

    assert token_repo.create.await_args.kwargs["purpose"] == "reset"
    assert token_repo.create.await_args.kwargs["user_id"] == user.id
    mailer.send.assert_awaited_once()


@pytest.mark.asyncio
async def test_reset_password_valid_token_succeeds() -> None:
    svc, user_repo, token_repo, _ = _svc()
    user = _make_user()
    token = _make_token(user_id=user.id, purpose="reset")
    token_repo.get_by_hash = AsyncMock(return_value=token)
    user_repo.update = AsyncMock(return_value=user)

    await svc.reset_password("valid", "NewPass123456!")

    user_repo.update.assert_awaited_once()
    token_repo.mark_used.assert_awaited_once_with(token.id)
    svc._refresh_token_repo.revoke_all_for_user.assert_awaited_once_with(user.id)


@pytest.mark.asyncio
async def test_reset_password_invalid_token_revokes_nothing() -> None:
    svc, _, token_repo, _ = _svc()
    token_repo.get_by_hash = AsyncMock(return_value=None)

    with pytest.raises(AuthError):
        await svc.reset_password("bad", "NewPass123456!")

    svc._refresh_token_repo.revoke_all_for_user.assert_not_awaited()


@pytest.mark.asyncio
async def test_reset_password_expired_token_raises() -> None:
    svc, user_repo, token_repo, _ = _svc()
    token_repo.get_by_hash = AsyncMock(
        return_value=_make_token(purpose="reset", expires_at=datetime.now(UTC) - timedelta(hours=2))
    )

    with pytest.raises(AuthError) as exc_info:
        await svc.reset_password("expired", "Pass123456789!")

    assert exc_info.value.code == "auth.token_expired"
    user_repo.update.assert_not_awaited()
