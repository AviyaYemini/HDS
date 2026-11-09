"""Unit tests for InvitationService (T099).

Covers: FR-006, FR-224, FR-228, FR-247, NFR-005.
"""

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from hds.core.exceptions import AuthError, ConflictError, ForbiddenError
from hds.models.user import User, UserRole
from hds.services.invitation import InvitationService
from hds.services.throttle import hash_key


def _make_user(**kw: object) -> MagicMock:
    u = MagicMock(spec=User)
    u.id = kw.get("id", uuid.uuid4())
    u.organisation_id = kw.get("org_id", uuid.uuid4())
    u.email = kw.get("email", "new@hds.test")
    u.role = kw.get("role", UserRole.employee)
    u.is_active = kw.get("is_active", False)
    # An active account always has an activation time; a pending one has none.
    u.activated_at = kw.get("activated_at", datetime.now(UTC) if u.is_active else None)
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
    config = MagicMock(
        RESET_REQUEST_LIMIT_PER_ADDRESS=0,
        LOGIN_THROTTLE_RETENTION_HOURS=24,
        throttle_key=b"k" * 32,
    )
    svc = InvitationService(user_repo, token_repo, mailer, refresh_repo, AsyncMock(), config)
    return svc, user_repo, token_repo, mailer


@pytest.mark.asyncio
async def test_create_invitation_mails_token_and_returns_nothing() -> None:
    svc, user_repo, token_repo, mailer = _svc()
    manager = _make_user(role=UserRole.manager, is_active=True)
    user_repo.get_by_email = AsyncMock(return_value=None)
    user_repo.create_if_absent = AsyncMock(return_value=(_make_user(is_active=False), True))

    await svc.create_invitation(manager, "new@hds.test", UserRole.employee)

    token_repo.create.assert_awaited_once()
    assert token_repo.create.await_args.kwargs["purpose"] == "invite"
    mailer.send.assert_awaited_once()
    assert mailer.send.await_args.kwargs["to"] == "new@hds.test"
    created = user_repo.create_if_absent.await_args.args[1]
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
    manager = _make_user(role=UserRole.manager, is_active=True)
    pending = _make_user(is_active=False, org_id=manager.organisation_id)
    user_repo.get_by_email = AsyncMock(return_value=pending)

    await svc.create_invitation(manager, "a@hds.test", UserRole.employee)

    user_repo.create_if_absent.assert_not_awaited()
    assert token_repo.create.await_args.kwargs["user_id"] == pending.id


@pytest.mark.asyncio
async def test_create_invitation_client_role_rejected() -> None:
    svc, user_repo, _, _ = _svc()

    with pytest.raises(ForbiddenError) as exc_info:
        await svc.create_invitation(_make_user(role=UserRole.admin), "a@hds.test", UserRole.client)

    assert exc_info.value.code == "users.role_invalid"
    user_repo.create_if_absent.assert_not_awaited()


@pytest.mark.asyncio
async def test_manager_cannot_invite_admin() -> None:
    svc, user_repo, _, _ = _svc()

    with pytest.raises(ForbiddenError) as exc_info:
        await svc.create_invitation(_make_user(role=UserRole.manager), "a@hds.test", UserRole.admin)

    assert exc_info.value.code == "users.admin_required"
    user_repo.create_if_absent.assert_not_awaited()


@pytest.mark.asyncio
async def test_admin_can_invite_admin() -> None:
    svc, user_repo, _, mailer = _svc()
    user_repo.get_by_email = AsyncMock(return_value=None)
    user_repo.create_if_absent = AsyncMock(return_value=(_make_user(), True))

    await svc.create_invitation(_make_user(role=UserRole.admin), "a@hds.test", UserRole.admin)

    mailer.send.assert_awaited_once()


@pytest.mark.asyncio
async def test_employee_cannot_invite() -> None:
    svc, user_repo, _, _ = _svc()

    with pytest.raises(ForbiddenError):
        await svc.create_invitation(
            _make_user(role=UserRole.employee), "a@hds.test", UserRole.employee
        )

    user_repo.create_if_absent.assert_not_awaited()


@pytest.mark.asyncio
async def test_accept_invitation_within_window_activates_user() -> None:
    svc, user_repo, token_repo, _ = _svc()
    user = _make_user(is_active=False)
    token = _make_token(user_id=user.id)
    token_repo.get_by_hash = AsyncMock(return_value=token)
    user_repo.get_for_auth = AsyncMock(return_value=user)
    user_repo.update = AsyncMock(return_value=_make_user(is_active=True))

    await svc.accept_invitation("valid-raw-token", "NewPass123456!")

    kwargs = user_repo.update.await_args.kwargs
    assert kwargs["is_active"] is True
    assert isinstance(kwargs["activated_at"], datetime)
    assert kwargs["password_hash"].startswith("$argon2id$")
    token_repo.spend.assert_awaited_once_with(token.id)
    token_repo.cancel_for_user.assert_awaited_once_with(user.id)  # every other link dies too


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
async def test_issuing_a_reset_link_unknown_email_is_silent() -> None:
    svc, user_repo, token_repo, mailer = _svc()
    user_repo.get_by_email = AsyncMock(return_value=None)

    await svc.issue_reset_link("unknown@hds.test")

    token_repo.create.assert_not_awaited()
    mailer.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_issuing_a_reset_link_inactive_user_is_silent() -> None:
    svc, user_repo, token_repo, mailer = _svc()
    user_repo.get_by_email = AsyncMock(return_value=_make_user(is_active=False))

    await svc.issue_reset_link("pending@hds.test")

    token_repo.create.assert_not_awaited()
    mailer.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_issuing_a_reset_link_known_user_mails_token() -> None:
    svc, user_repo, token_repo, mailer = _svc()
    user = _make_user(is_active=True)
    user_repo.get_by_email = AsyncMock(return_value=user)

    await svc.issue_reset_link(user.email)

    assert token_repo.create.await_args.kwargs["purpose"] == "reset"
    assert token_repo.create.await_args.kwargs["user_id"] == user.id
    mailer.send.assert_awaited_once()


@pytest.mark.asyncio
async def test_reset_password_valid_token_succeeds() -> None:
    svc, user_repo, token_repo, _ = _svc()
    user = _make_user(is_active=True)
    token = _make_token(user_id=user.id, purpose="reset")
    token_repo.get_by_hash = AsyncMock(return_value=token)
    user_repo.get_for_auth = AsyncMock(return_value=user)
    user_repo.update = AsyncMock(return_value=user)

    await svc.reset_password("valid", "NewPass123456!")

    user_repo.update.assert_awaited_once()
    token_repo.spend.assert_awaited_once_with(token.id)
    cast(Any, svc._refresh_token_repo).revoke_all_for_user.assert_awaited_once_with(user.id, "all")
    token_repo.cancel_for_user.assert_awaited_once_with(user.id)  # every purpose
    clear = cast(Any, svc._throttle_repo).clear
    clear.assert_awaited_once()
    assert clear.await_args.args[0] == "address"


@pytest.mark.asyncio
async def test_issuing_a_reset_link_cancels_nothing() -> None:
    svc, user_repo, token_repo, _ = _svc()
    user = _make_user(is_active=True)
    user_repo.get_by_email = AsyncMock(return_value=user)

    await svc.issue_reset_link(user.email)

    token_repo.create.assert_awaited_once()
    token_repo.cancel_for_user.assert_not_awaited()


@pytest.mark.asyncio
async def test_issuing_a_reset_link_takes_the_users_row_before_the_link() -> None:
    svc, user_repo, token_repo, _ = _svc()
    user = _make_user(is_active=True)
    order: list[str] = []
    user_repo.get_by_email = AsyncMock(return_value=user)
    user_repo.lock_user.side_effect = lambda *_: order.append("lock")
    token_repo.create.side_effect = lambda **_: order.append("link")

    await svc.issue_reset_link(user.email)

    assert order == ["lock", "link"]


@pytest.mark.asyncio
async def test_reset_password_invalid_token_revokes_nothing() -> None:
    svc, _, token_repo, _ = _svc()
    token_repo.get_by_hash = AsyncMock(return_value=None)

    with pytest.raises(AuthError):
        await svc.reset_password("bad", "NewPass123456!")

    cast(Any, svc._refresh_token_repo).revoke_all_for_user.assert_not_awaited()


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


@pytest.mark.asyncio
async def test_create_invitation_passes_the_token_as_secret_with_purpose_and_expiry() -> None:
    """The sender can only redact a token it is told about (FR-203)."""
    svc, user_repo, token_repo, mailer = _svc()
    manager = _make_user(role=UserRole.manager, is_active=True)
    user_repo.get_by_email = AsyncMock(return_value=None)
    user_repo.create_if_absent = AsyncMock(return_value=(_make_user(is_active=False), True))

    await svc.create_invitation(manager, "new@hds.test", UserRole.employee)

    sent = mailer.send.await_args.kwargs
    assert sent["secret"] and sent["secret"] in sent["body"]
    assert sent["purpose"] == "invite"
    assert sent["expires_at"] == token_repo.create.await_args.kwargs["expires_at"]


@pytest.mark.asyncio
async def test_issuing_a_reset_link_passes_the_token_as_secret_with_purpose_and_expiry() -> None:
    svc, user_repo, token_repo, mailer = _svc()
    user = _make_user(is_active=True)
    user_repo.get_by_email = AsyncMock(return_value=user)

    await svc.issue_reset_link(user.email)

    sent = mailer.send.await_args.kwargs
    assert sent["secret"] and sent["secret"] in sent["body"]
    assert sent["purpose"] == "reset"
    assert sent["expires_at"] == token_repo.create.await_args.kwargs["expires_at"]


# ── Pending-account rules (T233, spec Story 2) ─────────────────────────────────────────────────


def _manager() -> MagicMock:
    return _make_user(role=UserRole.manager, is_active=True)


@pytest.mark.asyncio
async def test_inviting_a_deactivated_account_is_refused_and_sends_nothing() -> None:
    svc, user_repo, token_repo, mailer = _svc()
    manager = _manager()
    deactivated = _make_user(
        is_active=False, org_id=manager.organisation_id, activated_at=datetime.now(UTC)
    )
    user_repo.get_by_email = AsyncMock(return_value=deactivated)

    with pytest.raises(ConflictError) as exc_info:
        await svc.create_invitation(manager, "gone@hds.test", UserRole.employee)

    assert exc_info.value.code == "invitations.account_deactivated"
    token_repo.create.assert_not_awaited()
    token_repo.cancel_for_user.assert_not_awaited()
    mailer.send.assert_not_awaited()
    user_repo.update.assert_not_awaited()


@pytest.mark.asyncio
async def test_re_inviting_a_pending_account_sets_the_new_role_and_replaces_the_link() -> None:
    svc, user_repo, token_repo, mailer = _svc()
    manager = _manager()
    pending = _make_user(is_active=False, role=UserRole.employee, org_id=manager.organisation_id)
    user_repo.get_by_email = AsyncMock(return_value=pending)

    await svc.create_invitation(manager, "p@hds.test", UserRole.manager)

    user_repo.update.assert_awaited_once_with(
        pending.id, pending.organisation_id, role=UserRole.manager
    )
    token_repo.cancel_for_user.assert_awaited_once_with(pending.id)  # the earlier link is replaced
    token_repo.create.assert_awaited_once()
    mailer.send.assert_awaited_once()


@pytest.mark.asyncio
async def test_re_inviting_with_the_same_role_does_not_rewrite_the_account() -> None:
    svc, user_repo, token_repo, _ = _svc()
    manager = _manager()
    pending = _make_user(is_active=False, role=UserRole.employee, org_id=manager.organisation_id)
    user_repo.get_by_email = AsyncMock(return_value=pending)

    await svc.create_invitation(manager, "p@hds.test", UserRole.employee)

    user_repo.update.assert_not_awaited()
    token_repo.cancel_for_user.assert_awaited_once_with(pending.id)


@pytest.mark.asyncio
async def test_a_manager_cannot_re_invite_a_pending_account_as_admin() -> None:
    svc, user_repo, token_repo, mailer = _svc()
    manager = _manager()
    pending = _make_user(is_active=False, role=UserRole.employee, org_id=manager.organisation_id)
    user_repo.get_by_email = AsyncMock(return_value=pending)

    with pytest.raises(ForbiddenError) as exc_info:
        await svc.create_invitation(manager, "p@hds.test", UserRole.admin)

    assert exc_info.value.code == "users.admin_required"
    user_repo.update.assert_not_awaited()  # the pending role is unchanged
    token_repo.create.assert_not_awaited()
    mailer.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_manager_cannot_change_the_role_of_a_pending_admin() -> None:
    """Only an Admin may change an Admin (FR-011a), including one who has not accepted yet."""
    svc, user_repo, token_repo, _ = _svc()
    manager = _manager()
    pending_admin = _make_user(is_active=False, role=UserRole.admin, org_id=manager.organisation_id)
    user_repo.get_by_email = AsyncMock(return_value=pending_admin)

    with pytest.raises(ForbiddenError) as exc_info:
        await svc.create_invitation(manager, "a@hds.test", UserRole.employee)

    assert exc_info.value.code == "users.admin_required"
    user_repo.update.assert_not_awaited()
    token_repo.create.assert_not_awaited()


@pytest.mark.asyncio
async def test_an_address_in_another_organisation_is_a_duplicate_and_nothing_changes() -> None:
    svc, user_repo, token_repo, mailer = _svc()
    other_org_pending = _make_user(is_active=False)  # a different organisation than the manager's
    user_repo.get_by_email = AsyncMock(return_value=other_org_pending)

    with pytest.raises(ConflictError) as exc_info:
        await svc.create_invitation(_manager(), "x@hds.test", UserRole.employee)

    assert exc_info.value.code == "users.duplicate_email"
    user_repo.update.assert_not_awaited()
    token_repo.cancel_for_user.assert_not_awaited()
    token_repo.create.assert_not_awaited()
    mailer.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_losing_the_race_to_create_the_account_continues_as_a_re_invitation() -> None:
    """If another request created the pending account first, this one re-invites it (Story 2.7)."""
    svc, user_repo, token_repo, mailer = _svc()
    manager = _manager()
    winner_created = _make_user(
        is_active=False, role=UserRole.employee, org_id=manager.organisation_id
    )
    user_repo.get_by_email = AsyncMock(side_effect=[None, winner_created])
    user_repo.create_if_absent = AsyncMock(return_value=(winner_created, False))

    await svc.create_invitation(manager, "race@hds.test", UserRole.employee)

    token_repo.cancel_for_user.assert_awaited_once_with(winner_created.id)
    token_repo.create.assert_awaited_once()
    mailer.send.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["deactivated", "active"])
async def test_accepting_for_an_account_that_is_not_pending_is_refused_as_used(state: str) -> None:
    svc, user_repo, token_repo, _ = _svc()
    owner = _make_user(is_active=(state == "active"), activated_at=datetime.now(UTC))
    token = _make_token(user_id=owner.id)
    token_repo.get_by_hash = AsyncMock(return_value=token)
    user_repo.get_for_auth = AsyncMock(return_value=owner)

    with pytest.raises(AuthError) as exc_info:
        await svc.accept_invitation("leftover", "Pass123456789!")

    assert exc_info.value.code == "auth.token_used"
    user_repo.update.assert_not_awaited()  # no password change, no reactivation
    cast(Any, svc._refresh_token_repo).revoke_all_for_user.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_reset_link_is_refused_for_an_account_that_is_not_active() -> None:
    svc, user_repo, token_repo, _ = _svc()
    owner = _make_user(is_active=False, activated_at=datetime.now(UTC))  # deactivated
    token = _make_token(user_id=owner.id, purpose="reset")
    token_repo.get_by_hash = AsyncMock(return_value=token)
    user_repo.get_for_auth = AsyncMock(return_value=owner)

    with pytest.raises(AuthError) as exc_info:
        await svc.reset_password("leftover", "Pass123456789!")

    assert exc_info.value.code == "auth.token_used"
    user_repo.update.assert_not_awaited()
    cast(Any, svc._refresh_token_repo).revoke_all_for_user.assert_not_awaited()


# ── Single use (T253, T261, FR-208) ─────────────────────────────────────────────────────────────


@pytest.mark.asyncio
@pytest.mark.parametrize("purpose", ["invite", "reset"])
async def test_losing_the_race_for_a_link_is_refused_before_anything_is_written(
    purpose: str,
) -> None:
    """``spend`` returns false when another request used the link first; no write may follow."""
    svc, user_repo, token_repo, _ = _svc()
    token = _make_token(purpose=purpose)
    token_repo.get_by_hash = AsyncMock(return_value=token)
    token_repo.spend = AsyncMock(return_value=False)

    with pytest.raises(AuthError) as exc_info:
        if purpose == "invite":
            await svc.accept_invitation("raw", "Pass123456789!")
        else:
            await svc.reset_password("raw", "Pass123456789!")

    assert exc_info.value.code == "auth.token_used"
    user_repo.update.assert_not_awaited()
    token_repo.cancel_for_user.assert_not_awaited()
    cast(Any, svc._refresh_token_repo).revoke_all_for_user.assert_not_awaited()


def _inviter() -> MagicMock:
    actor = MagicMock(spec=User)
    actor.role = UserRole.admin
    actor.organisation_id = uuid.uuid4()
    return actor


@pytest.mark.asyncio
async def test_a_re_invitation_locks_the_user_before_touching_its_links() -> None:
    """The user's row comes first, or accepting the old link can deadlock with this path."""
    svc, user_repo, token_repo, _ = _svc()
    actor = _inviter()
    pending = _make_user(org_id=actor.organisation_id, is_active=False)
    order: list[str] = []
    user_repo.get_by_email = AsyncMock(return_value=pending)
    user_repo.lock_user.side_effect = lambda *_: order.append("lock")
    token_repo.cancel_for_user.side_effect = lambda *_: order.append("cancel")

    await svc.create_invitation(actor, pending.email, pending.role)

    assert order == ["lock", "cancel"]


@pytest.mark.asyncio
async def test_a_re_invitation_judges_the_account_as_it_is_after_the_lock() -> None:
    """An invitee who accepted while this request waited is active now, so this is a duplicate."""
    svc, user_repo, token_repo, _ = _svc()
    actor = _inviter()
    pending = _make_user(org_id=actor.organisation_id, is_active=False)
    accepted = _make_user(org_id=actor.organisation_id, is_active=True, email=pending.email)
    user_repo.get_by_email = AsyncMock(side_effect=[pending, accepted])

    with pytest.raises(ConflictError) as refused:
        await svc.create_invitation(actor, pending.email, pending.role)

    assert refused.value.code == "users.duplicate_email"
    token_repo.cancel_for_user.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_re_invitation_is_refused_if_the_account_was_deleted_during_the_wait() -> None:
    """Otherwise the invitee would be mailed a link that can never be accepted."""
    svc, user_repo, token_repo, mailer = _svc()
    actor = _inviter()
    pending = _make_user(org_id=actor.organisation_id, is_active=False)
    user_repo.get_by_email = AsyncMock(side_effect=[pending, None])

    with pytest.raises(ConflictError) as refused:
        await svc.create_invitation(actor, pending.email, pending.role)

    assert refused.value.code == "users.duplicate_email"
    token_repo.create.assert_not_awaited()
    mailer.send.assert_not_awaited()


@pytest.mark.asyncio
async def test_a_reset_request_is_counted_under_the_folded_address() -> None:
    throttle_repo = AsyncMock()
    throttle_repo.register_failure = AsyncMock(return_value=(1, None))
    config = MagicMock(
        RESET_REQUEST_LIMIT_PER_ADDRESS=5,
        RESET_REQUEST_LIMIT_PER_NETWORK=0,
        LOGIN_THROTTLE_RETENTION_HOURS=24,
        throttle_key=b"k" * 32,
    )
    user_repo = AsyncMock()
    user_repo.get_by_email = AsyncMock(return_value=None)
    svc = InvitationService(user_repo, AsyncMock(), AsyncMock(), AsyncMock(), throttle_repo, config)

    await svc.request_password_reset(" Someone@HDS.test ", "203.0.113.9", lambda _: None)

    key = throttle_repo.register_failure.await_args.args[1]
    assert key == hash_key("someone@hds.test", config.throttle_key)


def _limited() -> tuple[InvitationService, AsyncMock, AsyncMock, AsyncMock, AsyncMock]:
    """Build a service with the reset limit on, returning its repositories and mailer."""
    user_repo, token_repo, mailer, throttle_repo = (AsyncMock() for _ in range(4))
    throttle_repo.register_failure = AsyncMock(return_value=(1, None))
    config = MagicMock(
        RESET_REQUEST_LIMIT_PER_ADDRESS=5,
        RESET_REQUEST_LIMIT_PER_NETWORK=0,
        LOGIN_THROTTLE_RETENTION_HOURS=24,
        throttle_key=b"k" * 32,
    )
    svc = InvitationService(user_repo, token_repo, mailer, AsyncMock(), throttle_repo, config)
    return svc, user_repo, token_repo, mailer, throttle_repo


@pytest.mark.asyncio
async def test_a_reset_request_touches_no_account_and_queues_one_job_for_the_address() -> None:
    svc, user_repo, token_repo, mailer, _ = _limited()
    queued: list[str] = []

    await svc.request_password_reset("someone@hds.test", "203.0.113.9", queued.append)

    assert queued == ["someone@hds.test"]
    assert user_repo.mock_calls == []
    assert token_repo.mock_calls == []
    assert mailer.mock_calls == []


@pytest.mark.asyncio
async def test_a_reset_request_over_the_address_limit_queues_nothing() -> None:
    svc, _, _, _, throttle_repo = _limited()
    throttle_repo.register_failure = AsyncMock(return_value=(6, None))
    queued: list[str] = []

    await svc.request_password_reset("someone@hds.test", "203.0.113.9", queued.append)

    assert queued == []


@pytest.mark.asyncio
async def test_a_reset_request_removes_stale_rows_before_it_takes_a_count_lock() -> None:
    svc, _, _, _, throttle_repo = _limited()
    svc._config.RESET_REQUEST_LIMIT_PER_NETWORK = 30
    throttle_repo.get = AsyncMock(return_value=None)
    order: list[str] = []
    throttle_repo.purge_stale.side_effect = lambda *_: order.append("purge")

    async def count(*_: object) -> tuple[int, None]:
        order.append("count")
        return 1, None

    throttle_repo.register_failure = AsyncMock(side_effect=count)

    await svc.request_password_reset("someone@hds.test", "203.0.113.9", lambda _: None)

    assert order == ["purge", "count", "count"]


@pytest.mark.parametrize("after_the_lock", [None, _make_user(is_active=False)])
@pytest.mark.asyncio
async def test_a_reset_job_stops_if_the_account_changed_while_it_waited_for_the_lock(
    after_the_lock: MagicMock | None,
) -> None:
    svc, user_repo, token_repo, mailer, _ = _limited()
    user_repo.get_by_email = AsyncMock(side_effect=[_make_user(is_active=True), after_the_lock])

    await svc.issue_reset_link("someone@hds.test")

    token_repo.create.assert_not_awaited()
    mailer.send.assert_not_awaited()
