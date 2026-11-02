"""Invitation and password-reset service (US6 — Proposed).

Layer: services. May import ``hds.repositories``, ``hds.models`` and ``hds.core``; MUST NOT
import routers or build HTTP responses (Constitution II).

Tokens are random, single-use and stored only as SHA-256 hashes; the raw value leaves the
system once, through ``MailSender``. Invitations last 7 days and resets 60 minutes.
"""

import hashlib
import logging
import uuid
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from hds.core import passwords
from hds.core.authz import MANAGER_ROLES, ensure_role
from hds.core.client_address import network_key
from hds.core.exceptions import AuthError, ConflictError, ForbiddenError
from hds.core.mail import MailSender
from hds.core.security import generate_refresh_token
from hds.models.refresh_token import RevocationReason
from hds.models.user import User, UserRole
from hds.repositories.invitation_token import InvitationTokenRepository
from hds.repositories.login_throttle import RESET, LoginThrottleRepository
from hds.repositories.refresh_token import RefreshTokenRepository
from hds.repositories.user import UserRepository
from hds.services.throttle import hash_key, live_lock

logger = logging.getLogger("hds.invitation")

#: How long an invitation link stays valid. Public because the development link script uses it.
INVITE_TTL_DAYS = 7
#: How long a password-reset link stays valid.
RESET_TTL_MINUTES = 60
#: Starts a network's reset count. No address can start like this, so the two never meet.
RESET_NETWORK_PREFIX = "network:"


def _hash_token(raw: str) -> str:
    return hashlib.sha256(raw.encode()).hexdigest()


class InvitationService:
    """Invite users, accept invitations and reset forgotten passwords."""

    def __init__(
        self,
        user_repo: UserRepository,
        token_repo: InvitationTokenRepository,
        mailer: MailSender,
        refresh_token_repo: RefreshTokenRepository,
        throttle_repo: LoginThrottleRepository,
        config: Any,
    ) -> None:
        """Wire the service.

        Args:
            user_repo: User lookup, creation and update.
            token_repo: Invitation and reset token storage.
            mailer: Delivery channel for the raw token. The router passes a ``DeferredMailer``, so
                a message is sent only after the transaction it belongs to commits, the request's
                or a reset job's, and is dropped if it rolls back; a link is never mailed for a
                change that was not saved.
            refresh_token_repo: Used to end sessions after a password reset.
            throttle_repo: Counts reset requests per address and per network.
            config: Settings supplying the reset request limits.
        """
        self._user_repo = user_repo
        self._token_repo = token_repo
        self._mailer = mailer
        self._refresh_token_repo = refresh_token_repo
        self._throttle_repo = throttle_repo
        self._config = config

    async def create_invitation(self, actor: User, email: str, role: UserRole) -> None:
        """Invite an email address to the actor's organisation and mail them a token.

        What happens depends on who already holds the address:

        * nobody: a *pending* account is created (random password, the email as the name, never
          activated) that the invitee completes on acceptance;
        * a pending account of this organisation: it is re-invited. It takes the new role under
          the same permission rules as a first invitation, and the earlier link is cancelled and
          replaced, so only the latest link works;
        * an active account, or any account of another organisation: a duplicate, and nothing is
          changed (an organisation must not edit another's accounts);
        * a deactivated account: refused. Bringing a person back is the explicit reactivation by
          staff, never something an invitation does.

        If another request creates the same new address at the same moment, this request carries
        on as a re-invitation of that account instead of failing.

        Args:
            actor: The Manager or Admin inviting.
            email: Address to invite.
            role: Role the invitee will get; Admin only if the actor is an Admin.

        Raises:
            ForbiddenError: ``users.manager_required`` (caller is not Manager+),
                ``users.role_invalid`` (Client users come from the client record) or
                ``users.admin_required`` (Admin role requested by a Manager, or a Manager
                changing a pending Admin). HTTP 403.
            ConflictError: ``users.duplicate_email`` for an active account or an account of
                another organisation; ``invitations.account_deactivated`` for a deactivated
                account. HTTP 409.

        Side effects:
            May create or re-role a pending account, cancels the earlier links of a pending
            account, stores a new token and queues an email, sent after the commit.
        """
        ensure_role(actor, *MANAGER_ROLES)
        if role == UserRole.client:
            raise ForbiddenError(
                code="users.role_invalid",
                detail="Client users are created from the client record.",
            )
        if role == UserRole.admin and actor.role != UserRole.admin:
            raise ForbiddenError(
                code="users.admin_required",
                detail="Only an Admin may assign the Admin role.",
            )

        account = await self._user_repo.get_by_email(email)
        created = False
        if account is None:
            await self._user_repo.release_connection()
            placeholder = await passwords.hash_password(str(uuid.uuid4()))
            account, created = await self._user_repo.create_if_absent(
                actor.organization_id,
                {
                    "email": email,
                    "full_name": email,
                    "role": role,
                    "password_hash": placeholder,
                    "is_active": False,
                },
            )
        user_id = account.id
        if not created:
            # The user's row comes first, before any of its links (see `lock_user_row`), and the
            # account is read again so the checks below judge its state after the lock.
            await self._user_repo.lock_user(account.id)
            current = await self._user_repo.get_by_email(email)
            if current is None:  # deleted during the wait: a link could never be accepted
                raise ConflictError(
                    code="users.duplicate_email",
                    detail=f"Email '{email}' changed during this request; try again.",
                    field="email",
                )
            account = current
            # The address was already taken (possibly a moment ago by a concurrent request).
            if account.organization_id != actor.organization_id or account.is_active:
                raise ConflictError(
                    code="users.duplicate_email",
                    detail=f"Email '{email}' is already in use.",
                    field="email",
                )
            if account.activated_at is not None:
                raise ConflictError(
                    code="invitations.account_deactivated",
                    detail="This account was deactivated; an invitation cannot bring it back.",
                    field="email",
                )
            # A pending account of this organisation, invited before: only an Admin may change an
            # Admin (FR-011a), the new role applies, and the earlier link is replaced.
            if account.role == UserRole.admin and actor.role != UserRole.admin:
                raise ForbiddenError(
                    code="users.admin_required",
                    detail="Only an Admin may change an Admin.",
                )
            if account.role != role:
                await self._user_repo.update(account.id, account.organization_id, role=role)
            await self._token_repo.cancel_for_user(account.id)

        raw, token_hash = generate_refresh_token()
        expires_at = datetime.now(UTC) + timedelta(days=INVITE_TTL_DAYS)
        await self._token_repo.create(
            user_id=user_id,
            token_hash=token_hash,
            purpose="invite",
            expires_at=expires_at,
        )
        # The token is passed as ``secret`` so any sender that stores or logs the message can
        # redact it; a bare token is not recognisable in text.
        await self._mailer.send(
            to=email,
            subject="You have been invited to HDS",
            body=f"Accept your invitation with this token (valid {INVITE_TTL_DAYS} days): {raw}",
            secret=raw,
            purpose="invite",
            expires_at=expires_at,
        )

    async def accept_invitation(self, raw_token: str, new_password: str) -> User:
        """Redeem an invite token: set the password and activate the account.

        Only a *pending* account (never activated, not active) can be completed this way. A link
        left over for a deactivated or already active account is refused exactly like a spent
        link, so the caller learns nothing about the account's state, and nothing is changed: no
        reactivation, no new password, no session ended.

        Args:
            raw_token: Token from the invitation email.
            new_password: The invitee's chosen password (hashed with Argon2id).

        Returns:
            The activated user.

        Raises:
            AuthError: ``auth.token_expired`` (unknown, wrong purpose, expired or owner
                gone) or ``auth.token_used`` (already used, cancelled, or the account is not
                pending). HTTP 401.

        Side effects:
            Activates the account and stamps ``activated_at``, marks the token used and cancels
            every other outstanding link of the account.
        """
        token = await self._valid_token(raw_token, "invite")
        # No connection or lock is held while the password is hashed (FR-222).
        await self._user_repo.release_connection()
        password_hash = await passwords.hash_password(new_password)
        # The user's row comes first, before the link (see `lock_user_row`). Then spend the link:
        # this single conditional write is what lets only one of two simultaneous redemptions
        # through. The loser finds the link used and is refused before it writes anything.
        await self._user_repo.lock_user(token.user_id)
        if not await self._token_repo.spend(token.id):
            raise AuthError(code="auth.token_used", detail="This token has already been used.")
        owner = await self._token_owner(token.user_id)
        if owner.is_active or owner.activated_at is not None:
            raise AuthError(code="auth.token_used", detail="This token has already been used.")
        user = await self._user_repo.update(
            token.user_id,
            owner.organization_id,
            password_hash=password_hash,
            is_active=True,
            activated_at=datetime.now(UTC),
        )
        await self._token_repo.cancel_for_user(owner.id)
        return user

    async def request_password_reset(
        self, email: str, network: str, queue_job: Callable[[str], None]
    ) -> None:
        """Count one reset request and queue its job if it is within its limits.

        This is all a request does, for every address, so its time says nothing about who has an
        account (FR-228). It reads no account and no link.

        Args:
            email: Address that asked for a reset.
            network: The client's network address.
            queue_job: Queues ``issue_reset_link`` for the address, to run after the response.

        Side effects:
            See ``_within_reset_limits``.
        """
        if await self._within_reset_limits(email, network):
            queue_job(email)

    async def _within_reset_limits(self, email: str, network: str) -> bool:
        """Count a reset request and say whether it is within both limits.

        The network comes first. A network already over its limit only reads its row, so a flood
        takes no lock and no address is counted, or one client could use up the allowance of every
        address for no mail at all. What it skips says nothing about any address. The decision comes
        from the counts and not from ``locked_until``, which is set when a count reaches its limit,
        so the request that reaches it is still within it (FR-247). Stale rows go before any count
        takes a row lock: this path has no hashing cost and can add rows faster than sign-in, and
        FR-221 promises no scheduled job.

        Args:
            email: Address that asked for a reset.
            network: The client's network address.

        Returns:
            ``True`` when the request is within both limits.

        Side effects:
            Counts the request and removes stale throttle rows, unless the network is over its
            limit. The first request over that limit in a window logs a warning, because a wrong
            ``TRUSTED_PROXY_COUNT`` makes every user share one address, and reset mail would stop
            for everyone with no other sign.
        """
        config = self._config
        now = datetime.now(UTC)
        address_limit: int = config.RESET_REQUEST_LIMIT_PER_ADDRESS
        network_limit: int = config.RESET_REQUEST_LIMIT_PER_NETWORK
        network_hash = hash_key(
            f"{RESET_NETWORK_PREFIX}{network_key(network)}", config.throttle_key
        )
        if network_limit > 0:
            row = await self._throttle_repo.get(RESET, network_hash)
            if (
                row is not None
                and row.failures > network_limit
                and live_lock(row.locked_until, now) is not None
            ):
                return False
        retention = timedelta(hours=config.LOGIN_THROTTLE_RETENTION_HOURS)
        await self._throttle_repo.purge_stale(now - retention, now)
        network_count = await self._count_reset(network_hash, network_limit, now)
        if network_count > network_limit:
            if network_count == network_limit + 1:
                logger.warning(
                    "reset request network limit reached; reset mail stops for this network until "
                    "the window ends. Check TRUSTED_PROXY_COUNT if this API is behind a proxy"
                )
            return False
        address_hash = hash_key(email.strip().lower(), config.throttle_key)
        return await self._count_reset(address_hash, address_limit, now) <= address_limit

    async def _count_reset(self, key_hash: str, limit: int, now: datetime) -> int:
        """Count one reset request under ``key_hash``, unless its limit is off.

        Args:
            key_hash: The HMAC of the folded address or of the prefixed network key.
            limit: Requests allowed in one window; 0 turns the limit off.
            now: Timezone-aware UTC current time.

        Returns:
            The count in this window, or 0 when the limit is off.
        """
        if limit == 0:
            return 0
        count, _ = await self._throttle_repo.register_failure(
            RESET,
            key_hash,
            limit,
            now + timedelta(minutes=RESET_TTL_MINUTES),
            RESET_TTL_MINUTES,
            now,
        )
        return count

    async def issue_reset_link(self, email: str) -> None:
        """Mail a reset token to an active account, and do nothing otherwise.

        Runs after the response, in its own session, for every address within its limits, so
        nothing here can reach the caller. Earlier links are left alone: anyone can make a request,
        so a request must not be able to take a link away. A completed reset cancels the rest
        (FR-224).

        Args:
            email: Address that asked for a reset.

        Side effects:
            For an active account, stores a 60-minute token and queues an email, sent after this
            job's commit.
        """
        user = await self._user_repo.get_by_email(email)
        if user is None or not user.is_active:
            return
        await self._user_repo.lock_user(user.id)  # before its links (see `lock_user_row`)
        # Staff may have deactivated or deleted the account while this waited for the lock.
        user = await self._user_repo.get_by_email(email)
        if user is None or not user.is_active:
            return
        raw, token_hash = generate_refresh_token()
        expires_at = datetime.now(UTC) + timedelta(minutes=RESET_TTL_MINUTES)
        await self._token_repo.create(
            user_id=user.id,
            token_hash=token_hash,
            purpose="reset",
            expires_at=expires_at,
        )
        await self._mailer.send(
            to=email,
            subject="Reset your HDS password",
            body=f"Reset your password with this token (valid {RESET_TTL_MINUTES} minutes): {raw}",
            secret=raw,
            purpose="reset",
            expires_at=expires_at,
        )

    async def reset_password(self, raw_token: str, new_password: str) -> None:
        """Redeem a reset token: replace the password and end every earlier credential.

        Sessions are revoked because a reset suggests the old credentials may be compromised
        (NFR-005), and every other outstanding link of the account is cancelled, so someone
        holding an earlier link loses it the moment the owner finishes (FR-224). Access tokens
        already issued stay valid until they expire. The owner has proved control of the mailbox,
        so the failed sign-in count and any lock are cleared too (FR-232). A reset is for an
        *active* account only: a link left over for a deactivated account is refused and changes
        nothing.

        Args:
            raw_token: Token from the reset email.
            new_password: The new password (hashed with Argon2id).

        Raises:
            AuthError: ``auth.token_expired`` or ``auth.token_used`` (already used, cancelled,
                or the account is not active). HTTP 401.

        Side effects:
            Marks the token used, cancels the account's other links, revokes all of the user's
            refresh tokens and clears the failed sign-in count and lock.
        """
        token = await self._valid_token(raw_token, "reset")
        await self._user_repo.release_connection()  # see accept_invitation
        password_hash = await passwords.hash_password(new_password)
        await self._user_repo.lock_user(token.user_id)
        if not await self._token_repo.spend(token.id):  # see accept_invitation
            raise AuthError(code="auth.token_used", detail="This token has already been used.")
        owner = await self._token_owner(token.user_id)
        if not owner.is_active:
            raise AuthError(code="auth.token_used", detail="This token has already been used.")
        await self._user_repo.update(
            token.user_id,
            owner.organization_id,
            password_hash=password_hash,
        )
        # A reset means the old credentials may be compromised: sign out every device.
        await self._refresh_token_repo.revoke_all_for_user(token.user_id, RevocationReason.all)
        await self._token_repo.cancel_for_user(token.user_id)
        await self._user_repo.reset_failed_attempts(token.user_id)

    async def _token_owner(self, user_id: uuid.UUID) -> User:
        """The token, not a caller's organisation, authorises redemption; look its owner up."""
        owner = await self._user_repo.get_by_id_for_token(user_id)
        if owner is None:
            raise AuthError(code="auth.token_expired", detail="Invalid or expired token.")
        return owner

    async def _valid_token(self, raw_token: str, purpose: str) -> Any:
        token = await self._token_repo.get_by_hash(_hash_token(raw_token))
        if token is None or getattr(token, "purpose", purpose) != purpose:
            raise AuthError(code="auth.token_expired", detail="Invalid or expired token.")
        if token.used_at is not None:
            raise AuthError(code="auth.token_used", detail="This token has already been used.")
        expires_at = token.expires_at
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=UTC)
        if datetime.now(UTC) >= expires_at:
            raise AuthError(code="auth.token_expired", detail="Token has expired.")
        return token
