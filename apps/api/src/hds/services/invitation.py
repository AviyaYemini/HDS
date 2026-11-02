"""Invitation and password-reset service (US6 — Proposed).

Layer: services. May import ``hds.repositories``, ``hds.models`` and ``hds.core``; MUST NOT
import routers or build HTTP responses (Constitution II).

Tokens are random, single-use and stored only as SHA-256 hashes; the raw value leaves the
system once, through ``MailSender``. Invitations last 7 days and resets 60 minutes.
"""

import hashlib
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from hds.core.authz import MANAGER_ROLES, ensure_role
from hds.core.exceptions import AuthError, ConflictError, ForbiddenError
from hds.core.mail import MailSender
from hds.core.security import generate_refresh_token, hash_password
from hds.models.refresh_token import RevocationReason
from hds.models.user import User, UserRole
from hds.repositories.invitation_token import InvitationTokenRepository
from hds.repositories.refresh_token import RefreshTokenRepository
from hds.repositories.user import UserRepository

#: How long an invitation link stays valid. Public because the development link script uses it.
INVITE_TTL_DAYS = 7
#: How long a password-reset link stays valid.
RESET_TTL_MINUTES = 60


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
    ) -> None:
        """Wire the service.

        Args:
            user_repo: User lookup, creation and update.
            token_repo: Invitation and reset token storage.
            mailer: Delivery channel for the raw token. The router passes a ``DeferredMailer``, so
                a message is sent only after the request's transaction commits and is dropped if
                it rolls back; a link is never mailed for a change that was not saved.
            refresh_token_repo: Used to end sessions after a password reset.
        """
        self._user_repo = user_repo
        self._token_repo = token_repo
        self._mailer = mailer
        self._refresh_token_repo = refresh_token_repo

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
            account, created = await self._user_repo.create_if_absent(
                actor.organization_id,
                {
                    "email": email,
                    "full_name": email,
                    "role": role,
                    "password_hash": hash_password(str(uuid.uuid4())),
                    "is_active": False,
                },
            )
        user_id = account.id
        if not created:
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
        # Spend the link first: this single conditional write is what lets only one of two
        # simultaneous redemptions through. The loser waits for the winner to commit, finds the
        # link used and is refused before it writes anything.
        if not await self._token_repo.spend(token.id):
            raise AuthError(code="auth.token_used", detail="This token has already been used.")
        owner = await self._token_owner(token.user_id)
        if owner.is_active or owner.activated_at is not None:
            raise AuthError(code="auth.token_used", detail="This token has already been used.")
        user = await self._user_repo.update(
            token.user_id,
            owner.organization_id,
            password_hash=hash_password(new_password),
            is_active=True,
            activated_at=datetime.now(UTC),
        )
        await self._token_repo.cancel_for_user(owner.id)
        return user

    async def request_password_reset(self, email: str) -> None:
        """Mail a reset token to an active account, and say nothing otherwise.

        Returns without error for unknown or inactive addresses so the endpoint cannot be used
        to discover which emails are registered.

        Args:
            email: Address that asked for a reset.

        Side effects:
            For an active account, cancels its earlier unused reset links, stores a 60-minute
            token and queues an email, sent after the commit. Only the latest reset link works;
            an invitation link is a different flow and is left alone.
        """
        user = await self._user_repo.get_by_email(email)
        if user is None or not user.is_active:
            return  # Silent — never reveal whether the email exists

        await self._token_repo.cancel_for_user(user.id, purpose="reset")
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
        if not await self._token_repo.spend(token.id):  # see accept_invitation
            raise AuthError(code="auth.token_used", detail="This token has already been used.")
        owner = await self._token_owner(token.user_id)
        if not owner.is_active:
            raise AuthError(code="auth.token_used", detail="This token has already been used.")
        await self._user_repo.update(
            token.user_id,
            owner.organization_id,
            password_hash=hash_password(new_password),
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
