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
            mailer: Delivery channel for the raw token.
            refresh_token_repo: Used to end sessions after a password reset.
        """
        self._user_repo = user_repo
        self._token_repo = token_repo
        self._mailer = mailer
        self._refresh_token_repo = refresh_token_repo

    async def create_invitation(self, actor: User, email: str, role: UserRole) -> None:
        """Invite an email address to the actor's organisation and mail them a token.

        A new address gets an inactive placeholder account (random password, the email as the
        name) that the invitee completes on acceptance. Inviting an address whose account is
        still inactive issues a fresh token for it; an active account is a conflict.

        Args:
            actor: The Manager or Admin inviting.
            email: Address to invite.
            role: Role the invitee will get; Admin only if the actor is an Admin.

        Raises:
            ForbiddenError: ``users.manager_required`` (caller is not Manager+),
                ``users.role_invalid`` (Client users come from the client record) or
                ``users.admin_required`` (Admin role requested by a Manager). HTTP 403.
            ConflictError: ``users.duplicate_email`` when the address belongs to an active
                account. HTTP 409.

        Side effects:
            May create a user, always stores a token and sends an email.
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

        existing = await self._user_repo.get_by_email(email)
        if existing is None:
            user = await self._user_repo.create(
                actor.organization_id,
                {
                    "email": email,
                    "full_name": email,
                    "role": role,
                    "password_hash": hash_password(str(uuid.uuid4())),
                    "is_active": False,
                },
            )
            user_id = user.id
        elif existing.is_active:
            raise ConflictError(
                code="users.duplicate_email",
                detail=f"Email '{email}' is already in use.",
                field="email",
            )
        else:
            user_id = existing.id

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

        Args:
            raw_token: Token from the invitation email.
            new_password: The invitee's chosen password (hashed with Argon2id).

        Returns:
            The activated user.

        Raises:
            AuthError: ``auth.token_expired`` (unknown, wrong purpose, expired or owner
                gone) or ``auth.token_used``. HTTP 401.

        Side effects:
            Marks the token used.
        """
        token = await self._valid_token(raw_token, "invite")
        owner = await self._token_owner(token.user_id)
        user = await self._user_repo.update(
            token.user_id,
            owner.organization_id,
            password_hash=hash_password(new_password),
            is_active=True,
            activated_at=datetime.now(UTC),
        )
        await self._token_repo.mark_used(token.id)
        return user

    async def request_password_reset(self, email: str) -> None:
        """Mail a reset token to an active account, and say nothing otherwise.

        Returns without error for unknown or inactive addresses so the endpoint cannot be used
        to discover which emails are registered.

        Args:
            email: Address that asked for a reset.

        Side effects:
            For an active account, stores a 60-minute token and sends an email.
        """
        user = await self._user_repo.get_by_email(email)
        if user is None or not user.is_active:
            return  # Silent — never reveal whether the email exists

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
        """Redeem a reset token: replace the password and end every session.

        Sessions are revoked because a reset suggests the old credentials may be compromised
        (NFR-005). Access tokens already issued stay valid until they expire.

        Args:
            raw_token: Token from the reset email.
            new_password: The new password (hashed with Argon2id).

        Raises:
            AuthError: ``auth.token_expired`` or ``auth.token_used``. HTTP 401.

        Side effects:
            Marks the token used and revokes all of the user's refresh tokens.
        """
        token = await self._valid_token(raw_token, "reset")
        owner = await self._token_owner(token.user_id)
        await self._user_repo.update(
            token.user_id,
            owner.organization_id,
            password_hash=hash_password(new_password),
        )
        await self._token_repo.mark_used(token.id)
        # A reset means the old credentials may be compromised: sign out every device.
        await self._refresh_token_repo.revoke_all_for_user(token.user_id)

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
