"""Invitation and password-reset service (US6 — Proposed)."""

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

_INVITE_TTL_DAYS = 7
_RESET_TTL_MINUTES = 60


def _hash_token(raw: str) -> str:
    return hashlib.sha256(raw.encode()).hexdigest()


class InvitationService:
    def __init__(
        self,
        user_repo: UserRepository,
        token_repo: InvitationTokenRepository,
        mailer: MailSender,
        refresh_token_repo: RefreshTokenRepository,
    ) -> None:
        self._user_repo = user_repo
        self._token_repo = token_repo
        self._mailer = mailer
        self._refresh_token_repo = refresh_token_repo

    async def create_invitation(self, actor: User, email: str, role: UserRole) -> None:
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
        await self._token_repo.create(
            user_id=user_id,
            token_hash=token_hash,
            purpose="invite",
            expires_at=datetime.now(UTC) + timedelta(days=_INVITE_TTL_DAYS),
        )
        await self._mailer.send(
            to=email,
            subject="You have been invited to HDS",
            body=f"Accept your invitation with this token (valid {_INVITE_TTL_DAYS} days): {raw}",
        )

    async def accept_invitation(self, raw_token: str, new_password: str) -> User:
        token = await self._valid_token(raw_token, "invite")
        user = await self._user_repo.update(
            token.user_id,
            password_hash=hash_password(new_password),
            is_active=True,
        )
        await self._token_repo.mark_used(token.id)
        return user

    async def request_password_reset(self, email: str) -> None:
        user = await self._user_repo.get_by_email(email)
        if user is None or not user.is_active:
            return  # Silent — never reveal whether the email exists

        raw, token_hash = generate_refresh_token()
        await self._token_repo.create(
            user_id=user.id,
            token_hash=token_hash,
            purpose="reset",
            expires_at=datetime.now(UTC) + timedelta(minutes=_RESET_TTL_MINUTES),
        )
        await self._mailer.send(
            to=email,
            subject="Reset your HDS password",
            body=f"Reset your password with this token (valid {_RESET_TTL_MINUTES} minutes): {raw}",
        )

    async def reset_password(self, raw_token: str, new_password: str) -> None:
        token = await self._valid_token(raw_token, "reset")
        await self._user_repo.update(
            token.user_id,
            password_hash=hash_password(new_password),
        )
        await self._token_repo.mark_used(token.id)
        # A reset means the old credentials may be compromised: sign out every device.
        await self._refresh_token_repo.revoke_all_for_user(token.user_id)

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
