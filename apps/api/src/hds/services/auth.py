"""Authentication service: login, refresh, logout, logout-all.

Layer: services. May import ``hds.repositories``, ``hds.models`` and ``hds.core``; MUST NOT
import routers or build HTTP responses (Constitution II).

Policy implemented here: Argon2id password check, lockout after repeated failures, short-lived
access tokens and rotating single-use refresh tokens (Constitution VI).
"""

import hashlib
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, NoReturn

from hds.core.exceptions import AuthError
from hds.core.security import (
    create_access_token,
    generate_refresh_token,
    verify_password,
)
from hds.models.refresh_token import RefreshToken, RevocationReason
from hds.repositories.refresh_token import RefreshTokenRepository
from hds.repositories.user import UserRepository


def _verify_password(plain: str, hashed: str) -> bool:
    return verify_password(plain, hashed)


@dataclass
class TokenResponse:
    """The credentials returned by a successful sign-in or refresh.

    Attributes:
        access_token: Short-lived JWT for the ``Authorization`` header.
        refresh_token: Raw single-use token; only its hash is stored server-side.
        token_type: Always ``bearer``.
    """

    access_token: str
    refresh_token: str
    token_type: str = "bearer"


class AuthService:
    """Sign-in, token rotation and sign-out."""

    def __init__(
        self,
        user_repo: UserRepository,
        token_repo: RefreshTokenRepository,
        config: Any,
    ) -> None:
        """Wire the service.

        Args:
            user_repo: User lookup and lockout counters.
            token_repo: Refresh-token storage.
            config: Settings supplying token lifetimes, the signing key and lockout limits.
        """
        self._user_repo = user_repo
        self._token_repo = token_repo
        self._config = config

    async def login(self, email: str, password: str) -> TokenResponse:
        """Authenticate by email and password and issue a token pair.

        Unknown email, wrong password and an inactive account all return the same
        ``auth.invalid_credentials`` so a caller cannot tell which accounts exist. The lock is
        checked before the password, so a locked account cannot be probed. A wrong password
        adds to the failure counter and, at ``LOGIN_MAX_FAILURES``, locks the account for
        ``LOGIN_LOCKOUT_MINUTES``; a success resets the counter.

        Args:
            email: Account email, matched exactly.
            password: Clear-text password.

        Returns:
            A new access and refresh token.

        Raises:
            AuthError: ``auth.invalid_credentials`` (HTTP 401) or ``auth.account_locked``
                (HTTP 423).

        Side effects:
            Updates the failure counter and lock time; stores the new refresh-token hash.
        """
        user = await self._user_repo.get_by_email(email)
        if user is None:
            raise AuthError(code="auth.invalid_credentials", detail="Invalid credentials")

        # Check account lock
        if user.locked_until is not None:
            now = datetime.now(UTC)
            locked_until = user.locked_until
            if locked_until.tzinfo is None:
                locked_until = locked_until.replace(tzinfo=UTC)
            if now < locked_until:
                raise AuthError(
                    code="auth.account_locked",
                    detail=f"Account locked until {locked_until.isoformat()}",
                )

        if not _verify_password(password, user.password_hash):
            await self._user_repo.increment_failed_attempts(user.id)
            new_count = user.failed_login_attempts + 1
            if new_count >= self._config.LOGIN_MAX_FAILURES:
                lockout = datetime.now(UTC) + timedelta(minutes=self._config.LOGIN_LOCKOUT_MINUTES)
                await self._user_repo.set_locked_until(user.id, lockout)
            raise AuthError(code="auth.invalid_credentials", detail="Invalid credentials")

        if not user.is_active:
            raise AuthError(code="auth.invalid_credentials", detail="Invalid credentials")

        await self._user_repo.reset_failed_attempts(user.id)

        access_token = create_access_token(
            sub=user.id,
            org=user.organization_id,
            role=user.role,
            ttl_seconds=self._config.ACCESS_TOKEN_TTL_SECONDS,
            secret=self._config.JWT_SECRET_KEY.get_secret_value(),
            algorithm=self._config.JWT_ALGORITHM,
        )
        raw_refresh, token_hash = generate_refresh_token()
        expires_at = datetime.now(UTC) + timedelta(days=self._config.REFRESH_TOKEN_TTL_DAYS)
        await self._token_repo.create(user.id, token_hash, expires_at)

        return TokenResponse(access_token=access_token, refresh_token=raw_refresh)

    async def refresh(self, raw_token: str) -> TokenResponse:
        """Exchange a refresh token for a new pair; the presented token is spent.

        Rotation means each refresh token works exactly once, even when it is presented twice at
        the same moment: the token is revoked by a single conditional statement, so only one
        request can win it. A request that loses, or that presents a token revoked earlier, is
        refused (see `_refuse_reuse` for when that also ends every session). The user is
        reloaded, so a deactivated account cannot refresh.

        Args:
            raw_token: The refresh token the client holds.

        Returns:
            A new access and refresh token.

        Raises:
            AuthError: ``auth.invalid_token`` (unknown token, or user gone or inactive),
                ``auth.token_revoked`` or ``auth.token_expired``. All map to HTTP 401.

        Side effects:
            Revokes the presented token (reason ``rotated``) and stores the new one. A replay of
            a token that was rotated longer ago than ``REFRESH_REUSE_GRACE_SECONDS`` also revokes
            every other token of the user and commits that before refusing.
        """
        token_hash = hashlib.sha256(raw_token.encode()).hexdigest()
        stored = await self._token_repo.get_by_hash(token_hash)
        if stored is None:
            raise AuthError(code="auth.invalid_token", detail="Invalid refresh token")
        if stored.revoked_at is not None:
            await self._refuse_reuse(stored)

        expires_at = stored.expires_at
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=UTC)
        if datetime.now(UTC) >= expires_at:
            raise AuthError(code="auth.token_expired", detail="Refresh token expired")

        user = await self._user_repo.get_for_auth(stored.user_id)
        if user is None or not user.is_active:
            raise AuthError(code="auth.invalid_token", detail="Invalid refresh token")

        if not await self._token_repo.revoke_if_live(stored.id, RevocationReason.rotated):
            # Lost the race: another request revoked it between our read and our write. Decide
            # again from what is stored now, exactly as if we had read it revoked.
            current = await self._token_repo.get_by_hash(token_hash)
            if current is None:
                raise AuthError(code="auth.invalid_token", detail="Invalid refresh token")
            await self._refuse_reuse(current)

        access_token = create_access_token(
            sub=user.id,
            org=user.organization_id,
            role=user.role,
            ttl_seconds=self._config.ACCESS_TOKEN_TTL_SECONDS,
            secret=self._config.JWT_SECRET_KEY.get_secret_value(),
            algorithm=self._config.JWT_ALGORITHM,
        )
        raw_refresh, new_hash = generate_refresh_token()
        new_expires_at = datetime.now(UTC) + timedelta(days=self._config.REFRESH_TOKEN_TTL_DAYS)
        await self._token_repo.create(user.id, new_hash, new_expires_at)

        return TokenResponse(access_token=access_token, refresh_token=raw_refresh)

    async def logout(self, raw_token: str) -> None:
        """Revoke one refresh token, ending that session.

        Idempotent and silent: an unknown or already-revoked token is ignored, so signing out
        twice, or with a stale token, is not an error.

        Args:
            raw_token: The refresh token to revoke.
        """
        token_hash = hashlib.sha256(raw_token.encode()).hexdigest()
        stored = await self._token_repo.get_by_hash(token_hash)
        if stored is None or stored.revoked_at is not None:
            return
        await self._token_repo.revoke_if_live(stored.id, RevocationReason.logout)

    async def logout_all(self, user_id: uuid.UUID) -> None:
        """Revoke every refresh token of a user, ending all their sessions.

        Existing access tokens stay valid until they expire (at most 15 minutes).

        Args:
            user_id: The user signing out everywhere (always the caller's own id).
        """
        await self._token_repo.revoke_all_for_user(user_id, RevocationReason.all)

    async def _refuse_reuse(self, token: RefreshToken) -> NoReturn:
        """Refuse a token that is already revoked, and decide whether that means theft.

        Only a token revoked by *rotation* can be evidence of theft, because a rotated token is
        replaced the moment it is used, so a second use means someone else holds a copy. The
        decision, from the stored reason and the time of revocation:

        * rotated, within ``REFRESH_REUSE_GRACE_SECONDS``: an honest retry (the response was
          lost) or the loser of a simultaneous pair. Refused, nothing else happens.
        * rotated, longer ago: a replay. Every other session of the user is revoked (reason
          ``all``) and that is committed BEFORE the refusal is raised, because the request's own
          rollback on an error would otherwise undo the one effect that matters.
        * any other reason (sign-out, account change, sessions already ended) or no recorded
          reason (revoked before reasons existed): refused, nothing else happens. Presenting
          such a token is not evidence of theft.

        Args:
            token: The stored token, known to be revoked.

        Raises:
            AuthError: Always ``auth.token_revoked`` (HTTP 401).
        """
        if token.revoked_reason == RevocationReason.rotated and token.revoked_at is not None:
            revoked_at = token.revoked_at
            if revoked_at.tzinfo is None:
                revoked_at = revoked_at.replace(tzinfo=UTC)
            age = (datetime.now(UTC) - revoked_at).total_seconds()
            if age > self._config.REFRESH_REUSE_GRACE_SECONDS:
                await self._token_repo.revoke_all_for_user(token.user_id, RevocationReason.all)
                await self._token_repo.commit()
        raise AuthError(code="auth.token_revoked", detail="Refresh token revoked")
