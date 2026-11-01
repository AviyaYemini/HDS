"""Authentication service: login, refresh, logout, logout-all."""

import hashlib
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from hds.core.exceptions import AuthError
from hds.core.security import (
    create_access_token,
    generate_refresh_token,
    verify_password,
)
from hds.repositories.refresh_token import RefreshTokenRepository
from hds.repositories.user import UserRepository


def _verify_password(plain: str, hashed: str) -> bool:
    return verify_password(plain, hashed)


@dataclass
class TokenResponse:
    access_token: str
    refresh_token: str
    token_type: str = "bearer"


class AuthService:
    def __init__(
        self,
        user_repo: UserRepository,
        token_repo: RefreshTokenRepository,
        config: Any,
    ) -> None:
        self._user_repo = user_repo
        self._token_repo = token_repo
        self._config = config

    async def login(self, email: str, password: str) -> TokenResponse:
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
        token_hash = hashlib.sha256(raw_token.encode()).hexdigest()
        stored = await self._token_repo.get_by_hash(token_hash)
        if stored is None:
            raise AuthError(code="auth.invalid_token", detail="Invalid refresh token")
        if stored.revoked_at is not None:
            raise AuthError(code="auth.token_revoked", detail="Refresh token revoked")

        expires_at = stored.expires_at
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=UTC)
        if datetime.now(UTC) >= expires_at:
            raise AuthError(code="auth.token_expired", detail="Refresh token expired")

        user = await self._user_repo.get_for_auth(stored.user_id)
        if user is None or not user.is_active:
            raise AuthError(code="auth.invalid_token", detail="Invalid refresh token")

        await self._token_repo.revoke(stored.id)

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
        token_hash = hashlib.sha256(raw_token.encode()).hexdigest()
        stored = await self._token_repo.get_by_hash(token_hash)
        if stored is None or stored.revoked_at is not None:
            return
        await self._token_repo.revoke(stored.id)

    async def logout_all(self, user_id: uuid.UUID) -> None:
        await self._token_repo.revoke_all_for_user(user_id)
