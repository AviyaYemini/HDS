"""Authentication service: login, refresh, logout, logout-all.

Layer: services. May import ``hds.repositories``, ``hds.models`` and ``hds.core``; MUST NOT
import routers or build HTTP responses (Constitution II).

Policy implemented here: Argon2id password check, lockout after repeated failures, short-lived
access tokens and rotating single-use refresh tokens (Constitution VI).
"""

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from math import ceil
from typing import Any, NoReturn

from hds.core import passwords
from hds.core.exceptions import AuthError, NotFoundError, RateLimitedError
from hds.core.security import create_access_token, generate_refresh_token, hash_token
from hds.models.refresh_token import RefreshToken, RevocationReason
from hds.repositories.invitation_token import InvitationTokenRepository
from hds.repositories.login_throttle import LoginThrottleRepository
from hds.repositories.refresh_token import RefreshTokenRepository
from hds.repositories.user import UserRepository
from hds.services.throttle import SignInThrottle


def _locked(locked_until: datetime) -> AuthError:
    """Build the answer for a locked address, registered or not."""
    return AuthError(
        code="auth.account_locked", detail=f"Account locked until {locked_until.isoformat()}"
    )


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
        link_repo: InvitationTokenRepository,
        throttle_repo: LoginThrottleRepository,
    ) -> None:
        """Wire the service.

        Args:
            user_repo: User lookup and lockout counters.
            token_repo: Refresh-token storage.
            config: Settings supplying token lifetimes, the signing key and lockout limits.
            link_repo: Used to remove the user's expired links after a successful sign-in.
            throttle_repo: Holds the failure counts that have no account row.
        """
        self._user_repo = user_repo
        self._token_repo = token_repo
        self._config = config
        self._link_repo = link_repo
        self._throttle = SignInThrottle(throttle_repo, config)

    async def login(
        self,
        email: str,
        password: str,
        network: str | None = None,
        client_kind: str = "web",
    ) -> TokenResponse:
        """Authenticate by email and password and issue a token pair.

        Unknown email, wrong password and an inactive account all return the same
        ``auth.invalid_credentials`` so a caller cannot tell which accounts exist. The lock is
        checked before the password, so a locked account cannot be probed. A lock that has
        expired no longer counts, and the next failure restarts the count, so the user has the
        full number of attempts again. Nothing is written and no connection is held while the
        password is checked (FR-222). A wrong password adds one to the count and the account
        locks for ``LOGIN_LOCKOUT_MINUTES`` on the failure that makes the count reach
        ``LOGIN_MAX_FAILURES``, not before; a success resets the count. After the password check
        the user's row is locked and read again, so a reset or deactivation that finished in
        between refuses this sign-in instead of leaving a session behind (FR-230).

        Args:
            email: Account email, matched without regard to letter case.
            password: Clear-text password.
            network: The client's network address, for the per-network limit.
            client_kind: ``web`` (default) or ``native``; a native refresh token lasts
                ``REFRESH_TOKEN_TTL_DAYS_NATIVE`` days, a web one ``REFRESH_TOKEN_TTL_DAYS``. The
                client declares it, so a browser could claim ``native``; ADR 016 records why that
                is acceptable (rotation and reuse detection still apply, the web keeps tokens in
                memory, biometrics are a convenience gate only).

        Returns:
            A new access and refresh token.

        Raises:
            AuthError: ``auth.invalid_credentials`` (HTTP 401) or ``auth.account_locked``
                (HTTP 423).
            RateLimitedError: ``auth.rate_limited`` (HTTP 429) when the network is over its limit.
            ServiceBusyError: ``service.busy`` (HTTP 503) when the hashing queue is full.

        Side effects:
            On a wrong password counts it, locks at the limit and commits that at once so the
            refusal does not undo it; on success stores the new refresh-token hash and removes
            the user's expired tokens and links.
        """
        now = datetime.now(UTC)
        network_lock = await self._throttle.network_locked_until(network, now)
        if network_lock is not None:
            raise RateLimitedError(max(1, ceil((network_lock - now).total_seconds())))

        user = await self._user_repo.get_by_email(email)
        # Judged from what was just read: a lock that has run out simply no longer counts, and the
        # answer is the same whether or not the address has an account (FR-221).
        locked_until = await self._throttle.address_locked_until(email, now)
        if locked_until is not None:
            raise _locked(locked_until)

        await self._user_repo.release_connection()
        if user is None:
            # Do the same expensive check as for a real account, so the answer takes as long and
            # an attacker cannot tell unknown addresses from known ones (FR-214).
            await passwords.verify_against_dummy(password)
            await self._throttle.register_failure(email, network, datetime.now(UTC))
            raise AuthError(code="auth.invalid_credentials", detail="Invalid credentials")

        if not await passwords.verify_password(password, user.password_hash):
            await self._throttle.register_failure(email, network, datetime.now(UTC))
            raise AuthError(code="auth.invalid_credentials", detail="Invalid credentials")

        if not user.is_active:
            raise AuthError(code="auth.invalid_credentials", detail="Invalid credentials")

        # Take the lock that "end every session" takes, then look again: a reset or deactivation
        # that finished after the password check must stop this sign-in (FR-230).
        verified_hash = user.password_hash
        await self._user_repo.lock_user(user.id)
        current = await self._user_repo.get_for_auth(user.id)
        if current is None or not current.is_active or current.password_hash != verified_hash:
            raise AuthError(code="auth.invalid_credentials", detail="Invalid credentials")
        # Guesses overlap while they wait for a hash, so the lock may have been set since the
        # first check. The current time matters: ``now`` is from before that wait (FR-221).
        locked_until = await self._throttle.address_locked_until(email, datetime.now(UTC))
        if locked_until is not None:
            raise _locked(locked_until)

        locked_until = await self._throttle.clear_address(email, datetime.now(UTC))
        if locked_until is not None:
            raise _locked(locked_until)

        access_token = create_access_token(
            sub=user.id,
            org=user.organization_id,
            role=user.role,
            ttl_seconds=self._config.ACCESS_TOKEN_TTL_SECONDS,
            secret=self._config.JWT_SECRET_KEY.get_secret_value(),
            algorithm=self._config.JWT_ALGORITHM,
        )
        raw_refresh, token_hash = generate_refresh_token()
        expires_at = datetime.now(UTC) + self._refresh_lifetime(client_kind)
        await self._token_repo.create(user.id, token_hash, expires_at, client_kind)
        await self._remove_expired_records(user.id)

        return TokenResponse(access_token=access_token, refresh_token=raw_refresh)

    async def refresh(self, raw_token: str) -> TokenResponse:
        """Exchange a refresh token for a new pair; the presented token is spent.

        Rotation means each refresh token works exactly once, even when it is presented twice at
        the same moment: the token is revoked by a single conditional statement, so only one
        request can win it. A token past its expiry is refused as expired first, whatever else
        happened to it: it cannot be used for anything, so presenting it is not evidence of theft
        and it ends no session (FR-209). A request that loses, or that presents an unexpired token
        revoked earlier, is refused (see `_refuse_reuse` for when that also ends every session).
        The user's row is locked first and the user read after that, so a deactivated account
        cannot refresh and a concurrent "end every session" cannot miss the new token (FR-230).

        Args:
            raw_token: The refresh token the client holds.

        Returns:
            A new access and refresh token.

        Raises:
            AuthError: ``auth.invalid_token`` (unknown token, or user gone or inactive),
                ``auth.token_revoked`` or ``auth.token_expired``. All map to HTTP 401.

        Side effects:
            Revokes the presented token (reason ``rotated``), stores the new one and removes the
            user's expired tokens and links. A replay of an unexpired token that was rotated
            longer ago than ``REFRESH_REUSE_GRACE_SECONDS`` also revokes every other token of the
            user and commits that before refusing.
        """
        token_hash = hash_token(raw_token)
        stored = await self._token_repo.get_by_hash(token_hash)
        if stored is None:
            raise AuthError(code="auth.invalid_token", detail="Invalid refresh token")
        expires_at = stored.expires_at
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=UTC)
        if datetime.now(UTC) >= expires_at:
            raise AuthError(code="auth.token_expired", detail="Refresh token expired")

        if stored.revoked_at is not None:
            await self._refuse_reuse(stored)

        # Lock before reading the account, so this never acts on a state from before the lock and
        # a concurrent "end every session" either sees the new token or refuses this one (FR-230).
        await self._user_repo.lock_user(stored.user_id)
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
        # The new token keeps the session's kind, so a phone stays signed in while it is used.
        new_expires_at = datetime.now(UTC) + self._refresh_lifetime(stored.client_kind)
        await self._token_repo.create(user.id, new_hash, new_expires_at, stored.client_kind)
        await self._remove_expired_records(user.id)

        return TokenResponse(access_token=access_token, refresh_token=raw_refresh)

    def _refresh_lifetime(self, client_kind: str) -> timedelta:
        """How long a refresh token of this kind lives (007 FR-010)."""
        if client_kind == "native":
            return timedelta(days=self._config.REFRESH_TOKEN_TTL_DAYS_NATIVE)
        return timedelta(days=self._config.REFRESH_TOKEN_TTL_DAYS)

    async def sessions(self, user_id: uuid.UUID) -> list[RefreshToken]:
        """The caller's signed-in sessions: one live refresh token per device (007 N12)."""
        return await self._token_repo.live_for_user(user_id, datetime.now(UTC))

    async def end_session(self, user_id: uuid.UUID, session_id: uuid.UUID) -> None:
        """End one of the caller's sessions by revoking its refresh token.

        The access token that device holds stays valid until it expires (at most 15 minutes,
        Constitution VI), then its next refresh is refused.

        Raises:
            NotFoundError: ``resource.not_found`` (HTTP 404) when the id is not one of the
                caller's live sessions, so another user's session ids cannot be probed.
        """
        if not await self._token_repo.revoke_for_user(session_id, user_id, RevocationReason.logout):
            raise NotFoundError(resource="Session", id=session_id)

    async def logout(self, raw_token: str, user_id: uuid.UUID) -> None:
        """Revoke the caller's refresh token, ending that session.

        Idempotent and silent: an unknown or already-revoked token is ignored, so signing out
        twice, or with a stale token, is not an error. A token of another user is ignored the same
        way, so the answer does not say whether it is real (FR-238).

        Args:
            raw_token: The refresh token to revoke.
            user_id: The caller.
        """
        token_hash = hash_token(raw_token)
        stored = await self._token_repo.get_by_hash(token_hash)
        if stored is None or stored.revoked_at is not None or stored.user_id != user_id:
            return
        await self._token_repo.revoke_if_live(stored.id, RevocationReason.logout)

    async def logout_all(self, user_id: uuid.UUID) -> None:
        """Revoke every refresh token of a user, ending all their sessions.

        Existing access tokens stay valid until they expire (at most 15 minutes).

        Args:
            user_id: The user signing out everywhere (always the caller's own id).
        """
        await self._token_repo.revoke_all_for_user(user_id, RevocationReason.all)

    async def _remove_expired_records(self, user_id: uuid.UUID) -> None:
        """Delete the user's expired tokens and links, on the success path only.

        Sign-in must write nothing before the password is verified, so this never runs earlier.

        Args:
            user_id: The user who has just signed in or refreshed.
        """
        now = datetime.now(UTC)
        await self._token_repo.delete_expired_for_user(user_id, now)
        await self._link_repo.delete_expired_for_user(user_id, now)

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
