"""The sign-in lockout rule, the same for a registered and an unregistered address.

Layer: services. May import ``hds.repositories``, ``hds.models`` and ``hds.core``; MUST NOT import
routers or build HTTP responses (Constitution II). This coordinates two stores, which is service
work: a registered address keeps its count on the user row, and an unregistered one in
``login_throttle``. Sign-in sees one rule, so the two give the same answers (FR-221).
"""

import hashlib
import hmac
from datetime import UTC, datetime, timedelta
from typing import Any

from hds.core.client_address import network_key
from hds.models.user import User
from hds.repositories.login_throttle import ADDRESS, NETWORK, LoginThrottleRepository
from hds.repositories.user import UserRepository


def hash_key(value: str, secret: bytes) -> str:
    """Key a throttle row by an HMAC, so a copy of the table cannot confirm an address.

    A plain hash could be checked against any list of addresses. With the HMAC that needs the server
    secret too. Whoever holds the secret can still confirm a known address, so the keys are
    protected, not anonymous.

    Args:
        value: An address or network key.
        secret: The server secret (``Settings.throttle_key``).

    Returns:
        The HMAC-SHA-256 hex digest.
    """
    return hmac.new(secret, value.encode(), hashlib.sha256).hexdigest()


def live_lock(locked_until: datetime | None, now: datetime) -> datetime | None:
    """Return a lock that is still in force, or ``None``.

    Args:
        locked_until: The stored lock end, if any.
        now: Timezone-aware UTC current time.

    Returns:
        The timezone-aware lock end if it is in the future.
    """
    if locked_until is None:
        return None
    if locked_until.tzinfo is None:
        locked_until = locked_until.replace(tzinfo=UTC)
    return locked_until if now < locked_until else None


class SignInThrottle:
    """Reads and writes lock state; it never writes before a password has been verified."""

    def __init__(
        self,
        user_repo: UserRepository,
        throttle_repo: LoginThrottleRepository,
        config: Any,
    ) -> None:
        """Wire the rule.

        Args:
            user_repo: Holds the count and lock of a registered address.
            throttle_repo: Holds the count and lock of everything else.
            config: Settings supplying the limits.
        """
        self._user_repo = user_repo
        self._throttle_repo = throttle_repo
        self._config = config

    async def network_locked_until(self, network: str | None, now: datetime) -> datetime | None:
        """Return the end of a live lock on a network address.

        Args:
            network: The client's address, or ``None`` when it is not known.
            now: Timezone-aware UTC current time.

        Returns:
            The lock end, or ``None`` when the limit is off or not reached.
        """
        if network is None or self._config.LOGIN_RATE_LIMIT_PER_NETWORK == 0:
            return None
        row = await self._throttle_repo.get(
            NETWORK, hash_key(network_key(network), self._config.throttle_key)
        )
        return live_lock(row.locked_until, now) if row else None

    async def address_locked_until(
        self, user: User | None, address: str, now: datetime
    ) -> datetime | None:
        """Return the end of a live lock on an address, registered or not.

        Args:
            user: The account holding the address, if any.
            address: The submitted address.
            now: Timezone-aware UTC current time.

        Returns:
            The lock end, or ``None``.
        """
        if user is not None:
            return live_lock(user.locked_until, now)
        row = await self._throttle_repo.get(
            ADDRESS, hash_key(address.strip().lower(), self._config.throttle_key)
        )
        return live_lock(row.locked_until, now) if row else None

    async def register_failure(
        self, user: User | None, address: str, network: str | None, now: datetime
    ) -> None:
        """Count a failed sign-in, remove stale rows and commit, so the refusal keeps them.

        Args:
            user: The account holding the address, if any.
            address: The submitted address.
            network: The client's address, or ``None`` when it is not known.
            now: Timezone-aware UTC current time.
        """
        config = self._config
        lock_until = now + timedelta(minutes=config.LOGIN_LOCKOUT_MINUTES)
        if user is not None:
            await self._user_repo.register_failed_login(
                user.id, config.LOGIN_MAX_FAILURES, lock_until, now
            )
        else:
            await self._throttle_repo.register_failure(
                ADDRESS,
                hash_key(address.strip().lower(), self._config.throttle_key),
                config.LOGIN_MAX_FAILURES,
                lock_until,
                config.LOGIN_LOCKOUT_MINUTES,
                now,
            )
        if network is not None and config.LOGIN_RATE_LIMIT_PER_NETWORK > 0:
            await self._throttle_repo.register_failure(
                NETWORK,
                hash_key(network_key(network), self._config.throttle_key),
                config.LOGIN_RATE_LIMIT_PER_NETWORK,
                lock_until,
                config.LOGIN_LOCKOUT_MINUTES,
                now,
            )
        retention = timedelta(hours=config.LOGIN_THROTTLE_RETENTION_HOURS)
        await self._throttle_repo.purge_stale(now - retention, now)
        await self._throttle_repo.commit()
