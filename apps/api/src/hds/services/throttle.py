"""The sign-in lockout rule, the same for a registered and an unregistered address.

Layer: services. May import ``hds.repositories``, ``hds.models`` and ``hds.core``; MUST NOT import
routers or build HTTP responses (Constitution II). Every address is counted in ``login_throttle``
under one rule, so the answers and the work are the same whether or not the address has an account
(FR-221). The rule itself, not storage, is why this is service work.
"""

import hashlib
import hmac
from datetime import UTC, datetime, timedelta
from typing import Any

from hds.core.client_address import network_key
from hds.repositories.login_throttle import ADDRESS, NETWORK, LoginThrottleRepository


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


def address_key(address: str, secret: bytes) -> str:
    """Return the stored key of a submitted address: its trimmed, lowercased form, keyed.

    The one place this is computed, so sign-in, a reset request and a completed reset always meet
    the same row.

    Args:
        address: The address as submitted.
        secret: The server secret (``Settings.throttle_key``).

    Returns:
        The key under which the address is counted.
    """
    return hash_key(address.strip().lower(), secret)


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

    def __init__(self, throttle_repo: LoginThrottleRepository, config: Any) -> None:
        """Wire the rule.

        Args:
            throttle_repo: Holds the count and lock of every address and network address.
            config: Settings supplying the limits.
        """
        self._throttle_repo = throttle_repo
        self._config = config

    def _address(self, address: str) -> str:
        """Return the stored key of a submitted address."""
        return address_key(address, self._config.throttle_key)

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

    async def address_locked_until(self, address: str, now: datetime) -> datetime | None:
        """Return the end of a live lock on an address, registered or not.

        Read fresh, so a re-check after waiting sees a lock set meanwhile.

        Args:
            address: The submitted address.
            now: Timezone-aware UTC current time.

        Returns:
            The lock end, or ``None``.
        """
        row = await self._throttle_repo.get(ADDRESS, self._address(address))
        return live_lock(row.locked_until, now) if row else None

    async def clear_address(self, address: str, now: datetime) -> datetime | None:
        """Forget an address's count after a successful sign-in, unless it was locked meanwhile.

        A failing guess can lock the address after the success's re-check, and nothing serialises
        the two, so the clean-up itself refuses to remove a live lock.

        Args:
            address: The submitted address.
            now: Timezone-aware UTC current time.

        Returns:
            The end of a live lock that must refuse this sign-in, or ``None``.
        """
        return await self._throttle_repo.clear_unless_locked(ADDRESS, self._address(address), now)

    async def register_failure(self, address: str, network: str | None, now: datetime) -> None:
        """Count a failed sign-in, remove stale rows and commit, so the refusal keeps them.

        Every address is counted the same way in the same store, so neither the answers nor the
        work tell a registered address from an unknown one (FR-221).

        Args:
            address: The submitted address.
            network: The client's address, or ``None`` when it is not known.
            now: Timezone-aware UTC current time.
        """
        config = self._config
        lock_until = now + timedelta(minutes=config.LOGIN_LOCKOUT_MINUTES)
        retention = timedelta(hours=config.LOGIN_THROTTLE_RETENTION_HOURS)
        await self._throttle_repo.register_failure(
            ADDRESS,
            self._address(address),
            config.LOGIN_MAX_FAILURES,
            lock_until,
            config.LOGIN_LOCKOUT_MINUTES,
            now,
            restart_before=now - retention,
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
        await self._throttle_repo.purge_stale(now - retention, now)
        await self._throttle_repo.commit()
