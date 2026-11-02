"""Storage for failure counts and locks of keys with no account row.

Layer: repositories. May import ``hds.models`` and ``hds.core``; MUST NOT import services or
routers. Keys are HMAC-SHA-256 hex digests, so no address or network address is stored in the clear
and a copy of the table alone cannot confirm one (see ``hds.services.throttle.hash_key``).
"""

from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import and_, case, delete, literal, null, or_, select, tuple_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.sql.elements import ColumnElement

from hds.models.login_throttle import LoginThrottle
from hds.repositories.base import BaseRepository

ADDRESS = "address"
NETWORK = "network"
RESET = "reset"
WINDOWED = frozenset({NETWORK, RESET})


class LoginThrottleRepository(BaseRepository[LoginThrottle]):
    """One row per kind and key: a count, an optional window and an optional lock."""

    model = LoginThrottle

    async def get(self, kind: str, key_hash: str) -> LoginThrottle | None:
        """Read one row fresh.

        Args:
            kind: ``address``, ``network`` or ``reset``.
            key_hash: HMAC-SHA-256 hex digest of the key.

        Returns:
            The row, or ``None``.
        """
        result = await self.session.execute(
            select(LoginThrottle)
            .where(LoginThrottle.kind == kind, LoginThrottle.key_hash == key_hash)
            .execution_options(populate_existing=True)
        )
        return result.scalar_one_or_none()

    async def register_failure(
        self,
        kind: str,
        key_hash: str,
        limit: int,
        lock_until: datetime,
        window_minutes: int,
        now: datetime,
    ) -> tuple[int, datetime | None]:
        """Count one failure in a single atomic statement and lock at the limit.

        For an ``address`` the count runs until it is cleared, and a stored lock that has run out
        restarts it at 1, exactly as ``UserRepository.register_failed_login`` does. For ``network``
        and ``reset`` the count lives in a fixed window: a failure after the window has ended
        restarts both, and the lock lasts until the window ends. ``updated_at`` is set here
        because the model's ``onupdate`` does not apply inside ``ON CONFLICT DO UPDATE``.

        The caller commits, through `BaseRepository.commit`, so that a refusal raised next does not
        undo the count.

        Args:
            kind: ``address``, ``network`` or ``reset``.
            key_hash: HMAC-SHA-256 hex digest of the key.
            limit: Count at which the key locks.
            lock_until: Timezone-aware UTC end of the lock, for an ``address``.
            window_minutes: Length of the fixed window, for ``network`` and ``reset``.
            now: Timezone-aware UTC current time.

        Returns:
            ``(count, locked_until)`` as stored after the statement.
        """
        table = LoginThrottle.__table__
        window = timedelta(minutes=window_minutes)
        statement = insert(LoginThrottle).values(
            kind=kind,
            key_hash=key_hash,
            failures=1,
            window_started_at=now if kind in WINDOWED else None,
            locked_until=(now + window if kind in WINDOWED else lock_until) if limit <= 1 else None,
            updated_at=now,
        )
        stored_lock = table.c.locked_until
        started: ColumnElement[Any] = table.c.window_started_at
        new_end: ColumnElement[Any] = literal(lock_until)
        if kind in WINDOWED:
            restart: ColumnElement[bool] = or_(
                table.c.window_started_at.is_(None), table.c.window_started_at + window <= now
            )
            new_end = case((restart, now + window), else_=table.c.window_started_at + window)
            started = case((restart, now), else_=table.c.window_started_at)
        else:
            restart = and_(stored_lock.is_not(None), stored_lock <= now)
        count = case((restart, 1), else_=table.c.failures + 1)
        upsert = statement.on_conflict_do_update(
            index_elements=[LoginThrottle.kind, LoginThrottle.key_hash],
            set_={
                "failures": count,
                "window_started_at": started,
                "locked_until": case(
                    (count >= limit, new_end), (restart, null()), else_=stored_lock
                ),
                "updated_at": now,
            },
        ).returning(LoginThrottle.failures, LoginThrottle.locked_until)
        failures, locked_until = (await self.session.execute(upsert)).one()
        return int(failures), locked_until

    async def purge_stale(self, older_than: datetime, now: datetime, limit: int = 50) -> None:
        """Delete up to ``limit`` unlocked rows not touched since ``older_than``.

        A row is unlocked when ``locked_until`` is empty or in the past: ``network`` and ``reset``
        rows keep the value after hitting a limit, so "not empty" would keep them for ever. Rows
        another request is using are skipped, so this never waits and cannot deadlock.

        Args:
            older_than: Timezone-aware UTC cut-off on ``updated_at``.
            now: Timezone-aware UTC current time.
            limit: The most rows to delete in one call.
        """
        stale = (
            select(LoginThrottle.kind, LoginThrottle.key_hash)
            .where(
                LoginThrottle.updated_at < older_than,
                or_(LoginThrottle.locked_until.is_(None), LoginThrottle.locked_until <= now),
            )
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        await self.session.execute(
            delete(LoginThrottle).where(
                tuple_(LoginThrottle.kind, LoginThrottle.key_hash).in_(stale)
            )
        )
