"""Remove credentials past their expiry and stale throttle rows, in bulk.

Usage:
    uv run python -m hds.scripts.purge_expired_records

Normal requests clean up after themselves (FR-209, FR-221), so nothing depends on this being
scheduled. It exists for accounts that never sign in again and for a table that has grown. Only the
expiry decides for a credential: a spent or cancelled link inside its lifetime is kept, so its
answer stays ``auth.token_used``. It prints counts only, never an address or a token.

It works in short batches and skips locked rows, so it never waits for a request and a request never
waits long for it. A skipped row goes on the next run.

Exit status: 0. A failure, such as a lost connection, stops the run with a traceback and a non-zero
status. Batches and counts printed before it stand, and the next run carries on.
"""

import asyncio
import sys
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from hds.core.config import get_settings
from hds.core.database import AsyncSessionLocal, engine, unit_of_work
from hds.repositories.idempotency_key import IdempotencyKeyRepository
from hds.repositories.invitation_token import InvitationTokenRepository
from hds.repositories.login_throttle import LoginThrottleRepository
from hds.repositories.refresh_token import RefreshTokenRepository
from hds.services.idempotency import KEY_LIFETIME

#: Rows deleted in one transaction.
BATCH = 1000


def throttle_cut_off(now: datetime, retention_hours: int) -> datetime:
    """Return the time before which an unlocked throttle row is stale.

    Args:
        now: Timezone-aware UTC current time.
        retention_hours: ``LOGIN_THROTTLE_RETENTION_HOURS``.

    Returns:
        The cut-off on ``updated_at``.
    """
    return now - timedelta(hours=retention_hours)


async def _drain(
    factory: async_sessionmaker[AsyncSession],
    delete: Callable[[AsyncSession], Awaitable[int]],
) -> int:
    """Repeat one delete, each batch in its own transaction, until a batch removes nothing.

    A short batch is not the end: rows held by a request, or moved by an update while the batch
    ran, are skipped and taken by a later batch. A backlog is finite, so the loop ends.
    """
    total = 0
    while True:
        async with factory() as session, unit_of_work(session):
            removed = await delete(session)
        total += removed
        if removed == 0:
            return total


async def run(factory: async_sessionmaker[AsyncSession]) -> int:
    """Remove everything that is due, printing each count as soon as it is known.

    A count is printed before the next step starts, so a failure part way still reports what was
    already committed.

    Args:
        factory: Makes the session for each batch.

    Returns:
        0, the exit status.
    """
    now = datetime.now(UTC)
    stale = throttle_cut_off(now, get_settings().LOGIN_THROTTLE_RETENTION_HOURS)

    async def tokens(session: AsyncSession) -> int:
        """Delete one batch of refresh tokens."""
        return await RefreshTokenRepository(session).delete_expired(now, BATCH)

    async def links(session: AsyncSession) -> int:
        """Delete one batch of links."""
        return await InvitationTokenRepository(session).delete_expired(now, BATCH)

    async def rows(session: AsyncSession) -> int:
        """Delete one batch of throttle rows."""
        return await LoginThrottleRepository(session).purge_stale(stale, now, BATCH)

    async def keys(session: AsyncSession) -> int:
        """Delete one batch of idempotency keys past their 24 hours (003 FR-115a)."""
        return await IdempotencyKeyRepository(session).delete_expired(now - KEY_LIFETIME, BATCH)

    for name, delete in (
        ("refresh_tokens", tokens),
        ("links", links),
        ("throttle_rows", rows),
        ("idempotency_keys", keys),
    ):
        print(f"{name} {await _drain(factory, delete)}", flush=True)
    return 0


async def _main() -> int:
    """Run against the configured database, then release its connections."""
    try:
        return await run(AsyncSessionLocal)
    finally:
        await engine.dispose()


def main() -> int:
    """Command-line entry point.

    Returns:
        The exit status described in the module docstring.
    """
    return asyncio.run(_main())


if __name__ == "__main__":
    sys.exit(main())
