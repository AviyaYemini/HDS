"""One test run per database at a time: a PostgreSQL advisory lock held for the whole session.

Layer: tests. Two runs on one named database (``hds_p3_test``) would empty each other's tables in
the middle of a test. The run now takes ``pg_try_advisory_lock`` on a connection of its own before
it touches the database, and a second run stops at once, saying why, instead of colliding.

The lock belongs to a connection, so that connection must live as long as the session. pytest's
session fixtures are synchronous and each ``asyncio.run`` ends its loop, so the connection lives
in an event loop on a thread of its own, which ``release`` stops.

Covers: none directly; the harness's test-database guard (.claude/hds/harness-log.md, item 4).
"""

import asyncio
import threading
from concurrent.futures import Future
from typing import Any

import asyncpg  # type: ignore[import-untyped]  # no stubs; one call, typed as Any here
from sqlalchemy.engine import make_url

from tests.run_database import lock_key


class RunLock:
    """The advisory lock of one test run on one database."""

    def __init__(self, url: str) -> None:
        """Prepare the lock for the database in ``url``; nothing connects until ``acquire``."""
        parsed = make_url(url)
        self.database = parsed.database or ""
        self._key = lock_key(self.database)
        # The lock is server-wide, so it is taken on the maintenance database, which always exists.
        self._dsn = parsed.set(drivername="postgresql", database="postgres").render_as_string(False)
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, daemon=True)
        self._connection: Any = None

    def _run(self, coroutine: Any) -> Any:
        """Run ``coroutine`` on the lock's own loop and wait for its result."""
        future: Future[Any] = asyncio.run_coroutine_threadsafe(coroutine, self._loop)
        return future.result(timeout=30)

    def acquire(self) -> None:
        """Take the lock, or stop the run at once.

        Raises:
            RuntimeError: Another run holds the lock on this database.
        """
        self._thread.start()
        try:
            self._run(self._acquire())
        except BaseException:
            self._stop()
            raise

    async def _acquire(self) -> None:
        """Connect and try the lock; on refusal, close and raise."""
        self._connection = await asyncpg.connect(self._dsn)
        if not await self._connection.fetchval("SELECT pg_try_advisory_lock($1)", self._key):
            await self._connection.close()
            self._connection = None
            raise RuntimeError(
                f"another test run is using the database {self.database!r}; wait for it to end "
                "(see pg_stat_activity), or run without TEST_DATABASE_URL to get a database of "
                "your own"
            )

    def release(self) -> None:
        """Release the lock by closing its connection, and stop the loop's thread."""
        if self._connection is not None:
            self._run(self._connection.close())
            self._connection = None
        self._stop()

    def _stop(self) -> None:
        """Stop the loop and wait for its thread."""
        if self._thread.is_alive():
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join(timeout=10)
        self._loop.close()
