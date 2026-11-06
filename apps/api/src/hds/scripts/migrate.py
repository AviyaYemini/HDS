"""Bring the database to the code's migrations, one run at a time (specs/006 FR-016, research R5).

Usage:
    uv run python -m hds.scripts.migrate

The chart's migration Job runs this before any new pod starts. It holds a PostgreSQL advisory lock
for the whole run, so two Jobs at once (a retried hook, two releases) never migrate together: the
second waits, then finds the database at head and changes nothing. A database at a revision the
code does not know is newer, as when the previous image runs after a rollback; it is left as it
is, and the command exits 0. Each migration keeps the previous release working (expand, then
contract), so the older code serves on it. A bare ``alembic upgrade head`` would stop there instead.

Layer: entry point, like ``hds.main``. The lock lives on a connection of its own, held while
``alembic upgrade head`` runs as a child process, because Alembic's environment starts an event
loop of its own.

The Job may start while PostgreSQL restarts, and it is retried only once, so the first connection
is tried ``CONNECT_ATTEMPTS`` times, ``CONNECT_DELAY_SECONDS`` apart. Should the lock's connection
drop while Alembic runs, a second run is not kept out; PostgreSQL's transactional DDL then makes
one of the two block or fail, never apply a migration twice.

Exit status: 0 at head, upgraded, or newer and left alone; otherwise Alembic's own status, or 1
when the database cannot be reached or fails the migration.
"""

import asyncio
import logging
import subprocess
import sys
from pathlib import Path

import asyncpg  # type: ignore[import-untyped]  # no stubs; three calls, typed as Any here
from sqlalchemy.engine import make_url

from hds.core.config import get_settings
from hds.core.health import Migrations, load_migrations
from hds.core.logging import configure_logging

logger = logging.getLogger("hds.migrate")

#: The advisory lock's name; ``hashtext`` of it is the key. A test takes the same lock.
LOCK_NAME = "hds.migrate"
API_DIR = Path(__file__).resolve().parents[3]
#: About half a minute for a restarting database to come back.
CONNECT_ATTEMPTS = 6
CONNECT_DELAY_SECONDS = 5.0


async def _connect(dsn: str) -> asyncpg.Connection:
    """Connect, trying again while the database refuses or is still starting up.

    Raises:
        OSError: The database never answered (the last attempt's error).
        asyncpg.CannotConnectNowError: It was still starting up at the last attempt.
    """
    for attempt in range(1, CONNECT_ATTEMPTS + 1):
        try:
            return await asyncpg.connect(dsn)
        except (OSError, asyncpg.CannotConnectNowError) as error:
            if attempt == CONNECT_ATTEMPTS:
                raise
            logger.warning(
                "the database is not answering yet (%s); attempt %d of %d",
                type(error).__name__,
                attempt,
                CONNECT_ATTEMPTS,
            )
            await asyncio.sleep(CONNECT_DELAY_SECONDS)
    raise AssertionError("unreachable")  # pragma: no cover


async def _current(connection: asyncpg.Connection) -> list[str]:
    """The rows of ``alembic_version``; empty when the table is empty or does not exist yet."""
    try:
        rows = await connection.fetch("SELECT version_num FROM alembic_version")
    except asyncpg.UndefinedTableError:
        return []
    return [str(row["version_num"]) for row in rows]


def _upgrade() -> int:
    """Run ``alembic upgrade head`` on the configured database; its exit status."""
    return subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"], cwd=API_DIR, check=False
    ).returncode


async def migrate(url: str, history: Migrations) -> int:
    """Take the lock, then upgrade unless the database is at head or newer; the exit status.

    Args:
        url: The application's database URL.
        history: The code's migrations.

    Returns:
        0 when the database ends at head or is newer, else Alembic's exit status.
    """
    dsn = make_url(url).set(drivername="postgresql").render_as_string(hide_password=False)
    connection = await _connect(dsn)
    try:
        if not await connection.fetchval("SELECT pg_try_advisory_lock(hashtext($1))", LOCK_NAME):
            logger.info("waiting for the migration lock: another run is migrating")
            await connection.execute("SELECT pg_advisory_lock(hashtext($1))", LOCK_NAME)
        current = await _current(connection)
        newer = [r for r in current if r not in history.known]
        if newer:
            logger.info(
                "the database is at %s, which this code does not know (newer); left as it is",
                ", ".join(newer),
            )
            return 0
        if current == [history.head]:
            logger.info("already at head (%s); nothing to do", history.head)
            return 0
        logger.info("upgrading from %s to %s", ", ".join(current) or "nothing", history.head)
        status = await asyncio.to_thread(_upgrade)
        if status == 0:
            logger.info("upgraded to %s", history.head)
        return status
    finally:
        await connection.close()  # releases the lock


def main() -> None:
    """Migrate the configured database and exit with the status."""
    configure_logging()
    history = load_migrations()
    try:
        status = asyncio.run(migrate(get_settings().DATABASE_URL, history))
    except OSError as error:
        logger.error("the database could not be reached (%s)", type(error).__name__)
        status = 1
    except (asyncpg.PostgresError, asyncpg.InterfaceError) as error:
        logger.error("the migration failed on the database (%s)", type(error).__name__)
        status = 1
    sys.exit(status)


if __name__ == "__main__":
    main()
