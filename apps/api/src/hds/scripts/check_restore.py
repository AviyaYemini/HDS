"""Check that a restored database works with this code, writing nothing (specs/006 FR-014, R11).

Usage:
    uv run python -m hds.scripts.check_restore

The weekly restore test's last step. It runs on the throwaway copy after ``migrate``, so a backup
from an older release has been brought to head, and one at a revision this code does not know
(newer) has been left as it is. It takes no manifest: ``restore-test.sh`` compared the revision and
the row counts before ``migrate``, which may change both.

It runs the ready checks (the database answers and is not behind the code), then loads every
organisation, its users and its shifts of the last month through the repositories, as the API
would. Its own reads run in read-only transactions, so it cannot write. It prints counts only:
never a name, an address or any other personal data.

Layer: entry point, like ``hds.main``.

Exit status: 0 when every check passes; 1 when the database is not ready, holds no organisation,
or cannot be read.
"""

import asyncio
import logging
import sys
from datetime import UTC, datetime, timedelta

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from hds.core.config import get_settings
from hds.core.logging import configure_logging
from hds.repositories.organisation import OrganisationRepository
from hds.repositories.shift import ShiftRepository
from hds.repositories.user import UserRepository
from hds.wiring import health_service

logger = logging.getLogger("hds.check_restore")

#: How far back the shifts are loaded.
SHIFT_WINDOW = timedelta(days=31)
PAGE = 500


class RestoreCheckError(Exception):
    """The restored database is not usable; the message says which check failed."""


async def _ready(factory: async_sessionmaker[AsyncSession]) -> None:
    """Run the API's ready checks on the copy."""
    ready, body = await health_service(factory).ready()
    if not ready:
        failing = body.get("failing")
        named = ", ".join(map(str, failing)) if isinstance(failing, list) else "unknown"
        raise RestoreCheckError(f"not ready: {named}")


async def _load(factory: async_sessionmaker[AsyncSession]) -> dict[str, int]:
    """Load the organisations, users and recent shifts; their counts."""
    now = datetime.now(UTC)
    since = now - SHIFT_WINDOW
    counts = {"organisations": 0, "users": 0, "shifts in the last month": 0}
    async with factory() as session:
        organisations = await OrganisationRepository(session).all()
        if not organisations:
            raise RestoreCheckError("no organisation in the restored database")
        counts["organisations"] = len(organisations)
        users, shifts = UserRepository(session), ShiftRepository(session)
        for org in organisations:
            counts["users"] += len(await users.search(org.id))
            after = None
            while True:
                page = await shifts.page(
                    org.id,
                    starts_from=since,
                    starts_before=now,
                    project_id=None,
                    status=None,
                    after=after,
                    limit=PAGE,
                )
                counts["shifts in the last month"] += len(page)
                if len(page) < PAGE:
                    break
                after = (page[-1].starts_at, page[-1].id)
    return counts


async def check(url: str) -> dict[str, int]:
    """Check the database at ``url``; the counts it loaded.

    Args:
        url: The restored copy's database URL.

    Returns:
        How many organisations, users and recent shifts were loaded.

    Raises:
        RestoreCheckError: A check failed.
    """
    engine = create_async_engine(
        url,
        poolclass=NullPool,
        connect_args={"server_settings": {"default_transaction_read_only": "on"}},
    )
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        await _ready(factory)
        return await _load(factory)
    finally:
        await engine.dispose()


def main() -> None:
    """Check the configured database and exit with the status."""
    configure_logging()
    try:
        counts = asyncio.run(check(get_settings().DATABASE_URL))
    except RestoreCheckError as failed:
        logger.error("restore check failed: %s", failed)
        sys.exit(1)
    except (OSError, SQLAlchemyError) as error:
        logger.error(
            "restore check failed: the database could not be read (%s)", type(error).__name__
        )
        sys.exit(1)
    for name, count in counts.items():
        logger.info("%s: %d", name, count)
    logger.info("restore check passed")
    sys.exit(0)


if __name__ == "__main__":
    main()
