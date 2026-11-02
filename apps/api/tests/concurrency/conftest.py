"""Fixtures for tests that need two requests or two database sessions at the same moment.

Layer: tests. These tests need real parallelism and a real database, so they always run on
PostgreSQL (the `migrated_test_db` fixture from the parent conftest) and never on SQLite. The
`race` fixture starts its actions together, behind a barrier, and returns every outcome in order
instead of raising, so a test can assert on who won and who lost.

Covers: none directly; helpers for FR-208, FR-209, FR-211.
"""

import asyncio
from collections.abc import AsyncGenerator, Awaitable, Callable
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from hds.models.base import Base

Action = Callable[[], Awaitable[Any]]


@pytest.fixture
def race() -> Callable[..., Awaitable[list[Any]]]:
    """Return a function that runs several async actions at the same moment.

    Every action first waits at a barrier that releases only when all of them have started, so
    none gets a head start. The returned list holds each action's result or the exception it
    raised, in the order the actions were given.
    """

    async def run(*actions: Action) -> list[Any]:
        barrier = asyncio.Barrier(len(actions))

        async def gated(action: Action) -> Any:
            await barrier.wait()
            return await action()

        return list(await asyncio.gather(*(gated(a) for a in actions), return_exceptions=True))

    return run


@pytest_asyncio.fixture
async def session_pair(
    migrated_test_db: str,
) -> AsyncGenerator[tuple[AsyncSession, AsyncSession], None]:
    """Yield two sessions on two separate database connections, then empty every table.

    Separate connections are what let one transaction block another, which is what the row-lock
    and conditional-update tests need to observe. The tables are truncated afterwards so tests do
    not leak data into each other.
    """
    engine = create_async_engine(migrated_test_db, echo=False, poolclass=NullPool)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    first, second = factory(), factory()
    try:
        yield first, second
    finally:
        await first.close()
        await second.close()
        tables = ", ".join(f'"{t.name}"' for t in Base.metadata.sorted_tables)
        async with engine.begin() as conn:
            await conn.execute(text(f"TRUNCATE {tables} RESTART IDENTITY CASCADE"))
        await engine.dispose()
