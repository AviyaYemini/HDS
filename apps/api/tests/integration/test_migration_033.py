"""Migration 033 adds saved views (007 T082): the table appears empty and the downgrade drops it.

Covers: 007 FR-043.
"""

import asyncio
from collections.abc import AsyncGenerator

import pytest_asyncio

from tests.integration.migration_support import Scratch, alembic, scratch_database
from tests.integration.test_migration_020 import run

SCRATCH_DB = "hds_migration_033_test"
PREVIOUS = "032_roster_extras"
REVISION = "033_saved_views"


@pytest_asyncio.fixture
async def scratch() -> AsyncGenerator[Scratch, None]:
    async with scratch_database(SCRATCH_DB, PREVIOUS) as pair:
        yield pair


async def test_upgrade_creates_saved_views_and_downgrade_drops_it(scratch: Scratch) -> None:
    url, engine = scratch
    sql = "SELECT to_regclass('saved_views') IS NOT NULL"
    assert (await run(engine, sql)).scalar_one() is False
    await asyncio.to_thread(alembic, url, "upgrade", REVISION)
    assert (await run(engine, sql)).scalar_one() is True
    await asyncio.to_thread(alembic, url, "downgrade", PREVIOUS)
    assert (await run(engine, sql)).scalar_one() is False
