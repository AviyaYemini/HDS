"""Migration 028 adds rule overrides, week templates and publish events (007 T067).

Runs on a scratch database at revision 027: the three tables appear, empty, and the downgrade
drops them.

Covers: 007 FR-034, FR-036, FR-038.
"""

import asyncio
from collections.abc import AsyncGenerator

import pytest_asyncio

from tests.integration.migration_support import Scratch, alembic, scratch_database
from tests.integration.test_migration_020 import run

SCRATCH_DB = "hds_migration_028_test"
PREVIOUS = "027_assignment_times"
REVISION = "028_roster_extras"
TABLES = ("rule_overrides", "week_templates", "publish_events")


@pytest_asyncio.fixture
async def scratch() -> AsyncGenerator[Scratch, None]:
    async with scratch_database(SCRATCH_DB, PREVIOUS) as pair:
        yield pair


async def present(scratch: Scratch) -> list[bool]:
    _, engine = scratch
    return [
        (await run(engine, f"SELECT to_regclass('{t}') IS NOT NULL")).scalar_one() for t in TABLES
    ]


async def test_upgrade_creates_the_tables_and_downgrade_drops_them(scratch: Scratch) -> None:
    url, _ = scratch
    assert await present(scratch) == [False, False, False]
    await asyncio.to_thread(alembic, url, "upgrade", REVISION)
    assert await present(scratch) == [True, True, True]
    await asyncio.to_thread(alembic, url, "downgrade", PREVIOUS)
    assert await present(scratch) == [False, False, False]
