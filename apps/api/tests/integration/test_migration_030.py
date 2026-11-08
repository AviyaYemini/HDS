"""Migration 030 records an Admin's hand change to a submission beside it (007 review R51, R52).

Runs on a scratch database at revision 026. ``availability_submissions`` gains ``changed_by`` and
``changed_at``, both nullable and empty on existing rows, so every existing submission stays the
employee's own; the downgrade drops both.

Covers: 007 FR-014, 003 FR-107.
"""

import asyncio
from collections.abc import AsyncGenerator

import pytest_asyncio

from tests.integration.migration_support import Scratch, alembic, scratch_database
from tests.integration.test_migration_020 import run

SCRATCH_DB = "hds_migration_030_test"
PREVIOUS = "026_availability_submissions"
REVISION = "030_submission_changes"
COLUMNS = (
    "SELECT column_name, is_nullable FROM information_schema.columns "
    "WHERE table_name = 'availability_submissions' AND column_name IN ('changed_by', 'changed_at') "
    "ORDER BY column_name"
)


@pytest_asyncio.fixture
async def scratch() -> AsyncGenerator[Scratch, None]:
    async with scratch_database(SCRATCH_DB, PREVIOUS) as pair:
        yield pair


async def test_the_two_columns_arrive_nullable(scratch: Scratch) -> None:
    url, engine = scratch
    await asyncio.to_thread(alembic, url, "upgrade", REVISION)
    rows = await run(engine, COLUMNS)
    assert [tuple(r) for r in rows.all()] == [("changed_at", "YES"), ("changed_by", "YES")]


async def test_downgrade_drops_both(scratch: Scratch) -> None:
    url, engine = scratch
    await asyncio.to_thread(alembic, url, "upgrade", REVISION)
    await asyncio.to_thread(alembic, url, "downgrade", PREVIOUS)
    rows = await run(engine, COLUMNS)
    assert rows.all() == []
