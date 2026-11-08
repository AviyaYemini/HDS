"""Migration 031 (written as 027) adds each assignment's own times (007 T065).

Runs on a scratch database at revision 026: the two columns and their checks (both or neither, the
end after the start) appear, and the downgrade drops them.

Covers: 007 FR-027.
"""

import asyncio
from collections.abc import AsyncGenerator

import pytest_asyncio

from tests.integration.migration_support import Scratch, alembic, scratch_database
from tests.integration.test_migration_020 import run

SCRATCH_DB = "hds_migration_031_test"
PREVIOUS = "030_submission_changes"
REVISION = "031_assignment_times"


@pytest_asyncio.fixture
async def scratch() -> AsyncGenerator[Scratch, None]:
    async with scratch_database(SCRATCH_DB, PREVIOUS) as pair:
        yield pair


async def columns(scratch: Scratch) -> int:
    _, engine = scratch
    rows = await run(
        engine,
        "SELECT count(*) FROM information_schema.columns WHERE table_name = 'assignments' "
        "AND column_name IN ('starts_at', 'ends_at')",
    )
    return int(rows.scalar_one())


async def test_upgrade_adds_paired_ordered_times_and_downgrade_drops_them(scratch: Scratch) -> None:
    url, engine = scratch
    await asyncio.to_thread(alembic, url, "upgrade", REVISION)
    assert await columns(scratch) == 2
    checks = await run(
        engine,
        "SELECT count(*) FROM pg_constraint WHERE conname IN "
        "('ck_assignments_times_paired', 'ck_assignments_times_order')",
    )
    assert checks.scalar_one() == 2
    await asyncio.to_thread(alembic, url, "downgrade", PREVIOUS)
    assert await columns(scratch) == 0


async def test_the_order_check_says_what_it_enforces(scratch: Scratch) -> None:
    """The constraint texts say what they enforce, so a refusal is readable in the logs."""
    url, engine = scratch
    await asyncio.to_thread(alembic, url, "upgrade", REVISION)
    found = await run(
        engine,
        "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
        "WHERE conname = 'ck_assignments_times_order'",
    )
    assert "ends_at > starts_at" in found.scalar_one()
