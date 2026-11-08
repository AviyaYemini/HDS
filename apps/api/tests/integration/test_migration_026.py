"""Migration 026 adds availability submissions and ``requires_submission`` (007 T050).

Runs on a scratch database at revision 025. Windows that had already closed get
``requires_submission = false``, so the new rule never reaches back into weeks without a Submit;
windows still open get true. The downgrade drops both.

Covers: 007 FR-014.
"""

import asyncio
from collections.abc import AsyncGenerator

import pytest_asyncio

from tests.integration.migration_support import Scratch, alembic, scratch_database
from tests.integration.test_migration_020 import org, run

SCRATCH_DB = "hds_migration_026_test"
PREVIOUS = "025_org_contact_sessions"
REVISION = "026_availability_submissions"


@pytest_asyncio.fixture
async def scratch() -> AsyncGenerator[Scratch, None]:
    async with scratch_database(SCRATCH_DB, PREVIOUS) as pair:
        yield pair


async def test_closed_windows_keep_their_old_rule_and_open_ones_require_submission(
    scratch: Scratch,
) -> None:
    url, engine = scratch
    org_id = await org(engine)
    for start, deadline in (
        ("2026-01-04", "now() - interval '1 day'"),
        ("2027-01-03", "now() + interval '1 day'"),
    ):
        await run(
            engine,
            "INSERT INTO submission_windows (id, organization_id, starts_on, ends_on, deadline) "
            f"VALUES (gen_random_uuid(), '{org_id}', DATE '{start}', DATE '{start}' + 6, "
            f"{deadline})",
        )

    await asyncio.to_thread(alembic, url, "upgrade", REVISION)

    rows = await run(
        engine,
        "SELECT deadline < now(), requires_submission FROM submission_windows ORDER BY starts_on",
    )
    assert [tuple(r) for r in rows.all()] == [(True, False), (False, True)]
    tables = await run(engine, "SELECT to_regclass('availability_submissions') IS NOT NULL")
    assert tables.scalar_one() is True


async def test_downgrade_drops_the_table_and_the_column(scratch: Scratch) -> None:
    url, engine = scratch
    await asyncio.to_thread(alembic, url, "upgrade", REVISION)
    await asyncio.to_thread(alembic, url, "downgrade", PREVIOUS)
    assert (
        await run(engine, "SELECT to_regclass('availability_submissions')")
    ).scalar_one() is None
    columns = await run(
        engine,
        "SELECT count(*) FROM information_schema.columns "
        "WHERE table_name = 'submission_windows' AND column_name = 'requires_submission'",
    )
    assert columns.scalar_one() == 0
