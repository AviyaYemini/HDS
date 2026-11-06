"""Migration 024 adds ``schedule_jobs.trace_context``, the trace that queued a job (006 T020).

Runs on a scratch database at revision 023. The column is nullable and existing rows keep null:
"written once when the job is queued and never changed; null for jobs queued before the migration
and for requests that carry no trace" (data-model.md). It is an expand step the previous release
ignores (FR-016), and the downgrade drops it.

Covers: 006 FR-008a, FR-016.
"""

import asyncio
import uuid
from collections.abc import AsyncGenerator
from typing import Any

import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncEngine

from tests.integration.migration_support import Scratch, alembic, scratch_database
from tests.integration.test_migration_020 import org, run, user
from tests.integration.test_migration_021 import job

SCRATCH_DB = "hds_migration_024_test"
PREVIOUS = "023_freeze_deleted_projects"
REVISION = "024_schedule_job_trace_context"
TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"


@pytest_asyncio.fixture
async def scratch() -> AsyncGenerator[Scratch, None]:
    async with scratch_database(SCRATCH_DB, PREVIOUS) as pair:
        yield pair


async def column(engine: AsyncEngine) -> Any:
    """The ``trace_context`` column's type, length and nullability, or None if it is absent."""
    rows = await run(
        engine,
        "SELECT data_type, character_maximum_length, is_nullable FROM information_schema.columns "
        "WHERE table_name = 'schedule_jobs' AND column_name = 'trace_context'",
    )
    return rows.one_or_none()


async def test_upgrade_adds_a_nullable_varchar_55_and_existing_jobs_keep_null(
    scratch: Scratch,
) -> None:
    url, engine = scratch
    org_id = await org(engine)
    await job(scratch, o=org_id, u=await user(engine, org_id))
    assert await column(engine) is None

    await asyncio.to_thread(alembic, url, "upgrade", REVISION)

    found = await column(engine)
    assert found is not None
    assert tuple(found) == ("character varying", 55, "YES")
    values = (await run(engine, "SELECT trace_context FROM schedule_jobs")).scalars().all()
    assert values == [None]


async def test_a_job_can_hold_a_traceparent_after_the_upgrade(scratch: Scratch) -> None:
    url, engine = scratch
    await asyncio.to_thread(alembic, url, "upgrade", REVISION)
    org_id = await org(engine)
    await job(scratch, o=org_id, u=await user(engine, org_id))
    job_id = uuid.uuid4()
    await run(
        engine,
        "UPDATE schedule_jobs SET id = :i, trace_context = :t",
        i=job_id,
        t=TRACEPARENT,
    )
    stored = (
        await run(engine, "SELECT trace_context FROM schedule_jobs WHERE id = :i", i=job_id)
    ).scalar_one()
    assert stored == TRACEPARENT


async def test_downgrade_drops_the_column_and_upgrading_again_works(scratch: Scratch) -> None:
    url, engine = scratch
    await asyncio.to_thread(alembic, url, "upgrade", REVISION)
    await asyncio.to_thread(alembic, url, "downgrade", PREVIOUS)
    assert await column(engine) is None
    await asyncio.to_thread(alembic, url, "upgrade", REVISION)
    assert await column(engine) is not None
