"""Migration 025 adds the organisation's contact columns and the refresh token's kind (007 T029).

Runs on a scratch database at revision 024. Existing refresh tokens become ``web`` (the lifetime
they were issued with) and organisations keep no contact details; the downgrade drops the four
columns. It is an expand step the previous release ignores (006 FR-016).

Covers: 007 FR-010, FR-012.
"""

import asyncio
from collections.abc import AsyncGenerator

import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncEngine

from tests.integration.migration_support import Scratch, alembic, scratch_database
from tests.integration.test_migration_020 import org, run, user

SCRATCH_DB = "hds_migration_025_test"
PREVIOUS = "024_schedule_job_trace_context"
REVISION = "025_org_contact_sessions"


@pytest_asyncio.fixture
async def scratch() -> AsyncGenerator[Scratch, None]:
    async with scratch_database(SCRATCH_DB, PREVIOUS) as pair:
        yield pair


async def columns(engine: AsyncEngine, table: str, names: tuple[str, ...]) -> set[str]:
    rows = await run(
        engine,
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_name = '{table}' AND column_name IN {names!r}",
    )
    return set(rows.scalars().all())


async def test_upgrade_adds_the_columns_and_existing_tokens_become_web(scratch: Scratch) -> None:
    url, engine = scratch
    org_id = await org(engine)
    user_id = await user(engine, org_id)
    await run(
        engine,
        "INSERT INTO refresh_tokens (id, user_id, token_hash, expires_at) "
        f"VALUES (gen_random_uuid(), '{user_id}', repeat('a', 64), now() + interval '1 day')",
    )

    await asyncio.to_thread(alembic, url, "upgrade", REVISION)

    contact = ("contact_phone", "contact_email", "opening_hours")
    assert await columns(engine, "organisations", contact) == set(contact)
    kinds = (await run(engine, "SELECT client_kind FROM refresh_tokens")).scalars().all()
    assert kinds == ["web"]
    phones = (await run(engine, "SELECT contact_phone FROM organisations")).scalars().all()
    assert phones == [None]


async def test_downgrade_drops_them(scratch: Scratch) -> None:
    url, engine = scratch
    await asyncio.to_thread(alembic, url, "upgrade", REVISION)
    await asyncio.to_thread(alembic, url, "downgrade", PREVIOUS)
    assert await columns(engine, "refresh_tokens", ("client_kind", "x")) == set()
    assert await columns(engine, "organisations", ("contact_phone", "contact_email")) == set()
