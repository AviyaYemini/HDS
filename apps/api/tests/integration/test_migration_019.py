"""Migration 019 indexes token expiry and drops the retired lock columns (003 Group D).

The bulk clean-up finds expired sign-in tokens and one-time links by ``expires_at``, which had no
index, so it read every row. ``users.failed_login_attempts`` and ``users.locked_until`` have held
nothing since 002 moved every address's count to ``login_throttle``. Runs on a scratch database at
revision 018.

Covers: 003 FR-008, FR-009, SC-004.
"""

import asyncio
import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest_asyncio
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from tests.integration.migration_support import Scratch, alembic, scratch_database

SCRATCH_DB = "hds_migration_019_test"
PREVIOUS = "018_login_throttle"
REVISION = "019_expiry_index_retire_locks"
INDEXES = {
    "refresh_tokens": "idx_refresh_tokens_expires_at",
    "invitation_tokens": "idx_invitation_tokens_expires_at",
}
RETIRED = ("failed_login_attempts", "locked_until")


@pytest_asyncio.fixture
async def scratch() -> AsyncGenerator[Scratch, None]:
    async with scratch_database(SCRATCH_DB, PREVIOUS) as pair:
        yield pair


async def seed(engine: AsyncEngine) -> list[uuid.UUID]:
    """An organisation with two accounts, one with a count and a lock left from before 002."""
    org, ids = uuid.uuid4(), [uuid.uuid4(), uuid.uuid4()]
    async with engine.begin() as conn:
        await conn.execute(
            text("INSERT INTO organisations (id, name, timezone) VALUES (:o, 'Org', 'UTC')"),
            {"o": org},
        )
        for n, user in enumerate(ids):
            await conn.execute(
                text(
                    "INSERT INTO users (id, organization_id, email, password_hash, full_name, "
                    "role, is_active, activated_at, failed_login_attempts, locked_until) VALUES "
                    "(:i, :o, :e, 'h', 'User', CAST('employee' AS user_role), true, now(), :f, "
                    ":l)"
                ),
                {
                    "i": user,
                    "o": org,
                    "e": f"user{n}@hds.test",
                    "f": 4 if n == 0 else 0,
                    "l": None if n else datetime(2026, 11, 3, 12, tzinfo=UTC),
                },
            )
    return ids


async def user_columns(engine: AsyncEngine) -> dict[str, tuple[Any, ...]]:
    async with engine.connect() as conn:
        rows = await conn.execute(
            text(
                "SELECT column_name, data_type, is_nullable, column_default "
                "FROM information_schema.columns WHERE table_name = 'users'"
            )
        )
        return {r[0]: tuple(r[1:]) for r in rows}


async def expiry_indexes(engine: AsyncEngine) -> dict[str, str]:
    async with engine.connect() as conn:
        rows = await conn.execute(
            text(
                "SELECT indexname, indexdef FROM pg_indexes "
                "WHERE tablename IN ('refresh_tokens', 'invitation_tokens')"
            )
        )
        return {name: definition for name, definition in rows if "expires_at" in definition}


async def user_ids(engine: AsyncEngine) -> set[uuid.UUID]:
    async with engine.connect() as conn:
        return {r[0] for r in await conn.execute(text("SELECT id FROM users"))}


async def test_upgrade_indexes_expiry_and_drops_the_retired_columns(scratch: Scratch) -> None:
    url, engine = scratch
    ids = await seed(engine)

    await asyncio.to_thread(alembic, url, "upgrade", REVISION)

    found = await expiry_indexes(engine)
    assert set(found) == set(INDEXES.values())
    for table, name in INDEXES.items():
        assert f"ON public.{table} USING btree (expires_at)" in found[name]
    assert not set(RETIRED) & set(await user_columns(engine))
    assert await user_ids(engine) == set(ids)


async def test_downgrade_restores_the_columns_empty_and_drops_the_indexes(scratch: Scratch) -> None:
    url, engine = scratch
    ids = await seed(engine)
    await asyncio.to_thread(alembic, url, "upgrade", REVISION)

    await asyncio.to_thread(alembic, url, "downgrade", PREVIOUS)

    assert await expiry_indexes(engine) == {}
    columns = await user_columns(engine)
    assert columns["failed_login_attempts"][:2] == ("smallint", "NO")
    assert columns["failed_login_attempts"][2] == "0"
    assert columns["locked_until"][:2] == ("timestamp with time zone", "YES")
    async with engine.connect() as conn:
        rows = await conn.execute(text("SELECT failed_login_attempts, locked_until FROM users"))
        assert {tuple(r) for r in rows} == {(0, None)}  # values are not restored
    assert await user_ids(engine) == set(ids)


async def test_upgrading_again_after_a_downgrade_works(scratch: Scratch) -> None:
    url, engine = scratch
    await seed(engine)
    await asyncio.to_thread(alembic, url, "upgrade", REVISION)
    await asyncio.to_thread(alembic, url, "downgrade", PREVIOUS)

    await asyncio.to_thread(alembic, url, "upgrade", REVISION)

    assert set(await expiry_indexes(engine)) == set(INDEXES.values())
    assert not set(RETIRED) & set(await user_columns(engine))


def test_every_revision_id_fits_the_version_table() -> None:
    """``alembic_version.version_num`` is VARCHAR(32); a longer id fails only when it is applied."""
    config = Config()
    config.set_main_option("script_location", str(Path(__file__).resolve().parents[2] / "alembic"))
    scripts = ScriptDirectory.from_config(config)
    found = {script.revision for script in scripts.walk_revisions()}
    assert REVISION in found
    assert all(len(revision) <= 32 for revision in found), sorted(found, key=len)[-1]
