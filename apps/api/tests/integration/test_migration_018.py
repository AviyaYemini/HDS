"""Migration 018 adds the table behind the sign-in lockout and reset-request limit (T339).

One row per address, network address or reset key. It mirrors the failure count and lock that the
users table holds, so an address with no account can be treated by the same rule. Runs on a scratch
database at revision 017.

Covers: FR-221, FR-247.
"""

import asyncio
from collections.abc import AsyncGenerator
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine

from tests.integration.migration_support import Scratch, alembic, scratch_database

SCRATCH_DB = "hds_migration_018_test"
PREVIOUS = "017_pending_backfill_correction"
REVISION = "018_login_throttle"


@pytest_asyncio.fixture
async def scratch() -> AsyncGenerator[Scratch, None]:
    async with scratch_database(SCRATCH_DB, PREVIOUS) as pair:
        yield pair


async def columns(engine: AsyncEngine) -> dict[str, tuple[Any, ...]]:
    async with engine.connect() as conn:
        rows = await conn.execute(
            text(
                "SELECT column_name, data_type, character_maximum_length, is_nullable, "
                "column_default IS NOT NULL FROM information_schema.columns "
                "WHERE table_name = 'login_throttle'"
            )
        )
        return {r[0]: tuple(r[1:]) for r in rows}


async def insert(engine: AsyncEngine, kind: str, key: str, failures: int = 0) -> None:
    async with engine.begin() as conn:
        await conn.execute(
            text("INSERT INTO login_throttle (kind, key_hash, failures) VALUES (:k, :h, :f)"),
            {"k": kind, "h": key, "f": failures},
        )


async def test_upgrade_creates_the_table_as_specified(scratch: Scratch) -> None:
    url, engine = scratch

    await asyncio.to_thread(alembic, url, "upgrade", REVISION)

    found = await columns(engine)
    assert set(found) == {
        "kind",
        "key_hash",
        "failures",
        "window_started_at",
        "locked_until",
        "updated_at",
    }
    assert found["key_hash"][1] == 64
    assert found["kind"][2] == "NO" and found["key_hash"][2] == "NO"
    assert found["failures"] == ("integer", None, "NO", True)
    assert found["window_started_at"][0] == "timestamp with time zone"
    assert found["window_started_at"][2] == "YES"
    assert found["locked_until"][0] == "timestamp with time zone"
    assert found["locked_until"][2] == "YES"
    assert found["updated_at"] == ("timestamp with time zone", None, "NO", True)
    async with engine.connect() as conn:
        indexes = await conn.execute(
            text("SELECT indexdef FROM pg_indexes WHERE tablename = 'login_throttle'")
        )
        definitions = [d for (d,) in indexes]
    assert any("ix_login_throttle_updated_at" in d and "(updated_at)" in d for d in definitions)
    assert any("(kind, key_hash)" in d for d in definitions)


@pytest.mark.parametrize("kind", ["address", "network", "reset"])
async def test_the_three_kinds_are_accepted(scratch: Scratch, kind: str) -> None:
    url, engine = scratch
    await asyncio.to_thread(alembic, url, "upgrade", REVISION)

    await insert(engine, kind, "a" * 64)


async def test_another_kind_and_a_negative_count_are_rejected(scratch: Scratch) -> None:
    url, engine = scratch
    await asyncio.to_thread(alembic, url, "upgrade", REVISION)

    with pytest.raises(IntegrityError, match="ck_login_throttle_kind"):
        await insert(engine, "ip", "a" * 64)
    with pytest.raises(IntegrityError, match="ck_login_throttle_failures"):
        await insert(engine, "address", "a" * 64, failures=-1)


async def test_one_row_per_kind_and_key(scratch: Scratch) -> None:
    url, engine = scratch
    await asyncio.to_thread(alembic, url, "upgrade", REVISION)
    await insert(engine, "address", "a" * 64)
    await insert(engine, "network", "a" * 64)  # the same key under another kind is a different row

    with pytest.raises(IntegrityError):
        await insert(engine, "address", "a" * 64)


async def test_downgrade_drops_the_table_and_upgrading_again_works(scratch: Scratch) -> None:
    url, engine = scratch
    await asyncio.to_thread(alembic, url, "upgrade", REVISION)
    await insert(engine, "address", "a" * 64)

    await asyncio.to_thread(alembic, url, "downgrade", PREVIOUS)
    assert await columns(engine) == {}

    await asyncio.to_thread(alembic, url, "upgrade", REVISION)
    assert await columns(engine) != {}
