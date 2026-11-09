"""Migration 037 spells organisation one way (ADR 021).

Every column and foreign key is renamed, a new organisation still gets its default shift types,
and the downgrade restores both.

Covers: docs/adr/021-organisation-spelling-in-one-step.md; the default shift types of migration 020.
"""

import asyncio
import uuid
from collections.abc import AsyncGenerator

import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncEngine

from tests.integration.migration_support import Scratch, alembic, scratch_database
from tests.integration.test_migration_020 import run

SCRATCH_DB = "hds_migration_037_test"
PREVIOUS = "036_roster_edits"
REVISION = "037_organisation_spelling"


@pytest_asyncio.fixture
async def scratch() -> AsyncGenerator[Scratch, None]:
    async with scratch_database(SCRATCH_DB, PREVIOUS) as pair:
        yield pair


async def named(engine: AsyncEngine, word: str) -> tuple[int, int]:
    """How many columns and constraints carry ``word``'s spelling of the organisation id."""
    columns = await run(
        engine,
        "SELECT count(*) FROM information_schema.columns "
        "WHERE table_schema = 'public' AND column_name = :c",
        c=f"{word}_id",
    )
    constraints = await run(
        engine, "SELECT count(*) FROM pg_constraint WHERE conname LIKE :c", c=f"%{word}_id_fkey"
    )
    return columns.scalar_one(), constraints.scalar_one()


async def defaults_of_a_new_organisation(engine: AsyncEngine, column: str) -> int:
    """Insert an organisation; how many default shift types the trigger gave it."""
    org = uuid.uuid4()
    await run(
        engine,
        "INSERT INTO organisations (id, name, timezone) VALUES (:o, 'Org', 'Asia/Jerusalem')",
        o=org,
    )
    found = await run(engine, f"SELECT count(*) FROM shift_types WHERE {column} = :o", o=org)
    return int(found.scalar_one())


async def test_upgrade_renames_every_column_and_key_and_downgrade_restores_them(
    scratch: Scratch,
) -> None:
    url, engine = scratch
    assert await named(engine, "organization") == (22, 22)

    await asyncio.to_thread(alembic, url, "upgrade", REVISION)
    assert await named(engine, "organisation") == (22, 22)
    assert await named(engine, "organization") == (0, 0)
    assert await defaults_of_a_new_organisation(engine, "organisation_id") == 3

    await asyncio.to_thread(alembic, url, "downgrade", PREVIOUS)
    assert await named(engine, "organization") == (22, 22)
    assert await named(engine, "organisation") == (0, 0)
    assert await defaults_of_a_new_organisation(engine, "organization_id") == 3
