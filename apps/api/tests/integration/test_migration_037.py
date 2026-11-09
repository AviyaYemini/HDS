"""Migration 037 spells organisation one way (ADR 021).

Every column and foreign key is renamed, rows from before keep their organisation, a stored
replay takes the new field name, a new organisation still gets its default shift types, and the
downgrade restores all of it.

Covers: docs/adr/021-organisation-spelling-in-one-step.md; the default shift types of migration 020.
"""

import asyncio
import uuid
from collections.abc import AsyncGenerator
from typing import Any

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


async def client_of(engine: AsyncEngine, column: str, client: uuid.UUID) -> Any:
    """The organisation a client row belongs to, and that organisation's name, by ``column``."""
    found = await run(
        engine,
        f"SELECT c.{column}, o.name FROM clients c JOIN organisations o ON o.id = c.{column} "
        "WHERE c.id = :c",
        c=client,
    )
    return found.one()


async def test_upgrade_renames_every_column_and_key_and_downgrade_restores_them(
    scratch: Scratch,
) -> None:
    url, engine = scratch
    assert await named(engine, "organization") == (22, 22)
    # A row from before, to follow through the round trip, and a stored replay naming the field.
    org, client = uuid.uuid4(), uuid.uuid4()
    await run(
        engine,
        "INSERT INTO organisations (id, name, timezone) VALUES (:o, 'Before', 'Asia/Jerusalem')",
        o=org,
    )
    await run(
        engine,
        "INSERT INTO clients (id, organization_id, name, contract_start) "
        "VALUES (:c, :o, 'Kept', DATE '2026-01-01')",
        c=client,
        o=org,
    )
    owner = uuid.uuid4()
    await run(
        engine,
        "INSERT INTO users (id, organization_id, email, password_hash, full_name, role, "
        "activated_at) VALUES (:u, :o, 'replay@hds.test', 'h', 'User', CAST('admin' AS user_role), "
        "now())",
        u=owner,
        o=org,
    )
    await run(
        engine,
        "INSERT INTO idempotency_keys (user_id, key, request_hash, status_code, response_body) "
        "VALUES (:u, 'k', :h, 201, CAST(:b AS jsonb))",
        u=owner,
        h="0" * 64,
        b='{"id": "x", "organization_id": "y"}',
    )

    await asyncio.to_thread(alembic, url, "upgrade", REVISION)
    assert await named(engine, "organisation") == (22, 22)
    assert await named(engine, "organization") == (0, 0)
    assert tuple(await client_of(engine, "organisation_id", client)) == (org, "Before")
    assert await defaults_of_a_new_organisation(engine, "organisation_id") == 3
    stored = await run(engine, "SELECT response_body::text FROM idempotency_keys WHERE key = 'k'")
    assert '"organisation_id"' in stored.scalar_one()

    await asyncio.to_thread(alembic, url, "downgrade", PREVIOUS)
    assert await named(engine, "organization") == (22, 22)
    assert await named(engine, "organisation") == (0, 0)
    assert tuple(await client_of(engine, "organization_id", client)) == (org, "Before")
    assert await defaults_of_a_new_organisation(engine, "organization_id") == 3
    stored = await run(engine, "SELECT response_body::text FROM idempotency_keys WHERE key = 'k'")
    assert '"organization_id"' in stored.scalar_one()
