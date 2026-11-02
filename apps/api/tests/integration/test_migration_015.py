"""Migration 015 records why a refresh token was revoked (T254, research D3).

Only a token revoked by *rotation* may trigger the "someone is replaying a stolen token" response,
so the reason has to be stored. Tokens revoked before this change keep an empty reason, which the
service treats as "unknown, refuse and do nothing else". The migration runs on its own scratch
database seeded at revision 014 with live, expired and already-revoked tokens.

Covers: FR-209.
"""

import asyncio
import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine

from tests.integration.migration_support import Scratch, alembic, scratch_database

SCRATCH_DB = "hds_migration_015_test"
CONSTRAINT = "ck_refresh_tokens_revoked_reason"
REASONS = ["rotated", "logout", "all", "account"]
NOW = datetime(2026, 11, 2, 12, 0, tzinfo=UTC)


@pytest_asyncio.fixture
async def scratch() -> AsyncGenerator[tuple[str, AsyncEngine], None]:
    async with scratch_database(SCRATCH_DB, "014_user_activation") as pair:
        yield pair


async def seed(engine: AsyncEngine) -> dict[str, uuid.UUID]:
    """One user with a live token, an expired one and two that were revoked before 015."""
    org, user = uuid.uuid4(), uuid.uuid4()
    ids = {name: uuid.uuid4() for name in ("live", "expired", "revoked_old", "revoked_older")}
    async with engine.begin() as conn:
        await conn.execute(
            text("INSERT INTO organisations (id, name, timezone) VALUES (:i, 'Acme', 'UTC')"),
            {"i": org},
        )
        await conn.execute(
            text(
                "INSERT INTO users (id, organization_id, email, password_hash, full_name, role, "
                "is_active, activated_at) VALUES (:i, :o, 'u@acme.test', 'h', 'U', "
                "CAST('employee' AS user_role), true, :t)"
            ),
            {"i": user, "o": org, "t": NOW},
        )
        for name, (expires, revoked) in {
            "live": (NOW + timedelta(days=7), None),
            "expired": (NOW - timedelta(days=1), None),
            "revoked_old": (NOW + timedelta(days=7), NOW - timedelta(days=2)),
            "revoked_older": (NOW + timedelta(days=7), NOW - timedelta(days=5)),
        }.items():
            await conn.execute(
                text(
                    "INSERT INTO refresh_tokens (id, user_id, token_hash, expires_at, revoked_at) "
                    "VALUES (:i, :u, :h, :x, :r)"
                ),
                {"i": ids[name], "u": user, "h": uuid.uuid4().hex, "x": expires, "r": revoked},
            )
    return ids


async def reasons(engine: AsyncEngine) -> dict[uuid.UUID, str | None]:
    async with engine.connect() as conn:
        rows = await conn.execute(text("SELECT id, revoked_reason FROM refresh_tokens"))
        return {i: r for i, r in rows}


async def test_upgrade_adds_a_nullable_column_and_leaves_existing_tokens_unexplained(
    scratch: Scratch,
) -> None:
    url, engine = scratch
    ids = await seed(engine)
    await asyncio.to_thread(alembic, url, "upgrade", "015_refresh_revoked_reason")

    async with engine.connect() as conn:
        column = (
            await conn.execute(
                text(
                    "SELECT data_type, is_nullable, character_maximum_length "
                    "FROM information_schema.columns "
                    "WHERE table_name = 'refresh_tokens' AND column_name = 'revoked_reason'"
                )
            )
        ).one()
    assert column.is_nullable == "YES" and column.data_type == "character varying"
    assert column.character_maximum_length is not None and column.character_maximum_length <= 16
    # Tokens revoked before this change have no recorded reason: the service treats that as
    # "unknown" and refuses without ending any other session.
    assert set((await reasons(engine)).values()) == {None}
    assert set(await reasons(engine)) == set(ids.values())


async def insert_revoked(engine: AsyncEngine, reason: str | None) -> None:
    async with engine.begin() as conn:
        user = (await conn.execute(text("SELECT id FROM users"))).scalar_one()
        await conn.execute(
            text(
                "INSERT INTO refresh_tokens (id, user_id, token_hash, expires_at, revoked_at, "
                "revoked_reason) VALUES (:i, :u, :h, :x, :r, :why)"
            ),
            {
                "i": uuid.uuid4(),
                "u": user,
                "h": uuid.uuid4().hex,
                "x": NOW + timedelta(days=7),
                "r": NOW,
                "why": reason,
            },
        )


@pytest.mark.parametrize("reason", [*REASONS, None])
async def test_the_four_reasons_and_null_are_accepted(scratch: Scratch, reason: str | None) -> None:
    url, engine = scratch
    await seed(engine)
    await asyncio.to_thread(alembic, url, "upgrade", "015_refresh_revoked_reason")
    await insert_revoked(engine, reason)


@pytest.mark.parametrize("reason", ["stolen", "", "ROTATED", "rotated "])
async def test_any_other_reason_is_rejected_by_the_database(scratch: Scratch, reason: str) -> None:
    url, engine = scratch
    await seed(engine)
    await asyncio.to_thread(alembic, url, "upgrade", "015_refresh_revoked_reason")
    with pytest.raises(IntegrityError, match=CONSTRAINT):
        await insert_revoked(engine, reason)


async def test_downgrade_removes_the_constraint_and_the_column_and_keeps_the_tokens(
    scratch: Scratch,
) -> None:
    url, engine = scratch
    ids = await seed(engine)
    await asyncio.to_thread(alembic, url, "upgrade", "015_refresh_revoked_reason")
    await insert_revoked(engine, "rotated")
    await asyncio.to_thread(alembic, url, "downgrade", "014_user_activation")

    async with engine.connect() as conn:
        columns = {
            c
            for (c,) in await conn.execute(
                text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = 'refresh_tokens'"
                )
            )
        }
        constraints = {
            c
            for (c,) in await conn.execute(
                text(
                    "SELECT conname FROM pg_constraint WHERE conrelid = 'refresh_tokens'::regclass"
                )
            )
        }
        kept = (await conn.execute(text("SELECT count(*) FROM refresh_tokens"))).scalar_one()
    assert "revoked_reason" not in columns
    assert CONSTRAINT not in constraints
    assert kept == len(ids) + 1  # no token is lost

    await asyncio.to_thread(alembic, url, "upgrade", "015_refresh_revoked_reason")  # and back up
