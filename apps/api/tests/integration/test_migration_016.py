"""Migration 016 makes account addresses unique regardless of letter case (T284, FR-215, FR-216).

Before the rule can take effect, accounts whose addresses differ only by case must be resolved by
staff: the migration stops and lists them, and changes nothing. With clean data it lowercases every
address and swaps the case-sensitive unique index for one on ``lower(email)``. It runs on its own
scratch database seeded at revision 015.

Covers: FR-215, FR-216, SC-208.
"""

import asyncio
import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine

from tests.integration.migration_support import Scratch, alembic, alembic_run, scratch_database

SCRATCH_DB = "hds_migration_016_test"
NOW = datetime(2026, 11, 2, 12, 0, tzinfo=UTC)
OLD, NEW = "uq_users_email", "uq_users_email_ci"


@pytest_asyncio.fixture
async def scratch() -> AsyncGenerator[tuple[str, AsyncEngine], None]:
    async with scratch_database(SCRATCH_DB, "015_refresh_revoked_reason") as pair:
        yield pair


async def add_org(engine: AsyncEngine) -> uuid.UUID:
    org = uuid.uuid4()
    async with engine.begin() as conn:
        await conn.execute(
            text("INSERT INTO organisations (id, name, timezone) VALUES (:i, 'Acme', 'UTC')"),
            {"i": org},
        )
    return org


async def add_user(
    engine: AsyncEngine, org: uuid.UUID, email: str, *, deleted: bool = False
) -> uuid.UUID:
    user = uuid.uuid4()
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO users (id, organization_id, email, password_hash, full_name, role, "
                "is_active, is_deleted, activated_at) VALUES (:i, :o, :e, 'h', 'U', "
                "CAST('employee' AS user_role), true, :d, :t)"
            ),
            {"i": user, "o": org, "e": email, "d": deleted, "t": NOW},
        )
    return user


async def emails(engine: AsyncEngine) -> set[str]:
    async with engine.connect() as conn:
        return {e for (e,) in await conn.execute(text("SELECT email FROM users"))}


async def indexes(engine: AsyncEngine) -> dict[str, str]:
    async with engine.connect() as conn:
        rows = await conn.execute(
            text("SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'users'")
        )
        return {name: definition for name, definition in rows}


async def test_clashing_addresses_stop_the_migration_and_it_lists_both_accounts(
    scratch: Scratch,
) -> None:
    url, engine = scratch
    org_a, org_b = await add_org(engine), await add_org(engine)
    first = await add_user(engine, org_a, "Bob@corp.com")
    second = await add_user(engine, org_b, "bob@corp.com")  # another organisation: same person
    await add_user(engine, org_a, "alone@corp.com")

    result = await asyncio.to_thread(alembic_run, url, "upgrade", "016_case_insensitive_email")

    assert result.returncode != 0
    message = result.stderr
    assert str(first) in message and str(second) in message, message[-800:]
    assert "alone@corp.com" not in message and "corp.com" not in message  # ids only, no addresses
    assert "resolve" in message.lower()
    # Nothing was changed: same addresses, old index, no new index.
    assert await emails(engine) == {"Bob@corp.com", "bob@corp.com", "alone@corp.com"}
    found = await indexes(engine)
    assert OLD in found and NEW not in found


async def test_a_deleted_account_does_not_clash_with_a_live_one(scratch: Scratch) -> None:
    url, engine = scratch
    org = await add_org(engine)
    await add_user(engine, org, "Bob@corp.com", deleted=True)
    await add_user(engine, org, "bob@corp.com")

    await asyncio.to_thread(alembic, url, "upgrade", "016_case_insensitive_email")

    assert await emails(engine) == {"bob@corp.com"}  # both lowercased, the same text, one deleted


async def test_clean_data_is_lowercased_and_the_index_is_replaced(scratch: Scratch) -> None:
    url, engine = scratch
    org = await add_org(engine)
    await add_user(engine, org, "Alice@Corp.com")
    await add_user(engine, org, "carol@corp.com")

    await asyncio.to_thread(alembic, url, "upgrade", "016_case_insensitive_email")

    assert await emails(engine) == {"alice@corp.com", "carol@corp.com"}
    found = await indexes(engine)
    assert OLD not in found
    definition = found[NEW].lower()
    assert "unique" in definition and "lower((email)::text)" in definition
    assert "is_deleted = false" in definition or "not is_deleted" in definition
    with pytest.raises(IntegrityError, match=NEW):
        await add_user(engine, org, "ALICE@corp.com")


async def test_downgrade_restores_the_case_sensitive_index_and_keeps_the_lowercase_data(
    scratch: Scratch,
) -> None:
    url, engine = scratch
    org = await add_org(engine)
    await add_user(engine, org, "Alice@Corp.com")
    await asyncio.to_thread(alembic, url, "upgrade", "016_case_insensitive_email")

    await asyncio.to_thread(alembic, url, "downgrade", "015_refresh_revoked_reason")

    found = await indexes(engine)
    assert NEW not in found and OLD in found
    assert await emails(engine) == {"alice@corp.com"}  # the lowercasing is not reversed
    await add_user(engine, org, "ALICE@corp.com")  # the old index allows it again


async def test_a_failure_after_the_lowercasing_leaves_every_address_as_it_was(
    scratch: Scratch,
) -> None:
    """The lowercasing, the drop of the old index and the new index are one transaction.

    A stray object already named ``uq_users_email_ci`` makes the LAST step fail, after the
    addresses were lowercased and the old index dropped. Nothing of that may remain.
    """
    url, engine = scratch
    org = await add_org(engine)
    await add_user(engine, org, "Alice@Corp.com")
    await add_user(engine, org, "carol@corp.com")
    async with engine.begin() as conn:
        await conn.execute(text(f"CREATE TABLE {NEW} (x int)"))  # the name is taken

    result = await asyncio.to_thread(alembic_run, url, "upgrade", "016_case_insensitive_email")

    assert result.returncode != 0 and NEW in result.stderr
    assert await emails(engine) == {"Alice@Corp.com", "carol@corp.com"}
    assert OLD in await indexes(engine)
    async with engine.connect() as conn:
        revision = (
            await conn.execute(text("SELECT version_num FROM alembic_version"))
        ).scalar_one()
    assert revision == "015_refresh_revoked_reason"


async def test_an_invitation_issued_before_the_migration_still_belongs_to_the_same_account(
    scratch: Scratch,
) -> None:
    """Links hold an account id, not an address, so lowercasing the address cannot orphan them."""
    url, engine = scratch
    org = await add_org(engine)
    pending = await add_user(engine, org, "New.Hire@Corp.com")
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO invitation_tokens (id, user_id, token_hash, purpose, expires_at) "
                "VALUES (:i, :u, 'abc123', 'invite', :x)"
            ),
            {"i": uuid.uuid4(), "u": pending, "x": datetime(2027, 1, 1, tzinfo=UTC)},
        )

    await asyncio.to_thread(alembic, url, "upgrade", "016_case_insensitive_email")

    async with engine.connect() as conn:
        row = (
            await conn.execute(
                text(
                    "SELECT u.id, u.email, t.used_at FROM invitation_tokens t "
                    "JOIN users u ON u.id = t.user_id WHERE t.token_hash = 'abc123'"
                )
            )
        ).one()
    assert (row.id, row.email, row.used_at) == (pending, "new.hire@corp.com", None)
