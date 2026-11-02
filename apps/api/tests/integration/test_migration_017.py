"""Migration 017 corrects accounts that migration 014 left pending by mistake (T334, F07).

Migration 014 called an account pending if it was inactive, held an invitation link and had never
used one. A staff-created employee who was deactivated and re-invited under the old code matches, so
a leftover link could reactivate them. Migration 017 marks an account as once active when there is
evidence of it: a refresh token, or any reset link, because reset links only go to active accounts.

Invitation links are NOT evidence. Cancelling a link also sets ``used_at``, and re-inviting a
pending account cancels its earlier link, so a genuine invitee can hold a "used" invitation link.
Accounts that cannot be decided stay pending and are listed by id. Runs on a scratch database at
revision 016.

Covers: FR-229.
"""

import asyncio
import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from tests.integration.migration_support import Scratch, alembic, alembic_run, scratch_database

SCRATCH_DB = "hds_migration_017_test"
PREVIOUS = "016_case_insensitive_email"
REVISION = "017_pending_backfill_correction"
NOW = datetime.now(UTC)
CREATED = NOW - timedelta(days=90)
FUTURE = NOW + timedelta(days=3)
PAST = NOW - timedelta(days=30)


@pytest_asyncio.fixture
async def scratch() -> AsyncGenerator[Scratch, None]:
    async with scratch_database(SCRATCH_DB, PREVIOUS) as pair:
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
    engine: AsyncEngine,
    org: uuid.UUID,
    email: str,
    *,
    active: bool = False,
    activated: datetime | None = None,
    deleted: bool = False,
) -> uuid.UUID:
    user = uuid.uuid4()
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO users (id, organization_id, email, password_hash, full_name, role, "
                "is_active, is_deleted, activated_at, created_at) VALUES (:i, :o, :e, 'h', 'U', "
                "CAST('employee' AS user_role), :a, :d, :t, :c)"
            ),
            {
                "i": user,
                "o": org,
                "e": email,
                "a": active,
                "d": deleted,
                "t": activated,
                "c": CREATED,
            },
        )
    return user


async def add_link(
    engine: AsyncEngine,
    user: uuid.UUID,
    purpose: str,
    *,
    used: bool = False,
    expires: datetime = FUTURE,
) -> None:
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO invitation_tokens "
                "(id, user_id, token_hash, purpose, expires_at, used_at) "
                "VALUES (:i, :u, :h, :p, :x, :at)"
            ),
            {
                "i": uuid.uuid4(),
                "u": user,
                "h": uuid.uuid4().hex,
                "p": purpose,
                "x": expires,
                "at": NOW if used else None,
            },
        )


async def add_session(engine: AsyncEngine, user: uuid.UUID) -> None:
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO refresh_tokens (id, user_id, token_hash, expires_at) "
                "VALUES (:i, :u, :h, :x)"
            ),
            {"i": uuid.uuid4(), "u": user, "h": uuid.uuid4().hex, "x": PAST},
        )


async def activated(engine: AsyncEngine, user: uuid.UUID) -> Any:
    async with engine.connect() as conn:
        return (
            await conn.execute(text("SELECT activated_at FROM users WHERE id = :u"), {"u": user})
        ).scalar_one()


async def snapshot(engine: AsyncEngine) -> list[Any]:
    async with engine.connect() as conn:
        rows = await conn.execute(text("SELECT id, is_active, activated_at FROM users ORDER BY id"))
        return [tuple(r) for r in rows]


async def upgrade(url: str) -> str:
    result = await asyncio.to_thread(alembic_run, url, "upgrade", REVISION)
    assert result.returncode == 0, result.stderr[-1500:]
    return result.stdout + result.stderr


async def test_any_reset_link_proves_the_account_was_active(scratch: Scratch) -> None:
    url, engine = scratch
    org = await add_org(engine)
    used_reset = await add_user(engine, org, "used@acme.test")
    await add_link(engine, used_reset, "invite")
    await add_link(engine, used_reset, "reset", used=True)
    unused_reset = await add_user(engine, org, "unused@acme.test")
    await add_link(engine, unused_reset, "invite")
    await add_link(engine, unused_reset, "reset", expires=PAST)

    await upgrade(url)

    assert await activated(engine, used_reset) == CREATED
    assert await activated(engine, unused_reset) == CREATED


async def test_a_session_history_proves_the_account_was_active(scratch: Scratch) -> None:
    url, engine = scratch
    org = await add_org(engine)
    user = await add_user(engine, org, "signed.in@acme.test")
    await add_link(engine, user, "invite")
    await add_session(engine, user)

    await upgrade(url)

    assert await activated(engine, user) == CREATED


async def test_invitation_links_are_not_evidence(scratch: Scratch) -> None:
    url, engine = scratch
    org = await add_org(engine)
    re_invited = await add_user(engine, org, "re.invited@acme.test")
    await add_link(engine, re_invited, "invite", used=True)  # cancelled by the re-invitation
    await add_link(engine, re_invited, "invite")
    only_cancelled = await add_user(engine, org, "cancelled@acme.test")
    await add_link(engine, only_cancelled, "invite", used=True)

    await upgrade(url)

    assert await activated(engine, re_invited) is None
    assert await activated(engine, only_cancelled) is None


async def test_a_genuine_pending_invitee_stays_pending(scratch: Scratch) -> None:
    url, engine = scratch
    org = await add_org(engine)
    invitee = await add_user(engine, org, "invitee@acme.test")
    await add_link(engine, invitee, "invite")

    output = await upgrade(url)

    assert await activated(engine, invitee) is None
    assert str(invitee) not in output  # its link is live, so it is not undecidable


async def test_undecidable_accounts_stay_pending_and_are_listed_by_id_only(
    scratch: Scratch,
) -> None:
    url, engine = scratch
    org = await add_org(engine)
    expired = await add_user(engine, org, "expired@acme.test")
    await add_link(engine, expired, "invite", expires=PAST)
    renewed = await add_user(engine, org, "renewed@acme.test")
    await add_link(engine, renewed, "invite", expires=PAST)
    await add_link(engine, renewed, "invite", expires=FUTURE)  # the latest link decides

    output = await upgrade(url)

    assert await activated(engine, expired) is None
    assert str(expired) in output
    assert str(renewed) not in output
    assert "acme.test" not in output


async def test_other_accounts_are_untouched(scratch: Scratch) -> None:
    url, engine = scratch
    org = await add_org(engine)
    stamped = NOW - timedelta(days=5)
    active = await add_user(engine, org, "active@acme.test", active=True, activated=stamped)
    await add_session(engine, active)
    deactivated = await add_user(engine, org, "off@acme.test", activated=stamped)
    await add_link(engine, deactivated, "reset")
    deleted = await add_user(engine, org, "gone@acme.test", deleted=True)
    await add_session(engine, deleted)

    await upgrade(url)

    assert await activated(engine, active) == stamped
    assert await activated(engine, deactivated) == stamped
    assert await activated(engine, deleted) is None


async def test_the_correction_is_stable_and_the_downgrade_changes_no_data(
    scratch: Scratch,
) -> None:
    url, engine = scratch
    org = await add_org(engine)
    user = await add_user(engine, org, "signed.in@acme.test")
    await add_session(engine, user)
    await upgrade(url)
    corrected = await snapshot(engine)

    await asyncio.to_thread(alembic, url, "downgrade", PREVIOUS)
    assert await snapshot(engine) == corrected

    await upgrade(url)  # the step runs a second time
    assert await snapshot(engine) == corrected
