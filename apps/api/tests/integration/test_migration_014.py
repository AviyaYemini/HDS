"""Migration 014 records when an account was first activated (T236, research D4, data-model).

Builds a realistic database at revision 013 (active, deactivated and pending accounts, with the
invitation links that distinguish them), upgrades to 014 and checks three things: the backfill
marks exactly the never-activated accounts as pending, the check constraint then stops an active
account from lacking an activation time, and the pre-check lists any row the constraint would
reject instead of failing with a bare database error. It also checks the downgrade.

The migration runs on its own scratch database (name ending in ``_test``, created and dropped by
the fixture) so the shared test database is never moved between revisions.

Covers: FR-205, FR-207.
"""

import asyncio
import importlib.util
import os
import subprocess
import sys
import uuid
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool

API_DIR = Path(__file__).resolve().parents[2]
MIGRATION_FILE = API_DIR / "alembic" / "versions" / "014_user_activation.py"
BASE_URL = os.environ.get(
    "TEST_DATABASE_URL", "postgresql+asyncpg://hds:hds_dev@localhost:5432/hds_test"
)
SCRATCH_DB = "hds_migration_014_test"  # must end in _test: it is dropped after every test
CONSTRAINT = "ck_users_active_has_activated_at"


def alembic(url: str, *args: str) -> None:
    """Run an Alembic command against ``url`` and fail loudly with its output."""
    result = subprocess.run(
        [sys.executable, "-m", "alembic", *args],
        cwd=API_DIR,
        env={**os.environ, "DATABASE_URL": url},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, f"alembic {' '.join(args)} failed:\n{result.stderr[-1500:]}"


async def admin_execute(sql: str) -> None:
    engine = create_async_engine(
        make_url(BASE_URL).set(database="postgres"),
        isolation_level="AUTOCOMMIT",
        poolclass=NullPool,
    )
    try:
        async with engine.connect() as conn:
            await conn.execute(text(sql))
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def scratch() -> AsyncGenerator[tuple[str, AsyncEngine], None]:
    """A fresh database migrated to 013, with an engine on it; dropped afterwards."""
    assert SCRATCH_DB.endswith("_test")
    await admin_execute(f'DROP DATABASE IF EXISTS "{SCRATCH_DB}" WITH (FORCE)')
    await admin_execute(f'CREATE DATABASE "{SCRATCH_DB}"')
    url = make_url(BASE_URL).set(database=SCRATCH_DB).render_as_string(hide_password=False)
    await asyncio.to_thread(alembic, url, "upgrade", "013_membership_cascade")
    engine = create_async_engine(url, poolclass=NullPool)
    try:
        yield url, engine
    finally:
        await engine.dispose()
        await admin_execute(f'DROP DATABASE IF EXISTS "{SCRATCH_DB}" WITH (FORCE)')


CREATED = datetime(2026, 1, 10, 9, 0, tzinfo=UTC)


async def seed(engine: AsyncEngine) -> dict[str, dict[str, Any]]:
    """Insert a realistic organisation as it looks at revision 013.

    Returns a map of account name to ``{"id", "created_at", "pending"}`` where ``pending`` says
    whether the migration must leave ``activated_at`` empty.
    """
    org = uuid.uuid4()
    client = uuid.uuid4()
    accounts: dict[str, dict[str, Any]] = {}
    async with engine.begin() as conn:
        await conn.execute(
            text("INSERT INTO organisations (id, name, timezone) VALUES (:i, 'Acme', 'UTC')"),
            {"i": org},
        )
        await conn.execute(
            text(
                "INSERT INTO clients (id, organization_id, name, contract_start) "
                "VALUES (:i, :o, 'Client Ltd', '2026-01-01')"
            ),
            {"i": client, "o": org},
        )

        async def user(
            name: str,
            role: str,
            *,
            active: bool,
            deleted: bool = False,
            days: int,
            pending: bool,
            client_id: uuid.UUID | None = None,
        ) -> uuid.UUID:
            uid = uuid.uuid4()
            created = CREATED + timedelta(days=days)
            await conn.execute(
                text(
                    "INSERT INTO users (id, organization_id, email, password_hash, full_name, "
                    "role, is_active, is_deleted, client_id, created_at) VALUES (:i, :o, :e, "
                    "'hash', :n, CAST(:r AS user_role), :a, :d, :c, :t)"
                ),
                {
                    "i": uid,
                    "o": org,
                    "e": f"{name}@acme.test",
                    "n": name,
                    "r": role,
                    "a": active,
                    "d": deleted,
                    "c": client_id,
                    "t": created,
                },
            )
            accounts[name] = {"id": uid, "created_at": created, "pending": pending}
            return uid

        async def link(user_id: uuid.UUID, purpose: str, *, used: bool) -> None:
            await conn.execute(
                text(
                    "INSERT INTO invitation_tokens (id, user_id, token_hash, purpose, "
                    "expires_at, used_at) VALUES (:i, :u, :h, :p, :x, :used)"
                ),
                {
                    "i": uuid.uuid4(),
                    "u": user_id,
                    "h": uuid.uuid4().hex,
                    "p": purpose,
                    "x": CREATED + timedelta(days=400),
                    "used": CREATED if used else None,
                },
            )

        # Accounts that were active at some point: activated_at must equal created_at.
        await user("admin", "admin", active=True, days=0, pending=False)
        await user("manager", "manager", active=True, days=1, pending=False)
        await user("employee", "employee", active=True, days=2, pending=False)
        await user("portal", "client", active=True, days=3, pending=False, client_id=client)
        await user("deleted-active", "employee", active=True, deleted=True, days=4, pending=False)
        # Deactivated by staff after a created-with-password start: no invitation at all.
        await user("deactivated-direct", "employee", active=False, days=5, pending=False)
        # Deactivated after accepting an invitation.
        accepted = await user(
            "deactivated-accepted", "employee", active=False, days=6, pending=False
        )
        await link(accepted, "invite", used=True)
        # Accepted, deactivated, then re-invited: the old used link proves it was active once.
        reinvited = await user(
            "deactivated-reinvited", "manager", active=False, days=7, pending=False
        )
        await link(reinvited, "invite", used=True)
        await link(reinvited, "invite", used=False)
        # An inactive account holding only a reset link was active (reset is for active accounts).
        reset_only = await user(
            "deactivated-reset", "employee", active=False, days=8, pending=False
        )
        await link(reset_only, "reset", used=False)

        # Never activated: inactive with an unused invitation and no used one.
        fresh = await user("pending-fresh", "employee", active=False, days=9, pending=True)
        await link(fresh, "invite", used=False)
        twice = await user("pending-reinvited", "employee", active=False, days=10, pending=True)
        await link(twice, "invite", used=False)
        await link(twice, "invite", used=False)
    return accounts


async def activation_times(engine: AsyncEngine) -> dict[str, datetime | None]:
    async with engine.connect() as conn:
        rows = await conn.execute(text("SELECT full_name, activated_at FROM users"))
        return {name: at for name, at in rows}


async def test_upgrade_marks_exactly_the_never_activated_accounts_as_pending(scratch) -> None:
    url, engine = scratch
    accounts = await seed(engine)
    await asyncio.to_thread(alembic, url, "upgrade", "014_user_activation")

    times = await activation_times(engine)
    assert set(times) == set(accounts)
    for name, info in accounts.items():
        if info["pending"]:
            assert times[name] is None, f"{name} was never activated"
        else:
            assert times[name] == info["created_at"], f"{name} must be backfilled with created_at"
    assert sorted(n for n, t in times.items() if t is None) == [
        "pending-fresh",
        "pending-reinvited",
    ]


async def test_check_constraint_rejects_an_active_account_without_an_activation_time(
    scratch,
) -> None:
    url, engine = scratch
    await seed(engine)
    await asyncio.to_thread(alembic, url, "upgrade", "014_user_activation")

    async def insert(active: bool, activated_at: datetime | None) -> None:
        async with engine.begin() as conn:
            org = (await conn.execute(text("SELECT id FROM organisations"))).scalar_one()
            await conn.execute(
                text(
                    "INSERT INTO users (id, organization_id, email, password_hash, full_name, "
                    "role, is_active, activated_at) VALUES (:i, :o, :e, 'h', 'x', "
                    "CAST('employee' AS user_role), :a, :t)"
                ),
                {
                    "i": uuid.uuid4(),
                    "o": org,
                    "e": f"{uuid.uuid4().hex}@acme.test",
                    "a": active,
                    "t": activated_at,
                },
            )

    with pytest.raises(IntegrityError, match=CONSTRAINT):
        await insert(True, None)
    await insert(False, None)  # a pending account is fine
    await insert(True, datetime.now(UTC))  # an active account with a time is fine
    await insert(False, datetime.now(UTC))  # a deactivated account keeps its time


def load_migration() -> Any:
    spec = importlib.util.spec_from_file_location("migration_014", MIGRATION_FILE)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


async def test_pre_check_lists_the_rows_the_constraint_would_reject(scratch) -> None:
    """If a row would break the constraint, the migration stops and names it, not a bare error."""
    url, engine = scratch
    accounts = await seed(engine)
    await asyncio.to_thread(alembic, url, "upgrade", "014_user_activation")
    async with engine.begin() as conn:
        await conn.execute(text(f"ALTER TABLE users DROP CONSTRAINT {CONSTRAINT}"))
        await conn.execute(
            text("UPDATE users SET activated_at = NULL WHERE id IN (:a, :b)"),
            {"a": accounts["admin"]["id"], "b": accounts["employee"]["id"]},
        )
        rows = await conn.run_sync(lambda sync_conn: _run_pre_check(sync_conn))
    assert isinstance(rows, RuntimeError)
    message = str(rows)
    assert "2 active" in message
    assert str(accounts["admin"]["id"]) in message
    assert str(accounts["employee"]["id"]) in message
    assert str(accounts["manager"]["id"]) not in message  # healthy rows are not listed
    assert "@" not in message  # identifiers only: the output is not a list of addresses


def _run_pre_check(sync_conn: Any) -> RuntimeError | None:
    try:
        load_migration().ensure_no_rejected_rows(sync_conn)
    except RuntimeError as error:
        return error
    return None


async def test_pre_check_is_silent_when_nothing_would_be_rejected(scratch) -> None:
    url, engine = scratch
    await seed(engine)
    await asyncio.to_thread(alembic, url, "upgrade", "014_user_activation")
    async with engine.connect() as conn:
        assert await conn.run_sync(_run_pre_check) is None


async def test_downgrade_removes_the_constraint_and_the_column_and_keeps_the_accounts(
    scratch,
) -> None:
    url, engine = scratch
    accounts = await seed(engine)
    await asyncio.to_thread(alembic, url, "upgrade", "014_user_activation")
    await asyncio.to_thread(alembic, url, "downgrade", "013_membership_cascade")

    async with engine.connect() as conn:
        columns = {
            c
            for (c,) in await conn.execute(
                text("SELECT column_name FROM information_schema.columns WHERE table_name='users'")
            )
        }
        constraints = {
            c
            for (c,) in await conn.execute(
                text("SELECT conname FROM pg_constraint WHERE conrelid = 'users'::regclass")
            )
        }
        count = (await conn.execute(text("SELECT count(*) FROM users"))).scalar_one()
    assert "activated_at" not in columns
    assert CONSTRAINT not in constraints
    assert count == len(accounts)  # no account is lost

    # And it can go up again, giving the same answer for the same data.
    await asyncio.to_thread(alembic, url, "upgrade", "014_user_activation")
    again = await activation_times(engine)
    assert sorted(n for n, t in again.items() if t is None) == [
        "pending-fresh",
        "pending-reinvited",
    ]
