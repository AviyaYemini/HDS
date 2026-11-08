"""The migration command runs once under an advisory lock, and leaves a newer database alone (T036).

``python -m hds.scripts.migrate`` is the chart's migration Job. It takes ``pg_advisory_lock`` before
``alembic upgrade head``, so two Jobs at once (a retried hook, two releases) never migrate together:
the second waits, then finds the database at head and changes nothing. A database at a revision the
code does not know is newer (the previous image after a rollback, FR-016); it is left as it is and
the command exits 0, where a bare ``alembic upgrade head`` would stop on the unknown revision.

Each test runs on a scratch database of its own, and holds the lock itself first, so the two
processes are sure to meet it.

Covers: 006 FR-016; research R5.
"""

import asyncio
import os
import subprocess
import sys
from collections.abc import AsyncGenerator
from pathlib import Path

import asyncpg  # type: ignore[import-untyped]
import pytest_asyncio
from sqlalchemy.engine import make_url

from hds.scripts.migrate import LOCK_NAME
from tests.integration.migration_support import (
    Scratch,
    alembic,
    head_and_previous,
    scratch_database,
)
from tests.integration.test_migration_020 import run

API_DIR = Path(__file__).resolve().parents[2]
HEAD, PREVIOUS = head_and_previous()
WAIT_SECONDS = 120


@pytest_asyncio.fixture
async def scratch() -> AsyncGenerator[Scratch, None]:
    async with scratch_database("hds_migrate_lock_test", PREVIOUS) as pair:
        yield pair


def plain(url: str) -> str:
    return make_url(url).set(drivername="postgresql").render_as_string(hide_password=False)


def start(url: str) -> subprocess.Popen[str]:
    env = {**os.environ, "DATABASE_URL": url, "ENVIRONMENT": "test"}
    env.setdefault("JWT_SECRET_KEY", "a-test-only-signing-key-for-migrate")
    return subprocess.Popen(
        [sys.executable, "-m", "hds.scripts.migrate"],
        cwd=API_DIR,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


async def finish(process: subprocess.Popen[str]) -> tuple[int, str]:
    """Wait for a migrate process; one still running at the timeout is killed, not left behind."""
    try:
        out, _ = await asyncio.to_thread(process.communicate, None, WAIT_SECONDS)
    except subprocess.TimeoutExpired:
        process.kill()
        process.communicate()
        raise
    return process.returncode, out


async def revision(scratch: Scratch) -> str:
    _, engine = scratch
    return str((await run(engine, "SELECT version_num FROM alembic_version")).scalar_one())


async def waiting_for_lock(url: str, count: int) -> None:
    """Wait until ``count`` sessions on this database (not any other) wait on an advisory lock."""
    watcher = await asyncpg.connect(plain(url))
    try:
        for _ in range(WAIT_SECONDS * 10):
            blocked = await watcher.fetchval(
                "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted"
                " AND database = (SELECT oid FROM pg_database WHERE datname = current_database())"
            )
            if blocked >= count:
                return
            await asyncio.sleep(0.1)
        raise AssertionError("the migrate processes never waited on the lock")
    finally:
        await watcher.close()


async def test_two_runs_together_both_succeed_and_the_upgrade_runs_once(scratch: Scratch) -> None:
    url, _ = scratch
    holder = await asyncpg.connect(plain(url))
    try:
        await holder.execute("SELECT pg_advisory_lock(hashtext($1))", LOCK_NAME)
        first, second = start(url), start(url)
        await waiting_for_lock(url, 2)  # both wait behind the lock
    finally:
        await holder.close()  # release it
    results = [await finish(first), await finish(second)]
    for status, out in results:
        assert status == 0, out
    outputs = [out for _, out in results]
    assert sum("upgrading" in out for out in outputs) == 1, outputs
    assert sum("already at head" in out for out in outputs) == 1, outputs
    assert all("waiting for the migration lock" in out for out in outputs), outputs
    assert await revision(scratch) == HEAD


async def test_a_database_at_an_unknown_newer_revision_is_left_alone(scratch: Scratch) -> None:
    url, _ = scratch
    await asyncio.to_thread(alembic, url, "upgrade", HEAD)
    _, engine = scratch
    await run(engine, "UPDATE alembic_version SET version_num = '999_from_a_newer_release'")

    status, out = await finish(start(url))
    assert status == 0, out
    assert "does not know" in out
    assert await revision(scratch) == "999_from_a_newer_release"
