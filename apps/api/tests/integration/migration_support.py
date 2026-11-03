"""Helpers for tests that move a database between Alembic revisions.

A migration test needs its own database: it upgrades and downgrades it, which would pull the shared
test database out from under every other test. `scratch_database` creates a fresh database whose
name ends in ``_test`` (the same guard the shared fixture uses), migrates it to a chosen revision,
and drops it afterwards.

Covers: none directly; support for the migration tests.
"""

import asyncio
import os
import subprocess
import sys
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.pool import NullPool

from tests.run_database import scratch_name

API_DIR = Path(__file__).resolve().parents[2]
BASE_URL = os.environ.get(
    "TEST_DATABASE_URL", "postgresql+asyncpg://hds:hds_dev@localhost:5432/hds_test"
)


def alembic_run(url: str, *args: str) -> subprocess.CompletedProcess[str]:
    """Run an Alembic command against ``url`` and return the finished process, success or not."""
    return subprocess.run(
        [sys.executable, "-m", "alembic", *args],
        cwd=API_DIR,
        env={**os.environ, "DATABASE_URL": url},
        capture_output=True,
        text=True,
        check=False,
    )


def alembic(url: str, *args: str) -> None:
    """Run an Alembic command against ``url`` and fail loudly with its output."""
    result = alembic_run(url, *args)
    assert result.returncode == 0, f"alembic {' '.join(args)} failed:\n{result.stderr[-1500:]}"


async def admin_execute(sql: str) -> None:
    """Run one statement on the server's maintenance database (for CREATE and DROP DATABASE)."""
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


Scratch = tuple[str, AsyncEngine]
"""``(url, engine)`` of a scratch database, as the migration tests receive it."""


@asynccontextmanager
async def scratch_database(name: str, revision: str) -> AsyncIterator[tuple[str, AsyncEngine]]:
    """A fresh database migrated to ``revision``, with an engine on it; dropped afterwards.

    Args:
        name: Database name; it must end in ``_test`` because it is dropped without asking.
        revision: The Alembic revision to start from.

    Yields:
        ``(url, engine)`` for the scratch database.
    """
    assert name.endswith("_test"), "a scratch database is dropped, so its name must end in _test"
    name = scratch_name(name, os.getpid())
    await admin_execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
    await admin_execute(f'CREATE DATABASE "{name}"')
    url = make_url(BASE_URL).set(database=name).render_as_string(hide_password=False)
    await asyncio.to_thread(alembic, url, "upgrade", revision)
    engine = create_async_engine(url, poolclass=NullPool)
    try:
        yield url, engine
    finally:
        await engine.dispose()
        await admin_execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
