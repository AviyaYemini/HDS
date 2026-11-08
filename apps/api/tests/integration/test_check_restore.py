"""The restore check reads a restored database through the code, and changes nothing (T041).

``python -m hds.scripts.check_restore`` is the restore test's last step (research R11). It runs
after ``migrate`` on the throwaway copy. It takes no manifest: the restore test compares the
revision and the row counts before ``migrate``, which may change both. It runs the ready checks,
then loads the organisation, its users and the last month's shifts through the repositories. It
writes nothing, and it prints counts only, never a name or an address.

Each test runs on a scratch database of its own, seeded with the development data.

Covers: 006 FR-014; research R11.
"""

import asyncio
import os
import re
import subprocess
import sys
from collections.abc import AsyncGenerator
from pathlib import Path

import pytest_asyncio

from tests.integration.migration_support import (
    Scratch,
    alembic,
    head_and_previous,
    scratch_database,
)
from tests.integration.test_migration_020 import run

API_DIR = Path(__file__).resolve().parents[2]
HEAD, PREVIOUS = head_and_previous()
SEEDED_PERSONAL = ["admin@hds.test", "employee@hds.test", "HDS Dev", "Seed Client"]
# Every table the seed fills, fingerprinted to show the check wrote nothing.
FINGERPRINT = """
SELECT md5(string_agg(t.dump, '|' ORDER BY t.dump)) FROM (
    SELECT 'o' || o::text AS dump FROM organisations o
    UNION ALL SELECT 'u' || u::text FROM users u
    UNION ALL SELECT 'p' || p::text FROM projects p
    UNION ALL SELECT 'v' || v::text FROM alembic_version v
) t
"""


async def seeded(url: str) -> None:
    status, out = await asyncio.to_thread(command, url, "hds.scripts.seed_dev")
    assert status == 0, out


@pytest_asyncio.fixture
async def at_head() -> AsyncGenerator[Scratch, None]:
    async with scratch_database("hds_check_restore_test", HEAD) as pair:
        await seeded(pair[0])
        yield pair


@pytest_asyncio.fixture
async def at_previous() -> AsyncGenerator[Scratch, None]:
    # A backup taken before the last migration: seeded at head (the seed is today's code), then
    # taken one revision back, as that backup would have been.
    async with scratch_database("hds_check_restore_old_test", HEAD) as pair:
        await seeded(pair[0])
        await asyncio.to_thread(alembic, pair[0], "downgrade", PREVIOUS)
        yield pair


def command(url: str, module: str) -> tuple[int, str]:
    env = {**os.environ, "DATABASE_URL": url, "ENVIRONMENT": "test"}
    env.setdefault("JWT_SECRET_KEY", "a-test-only-signing-key-for-check-restore")
    done = subprocess.run(
        [sys.executable, "-m", module],
        cwd=API_DIR,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    return done.returncode, done.stdout + done.stderr


def last_month(out: str) -> int:
    found = re.search(r"shifts in the last month: (\d+)", out)
    assert found, out
    return int(found.group(1))


async def check(url: str) -> tuple[int, str]:
    return await asyncio.to_thread(command, url, "hds.scripts.check_restore")


async def test_a_restored_database_at_head_passes_and_shows_counts_only(at_head: Scratch) -> None:
    url, _ = at_head
    status, out = await check(url)
    assert status == 0, out
    assert "organisations: 1" in out
    assert "users: 5" in out
    assert not [p for p in SEEDED_PERSONAL if p in out], out


async def test_only_the_last_month_of_shifts_is_counted(at_head: Scratch) -> None:
    # One shift a week ago, one long ago and one next week: only the first is "the last month".
    # The seed has shifts of its own, on dates that move with the day of the week, so the count is
    # compared with the one before these three.
    url, engine = at_head
    status, out = await check(url)
    assert status == 0, out
    before = last_month(out)
    await run(
        engine,
        """
        INSERT INTO shifts (id, organization_id, project_id, shift_type_id, starts_at, ends_at,
            headcount, status, created_at, updated_at)
        SELECT gen_random_uuid(), p.organization_id, p.id,
            (SELECT st.id FROM shift_types st WHERE st.organization_id = p.organization_id
             ORDER BY st.name LIMIT 1),
            now() + d * interval '1 day', now() + d * interval '1 day' + interval '8 hours',
            1, 'draft', now(), now()
        FROM projects p, unnest(ARRAY[-7, -90, 7]) d
        WHERE p.name = 'Night Watch'
        """,
    )
    status, out = await check(url)
    assert status == 0, out
    assert last_month(out) == before + 1


async def test_the_check_writes_nothing(at_head: Scratch) -> None:
    url, engine = at_head
    before = (await run(engine, FINGERPRINT)).scalar_one()
    status, out = await check(url)
    assert status == 0, out
    assert (await run(engine, FINGERPRINT)).scalar_one() == before


async def test_a_backup_from_an_older_revision_migrates_and_then_passes(
    at_previous: Scratch,
) -> None:
    url, _ = at_previous
    status, out = await asyncio.to_thread(command, url, "hds.scripts.migrate")
    assert status == 0, out
    status, out = await check(url)
    assert status == 0, out


async def test_a_database_behind_the_code_fails_the_check(at_previous: Scratch) -> None:
    # Without migrate, the ready checks find the database behind: the check must not pass.
    url, _ = at_previous
    status, out = await check(url)
    assert status != 0, out
    assert "migrations" in out


async def test_a_backup_at_an_unknown_newer_revision_passes_without_migrating(
    at_head: Scratch,
) -> None:
    url, engine = at_head
    await run(engine, "UPDATE alembic_version SET version_num = '999_from_a_newer_release'")
    status, out = await check(url)
    assert status == 0, out


async def test_an_empty_restore_fails_the_check() -> None:
    # A restore that brought back no organisation is not a usable database.
    async with scratch_database("hds_check_restore_empty_test", HEAD) as (url, _):
        status, out = await check(url)
    assert status != 0, out
    assert "no organisation" in out
