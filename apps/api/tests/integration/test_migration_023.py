"""Migration 023 freezes the pay of projects deleted without being ended (005 T042).

Runs on a scratch database at revision 022. Before 005 Group H, deleting a project did not fix its
pay as ending does, so a later change to the defaults or the organisation's special days rewrote
the pay of its past shifts.

Covers: 005 FR-007 (as amended after the review of PR 7).
"""

import asyncio
import uuid
from collections.abc import AsyncGenerator
from datetime import date
from decimal import Decimal

import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncEngine

from tests.integration.migration_support import Scratch, alembic, scratch_database
from tests.integration.test_migration_020 import org, project, run
from tests.integration.test_migration_022 import ORG_DEFAULTS

SCRATCH_DB = "hds_migration_023_test"
PREVIOUS = "022_pay_reports"
REVISION = "023_freeze_deleted_projects"


@pytest_asyncio.fixture
async def scratch() -> AsyncGenerator[Scratch, None]:
    async with scratch_database(SCRATCH_DB, PREVIOUS) as pair:
        yield pair


async def settings_of(engine: AsyncEngine, project_id: uuid.UUID) -> dict[str, str | None]:
    columns = ", ".join(f"{name}::text" for name in ORG_DEFAULTS)
    row = (await run(engine, f"SELECT {columns} FROM projects WHERE id = :p", p=project_id)).one()
    return dict(zip(ORG_DEFAULTS, row, strict=True))


async def org_day(engine: AsyncEngine, org_id: uuid.UUID, day: date, rate: str) -> None:
    await run(
        engine,
        "INSERT INTO special_days (id, organization_id, project_id, day, name, rate_pct) "
        "VALUES (gen_random_uuid(), :o, NULL, :d, 'Day', :r)",
        o=org_id,
        d=day,
        r=Decimal(rate),
    )


async def test_a_deleted_project_gets_the_defaults_and_the_organisations_days(
    scratch: Scratch,
) -> None:
    url, engine = scratch
    org_id = await org(engine)
    deleted = await project(engine, org_id)
    await run(
        engine,
        "UPDATE projects SET is_deleted = true, deleted_at = now(), pay_rest_pct = 175.00 "
        "WHERE id = :p",
        p=deleted,
    )
    await org_day(engine, org_id, date(2026, 10, 2), "200.00")

    await asyncio.to_thread(alembic, url, "upgrade", REVISION)

    expected = {k: str(v) for k, v in ORG_DEFAULTS.items()}
    assert await settings_of(engine, deleted) == {**expected, "pay_rest_pct": "175.00"}
    days = (
        await run(
            engine,
            "SELECT day::text, rate_pct::text FROM special_days WHERE project_id = :p",
            p=deleted,
        )
    ).all()
    assert [tuple(d) for d in days] == [("2026-10-02", "200.00")]


async def test_projects_not_deleted_keep_following_the_defaults(scratch: Scratch) -> None:
    url, engine = scratch
    org_id = await org(engine)
    live = await project(engine, org_id)
    await org_day(engine, org_id, date(2026, 10, 2), "200.00")

    await asyncio.to_thread(alembic, url, "upgrade", REVISION)

    assert set((await settings_of(engine, live)).values()) == {None}
    own = (
        await run(engine, "SELECT count(*) FROM special_days WHERE project_id = :p", p=live)
    ).scalar_one()
    assert own == 0


async def test_downgrade_and_upgrade_again_run(scratch: Scratch) -> None:
    url, engine = scratch
    org_id = await org(engine)
    await project(engine, org_id)

    await asyncio.to_thread(alembic, url, "upgrade", REVISION)
    await asyncio.to_thread(alembic, url, "downgrade", PREVIOUS)
    await asyncio.to_thread(alembic, url, "upgrade", REVISION)
