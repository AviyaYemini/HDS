"""Migration 022 adds pay settings, special days and managers on projects (005 T006).

Runs on a scratch database at revision 021. Every constraint the data model names is checked by
trying a row that breaks it.

Covers: 005 FR-003, FR-005 to FR-008a, FR-021; data-model.md.
"""

import asyncio
import uuid
from collections.abc import AsyncGenerator
from datetime import date
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy.exc import IntegrityError

from tests.integration.migration_support import Scratch, alembic, alembic_run, scratch_database
from tests.integration.test_migration_020 import org, project, run, tables, user

SCRATCH_DB = "hds_migration_022_test"
PREVIOUS = "021_schedule_generation"
REVISION = "022_pay_reports"
TABLES = {"special_days", "project_managers"}
ORG_DEFAULTS = {
    "pay_day_normal_minutes": 480,
    "pay_night_normal_minutes": 420,
    "pay_tier1_minutes": 120,
    "pay_overtime1_pct": "125.00",
    "pay_overtime2_pct": "150.00",
    "pay_rest_start_weekday": 5,
    "pay_rest_start_time": "18:00:00",
    "pay_rest_end_weekday": 6,
    "pay_rest_end_time": "22:00:00",
    "pay_rest_pct": "150.00",
    "pay_increase1_points": "25.00",
    "pay_increase2_points": "50.00",
}


@pytest_asyncio.fixture
async def scratch() -> AsyncGenerator[Scratch, None]:
    async with scratch_database(SCRATCH_DB, PREVIOUS) as pair:
        yield pair


async def upgraded(scratch: Scratch) -> tuple[uuid.UUID, uuid.UUID]:
    """An organisation and a project made at 021, then the upgrade."""
    url, engine = scratch
    org_id = await org(engine)
    project_id = await project(engine, org_id)
    await asyncio.to_thread(alembic, url, "upgrade", REVISION)
    return org_id, project_id


async def test_upgrade_gives_existing_organisations_the_defaults(scratch: Scratch) -> None:
    _, engine = scratch
    org_id, _ = await upgraded(scratch)

    columns = ", ".join(f"{name}::text" for name in ORG_DEFAULTS)
    row = (await run(engine, f"SELECT {columns} FROM organisations WHERE id = :o", o=org_id)).one()

    assert dict(zip(ORG_DEFAULTS, row, strict=True)) == {k: str(v) for k, v in ORG_DEFAULTS.items()}
    assert TABLES <= await tables(engine)


async def test_upgrade_leaves_existing_projects_following_the_defaults(scratch: Scratch) -> None:
    _, engine = scratch
    _, project_id = await upgraded(scratch)

    columns = ", ".join(ORG_DEFAULTS)
    row = (
        await run(
            engine,
            f"SELECT pay_enabled, managers_see_pay, {columns} FROM projects WHERE id = :p",
            p=project_id,
        )
    ).one()

    assert row[0] is True and row[1] is False
    assert set(row[2:]) == {None}


async def test_upgrade_freezes_projects_that_have_already_ended(scratch: Scratch) -> None:
    """An ended project's pay stops changing (005 FR-007), also for one ended before 022."""
    url, engine = scratch
    org_id = await org(engine)
    project_id = await project(engine, org_id)
    await run(engine, "UPDATE projects SET status = 'ended' WHERE id = :p", p=project_id)

    await asyncio.to_thread(alembic, url, "upgrade", REVISION)

    columns = ", ".join(f"{name}::text" for name in ORG_DEFAULTS)
    row = (await run(engine, f"SELECT {columns} FROM projects WHERE id = :p", p=project_id)).one()
    assert dict(zip(ORG_DEFAULTS, row, strict=True)) == {k: str(v) for k, v in ORG_DEFAULTS.items()}


@pytest.mark.parametrize(
    "assignment",
    [
        "pay_day_normal_minutes = 0",
        "pay_night_normal_minutes = 1441",
        "pay_tier1_minutes = 0",
        "pay_overtime1_pct = 99.99",
        "pay_overtime2_pct = 300.01",
        "pay_rest_pct = 99",
        "pay_increase1_points = 0",
        "pay_increase2_points = 200.01",
        "pay_rest_start_weekday = 7",
        "pay_rest_end_weekday = -1",
        "pay_rest_end_weekday = 5, pay_rest_end_time = '18:00'",
    ],
)
async def test_organisation_settings_are_bounded(scratch: Scratch, assignment: str) -> None:
    _, engine = scratch
    await upgraded(scratch)

    with pytest.raises(IntegrityError):
        await run(engine, f"UPDATE organisations SET {assignment}")


@pytest.mark.parametrize(
    "assignment",
    [
        "pay_day_normal_minutes = 0",
        "pay_overtime1_pct = 99.99",
        "pay_increase2_points = 0",
        "pay_rest_start_weekday = 5",  # a window is all four values or none
        "pay_rest_start_weekday = 2, pay_rest_start_time = '10:00', "
        "pay_rest_end_weekday = 2, pay_rest_end_time = '10:00'",
    ],
)
async def test_project_overrides_are_bounded(scratch: Scratch, assignment: str) -> None:
    _, engine = scratch
    await upgraded(scratch)

    with pytest.raises(IntegrityError):
        await run(engine, f"UPDATE projects SET {assignment}")


async def test_valid_project_overrides_are_stored(scratch: Scratch) -> None:
    _, engine = scratch
    _, project_id = await upgraded(scratch)

    await run(
        engine,
        "UPDATE projects SET pay_enabled = false, managers_see_pay = true, "
        "pay_day_normal_minutes = 540, pay_rest_start_weekday = 6, pay_rest_start_time = '00:00', "
        "pay_rest_end_weekday = 0, pay_rest_end_time = '00:00', pay_rest_pct = 200 "
        "WHERE id = :p",
        p=project_id,
    )

    row = (
        await run(
            engine,
            "SELECT pay_enabled, managers_see_pay, pay_day_normal_minutes, "
            "pay_rest_start_weekday, pay_rest_start_time::text, pay_rest_end_weekday, "
            "pay_rest_end_time::text, pay_rest_pct::text, pay_overtime1_pct FROM projects "
            "WHERE id = :p",
            p=project_id,
        )
    ).one()
    assert tuple(row) == (False, True, 540, 6, "00:00:00", 0, "00:00:00", "200.00", None)


async def day(engine: Any, org_id: uuid.UUID, **values: Any) -> None:
    row = {"p": None, "d": date(2027, 9, 11), "n": "Yom Kippur", **values}
    columns = "id, organization_id, project_id, day, name" + (", rate_pct" if "r" in row else "")
    params = ":i, :o, :p, :d, :n" + (", :r" if "r" in row else "")
    await run(
        engine,
        f"INSERT INTO special_days ({columns}) VALUES ({params})",
        i=uuid.uuid4(),
        o=org_id,
        **row,
    )


async def test_special_day_rules_are_enforced(scratch: Scratch) -> None:
    _, engine = scratch
    org_id, project_id = await upgraded(scratch)

    await day(engine, org_id)
    rate = await run(engine, "SELECT rate_pct::text FROM special_days")
    assert rate.scalar_one() == "150.00"
    await day(engine, org_id, p=project_id)  # a project may override the same date
    await day(engine, org_id, d=date(2027, 11, 1), r=200)
    for bad in (
        {},
        {"p": project_id},
        {"d": date(2027, 12, 1), "r": 99},
        {"d": date(2027, 12, 2), "r": 301},
    ):
        with pytest.raises(IntegrityError):
            await day(engine, org_id, **bad)
    with pytest.raises(IntegrityError):
        await day(engine, org_id, d=date(2027, 12, 3), n=" ")


async def test_a_manager_is_on_a_project_once(scratch: Scratch) -> None:
    url, engine = scratch
    org_id = await org(engine)
    project_id = await project(engine, org_id)
    person = await user(engine, org_id)
    await asyncio.to_thread(alembic, url, "upgrade", REVISION)
    insert = (
        "INSERT INTO project_managers (organization_id, project_id, user_id) VALUES (:o, :p, :u)"
    )

    await run(engine, insert, o=org_id, p=project_id, u=person)
    with pytest.raises(IntegrityError):
        await run(engine, insert, o=org_id, p=project_id, u=person)


async def test_the_new_indexes_exist(scratch: Scratch) -> None:
    _, engine = scratch
    await upgraded(scratch)

    rows = await run(engine, "SELECT indexname FROM pg_indexes WHERE schemaname = 'public'")
    names = {r[0] for r in rows}

    assert {
        "idx_shifts_org_published_start",
        "idx_project_managers_user",
        "uq_special_days_project_day",
    } <= names


async def test_downgrade_removes_everything_and_upgrading_again_works(scratch: Scratch) -> None:
    url, engine = scratch
    await upgraded(scratch)

    await asyncio.to_thread(alembic, url, "downgrade", PREVIOUS)

    assert not TABLES & await tables(engine)
    columns = await run(
        engine,
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name IN ('organisations', 'projects') AND column_name LIKE 'pay%'",
    )
    assert columns.all() == []
    await asyncio.to_thread(alembic, url, "upgrade", REVISION)


@pytest.mark.parametrize(
    "span",
    [f"{PREVIOUS}:{REVISION}", f"{REVISION}:{PREVIOUS}"],
    ids=["upgrade", "downgrade"],
)
def test_the_migration_can_be_written_out_as_sql(span: str) -> None:
    """Offline mode (``--sql``) has no database to ask, so the migration must not need one."""
    command = "upgrade" if span.startswith(PREVIOUS) else "downgrade"

    result = alembic_run("postgresql+asyncpg://offline/none", command, span, "--sql")

    assert result.returncode == 0, result.stderr[-1500:]
    assert "special_days" in result.stdout
