"""Migration 021 adds staffing needs, schedule jobs and the source of an assignment (004 T008).

Runs on a scratch database at revision 020. Every constraint the data model names is checked by
trying a row that breaks it.

Covers: 004 FR-001, FR-004 to FR-010, FR-023 to FR-026; data-model.md.
"""

import asyncio
import json
import uuid
from collections.abc import AsyncGenerator
from datetime import date
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy.exc import IntegrityError

from tests.integration.migration_support import Scratch, alembic, scratch_database
from tests.integration.test_migration_020 import org, project, run, tables, user

SCRATCH_DB = "hds_migration_021_test"
PREVIOUS = "020_availability_shifts"
REVISION = "021_schedule_generation"
TABLES = {"staffing_needs", "schedule_jobs"}


@pytest_asyncio.fixture
async def scratch() -> AsyncGenerator[Scratch, None]:
    async with scratch_database(SCRATCH_DB, PREVIOUS) as pair:
        yield pair


async def shift_type(scratch: Scratch, org_id: uuid.UUID) -> uuid.UUID:
    _, engine = scratch
    rows = await run(
        engine, "SELECT id FROM shift_types WHERE organization_id = :o AND name = 'night'", o=org_id
    )
    found: uuid.UUID = rows.scalar_one()
    return found


async def test_upgrade_adds_the_tables_and_marks_existing_assignments_manual(
    scratch: Scratch,
) -> None:
    url, engine = scratch
    org_id = await org(engine)
    person, project_id = await user(engine, org_id), await project(engine, org_id)
    night = await shift_type(scratch, org_id)
    shift_id = uuid.uuid4()
    await run(
        engine,
        "INSERT INTO shifts (id, organization_id, project_id, shift_type_id, starts_at, ends_at, "
        "headcount) VALUES (:s, :o, :p, :t, '2027-06-07 20:00Z', '2027-06-08 04:00Z', 1)",
        s=shift_id,
        o=org_id,
        p=project_id,
        t=night,
    )
    await run(
        engine,
        "INSERT INTO assignments (id, organization_id, shift_id, user_id) VALUES (:a, :o, :s, :u)",
        a=uuid.uuid4(),
        o=org_id,
        s=shift_id,
        u=person,
    )

    await asyncio.to_thread(alembic, url, "upgrade", REVISION)

    assert TABLES <= await tables(engine)
    source = await run(engine, "SELECT source FROM assignments")
    assert source.scalar_one() == "manual"


async def need(scratch: Scratch, **values: Any) -> None:
    _, engine = scratch
    await run(
        engine,
        "INSERT INTO staffing_needs (id, organization_id, project_id, shift_type_id, weekday, "
        "headcount) VALUES (:i, :o, :p, :t, :w, :h)",
        i=uuid.uuid4(),
        **values,
    )


async def test_staffing_need_rules_are_enforced(scratch: Scratch) -> None:
    url, engine = scratch
    org_id = await org(engine)
    project_id = await project(engine, org_id)
    await asyncio.to_thread(alembic, url, "upgrade", REVISION)
    night = await shift_type(scratch, org_id)
    base = {"o": org_id, "p": project_id, "t": night}

    await need(scratch, **base, w=0, h=2)
    for bad in ({"w": 7, "h": 1}, {"w": -1, "h": 1}, {"w": 1, "h": 0}):
        with pytest.raises(IntegrityError):
            await need(scratch, **base, **bad)
    with pytest.raises(IntegrityError):  # one need per project, shift type and weekday
        await need(scratch, **base, w=0, h=3)


async def job(scratch: Scratch, **values: Any) -> None:
    _, engine = scratch
    row = {
        "first_day": date(2027, 6, 6),
        "last_day": date(2027, 6, 12),
        "status": "queued",
        "solver": "cpsat",
        "proposal_state": None,
        **values,
    }
    await run(
        engine,
        "INSERT INTO schedule_jobs (id, organization_id, requested_by, first_day, last_day, "
        "project_ids, solver, status, proposal_state, settings) VALUES (:i, :o, :u, "
        ":first_day, :last_day, CAST(:projects AS jsonb), :solver, "
        ":status, :proposal_state, CAST(:settings AS jsonb))",
        i=uuid.uuid4(),
        projects=json.dumps([]),
        settings=json.dumps({"min_rest_hours": 11, "max_weekly_hours": 48}),
        **row,
    )


async def test_schedule_job_rules_are_enforced(scratch: Scratch) -> None:
    url, engine = scratch
    org_id = await org(engine)
    person = await user(engine, org_id)
    await asyncio.to_thread(alembic, url, "upgrade", REVISION)
    who = {"o": org_id, "u": person}

    await job(scratch, **who)
    await job(scratch, **who, first_day=date(2027, 6, 1), last_day=date(2027, 7, 1))  # 31 days
    await job(scratch, **who, status="succeeded", proposal_state="open")
    for bad in (
        {"last_day": date(2027, 6, 5)},
        {"first_day": date(2027, 6, 1), "last_day": date(2027, 7, 2)},  # 32 days
        {"status": "done"},
        {"solver": "magic"},
        {"status": "succeeded", "proposal_state": "maybe"},
    ):
        with pytest.raises(IntegrityError):
            await job(scratch, **who, **bad)


@pytest.mark.parametrize(
    "sql",
    [
        "UPDATE organisations SET min_rest_hours = 25",
        "UPDATE organisations SET max_weekly_hours = 169",
        "UPDATE assignments SET source = 'imported'",
    ],
)
async def test_setting_bounds_and_the_assignment_source_are_enforced(
    scratch: Scratch, sql: str
) -> None:
    url, engine = scratch
    await org(engine)
    await asyncio.to_thread(alembic, url, "upgrade", REVISION)
    org_id = await org(engine)
    person, project_id = await user(engine, org_id), await project(engine, org_id)
    night = await shift_type(scratch, org_id)
    shift_id = uuid.uuid4()
    await run(
        engine,
        "INSERT INTO shifts (id, organization_id, project_id, shift_type_id, starts_at, ends_at, "
        "headcount) VALUES (:s, :o, :p, :t, '2027-06-07 20:00Z', '2027-06-08 04:00Z', 1)",
        s=shift_id,
        o=org_id,
        p=project_id,
        t=night,
    )
    await run(
        engine,
        "INSERT INTO assignments (id, organization_id, shift_id, user_id) VALUES (:a, :o, :s, :u)",
        a=uuid.uuid4(),
        o=org_id,
        s=shift_id,
        u=person,
    )
    await run(engine, "UPDATE organisations SET min_rest_hours = 24, max_weekly_hours = 168")

    with pytest.raises(IntegrityError):
        await run(engine, sql)


async def test_downgrade_removes_everything_and_upgrading_again_works(scratch: Scratch) -> None:
    url, engine = scratch
    await org(engine)
    await asyncio.to_thread(alembic, url, "upgrade", REVISION)

    await asyncio.to_thread(alembic, url, "downgrade", PREVIOUS)

    assert not TABLES & await tables(engine)
    columns = await run(
        engine,
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'assignments'",
    )
    assert "source" not in {r[0] for r in columns}
    await run(engine, "UPDATE organisations SET min_rest_hours = 30")  # 020's own check only

    await run(engine, "UPDATE organisations SET min_rest_hours = 11")
    await asyncio.to_thread(alembic, url, "upgrade", REVISION)
    assert TABLES <= await tables(engine)
