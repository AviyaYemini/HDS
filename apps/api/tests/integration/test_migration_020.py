"""Migration 020 adds availability, submission windows, shift types, shifts and assignments (T037).

Runs on a scratch database at revision 019. Every constraint the data model names is checked by
trying a row that breaks it.

Covers: 003 FR-101 to FR-115a; data-model.md Part 2.
"""

import asyncio
import uuid
from collections.abc import AsyncGenerator
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine

from tests.integration.migration_support import Scratch, alembic, alembic_run, scratch_database

SCRATCH_DB = "hds_migration_020_test"
PREVIOUS = "019_expiry_index_retire_locks"
REVISION = "020_availability_shifts"
TABLES = {
    "shift_types",
    "project_shift_types",
    "submission_windows",
    "availability_entries",
    "shifts",
    "assignments",
    "idempotency_keys",
}


@pytest_asyncio.fixture
async def scratch() -> AsyncGenerator[Scratch, None]:
    async with scratch_database(SCRATCH_DB, PREVIOUS) as pair:
        yield pair


async def run(engine: AsyncEngine, sql: str, **params: Any) -> Any:
    async with engine.begin() as conn:
        return await conn.execute(text(sql), params)


async def tables(engine: AsyncEngine) -> set[str]:
    rows = await run(
        engine,
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",
    )
    return {r[0] for r in rows}


async def org(engine: AsyncEngine) -> uuid.UUID:
    org_id = uuid.uuid4()
    await run(
        engine,
        "INSERT INTO organisations (id, name, timezone) VALUES (:o, 'Org', 'Asia/Jerusalem')",
        o=org_id,
    )
    return org_id


async def user(engine: AsyncEngine, org_id: uuid.UUID) -> uuid.UUID:
    user_id = uuid.uuid4()
    await run(
        engine,
        "INSERT INTO users (id, organization_id, email, password_hash, full_name, role, "
        "is_active, activated_at) VALUES (:u, :o, :e, 'h', 'A', CAST('employee' AS user_role), "
        "true, now())",
        u=user_id,
        o=org_id,
        e=f"{user_id}@hds.test",
    )
    return user_id


async def project(engine: AsyncEngine, org_id: uuid.UUID) -> uuid.UUID:
    client_id, site_id, project_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    await run(
        engine,
        "INSERT INTO clients (id, organization_id, name, contract_start) "
        "VALUES (:c, :o, 'C', '2026-01-01')",
        c=client_id,
        o=org_id,
    )
    await run(
        engine,
        "INSERT INTO sites (id, organization_id, name, address, timezone) "
        "VALUES (:s, :o, 'S', 'A', 'Asia/Jerusalem')",
        s=site_id,
        o=org_id,
    )
    await run(
        engine,
        "INSERT INTO projects (id, organization_id, client_id, site_id, name, type, "
        "billing_rate, start_date) VALUES (:p, :o, :c, :s, 'P', "
        "CAST('security' AS project_type), 10, '2026-01-01')",
        p=project_id,
        o=org_id,
        c=client_id,
        s=site_id,
    )
    return project_id


async def shift_types(engine: AsyncEngine, org_id: uuid.UUID) -> list[tuple[Any, ...]]:
    rows = await run(
        engine,
        "SELECT name, to_char(starts_at_local, 'HH24:MI'), to_char(ends_at_local, 'HH24:MI'), "
        "crosses_midnight FROM shift_types WHERE organization_id = :o ORDER BY starts_at_local",
        o=org_id,
    )
    return [tuple(r) for r in rows]


DEFAULTS = [
    ("morning", "07:00", "15:00", False),
    ("afternoon", "15:00", "23:00", False),
    ("night", "23:00", "07:00", True),
]


async def test_upgrade_creates_the_tables_and_seeds_existing_organisations(
    scratch: Scratch,
) -> None:
    url, engine = scratch
    existing = await org(engine)

    await asyncio.to_thread(alembic, url, "upgrade", REVISION)

    assert TABLES <= await tables(engine)
    assert await shift_types(engine, existing) == DEFAULTS
    row = (
        await run(
            engine,
            "SELECT min_rest_hours, max_weekly_hours FROM organisations WHERE id = :o",
            o=existing,
        )
    ).one()
    assert tuple(row) == (11, 48)


async def test_a_new_organisation_starts_with_the_three_shift_types(scratch: Scratch) -> None:
    url, engine = scratch
    await asyncio.to_thread(alembic, url, "upgrade", REVISION)

    created = await org(engine)

    assert await shift_types(engine, created) == DEFAULTS


@pytest.mark.parametrize(
    "sql",
    [
        "UPDATE organisations SET min_rest_hours = 0",
        "UPDATE organisations SET max_weekly_hours = 0",
        # A non-crossing type must end after it starts, and a crossing one must not.
        "UPDATE shift_types SET crosses_midnight = false WHERE name = 'night'",
        "UPDATE shift_types SET crosses_midnight = true WHERE name = 'morning'",
        # Two active types with one name, in any letter case.
        "UPDATE shift_types SET name = 'MORNING' WHERE name = 'afternoon'",
    ],
)
async def test_organisation_and_shift_type_rules_are_enforced(scratch: Scratch, sql: str) -> None:
    url, engine = scratch
    await org(engine)
    await asyncio.to_thread(alembic, url, "upgrade", REVISION)

    with pytest.raises(IntegrityError):
        await run(engine, sql)


async def test_a_retired_shift_type_frees_its_name(scratch: Scratch) -> None:
    url, engine = scratch
    org_id = await org(engine)
    await asyncio.to_thread(alembic, url, "upgrade", REVISION)

    await run(engine, "UPDATE shift_types SET is_active = false WHERE name = 'morning'")
    await run(
        engine,
        "INSERT INTO shift_types (id, organization_id, name, starts_at_local, ends_at_local, "
        "crosses_midnight) VALUES (gen_random_uuid(), :o, 'Morning', '06:00', '14:00', false)",
        o=org_id,
    )


async def window(engine: AsyncEngine, org_id: uuid.UUID, first: str, last: str) -> None:
    await run(
        engine,
        "INSERT INTO submission_windows (id, organization_id, starts_on, ends_on, deadline) "
        "VALUES (gen_random_uuid(), :o, :a, :b, now())",
        o=org_id,
        a=date.fromisoformat(first),
        b=date.fromisoformat(last),
    )


async def test_windows_of_one_organisation_cannot_overlap(scratch: Scratch) -> None:
    url, engine = scratch
    first, second = await org(engine), await org(engine)
    await asyncio.to_thread(alembic, url, "upgrade", REVISION)
    await window(engine, first, "2026-06-07", "2026-06-13")

    await window(engine, first, "2026-06-14", "2026-06-20")  # the next week touches, no overlap
    await window(engine, second, "2026-06-07", "2026-06-13")  # another organisation
    with pytest.raises(IntegrityError, match="ex_submission_windows_overlap"):
        await window(engine, first, "2026-06-13", "2026-06-15")
    with pytest.raises(IntegrityError, match="ck_submission_windows_dates"):
        await window(engine, first, "2026-07-02", "2026-07-01")


async def entry(engine: AsyncEngine, org_id: uuid.UUID, user_id: uuid.UUID, **values: Any) -> None:
    row = {
        "kind": "blocked",
        "absence_kind": None,
        "starts_on": "2026-06-01",
        "ends_on": "2026-06-01",
        "weekday": None,
        "shift_type_id": None,
        **values,
    }
    await run(
        engine,
        "INSERT INTO availability_entries (id, organization_id, user_id, kind, absence_kind, "
        "starts_on, ends_on, weekday, shift_type_id) VALUES (gen_random_uuid(), :o, :u, :kind, "
        ":absence_kind, :starts_on, :ends_on, :weekday, :shift_type_id)",
        o=org_id,
        u=user_id,
        **{
            **row,
            "starts_on": date.fromisoformat(row["starts_on"]),
            "ends_on": date.fromisoformat(row["ends_on"]),
        },
    )


@pytest.mark.parametrize(
    "values",
    [
        {"kind": "maybe"},
        {"kind": "absence"},  # an absence needs its kind
        {"kind": "blocked", "absence_kind": "sick"},  # only an absence has one
        {"kind": "absence", "absence_kind": "holiday"},
        {"kind": "absence", "absence_kind": "sick", "weekday": 1},
        {"ends_on": "2026-05-31"},
        {"weekday": 7},
    ],
    ids=str,
)
async def test_entry_rules_are_enforced(scratch: Scratch, values: dict[str, Any]) -> None:
    url, engine = scratch
    org_id = await org(engine)
    await asyncio.to_thread(alembic, url, "upgrade", REVISION)
    user_id = await user(engine, org_id)

    with pytest.raises(IntegrityError):
        await entry(engine, org_id, user_id, **values)


async def test_valid_entries_are_stored(scratch: Scratch) -> None:
    url, engine = scratch
    org_id = await org(engine)
    await asyncio.to_thread(alembic, url, "upgrade", REVISION)
    user_id = await user(engine, org_id)

    for kind in ("blocked", "open", "close"):
        await entry(engine, org_id, user_id, kind=kind, weekday=0)
    for absence in ("leave", "reserve_duty", "sick"):
        await entry(engine, org_id, user_id, kind="absence", absence_kind=absence)


async def a_shift(engine: AsyncEngine, org_id: uuid.UUID, **values: Any) -> uuid.UUID:
    project_id = await project(engine, org_id)
    type_id = (
        await run(engine, "SELECT id FROM shift_types WHERE organization_id = :o LIMIT 1", o=org_id)
    ).scalar_one()
    shift_id = uuid.uuid4()
    row = {
        "starts_at": "2026-06-01T04:00:00Z",
        "ends_at": "2026-06-01T12:00:00Z",
        "headcount": 1,
        "pay_rate": None,
        "status": "draft",
        "published_at": None,
        **values,
    }
    await run(
        engine,
        "INSERT INTO shifts (id, organization_id, project_id, shift_type_id, starts_at, ends_at, "
        "headcount, pay_rate, status, published_at) VALUES (:i, :o, :p, :t, :starts_at, :ends_at, "
        ":headcount, :pay_rate, :status, :published_at)",
        i=shift_id,
        o=org_id,
        p=project_id,
        t=type_id,
        **{
            **row,
            "starts_at": datetime.fromisoformat(row["starts_at"]),
            "ends_at": datetime.fromisoformat(row["ends_at"]),
            "pay_rate": None if row["pay_rate"] is None else Decimal(str(row["pay_rate"])),
            "published_at": None
            if row["published_at"] is None
            else datetime.fromisoformat(row["published_at"]),
        },
    )
    return shift_id


@pytest.mark.parametrize(
    "values",
    [
        {"ends_at": "2026-06-01T04:00:00Z"},
        {"headcount": 0},
        {"pay_rate": 0},
        {"status": "locked"},
    ],
    ids=str,
)
async def test_shift_rules_are_enforced(scratch: Scratch, values: dict[str, Any]) -> None:
    url, engine = scratch
    org_id = await org(engine)
    await asyncio.to_thread(alembic, url, "upgrade", REVISION)

    with pytest.raises(IntegrityError):
        await a_shift(engine, org_id, **values)


async def test_an_employee_is_on_a_shift_once_and_assignments_go_with_their_shift(
    scratch: Scratch,
) -> None:
    url, engine = scratch
    org_id = await org(engine)
    await asyncio.to_thread(alembic, url, "upgrade", REVISION)
    user_id = await user(engine, org_id)
    shift_id = await a_shift(engine, org_id, pay_rate="12.5000")
    insert = (
        "INSERT INTO assignments (id, organization_id, shift_id, user_id) "
        "VALUES (gen_random_uuid(), :o, :s, :u)"
    )
    await run(engine, insert, o=org_id, s=shift_id, u=user_id)

    with pytest.raises(IntegrityError, match="uq_assignments_shift_user"):
        await run(engine, insert, o=org_id, s=shift_id, u=user_id)
    await run(engine, "DELETE FROM shifts WHERE id = :s", s=shift_id)
    assert (await run(engine, "SELECT count(*) FROM assignments")).scalar_one() == 0


async def test_one_idempotency_key_per_user(scratch: Scratch) -> None:
    url, engine = scratch
    org_id = await org(engine)
    await asyncio.to_thread(alembic, url, "upgrade", REVISION)
    first, second = await user(engine, org_id), await user(engine, org_id)
    insert = (
        "INSERT INTO idempotency_keys (user_id, key, request_hash, status_code, response_body) "
        "VALUES (:u, 'k', :h, 201, '{}')"
    )
    await run(engine, insert, u=first, h="a" * 64)
    await run(engine, insert, u=second, h="a" * 64)  # another user, the same key

    with pytest.raises(IntegrityError):
        await run(engine, insert, u=first, h="b" * 64)


async def test_downgrade_removes_everything_and_upgrading_again_works(scratch: Scratch) -> None:
    url, engine = scratch
    await org(engine)
    await asyncio.to_thread(alembic, url, "upgrade", REVISION)

    await asyncio.to_thread(alembic, url, "downgrade", PREVIOUS)

    assert not TABLES & await tables(engine)
    columns = await run(
        engine,
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'organisations'",
    )
    assert not {"min_rest_hours", "max_weekly_hours"} & {r[0] for r in columns}
    extensions = await run(engine, "SELECT extname FROM pg_extension")
    assert "btree_gist" not in {r[0] for r in extensions}
    with pytest.raises(DBAPIError):  # the trigger and its function are gone with it
        await run(engine, "SELECT hds_default_shift_types()")

    await asyncio.to_thread(alembic, url, "upgrade", REVISION)
    assert TABLES <= await tables(engine)


async def test_an_extension_that_was_there_before_is_kept_on_downgrade(scratch: Scratch) -> None:
    url, engine = scratch
    await run(engine, "CREATE EXTENSION btree_gist")
    await asyncio.to_thread(alembic, url, "upgrade", REVISION)

    await asyncio.to_thread(alembic, url, "downgrade", PREVIOUS)

    extensions = await run(engine, "SELECT extname FROM pg_extension")
    assert "btree_gist" in {r[0] for r in extensions}


async def test_an_organisation_with_only_its_default_shift_types_can_be_deleted(
    scratch: Scratch,
) -> None:
    """The trigger's rows must not stop an organisation being removed, as before 020."""
    url, engine = scratch
    await asyncio.to_thread(alembic, url, "upgrade", REVISION)
    created = await org(engine)

    await run(engine, "DELETE FROM organisations WHERE id = :o", o=created)

    assert await shift_types(engine, created) == []


async def test_the_trigger_seeds_whatever_the_sessions_search_path(scratch: Scratch) -> None:
    url, engine = scratch
    await asyncio.to_thread(alembic, url, "upgrade", REVISION)
    org_id = uuid.uuid4()

    async with engine.begin() as conn:
        await conn.execute(text("SET LOCAL search_path TO pg_catalog"))
        await conn.execute(
            text(
                "INSERT INTO public.organisations (id, name, timezone) "
                "VALUES (:o, 'Org', 'Asia/Jerusalem')"
            ),
            {"o": org_id},
        )

    assert await shift_types(engine, org_id) == DEFAULTS


async def test_new_rows_get_an_id_from_the_database(scratch: Scratch) -> None:
    url, engine = scratch
    org_id = await org(engine)
    await asyncio.to_thread(alembic, url, "upgrade", REVISION)

    await run(
        engine,
        "INSERT INTO shift_types (organization_id, name, starts_at_local, ends_at_local, "
        "crosses_midnight) VALUES (:o, 'long day', '07:00', '19:00', false)",
        o=org_id,
    )


@pytest.mark.parametrize(
    "values",
    [{"status": "published"}, {"published_at": "2026-06-01T00:00:00Z"}],
    ids=["published-without-time", "draft-with-time"],
)
async def test_status_and_published_time_agree(scratch: Scratch, values: dict[str, Any]) -> None:
    url, engine = scratch
    org_id = await org(engine)
    await asyncio.to_thread(alembic, url, "upgrade", REVISION)

    with pytest.raises(IntegrityError, match="ck_shifts_published_at"):
        await a_shift(engine, org_id, **values)


async def test_foreign_keys_used_in_lookups_are_indexed(scratch: Scratch) -> None:
    url, engine = scratch
    await asyncio.to_thread(alembic, url, "upgrade", REVISION)

    rows = await run(engine, "SELECT indexname FROM pg_indexes WHERE schemaname = 'public'")
    names = {r[0] for r in rows}

    assert {
        "idx_shift_types_org",
        "idx_project_shift_types_type",
        "idx_availability_entries_shift_type",
    } <= names


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
    assert "shift_types" in result.stdout
