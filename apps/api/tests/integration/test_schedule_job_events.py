"""The run record only grows (008 T004).

Covers: 008 FR-030; ADR 020.
"""

from datetime import UTC, datetime

import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from hds.models.user import User
from hds.repositories.schedule_job_event import ScheduleJobEventRepository
from tests.roster_support import headers_for
from tests.schedule_support import ask, staffed_project

AT = datetime(2027, 6, 1, 9, 0, tzinfo=UTC)


async def recorded(
    client: AsyncClient, test_db: AsyncSession, users: dict[str, User]
) -> ScheduleJobEventRepository:
    """A queued job with two steps in its record: the "created" its request wrote, and one more."""
    admin = await headers_for(client, "admin")
    await staffed_project(client, admin)
    job = (await ask(client, admin)).json()
    events = ScheduleJobEventRepository(test_db)
    org = users["admin"].organization_id
    await events.append(org, job["id"], "finished", AT, {"filled": 7}, version=0)
    return events


async def test_the_record_reads_back_in_order(
    async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    events = await recorded(async_client, test_db, seed_users)
    org = seed_users["admin"].organization_id
    job_id = (await test_db.execute(text("SELECT id FROM schedule_jobs"))).scalar_one()

    rows = await events.for_job(org, job_id)

    assert [r.kind for r in rows] == ["created", "finished"]
    assert rows[0].actor_id == seed_users["admin"].id and rows[1].actor_id is None
    assert await events.count_for_job(org, job_id) == 2


@pytest.mark.parametrize(
    "statement",
    ["UPDATE schedule_job_events SET kind = 'discarded'", "DELETE FROM schedule_job_events"],
)
async def test_a_step_can_be_neither_changed_nor_removed(
    async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User], statement: str
) -> None:
    await recorded(async_client, test_db, seed_users)

    with pytest.raises(DBAPIError, match="append-only"):
        async with test_db.begin_nested():
            await test_db.execute(text(statement))

    count = await test_db.execute(text("SELECT count(*) FROM schedule_job_events"))
    assert count.scalar_one() == 2


async def test_an_unknown_kind_is_refused_before_the_database(
    async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    events = await recorded(async_client, test_db, seed_users)
    job_id = (await test_db.execute(text("SELECT id FROM schedule_jobs"))).scalar_one()

    with pytest.raises(ValueError, match="unknown run record kind"):
        await events.append(seed_users["admin"].organization_id, job_id, "renamed", AT, {})
