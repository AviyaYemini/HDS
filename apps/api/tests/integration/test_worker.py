"""The worker: claiming jobs, leases, retries and failures (004 T017).

Covers: 004 FR-008, FR-010; research G7.
"""

from datetime import UTC, datetime, timedelta

import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hds.models.user import User
from hds.scheduling import SolverError
from tests.roster_support import headers_for
from tests.schedule_support import JOBS, ask, staffed_project, work


async def job_row(test_db: AsyncSession, job_id: str) -> dict[str, object]:
    await test_db.rollback()
    found = await test_db.execute(
        text("SELECT status, attempts, error_code, result FROM schedule_jobs WHERE id = :i"),
        {"i": job_id},
    )
    return dict(found.mappings().one())


async def test_with_nothing_queued_the_worker_does_nothing(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    assert await work() is None


async def test_a_job_runs_once_and_succeeds(
    async_client: AsyncClient, seed_users: dict[str, User], test_db: AsyncSession
) -> None:
    manager = await headers_for(async_client, "manager")
    await staffed_project(async_client, manager)
    job = (await ask(async_client, manager)).json()

    assert str(await work()) == job["id"]
    assert await work() is None

    row = await job_row(test_db, job["id"])
    assert (row["status"], row["attempts"]) == ("succeeded", 1)


async def test_a_job_whose_worker_died_is_claimed_again_after_its_lease(
    async_client: AsyncClient, seed_users: dict[str, User], test_db: AsyncSession
) -> None:
    manager = await headers_for(async_client, "manager")
    await staffed_project(async_client, manager)
    job = (await ask(async_client, manager)).json()
    await test_db.execute(
        text(
            "UPDATE schedule_jobs SET status = 'running', attempts = 1, lease_until = :t "
            "WHERE id = :i"
        ),
        {"t": datetime.now(UTC) + timedelta(minutes=1), "i": job["id"]},
    )
    await test_db.commit()
    assert await work() is None  # its lease still runs

    await test_db.execute(
        text("UPDATE schedule_jobs SET lease_until = :t WHERE id = :i"),
        {"t": datetime.now(UTC) - timedelta(seconds=1), "i": job["id"]},
    )
    await test_db.commit()
    assert str(await work()) == job["id"]

    row = await job_row(test_db, job["id"])
    assert (row["status"], row["attempts"]) == ("succeeded", 2)


async def test_a_job_lost_three_times_fails_as_worker_lost(
    async_client: AsyncClient, seed_users: dict[str, User], test_db: AsyncSession
) -> None:
    manager = await headers_for(async_client, "manager")
    job = (await ask(async_client, manager)).json()
    await test_db.execute(
        text(
            "UPDATE schedule_jobs SET status = 'running', attempts = 3, lease_until = :t "
            "WHERE id = :i"
        ),
        {"t": datetime.now(UTC) - timedelta(seconds=1), "i": job["id"]},
    )
    await test_db.commit()

    assert await work() is None

    row = await job_row(test_db, job["id"])
    assert (row["status"], row["error_code"]) == ("failed", "jobs.worker_lost")
    read = (await async_client.get(f"{JOBS}/{job['id']}", headers=manager)).json()
    assert read["status"] == "failed" and read["proposal"] is None


@pytest.mark.parametrize("error", [SolverError("bad"), RuntimeError("boom")])
async def test_a_solver_failure_fails_the_job_and_stores_nothing(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    test_db: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
    error: Exception,
) -> None:
    manager = await headers_for(async_client, "manager")
    await staffed_project(async_client, manager)
    job = (await ask(async_client, manager)).json()

    def broken(*args: object, **kwargs: object) -> object:
        raise error

    monkeypatch.setattr("hds.services.schedule_job.generate", broken)
    assert str(await work()) == job["id"]

    row = await job_row(test_db, job["id"])
    assert (row["status"], row["error_code"], row["result"]) == (
        "failed",
        "jobs.solver_failed",
        None,
    )


async def test_a_run_that_ends_after_its_job_was_claimed_again_changes_nothing(
    async_client: AsyncClient, seed_users: dict[str, User], test_db: AsyncSession
) -> None:
    """The newer run keeps the job; the late one finds it no longer its own."""
    from hds.core.database import get_session_factory
    from hds.main import app
    from hds.wiring import job_service

    manager = await headers_for(async_client, "manager")
    await staffed_project(async_client, manager)
    job = (await ask(async_client, manager)).json()
    factory = app.dependency_overrides[get_session_factory]()
    async with factory() as session:
        late = await job_service(session).claim()
        await session.commit()
    assert late is not None
    await test_db.execute(
        text("UPDATE schedule_jobs SET lease_until = :t WHERE id = :i"),
        {"t": datetime.now(UTC) - timedelta(seconds=1), "i": job["id"]},
    )
    await test_db.commit()
    async with factory() as session:
        newer = await job_service(session).claim()  # claimed again, still running
        await session.commit()
    assert newer is not None and newer.attempt == 2

    async with factory() as session:
        still_ours = await job_service(session).fail(late, "jobs.solver_failed")
        await session.commit()

    assert still_ours is False
    row = await job_row(test_db, job["id"])
    assert (row["status"], row["attempts"], row["error_code"]) == ("running", 2, None)


async def test_a_job_is_solved_with_the_limits_it_was_asked_for_with(
    async_client: AsyncClient, seed_users: dict[str, User], test_db: AsyncSession
) -> None:
    """Review finding: the worker read the limits at claim time, not the job's own."""
    from datetime import timedelta as days

    from tests.schedule_support import SUNDAY

    admin = await headers_for(async_client, "admin")
    await staffed_project(async_client, admin)
    job = (await ask(async_client, admin, SUNDAY, SUNDAY + days(days=4))).json()
    assert job["settings"]["max_weekly_hours"] == 48
    await async_client.patch(
        "/api/v1/organisation/scheduling", json={"max_weekly_hours": 8}, headers=admin
    )

    await work()

    done = (await async_client.get(f"{JOBS}/{job['id']}", headers=admin)).json()
    assert done["proposal"]["unfilled"] == []  # five 8-hour mornings fit 48, not 8


async def test_a_chosen_project_that_ended_before_the_run_fails_the_job(
    async_client: AsyncClient, seed_users: dict[str, User], test_db: AsyncSession
) -> None:
    """Review finding: the project was silently left out and the job still succeeded."""
    manager = await headers_for(async_client, "manager")
    project = await staffed_project(async_client, manager)
    job = (await ask(async_client, manager, project_ids=[project["id"]])).json()
    await async_client.patch(
        f"/api/v1/projects/{project['id']}", json={"status": "ended"}, headers=manager
    )

    await work()

    row = await job_row(test_db, job["id"])
    assert (row["status"], row["error_code"]) == ("failed", "jobs.project_not_active")
