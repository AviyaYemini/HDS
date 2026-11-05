"""Two workers never run one job twice (004 T017).

Covers: 004 FR-008; research G7 (``FOR UPDATE SKIP LOCKED``).
"""

from collections.abc import Awaitable, Callable
from typing import Any

from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hds.models.user import User
from tests.roster_support import headers_for
from tests.schedule_support import ask, staffed_project, work

ROUNDS = 10


async def test_two_workers_claim_one_job_once(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    test_db: AsyncSession,
    race: Callable[..., Awaitable[list[Any]]],
) -> None:
    manager = await headers_for(async_client, "manager")
    await staffed_project(async_client, manager)

    for n in range(ROUNDS):
        job = (await ask(async_client, manager)).json()

        results = await race(work, work)

        assert sorted(str(r) for r in results) == sorted(["None", job["id"]]), f"round {n}"
        await test_db.rollback()
        attempts = await test_db.execute(
            text("SELECT attempts, status FROM schedule_jobs WHERE id = :i"), {"i": job["id"]}
        )
        assert tuple(attempts.one()) == (1, "succeeded"), f"round {n}"
