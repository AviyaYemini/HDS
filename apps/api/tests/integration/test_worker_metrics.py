"""Schedule jobs counted by state from the real table (T017).

Covers: specs/006-deploy-operations FR-008; contracts/ops.md section 2.
"""

from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.database import get_session_factory
from hds.main import app
from hds.models.user import User
from hds.wiring import job_service
from tests.roster_support import headers_for
from tests.schedule_support import ask, staffed_project


async def test_the_counts_come_from_the_table_and_omit_empty_states(
    async_client: AsyncClient, seed_users: dict[str, User], test_db: AsyncSession
) -> None:
    manager = await headers_for(async_client, "manager")
    await staffed_project(async_client, manager)
    first = (await ask(async_client, manager)).json()
    await ask(async_client, manager)
    third = (await ask(async_client, manager)).json()
    await test_db.execute(
        text(
            "UPDATE schedule_jobs SET status = 'failed', error_code = 'jobs.solver_failed' "
            "WHERE id = :i"
        ),
        {"i": third["id"]},
    )
    await test_db.commit()

    factory = app.dependency_overrides[get_session_factory]()
    async with factory() as session:
        counted = await job_service(session).counts_by_state()

    assert counted == {"queued": 2, "failed": 1}
    assert first["id"] != third["id"]
