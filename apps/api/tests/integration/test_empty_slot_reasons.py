"""Why a run left a place empty, with names and numbers, through the API (008 T020, FR-013).

The pure explanations are tested in tests/unit/scheduling/test_explanations.py; this checks that a
run stores them and the job's answer carries them.
"""

from datetime import timedelta
from typing import Any

from httpx import AsyncClient

from hds.models.user import User
from tests.roster_support import add_project, add_shift, assign, headers_for
from tests.schedule_support import JOBS, SUNDAY, ask, staffed_project, work


async def test_rest_names_the_employee_with_the_rest_they_would_have_and_need(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    await staffed_project(async_client, admin)
    elsewhere = await add_project(async_client, admin, "Elsewhere")
    night = (await add_shift(async_client, admin, elsewhere, SUNDAY - timedelta(days=1), "night"))[
        "shift"
    ]
    worker = str(seed_users["employee"].id)
    assert (await assign(async_client, admin, night["id"], worker)).status_code == 201

    job = (await ask(async_client, admin, SUNDAY, SUNDAY)).json()
    await work()
    done: dict[str, Any] = (await async_client.get(f"{JOBS}/{job['id']}", headers=admin)).json()

    assert done["status"] == "succeeded", done
    (empty,) = done["proposal"]["unfilled"]
    rest = next(r for r in empty["reasons"] if r["code"] == "schedule.minimum_rest")
    assert rest["employees"] == 1
    assert rest["people"] == [
        {
            "user_id": worker,
            "code": "schedule.minimum_rest",
            "have": 0,
            "need": done["settings"]["min_rest_hours"] * 60,
            "week_minutes": None,
            "slot_minutes": None,
            "limit": None,
        }
    ]
