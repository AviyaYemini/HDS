"""Who else could cover a slot of a proposal, through the API (008 T022).

Covers: 008 FR-014, FR-016, FR-035. Guards on other projects, of either kind, are offered when
free and judged by all their shifts; the answer carries ids, codes and numbers only, never pay, a
site or another project's shift. Managers of every project in the run and Admins may read it; an
Employee and a Client may not; another organisation's job is not found.
"""

from datetime import timedelta
from typing import Any

import pytest
from httpx import AsyncClient

from hds.models.user import User
from tests.integration.conftest import OtherOrg
from tests.report_support import employee
from tests.roster_support import add_project, add_shift, assign, headers_for
from tests.schedule_support import JOBS, SUNDAY, ask, staffed_project, work

OPTION_FIELDS = {
    "user_id",
    "free",
    "code",
    "have",
    "need",
    "week_minutes",
    "slot_minutes",
    "limit",
    "close",
    "open",
    "minutes",
    "nights",
    "weekends",
}


async def solved(
    client: AsyncClient, headers: dict[str, str], project: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Sunday only, solved (over ``project`` alone when given); the finished job."""
    only = {"project_ids": [project["id"]]} if project else {}
    job = (await ask(client, headers, SUNDAY, SUNDAY, **only)).json()
    await work()
    done: dict[str, Any] = (await client.get(f"{JOBS}/{job['id']}", headers=headers)).json()
    assert done["status"] == "succeeded", done
    return done


async def first_key(client: AsyncClient, headers: dict[str, str], job: dict[str, Any]) -> str:
    view = (await client.get(f"{JOBS}/{job['id']}/proposal", headers=headers)).json()
    return str(view["slots"][0]["key"])


async def test_guards_elsewhere_are_offered_free_or_with_the_rule_they_break(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    guarded = await staffed_project(async_client, admin)
    trainer_project = await add_project(async_client, admin, "Academy")
    free = await employee(async_client, admin, "Free Guard")
    busy = await employee(async_client, admin, "Busy Trainer")
    taken = (await add_shift(async_client, admin, trainer_project, SUNDAY))["shift"]
    assert (await assign(async_client, admin, taken["id"], busy)).status_code == 201
    # The run covers the guards' project alone; the trainer's shift is outside it.
    job = await solved(async_client, admin, guarded)
    key = await first_key(async_client, admin, job)

    answer = await async_client.get(f"{JOBS}/{job['id']}/slots/{key}/cover", headers=admin)

    assert answer.status_code == 200, answer.text
    body = answer.json()
    by = {o["user_id"]: o for o in body["options"]}
    assert by[free]["free"] is True and by[free]["code"] is None
    assert by[busy]["free"] is False and by[busy]["code"] == "schedule.overlap"
    assert [o["free"] for o in body["options"]] == sorted(
        (o["free"] for o in body["options"]), reverse=True
    )
    # Ids, codes and numbers only: nothing names a shift, a site, a project or pay.
    assert all(set(o) == OPTION_FIELDS for o in body["options"])
    assert set(body) == {"key", "options", "totals"}
    assert taken["id"] not in answer.text and trainer_project["id"] not in answer.text
    # The average counts those who work: every active Employee, not the Admin and Manager idle here.
    listed = (await async_client.get("/api/v1/employees", headers=admin)).json()["items"]
    employees = [p for p in listed if p["role"] == "employee" and p["is_active"]]
    assert body["totals"]["people"] == len(employees)


async def test_rest_is_given_with_its_numbers(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    guarded = await staffed_project(async_client, admin)
    elsewhere = await add_project(async_client, admin, "Elsewhere")
    tired = await employee(async_client, admin, "Night Guard")
    night = (await add_shift(async_client, admin, elsewhere, SUNDAY - timedelta(days=1), "night"))[
        "shift"
    ]
    assert (await assign(async_client, admin, night["id"], tired)).status_code == 201
    job = await solved(async_client, admin, guarded)
    key = await first_key(async_client, admin, job)

    body = (await async_client.get(f"{JOBS}/{job['id']}/slots/{key}/cover", headers=admin)).json()

    (option,) = [o for o in body["options"] if o["user_id"] == tired]
    assert (option["code"], option["have"]) == ("schedule.minimum_rest", 0)
    assert option["need"] == job["settings"]["min_rest_hours"] * 60


async def test_a_slot_not_in_the_proposal_is_not_found(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    await staffed_project(async_client, admin)
    job = await solved(async_client, admin)

    answer = await async_client.get(f"{JOBS}/{job['id']}/slots/need:nowhere/cover", headers=admin)

    assert answer.status_code == 404, answer.text


async def test_a_manager_of_the_run_may_read_it(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    manager = await headers_for(async_client, "manager")
    await staffed_project(async_client, admin)
    job = await solved(async_client, admin)
    key = await first_key(async_client, admin, job)

    answer = await async_client.get(f"{JOBS}/{job['id']}/slots/{key}/cover", headers=manager)

    assert answer.status_code == 200, answer.text


async def test_a_manager_of_only_some_of_the_run_cannot_read_it(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    manager = await headers_for(async_client, "manager")
    await staffed_project(async_client, admin)
    await staffed_project(async_client, admin, "Not Theirs", managed=False)
    job = await solved(async_client, admin)
    key = await first_key(async_client, admin, job)

    answer = await async_client.get(f"{JOBS}/{job['id']}/slots/{key}/cover", headers=manager)

    assert answer.status_code == 403, answer.text
    assert answer.json()["code"] == "roster.project_not_managed"


@pytest.mark.parametrize("role", ["employee", "client"])
async def test_employees_and_clients_cannot_read_it(
    async_client: AsyncClient, seed_users: dict[str, User], role: str
) -> None:
    admin = await headers_for(async_client, "admin")
    await staffed_project(async_client, admin)
    job = await solved(async_client, admin)
    key = await first_key(async_client, admin, job)

    answer = await async_client.get(
        f"{JOBS}/{job['id']}/slots/{key}/cover", headers=await headers_for(async_client, role)
    )

    assert answer.status_code == 403, answer.text


async def test_another_organisations_job_is_not_found(
    async_client: AsyncClient, seed_users: dict[str, User], other_org: OtherOrg
) -> None:
    admin = await headers_for(async_client, "admin")
    assert other_org.job is not None

    answer = await async_client.get(f"{JOBS}/{other_org.job.id}/slots/need:x/cover", headers=admin)

    assert answer.status_code == 404, answer.text
