"""Schedule runs are scoped to the projects a Manager manages (007 T108, FR-081).

The reviewer's ruling of 2026-11-08, from the owner's decisions O9 (Managers edit only their
projects' rosters) and T098 (publish per project): a Manager asks for, accepts and discards runs
only over projects they manage; an Admin over any. The runs stay readable by any Manager.
"""

from typing import Any

from httpx import AsyncClient

from hds.models.user import User
from tests.pay_support import project_managers
from tests.roster_support import headers_for
from tests.schedule_support import JOBS, SUNDAY, ask, staffed_project, work


async def managed_by(
    client: AsyncClient, admin: dict[str, str], project: dict[str, Any], users: list[User]
) -> None:
    answer = await client.put(
        project_managers(project["id"]),
        json={"user_ids": [str(u.id) for u in users]},
        headers=admin,
    )
    assert answer.status_code == 200, answer.text


async def test_a_manager_cannot_ask_for_a_project_they_do_not_manage(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    theirs = await staffed_project(async_client, admin, "Theirs")
    other = await staffed_project(async_client, admin, "Not theirs")
    await managed_by(async_client, admin, theirs, [seed_users["manager"]])
    manager = await headers_for(async_client, "manager")

    refused = await ask(async_client, manager, project_ids=[theirs["id"], other["id"]])

    assert refused.status_code == 403, refused.text
    assert refused.json()["code"] == "roster.project_not_managed"
    listed = await async_client.get(JOBS, headers=admin)
    assert listed.json()["items"] == []  # no job was created


async def test_an_empty_list_means_the_managers_own_projects_and_an_admins_every_one(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    theirs = await staffed_project(async_client, admin, "Theirs")
    other = await staffed_project(async_client, admin, "Not theirs")
    await managed_by(async_client, admin, theirs, [seed_users["manager"]])
    manager = await headers_for(async_client, "manager")

    mine = await ask(async_client, manager)
    everyone = await ask(async_client, admin)

    assert mine.status_code == 202, mine.text
    assert mine.json()["project_ids"] == [theirs["id"]]
    assert everyone.status_code == 202, everyone.text
    assert sorted(everyone.json()["project_ids"]) == sorted([theirs["id"], other["id"]])


async def test_a_manager_of_no_project_is_told_there_is_nothing_to_schedule(
    async_client: AsyncClient,
) -> None:
    admin = await headers_for(async_client, "admin")
    await staffed_project(async_client, admin, "Not theirs")
    manager = await headers_for(async_client, "manager")

    refused = await ask(async_client, manager)

    assert refused.status_code == 422, refused.text
    assert refused.json()["code"] == "schedule.no_projects"


async def test_a_run_for_one_project_proposes_only_its_slots(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    theirs = await staffed_project(async_client, admin, "Theirs")
    await staffed_project(async_client, admin, "Not theirs")
    await managed_by(async_client, admin, theirs, [seed_users["manager"]])
    manager = await headers_for(async_client, "manager")
    job = (await ask(async_client, manager, project_ids=[theirs["id"]])).json()

    assert str(await work()) == job["id"]

    done = (await async_client.get(f"{JOBS}/{job['id']}", headers=manager)).json()
    slots = done["proposal"]["slots"]
    assert slots and {s["project_id"] for s in slots} == {theirs["id"]}


async def test_accepting_after_the_manager_lost_a_project_is_refused_whole(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    theirs = await staffed_project(async_client, admin, "Theirs")
    await managed_by(async_client, admin, theirs, [seed_users["manager"]])
    manager = await headers_for(async_client, "manager")
    job = (await ask(async_client, manager, project_ids=[theirs["id"]])).json()
    assert str(await work()) == job["id"]
    await managed_by(async_client, admin, theirs, [])

    refused = await async_client.post(f"{JOBS}/{job['id']}/accept", headers=manager)

    assert refused.status_code == 403, refused.text
    assert refused.json()["code"] == "roster.project_not_managed"
    first, last = SUNDAY.isoformat(), SUNDAY.replace(day=SUNDAY.day + 6).isoformat()
    roster = await async_client.get(f"/api/v1/shifts?from={first}&to={last}", headers=admin)
    assert roster.json()["items"] == []  # nothing stored
    read = await async_client.get(f"{JOBS}/{job['id']}", headers=manager)
    assert read.status_code == 200  # any Manager still reads the run
    assert read.json()["proposal_state"] == "open"


async def test_discarding_takes_the_same_scope_as_accepting(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await staffed_project(async_client, admin, "Not theirs")
    job = (await ask(async_client, admin, project_ids=[project["id"]])).json()
    assert str(await work()) == job["id"]
    manager = await headers_for(async_client, "manager")

    refused = await async_client.post(f"{JOBS}/{job['id']}/discard", headers=manager)

    assert refused.status_code == 403, refused.text
    assert refused.json()["code"] == "roster.project_not_managed"
    await managed_by(async_client, admin, project, [seed_users["manager"]])
    allowed = await async_client.post(f"{JOBS}/{job['id']}/discard", headers=manager)
    assert allowed.status_code == 200, allowed.text
