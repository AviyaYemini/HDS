"""Asking for a roster, and reading the run and its proposal (004 T015).

Covers: 004 FR-004, FR-005, FR-008 to FR-010, FR-019, FR-022, FR-027; US2.
"""

from datetime import timedelta

import pytest
from httpx import AsyncClient

from hds.models.user import User
from tests.integration.conftest import OtherOrg
from tests.roster_support import headers_for
from tests.schedule_support import JOBS, SUNDAY, ask, staffed_project, work


@pytest.mark.parametrize("role", ["manager", "admin"])
async def test_a_manager_or_admin_asks_for_a_roster_and_reads_the_queued_job(
    async_client: AsyncClient, seed_users: dict[str, User], role: str
) -> None:
    headers = await headers_for(async_client, role)
    await staffed_project(async_client, headers)

    answer = await ask(async_client, headers)

    assert answer.status_code == 202, answer.text
    job = answer.json()
    assert job["status"] == "queued"
    assert (job["from"], job["to"]) == ("2027-06-06", "2027-06-12")
    assert job["requested_by"] == str(seed_users[role].id)
    assert job["settings"] == {"min_rest_hours": 11, "max_weekly_hours": 48}
    read = await async_client.get(f"{JOBS}/{job['id']}", headers=headers)
    assert read.status_code == 200
    assert read.json()["proposal"] is None
    listed = await async_client.get(JOBS, headers=headers)
    assert [j["id"] for j in listed.json()["items"]] == [job["id"]]


async def test_the_worker_turns_the_job_into_a_proposal_and_changes_no_roster(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    manager = await headers_for(async_client, "manager")
    await staffed_project(async_client, manager)
    # Sunday to Thursday: five 8-hour mornings for the one employee, within the weekly 48.
    job = (await ask(async_client, manager, SUNDAY, SUNDAY + timedelta(days=4))).json()

    assert str(await work()) == job["id"]

    done = (await async_client.get(f"{JOBS}/{job['id']}", headers=manager)).json()
    assert done["status"] == "succeeded", done
    assert isinstance(done["optimal"], bool)
    assert done["proposal_state"] == "open"
    proposal = done["proposal"]
    assert len(proposal["slots"]) == 5
    assert proposal["unfilled"] == []
    assert all(len(s["assigned"]) == 1 for s in proposal["slots"])
    roster = await async_client.get("/api/v1/shifts?from=2027-06-06&to=2027-06-12", headers=manager)
    assert roster.json()["items"] == []  # nothing stored until accepted (FR-022)


@pytest.mark.parametrize("days", [-1, 31])
async def test_a_backwards_or_too_long_range_is_refused(
    async_client: AsyncClient, seed_users: dict[str, User], days: int
) -> None:
    manager = await headers_for(async_client, "manager")

    answer = await ask(async_client, manager, SUNDAY, SUNDAY + timedelta(days=days))

    assert answer.status_code == 422
    assert answer.json()["code"] == "schedule.range_invalid"
    assert (await async_client.get(JOBS, headers=manager)).json()["items"] == []


async def test_thirty_one_days_are_allowed(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    manager = await headers_for(async_client, "manager")
    answer = await ask(async_client, manager, SUNDAY, SUNDAY + timedelta(days=30))
    assert answer.status_code == 202, answer.text


async def test_an_ended_or_foreign_project_is_refused(
    async_client: AsyncClient, seed_users: dict[str, User], other_org: OtherOrg
) -> None:
    manager = await headers_for(async_client, "manager")
    project = await staffed_project(async_client, manager)
    ended = await async_client.patch(
        f"/api/v1/projects/{project['id']}", json={"status": "ended"}, headers=manager
    )
    assert ended.status_code == 200, ended.text

    for ids in ([project["id"]], [str(other_org.project.id)]):
        answer = await ask(async_client, manager, project_ids=ids)
        assert answer.status_code == 422, ids
        assert answer.json()["code"] == "schedule.project_not_active"


async def test_a_repeat_with_the_same_key_answers_the_first_job(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    manager = await headers_for(async_client, "manager")
    keyed = {**manager, "Idempotency-Key": "week-23"}

    first = await ask(async_client, keyed)
    again = await ask(async_client, keyed)
    other = await ask(async_client, keyed, SUNDAY + timedelta(days=7))

    assert (first.status_code, again.status_code) == (202, 202)
    assert again.json()["id"] == first.json()["id"]
    assert other.status_code == 422
    assert other.json()["code"] == "idempotency.key_reused"
    assert len((await async_client.get(JOBS, headers=manager)).json()["items"]) == 1


@pytest.mark.parametrize("role", ["employee", "client"])
async def test_employees_and_clients_cannot_ask_for_or_read_jobs(
    async_client: AsyncClient, seed_users: dict[str, User], role: str
) -> None:
    job = (await ask(async_client, await headers_for(async_client, "manager"))).json()
    headers = await headers_for(async_client, role)

    assert (await ask(async_client, headers)).status_code == 403
    assert (await async_client.get(JOBS, headers=headers)).status_code == 403
    assert (await async_client.get(f"{JOBS}/{job['id']}", headers=headers)).status_code == 403


async def test_another_organisations_job_is_not_found(
    async_client: AsyncClient, seed_users: dict[str, User], other_org: OtherOrg
) -> None:
    manager = await headers_for(async_client, "manager")
    assert other_org.job is not None

    answer = await async_client.get(f"{JOBS}/{other_org.job.id}", headers=manager)

    assert answer.status_code == 404
    assert (await async_client.get(JOBS, headers=manager)).json()["items"] == []
