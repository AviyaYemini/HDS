"""Locks, chips and controls through the API, and a proposal's fairness (008 T033).

Covers: 008 FR-017 to FR-026, FR-035. Lock and unlock on the roster within the projects a person
may edit (403 otherwise); a locked draft kept by the next run, which says how many it kept; a lock
gone with its removed assignment; chips and controls stored on a run; a chip naming someone who is
not on the staff refused; a rerun copying its earlier run's chips, never another organisation's;
the fairness of a proposal following an edit; lock as a proposal edit. Employees and Clients are
refused; another organisation's assignment or job is not found.
"""

import uuid
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hds.models.user import User
from tests.integration.conftest import OtherOrg
from tests.pay_support import project_managers
from tests.roster_support import add_project, add_shift, assign, headers_for
from tests.schedule_support import JOBS, SUNDAY, ask, staffed_project, work

ASSIGNMENTS = "/api/v1/assignments"


async def solved(client: AsyncClient, headers: dict[str, str], **extra: Any) -> dict[str, Any]:
    job = await ask(client, headers, SUNDAY, SUNDAY, **extra)
    assert job.status_code == 202, job.text
    await work()
    done: dict[str, Any] = (await client.get(f"{JOBS}/{job.json()['id']}", headers=headers)).json()
    assert done["status"] == "succeeded", done
    return done


async def locked_draft(
    client: AsyncClient, admin: dict[str, str], project: dict[str, Any], worker: User
) -> tuple[dict[str, Any], str]:
    """A draft on Sunday morning with ``worker`` on it, locked; the shift and assignment id."""
    shift = (await add_shift(client, admin, project, SUNDAY))["shift"]
    made = await assign(client, admin, shift["id"], worker.id)
    assert made.status_code == 201, made.text
    assignment = str(made.json()["assignment"]["id"])
    locked = await client.put(f"{ASSIGNMENTS}/{assignment}/lock", headers=admin)
    assert locked.status_code == 200, locked.text
    assert locked.json()["assignment"]["locked"] is True
    return shift, assignment


async def test_a_locked_draft_is_kept_by_the_next_run_which_says_so(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await staffed_project(async_client, admin)
    shift, _ = await locked_draft(async_client, admin, project, seed_users["employee"])

    done = await solved(async_client, admin, project_ids=[project["id"]])

    slot = next(s for s in done["proposal"]["slots"] if s["shift_id"] == shift["id"])
    assert str(seed_users["employee"].id) in slot["kept"]
    assert str(seed_users["employee"].id) not in slot["assigned"]
    assert done["proposal"]["locks_kept"] == 1


async def test_unlock_frees_it_and_a_removed_assignment_takes_its_lock_with_it(
    async_client: AsyncClient, seed_users: dict[str, User], test_db: AsyncSession
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin, "Unlocks")
    _, assignment = await locked_draft(async_client, admin, project, seed_users["employee"])

    unlocked = await async_client.delete(f"{ASSIGNMENTS}/{assignment}/lock", headers=admin)
    again = await async_client.put(f"{ASSIGNMENTS}/{assignment}/lock", headers=admin)
    removed = await async_client.delete(f"{ASSIGNMENTS}/{assignment}", headers=admin)

    assert unlocked.json()["assignment"]["locked"] is False
    assert again.json()["assignment"]["locked"] is True
    assert removed.status_code == 200, removed.text
    left = await test_db.execute(
        text("SELECT count(*) FROM assignments WHERE id = :a"), {"a": assignment}
    )
    assert left.scalar_one() == 0


async def test_a_manager_locks_only_in_projects_they_manage(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    manager = await headers_for(async_client, "manager")
    mine = await add_project(async_client, admin, "Mine")
    put = await async_client.put(
        project_managers(mine["id"]),
        json={"user_ids": [str(seed_users["manager"].id)]},
        headers=admin,
    )
    assert put.status_code == 200, put.text
    theirs = await add_project(async_client, admin, "Theirs")
    a = (
        await assign(
            async_client,
            admin,
            (await add_shift(async_client, admin, mine, SUNDAY))["shift"]["id"],
            seed_users["employee"].id,
        )
    ).json()["assignment"]["id"]
    b = (
        await assign(
            async_client,
            admin,
            (await add_shift(async_client, admin, theirs, SUNDAY))["shift"]["id"],
            seed_users["employee"].id,
        )
    ).json()["assignment"]["id"]

    allowed = await async_client.put(f"{ASSIGNMENTS}/{a}/lock", headers=manager)
    refused = await async_client.put(f"{ASSIGNMENTS}/{b}/lock", headers=manager)

    assert allowed.status_code == 200, allowed.text
    assert (refused.status_code, refused.json()["code"]) == (403, "roster.project_not_managed")


async def test_chips_and_controls_are_stored_and_a_rerun_copies_its_chips(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await staffed_project(async_client, admin)
    worker = str(seed_users["employee"].id)
    chips = [{"kind": "keep_off", "user_id": worker, "weekday": 0}]
    controls = {"keep_changes_small": True, "wishes_strictly": False}

    first = await solved(
        async_client, admin, project_ids=[project["id"]], chips=chips, controls=controls
    )
    rerun = await solved(async_client, admin, project_ids=[project["id"]], based_on=first["id"])

    assert first["controls"] == controls
    assert first["chips"][0]["kind"] == "keep_off" and first["chips"][0]["weekday"] == 0
    assert rerun["based_on"] == first["id"]
    assert rerun["chips"] == first["chips"]


async def test_a_chip_naming_someone_not_on_the_staff_is_refused(
    async_client: AsyncClient, seed_users: dict[str, User], other_org: OtherOrg
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await staffed_project(async_client, admin)

    for who in (uuid.uuid4(), other_org.user.id):
        answer = await ask(
            async_client,
            admin,
            SUNDAY,
            SUNDAY,
            project_ids=[project["id"]],
            chips=[{"kind": "max_shifts", "user_id": str(who), "count": 1}],
        )
        assert (answer.status_code, answer.json()["code"]) == (422, "schedule.chip_invalid")


async def test_a_rerun_of_another_organisations_job_is_not_found(
    async_client: AsyncClient, seed_users: dict[str, User], other_org: OtherOrg
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await staffed_project(async_client, admin)
    assert other_org.job is not None

    answer = await ask(
        async_client,
        admin,
        SUNDAY,
        SUNDAY,
        project_ids=[project["id"]],
        based_on=str(other_org.job.id),
    )

    assert answer.status_code == 404, answer.text


async def test_fairness_follows_an_edit_and_counts_a_lock_set_in_the_proposal(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await staffed_project(async_client, admin)
    worker = str(seed_users["employee"].id)
    done = await solved(
        async_client,
        admin,
        project_ids=[project["id"]],
        chips=[{"kind": "max_shifts", "user_id": worker, "count": 0}],
    )
    before = (await async_client.get(f"{JOBS}/{done['id']}/fairness", headers=admin)).json()
    slot = next(s for s in done["proposal"]["slots"] if s["assigned"])
    person = slot["assigned"][0]

    locked = await async_client.post(
        f"{JOBS}/{done['id']}/edits",
        json={"op": "lock", "key": slot["key"], "user_id": person},
        headers=admin,
    )
    after = (await async_client.get(f"{JOBS}/{done['id']}/fairness", headers=admin)).json()

    assert locked.status_code == 200, locked.text
    placed = next(
        p
        for p in next(s for s in locked.json()["proposal"]["slots"] if s["key"] == slot["key"])[
            "people"
        ]
        if p["user_id"] == person
    )
    assert placed["locked"] is True
    assert (before["locks"], after["locks"]) == (0, 1)
    assert before["totals"]["people"] >= 1
    assert [c["kind"] for c in after["chips"]] == ["max_shifts"]
    assert after["chips"][0]["state"] in ("met", "not_met")


@pytest.mark.parametrize("role", ["employee", "client"])
async def test_employees_and_clients_cannot_lock_or_read_fairness(
    async_client: AsyncClient, seed_users: dict[str, User], role: str
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await staffed_project(async_client, admin)
    done = await solved(async_client, admin, project_ids=[project["id"]])
    shift = (await add_shift(async_client, admin, project, SUNDAY))["shift"]
    a = (await assign(async_client, admin, shift["id"], seed_users["employee"].id)).json()[
        "assignment"
    ]["id"]
    headers = await headers_for(async_client, role)

    answers = [
        await async_client.put(f"{ASSIGNMENTS}/{a}/lock", headers=headers),
        await async_client.get(f"{JOBS}/{done['id']}/fairness", headers=headers),
    ]

    assert [x.status_code for x in answers] == [403, 403]
