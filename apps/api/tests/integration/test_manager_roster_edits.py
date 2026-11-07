"""Managers edit the rosters of the projects they manage (007 FR-028, decision O9, Proposed).

Covers: a Manager of the project creates, changes, staffs and deletes its shifts; a Manager of
other projects is refused with ``roster.project_not_managed``; only an Admin sets or clears a
shift's pay rate; employees and clients are refused at the gate.
"""

from typing import Any

import pytest
from httpx import AsyncClient

from hds.models.user import User
from tests.pay_support import project_managers
from tests.roster_support import FIRST, SHIFTS, add_project, add_shift, assign, headers_for


async def managed_project(client: AsyncClient, seed_users: dict[str, User]) -> dict[str, Any]:
    admin = await headers_for(client, "admin")
    project = await add_project(client, admin)
    put = await client.put(
        project_managers(project["id"]),
        json={"user_ids": [str(seed_users["manager"].id)]},
        headers=admin,
    )
    assert put.status_code == 200, put.text
    return project


async def test_a_manager_runs_the_roster_of_a_project_they_manage(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    project = await managed_project(async_client, seed_users)
    manager = await headers_for(async_client, "manager")

    created = await add_shift(async_client, manager, project, FIRST)
    shift = created["shift"]
    changed = await async_client.patch(
        f"{SHIFTS}/{shift['id']}", json={"headcount": 2}, headers=manager
    )
    assert changed.status_code == 200, changed.text
    assigned = await assign(async_client, manager, shift["id"], seed_users["employee"].id)
    assert assigned.status_code == 201, assigned.text
    removed = await async_client.delete(
        f"/api/v1/assignments/{assigned.json()['assignment']['id']}", headers=manager
    )
    assert removed.status_code == 200, removed.text
    deleted = await async_client.delete(f"{SHIFTS}/{shift['id']}", headers=manager)
    assert deleted.status_code == 204


async def test_a_manager_of_other_projects_is_refused(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    other = await add_project(async_client, admin, "Not theirs")
    shift = (await add_shift(async_client, admin, other, FIRST))["shift"]
    manager = await headers_for(async_client, "manager")

    refused = await async_client.patch(
        f"{SHIFTS}/{shift['id']}", json={"headcount": 2}, headers=manager
    )
    assert refused.status_code == 403
    assert refused.json()["code"] == "roster.project_not_managed"
    read = await async_client.get(SHIFTS, params={"project_id": other["id"]}, headers=manager)
    assert read.status_code == 200  # read-only, not hidden


async def test_only_an_admin_sets_or_clears_a_shifts_pay_rate(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    project = await managed_project(async_client, seed_users)
    manager = await headers_for(async_client, "manager")
    shift = (await add_shift(async_client, manager, project, FIRST))["shift"]
    for sent in ({"pay_rate": "60.0000"}, {"pay_rate": None}):
        refused = await async_client.patch(f"{SHIFTS}/{shift['id']}", json=sent, headers=manager)
        assert refused.status_code == 403, sent
        assert refused.json()["code"] == "shift.pay_rate_admin_only"
    admin = await headers_for(async_client, "admin")
    ok = await async_client.patch(
        f"{SHIFTS}/{shift['id']}", json={"pay_rate": "60.0000"}, headers=admin
    )
    assert ok.status_code == 200, ok.text


@pytest.mark.parametrize("role", ["employee", "client"])
async def test_employees_and_clients_cannot_change_any_roster(
    role: str, async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    project = await managed_project(async_client, seed_users)
    shift = (
        await add_shift(async_client, await headers_for(async_client, "admin"), project, FIRST)
    )["shift"]
    refused = await async_client.delete(
        f"{SHIFTS}/{shift['id']}", headers=await headers_for(async_client, role)
    )
    assert refused.status_code == 403
