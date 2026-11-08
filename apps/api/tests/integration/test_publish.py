"""Managers and Admins publish drafts; a published shift stays published when changed (T058).

Covers: 003 FR-121, FR-122; US7.
"""

from datetime import date
from typing import Any

import pytest
from httpx import AsyncClient

from hds.models.user import User
from tests.pay_support import project_managers
from tests.roster_support import SHIFTS, add_project, add_shift, headers_for


def publish_url(project: dict[str, Any]) -> str:
    """Where a project's drafts are published."""
    return f"/api/v1/projects/{project['id']}/publish"


@pytest.fixture
async def admin(async_client: AsyncClient, seed_users: dict[str, User]) -> dict[str, str]:
    return await headers_for(async_client, "admin")


@pytest.fixture
async def project(async_client: AsyncClient, admin: dict[str, str]) -> dict[str, Any]:
    return await add_project(async_client, admin)


async def statuses(client: AsyncClient, headers: dict[str, str]) -> dict[str, str]:
    items = (await client.get(SHIFTS, headers=headers)).json()["items"]
    return {item["starts_at"][:10]: item["status"] for item in items}


@pytest.mark.parametrize("role", ["manager", "admin"])
async def test_a_manager_or_admin_publishes_a_projects_drafts_in_a_date_range(
    role: str,
    async_client: AsyncClient,
    admin: dict[str, str],
    project: dict[str, Any],
    seed_users: dict[str, User],
) -> None:
    # A Manager publishes the projects they manage (the owner's decision of 2026-11-08, FR-036).
    managers = await async_client.put(
        project_managers(project["id"]),
        json={"user_ids": [str(seed_users["manager"].id)]},
        headers=admin,
    )
    assert managers.status_code == 200, managers.text
    for day in (7, 8, 9):
        await add_shift(async_client, admin, project, date(2027, 6, day))
    other = await add_project(async_client, admin, "Day Watch")
    await add_shift(async_client, admin, other, date(2027, 6, 8))
    headers = await headers_for(async_client, role)

    published = await async_client.post(
        f"/api/v1/projects/{project['id']}/publish",
        json={"from": "2027-06-07", "to": "2027-06-08"},
        headers=headers,
    )

    assert published.status_code == 200, published.text
    assert published.json() == {"published": 2}
    items = (await async_client.get(SHIFTS, headers=admin)).json()["items"]
    states = sorted(
        (i["project_id"] == project["id"], i["starts_at"][:10], i["status"]) for i in items
    )
    assert states == [
        (False, "2027-06-08", "draft"),
        (True, "2027-06-07", "published"),
        (True, "2027-06-08", "published"),
        (True, "2027-06-09", "draft"),
    ]
    assert all(i["published_at"] for i in items if i["status"] == "published")


async def test_publishing_again_counts_only_new_drafts(
    async_client: AsyncClient, admin: dict[str, str], project: dict[str, Any]
) -> None:
    await add_shift(async_client, admin, project, date(2027, 6, 7))
    url, body = publish_url(project), {"from": "2027-06-01", "to": "2027-06-30"}

    first = await async_client.post(url, json=body, headers=admin)
    again = await async_client.post(url, json=body, headers=admin)

    assert (first.json(), again.json()) == ({"published": 1}, {"published": 0})


@pytest.mark.parametrize("role", ["employee", "client"])
async def test_employees_and_clients_cannot_publish(
    role: str, async_client: AsyncClient, admin: dict[str, str], project: dict[str, Any]
) -> None:
    headers = await headers_for(async_client, role)

    refused = await async_client.post(
        f"/api/v1/projects/{project['id']}/publish",
        json={"from": "2027-06-01", "to": "2027-06-30"},
        headers=headers,
    )

    assert refused.status_code == 403


async def test_a_range_that_ends_before_it_starts_is_refused(
    async_client: AsyncClient, admin: dict[str, str], project: dict[str, Any]
) -> None:
    refused = await async_client.post(
        f"/api/v1/projects/{project['id']}/publish",
        json={"from": "2027-06-30", "to": "2027-06-01"},
        headers=admin,
    )

    assert refused.status_code == 422
    assert refused.json()["code"] == "shifts.date_range_invalid"


async def test_a_published_shift_stays_published_when_an_admin_changes_it(
    async_client: AsyncClient, admin: dict[str, str], project: dict[str, Any]
) -> None:
    shift = (await add_shift(async_client, admin, project, date(2027, 6, 7)))["shift"]
    await async_client.post(
        f"/api/v1/projects/{project['id']}/publish",
        json={"from": "2027-06-07", "to": "2027-06-07"},
        headers=admin,
    )

    changed = await async_client.patch(
        f"{SHIFTS}/{shift['id']}", json={"ends_at": "2027-06-07T13:00:00Z"}, headers=admin
    )

    assert changed.status_code == 200
    assert changed.json()["shift"]["status"] == "published"


async def test_publishing_uses_the_dates_at_the_projects_site(
    async_client: AsyncClient, admin: dict[str, str], seed_users: dict[str, User]
) -> None:
    """At a site 14 hours ahead, a 07:00 shift on 8 June starts on 7 June in UTC."""
    owner = await async_client.post(
        "/api/v1/clients", json={"name": "Far", "contract_start": "2027-01-01"}, headers=admin
    )
    site = await async_client.post(
        "/api/v1/sites",
        json={"name": "Far site", "address": "1 Far St", "timezone": "Pacific/Kiritimati"},
        headers=admin,
    )
    created = await async_client.post(
        "/api/v1/projects",
        json={
            "client_id": owner.json()["id"],
            "site_id": site.json()["id"],
            "name": "Far Watch",
            "type": "security",
            "billing_rate": "75.0000",
            "start_date": "2027-01-01",
        },
        headers=admin,
    )
    types = (await async_client.get("/api/v1/shift-types", headers=admin)).json()["items"]
    ids = {t["name"]: t["id"] for t in types}
    await async_client.put(
        f"/api/v1/projects/{created.json()['id']}/shift-types",
        json={"ids": [ids["morning"]]},
        headers=admin,
    )
    far = {**created.json(), "types": ids}
    await add_shift(async_client, admin, far, date(2027, 6, 8))

    published = await async_client.post(
        f"/api/v1/projects/{far['id']}/publish",
        json={"from": "2027-06-08", "to": "2027-06-08"},
        headers=admin,
    )

    assert published.json() == {"published": 1}
