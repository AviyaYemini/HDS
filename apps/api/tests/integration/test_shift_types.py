"""The organisation's shift types, and which of them each project uses (T049, T053).

Covers: 003 FR-110 to FR-112; US8.
"""

from typing import Any

import pytest
from httpx import AsyncClient

from hds.models.user import User
from tests.roster_support import headers_for

TYPES = "/api/v1/shift-types"


async def add_project(client: AsyncClient, headers: dict[str, str]) -> Any:
    owner = await client.post(
        "/api/v1/clients", json={"name": "Acme", "contract_start": "2027-01-01"}, headers=headers
    )
    site = await client.post(
        "/api/v1/sites",
        json={"name": "HQ", "address": "1 HQ St", "timezone": "Asia/Jerusalem"},
        headers=headers,
    )
    project = await client.post(
        "/api/v1/projects",
        json={
            "client_id": owner.json()["id"],
            "site_id": site.json()["id"],
            "name": "Night Watch",
            "type": "security",
            "billing_rate": "75.0000",
            "start_date": "2027-01-01",
        },
        headers=headers,
    )
    assert project.status_code == 201, project.text
    return project.json()


async def types_by_name(client: AsyncClient, headers: dict[str, str]) -> dict[str, Any]:
    listed = await client.get(TYPES, headers=headers)
    assert listed.status_code == 200, listed.text
    return {t["name"]: t for t in listed.json()["items"]}


async def test_a_new_organisation_has_morning_afternoon_and_night(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    found = await types_by_name(async_client, await headers_for(async_client, "employee"))

    assert {
        (name, t["starts_at_local"], t["ends_at_local"], t["crosses_midnight"])
        for name, t in found.items()
    } == {
        ("morning", "07:00:00", "15:00:00", False),
        ("afternoon", "15:00:00", "23:00:00", False),
        ("night", "23:00:00", "07:00:00", True),
    }


@pytest.mark.parametrize("role", ["manager", "admin"])
async def test_a_manager_or_admin_adds_a_type_and_crossing_midnight_follows_from_the_times(
    role: str, async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    headers = await headers_for(async_client, role)

    day = await async_client.post(
        TYPES,
        json={"name": "long day", "starts_at_local": "07:00", "ends_at_local": "19:00"},
        headers=headers,
    )
    night = await async_client.post(
        TYPES,
        json={"name": "long night", "starts_at_local": "19:00", "ends_at_local": "07:00"},
        headers=headers,
    )

    assert (day.status_code, night.status_code) == (201, 201), night.text
    assert (day.json()["crosses_midnight"], night.json()["crosses_midnight"]) == (False, True)


async def test_an_employee_reads_but_cannot_change_types(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    employee = await headers_for(async_client, "employee")
    morning = (await types_by_name(async_client, employee))["morning"]

    added = await async_client.post(
        TYPES,
        json={"name": "x", "starts_at_local": "07:00", "ends_at_local": "08:00"},
        headers=employee,
    )
    changed = await async_client.patch(
        f"{TYPES}/{morning['id']}", json={"name": "early"}, headers=employee
    )

    assert (added.status_code, changed.status_code) == (403, 403)


async def test_a_client_sees_no_types(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    client = await headers_for(async_client, "client")

    assert (await async_client.get(TYPES, headers=client)).status_code == 403


async def test_a_duplicate_active_name_is_refused_in_any_letter_case(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    manager = await headers_for(async_client, "manager")

    refused = await async_client.post(
        TYPES,
        json={"name": "Morning", "starts_at_local": "06:00", "ends_at_local": "14:00"},
        headers=manager,
    )

    assert refused.status_code == 409
    assert refused.json()["code"] == "shift_types.duplicate_name"


async def test_a_retired_type_frees_its_name_and_stays_listed_as_retired(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    manager = await headers_for(async_client, "manager")
    morning = (await types_by_name(async_client, manager))["morning"]

    retired = await async_client.patch(
        f"{TYPES}/{morning['id']}", json={"is_active": False}, headers=manager
    )
    again = await async_client.post(
        TYPES,
        json={"name": "morning", "starts_at_local": "06:00", "ends_at_local": "14:00"},
        headers=manager,
    )

    assert retired.status_code == 200, retired.text
    assert retired.json()["is_active"] is False
    assert again.status_code == 201, again.text


async def test_renaming_to_a_taken_name_is_refused(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    manager = await headers_for(async_client, "manager")
    morning = (await types_by_name(async_client, manager))["morning"]

    refused = await async_client.patch(
        f"{TYPES}/{morning['id']}", json={"name": "NIGHT"}, headers=manager
    )

    assert refused.status_code == 409
    assert refused.json()["code"] == "shift_types.duplicate_name"


async def test_changing_the_times_recomputes_crossing_midnight(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    manager = await headers_for(async_client, "manager")
    morning = (await types_by_name(async_client, manager))["morning"]

    changed = await async_client.patch(
        f"{TYPES}/{morning['id']}", json={"ends_at_local": "06:00"}, headers=manager
    )

    assert changed.status_code == 200, changed.text
    assert changed.json()["crosses_midnight"] is True


async def test_a_project_lists_the_complete_set_of_types_it_uses(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    manager = await headers_for(async_client, "manager")
    project = await add_project(async_client, manager)
    types = await types_by_name(async_client, manager)
    url = f"/api/v1/projects/{project['id']}/shift-types"

    first = await async_client.put(
        url, json={"ids": [types["morning"]["id"], types["night"]["id"]]}, headers=manager
    )
    second = await async_client.put(url, json={"ids": [types["night"]["id"]]}, headers=manager)
    listed = await async_client.get(url, headers=manager)

    assert first.status_code == 200, first.text
    assert sorted(t["name"] for t in first.json()["items"]) == ["morning", "night"]
    assert [t["name"] for t in second.json()["items"]] == ["night"]
    assert [t["name"] for t in listed.json()["items"]] == ["night"]


async def test_a_project_cannot_use_another_organisations_or_a_retired_type(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    manager = await headers_for(async_client, "manager")
    project = await add_project(async_client, manager)
    morning = (await types_by_name(async_client, manager))["morning"]
    url = f"/api/v1/projects/{project['id']}/shift-types"
    await async_client.patch(f"{TYPES}/{morning['id']}", json={"is_active": False}, headers=manager)

    foreign = await async_client.put(
        url, json={"ids": ["00000000-0000-4000-8000-000000000000"]}, headers=manager
    )
    retired = await async_client.put(url, json={"ids": [morning["id"]]}, headers=manager)

    assert foreign.status_code == 404
    assert retired.status_code == 422
    assert retired.json()["code"] == "shift_types.retired"


async def test_an_employee_cannot_change_a_projects_types(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    manager = await headers_for(async_client, "manager")
    project = await add_project(async_client, manager)
    employee = await headers_for(async_client, "employee")

    refused = await async_client.put(
        f"/api/v1/projects/{project['id']}/shift-types", json={"ids": []}, headers=employee
    )

    assert refused.status_code == 403


async def test_a_retired_type_leaves_every_projects_set(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    """A project's set holds active types only, so sending it back is always accepted."""
    manager = await headers_for(async_client, "manager")
    project = await add_project(async_client, manager)
    types = await types_by_name(async_client, manager)
    url = f"/api/v1/projects/{project['id']}/shift-types"
    await async_client.put(
        url, json={"ids": [types["morning"]["id"], types["night"]["id"]]}, headers=manager
    )
    await async_client.patch(
        f"{TYPES}/{types['morning']['id']}", json={"is_active": False}, headers=manager
    )

    listed = await async_client.get(url, headers=manager)
    resent = await async_client.put(
        url, json={"ids": [t["id"] for t in listed.json()["items"]]}, headers=manager
    )

    assert [t["name"] for t in listed.json()["items"]] == ["night"]
    assert resent.status_code == 200, resent.text


@pytest.mark.parametrize(
    "times",
    [{"starts_at_local": "07:00", "ends_at_local": "07:00"}],
    ids=["equal"],
)
async def test_a_type_that_starts_and_ends_at_the_same_time_is_refused(
    times: dict[str, str], async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    manager = await headers_for(async_client, "manager")
    morning = (await types_by_name(async_client, manager))["morning"]

    added = await async_client.post(TYPES, json={"name": "odd", **times}, headers=manager)
    changed = await async_client.patch(
        f"{TYPES}/{morning['id']}", json={"ends_at_local": "07:00"}, headers=manager
    )

    for refused in (added, changed):
        assert refused.status_code == 422, refused.text
        assert refused.json()["code"] == "shift_types.times_equal"
