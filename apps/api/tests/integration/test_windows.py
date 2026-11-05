"""Managers open submission windows; everyone but a Client can see them (T041).

Covers: 003 FR-106, FR-109, FR-125; US5.
"""

from datetime import UTC, date, datetime, timedelta

import pytest
from httpx import AsyncClient

from hds.models.user import User
from tests.roster_support import FIRST, LAST, WINDOWS, headers_for, open_window


@pytest.mark.parametrize("role", ["manager", "admin"])
async def test_a_manager_or_admin_opens_a_window(
    role: str, async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    headers = await headers_for(async_client, role)

    created = await open_window(async_client, headers)

    assert (created["starts_on"], created["ends_on"]) == (FIRST.isoformat(), LAST.isoformat())
    assert created["is_open"] is True


async def test_an_employee_sees_windows_but_cannot_open_one(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    manager = await headers_for(async_client, "manager")
    employee = await headers_for(async_client, "employee")
    created = await open_window(async_client, manager)

    listed = await async_client.get(WINDOWS, headers=employee)
    refused = await async_client.post(
        WINDOWS,
        json={"starts_on": "2027-07-01", "ends_on": "2027-07-07", "deadline": created["deadline"]},
        headers=employee,
    )

    assert listed.status_code == 200
    assert [w["id"] for w in listed.json()["items"]] == [created["id"]]
    assert refused.status_code == 403


async def test_a_client_sees_no_windows(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    client = await headers_for(async_client, "client")

    assert (await async_client.get(WINDOWS, headers=client)).status_code == 403


async def test_windows_are_listed_by_the_dates_they_cover(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    manager = await headers_for(async_client, "manager")
    june = await open_window(async_client, manager)
    await open_window(async_client, manager, date(2027, 8, 1), date(2027, 8, 7))

    listed = await async_client.get(
        WINDOWS, params={"from": "2027-06-10", "to": "2027-06-30"}, headers=manager
    )

    assert [w["id"] for w in listed.json()["items"]] == [june["id"]]


async def test_overlapping_windows_are_refused(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    manager = await headers_for(async_client, "manager")
    await open_window(async_client, manager)

    refused = await async_client.post(
        WINDOWS,
        json={
            "starts_on": LAST.isoformat(),
            "ends_on": (LAST + timedelta(days=6)).isoformat(),
            "deadline": datetime.now(UTC).isoformat(),
        },
        headers=manager,
    )

    assert refused.status_code == 409
    assert refused.json()["code"] == "windows.overlap"


async def test_a_window_cannot_end_before_it_starts(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    manager = await headers_for(async_client, "manager")

    refused = await async_client.post(
        WINDOWS,
        json={
            "starts_on": "2027-07-07",
            "ends_on": "2027-07-01",
            "deadline": datetime.now(UTC).isoformat(),
        },
        headers=manager,
    )

    assert refused.status_code == 422
    assert refused.json()["code"] == "windows.date_range_invalid"


async def test_the_deadline_can_change_and_the_window_can_be_removed(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    manager = await headers_for(async_client, "manager")
    created = await open_window(async_client, manager)
    url = f"{WINDOWS}/{created['id']}"
    past = (datetime.now(UTC) - timedelta(hours=1)).isoformat()

    changed = await async_client.patch(url, json={"deadline": past}, headers=manager)
    removed = await async_client.delete(url, headers=manager)

    assert changed.status_code == 200, changed.text
    assert changed.json()["is_open"] is False
    assert removed.status_code == 204
    assert (await async_client.get(WINDOWS, headers=manager)).json()["items"] == []


async def test_an_employee_cannot_change_or_remove_a_window(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    manager = await headers_for(async_client, "manager")
    employee = await headers_for(async_client, "employee")
    url = f"{WINDOWS}/{(await open_window(async_client, manager))['id']}"

    changed = await async_client.patch(
        url, json={"deadline": datetime.now(UTC).isoformat()}, headers=employee
    )
    removed = await async_client.delete(url, headers=employee)

    assert (changed.status_code, removed.status_code) == (403, 403)


async def test_a_window_of_another_organisation_is_not_found(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    manager = await headers_for(async_client, "manager")
    missing = f"{WINDOWS}/00000000-0000-4000-8000-000000000000"

    changed = await async_client.patch(
        missing, json={"deadline": datetime.now(UTC).isoformat()}, headers=manager
    )

    assert changed.status_code == 404
