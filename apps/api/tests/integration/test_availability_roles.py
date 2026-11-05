"""Managers see everyone's availability; only an Admin edits another employee's (T044).

Covers: 003 FR-107, FR-108, FR-109, FR-125; US5; docs/hds-spec.md section 2.
"""

from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from hds.models.user import User
from tests.roster_support import FIRST, body, close_window, constraints, headers_for, open_window


@pytest.fixture
async def window(async_client: AsyncClient, seed_users: dict[str, User]) -> Any:
    return await open_window(async_client, await headers_for(async_client, "manager"))


async def employee_entry(client: AsyncClient, users: dict[str, User]) -> Any:
    employee = await headers_for(client, "employee")
    created = await client.post(constraints(users["employee"].id), json=body(), headers=employee)
    assert created.status_code == 201, created.text
    return created.json()


@pytest.mark.parametrize("role", ["manager", "admin"])
async def test_a_manager_or_admin_sees_an_employees_entries(
    role: str, async_client: AsyncClient, seed_users: dict[str, User], window: Any
) -> None:
    created = await employee_entry(async_client, seed_users)
    staff = await headers_for(async_client, role)

    listed = await async_client.get(
        constraints(seed_users["employee"].id),
        params={"from": FIRST.isoformat(), "to": FIRST.isoformat()},
        headers=staff,
    )

    assert [e["id"] for e in listed.json()["items"]] == [created["id"]]


async def test_an_admin_edits_anyones_entries_after_the_deadline(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    window: Any,
) -> None:
    created = await employee_entry(async_client, seed_users)
    await close_window(test_db, window["id"])
    admin = await headers_for(async_client, "admin")
    entry = f"/api/v1/constraints/{created['id']}"

    added = await async_client.post(
        constraints(seed_users["employee"].id),
        json=body("open", FIRST.replace(day=20)),
        headers=admin,
    )
    changed = await async_client.patch(entry, json={"kind": "close"}, headers=admin)
    removed = await async_client.delete(entry, headers=admin)

    assert [r.status_code for r in (added, changed, removed)] == [201, 200, 204], added.text


async def test_a_manager_cannot_edit_another_employees_entries(
    async_client: AsyncClient, seed_users: dict[str, User], window: Any
) -> None:
    created = await employee_entry(async_client, seed_users)
    manager = await headers_for(async_client, "manager")
    entry = f"/api/v1/constraints/{created['id']}"

    attempts = [
        await async_client.post(
            constraints(seed_users["employee"].id),
            json=body("open", FIRST.replace(day=7)),
            headers=manager,
        ),
        await async_client.patch(entry, json={"kind": "close"}, headers=manager),
        await async_client.delete(entry, headers=manager),
    ]

    for refused in attempts:
        assert refused.status_code == 403, refused.text
        assert refused.json()["code"] == "availability.admin_required"


async def test_a_manager_edits_their_own_entries_inside_the_window(
    async_client: AsyncClient, seed_users: dict[str, User], window: Any
) -> None:
    manager = await headers_for(async_client, "manager")

    created = await async_client.post(
        constraints(seed_users["manager"].id), json=body(), headers=manager
    )

    assert created.status_code == 201, created.text


async def test_an_employee_neither_sees_nor_changes_another_employees_entries(
    async_client: AsyncClient, seed_users: dict[str, User], window: Any
) -> None:
    created = await employee_entry(async_client, seed_users)
    other = await headers_for(async_client, "manager")
    mine = await async_client.post(
        constraints(seed_users["manager"].id), json=body(), headers=other
    )
    employee = await headers_for(async_client, "employee")
    theirs = f"/api/v1/constraints/{mine.json()['id']}"

    read = await async_client.get(constraints(seed_users["manager"].id), headers=employee)
    added = await async_client.post(
        constraints(seed_users["manager"].id), json=body(), headers=employee
    )
    changed = await async_client.patch(theirs, json={"kind": "open"}, headers=employee)
    removed = await async_client.delete(theirs, headers=employee)

    assert [r.status_code for r in (read, added, changed, removed)] == [404, 404, 404, 404]
    assert created["user_id"] == str(seed_users["employee"].id)


async def test_a_client_account_has_no_availability_access(
    async_client: AsyncClient, seed_users: dict[str, User], window: Any
) -> None:
    created = await employee_entry(async_client, seed_users)
    client = await headers_for(async_client, "client")

    read = await async_client.get(constraints(seed_users["employee"].id), headers=client)
    own = await async_client.post(constraints(seed_users["client"].id), json=body(), headers=client)
    changed = await async_client.patch(
        f"/api/v1/constraints/{created['id']}", json={"kind": "open"}, headers=client
    )

    assert [r.status_code for r in (read, own, changed)] == [403, 403, 403]


async def test_entries_cannot_be_kept_for_a_client_account(
    async_client: AsyncClient, seed_users: dict[str, User], window: Any
) -> None:
    admin = await headers_for(async_client, "admin")

    refused = await async_client.post(
        constraints(seed_users["client"].id), json=body(), headers=admin
    )

    assert refused.status_code == 404
