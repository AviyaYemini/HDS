"""An employee's default pay rate is set by Admins only (005 T008).

Managers still create and edit employees; only the rate is refused, whatever its value. On a create,
a rate of ``null`` sets nothing, so it is allowed; on a change, ``null`` would clear the rate, so it
is refused too.

Covers: 005 FR-001, FR-003; US2 acceptance scenarios 1, 2 and 7.
"""

from typing import Any

import pytest
from httpx import AsyncClient

from hds.models.user import User
from tests.roster_support import headers_for

EMPLOYEES = "/api/v1/employees"


def new(email: str, **extra: Any) -> dict[str, Any]:
    """A new employee's body."""
    return {
        "email": email,
        "full_name": "New Guard",
        "initial_password": "a-long-password-1",
        **extra,
    }


async def test_an_admin_sets_changes_and_clears_a_default_rate(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    created = await async_client.post(
        EMPLOYEES, json=new("rated@hds.test", default_hourly_rate="50.0000"), headers=admin
    )
    assert created.status_code == 201, created.text
    url = f"{EMPLOYEES}/{created.json()['id']}"

    changed = await async_client.patch(url, json={"default_hourly_rate": "55.0000"}, headers=admin)
    cleared = await async_client.patch(url, json={"default_hourly_rate": None}, headers=admin)

    assert changed.json()["default_hourly_rate"] == "55.0000"
    assert cleared.status_code == 200
    assert cleared.json()["default_hourly_rate"] is None


@pytest.mark.parametrize("rate", ["60.0000", None])
async def test_a_manager_cannot_change_or_clear_a_rate(
    async_client: AsyncClient, seed_users: dict[str, User], rate: str | None
) -> None:
    manager = await headers_for(async_client, "manager")
    employee = seed_users["employee"]
    before = (await async_client.get(f"{EMPLOYEES}/{employee.id}", headers=manager)).json()[
        "default_hourly_rate"
    ]

    answer = await async_client.patch(
        f"{EMPLOYEES}/{employee.id}", json={"default_hourly_rate": rate}, headers=manager
    )

    assert answer.status_code == 403
    assert answer.json()["code"] == "users.rate_admin_only"
    after = (await async_client.get(f"{EMPLOYEES}/{employee.id}", headers=manager)).json()
    assert after["default_hourly_rate"] == before


async def test_a_manager_cannot_create_an_employee_with_a_rate(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    manager = await headers_for(async_client, "manager")

    answer = await async_client.post(
        EMPLOYEES, json=new("ratedm@hds.test", default_hourly_rate="50.0000"), headers=manager
    )

    assert answer.status_code == 403
    assert answer.json()["code"] == "users.rate_admin_only"
    listed = await async_client.get(EMPLOYEES, params={"search": "ratedm"}, headers=manager)
    assert listed.json()["items"] == []


async def test_a_manager_still_creates_and_edits_employees_without_a_rate(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    manager = await headers_for(async_client, "manager")

    created = await async_client.post(
        EMPLOYEES, json=new("plain@hds.test", default_hourly_rate=None), headers=manager
    )
    assert created.status_code == 201, created.text
    renamed = await async_client.patch(
        f"{EMPLOYEES}/{created.json()['id']}", json={"full_name": "Renamed"}, headers=manager
    )

    assert renamed.status_code == 200
    assert renamed.json()["full_name"] == "Renamed"


async def test_an_employee_still_sees_their_own_rate(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    employee = seed_users["employee"]
    await async_client.patch(
        f"{EMPLOYEES}/{employee.id}", json={"default_hourly_rate": "48.5000"}, headers=admin
    )

    me = await async_client.get("/me", headers=await headers_for(async_client, "employee"))

    assert me.json()["default_hourly_rate"] == "48.5000"
