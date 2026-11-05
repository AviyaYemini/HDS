"""Hours and pay per employee and per project (005 T023).

Covers: 005 FR-022, FR-027 to FR-032; US5 acceptance scenarios 1 to 7; US6 scenario 3.
"""

from typing import Any

import pytest
from httpx import AsyncClient

from hds.models.user import User
from tests.pay_support import project_managers, project_pay
from tests.report_support import HOURS, employee, worked
from tests.roster_support import add_project, headers_for

OCTOBER = {"from": "2026-10-01", "to": "2026-10-31"}


async def setup(client: AsyncClient, seed_users: dict[str, User]) -> dict[str, Any]:
    """Two employees on two projects; the Manager assigned to A, and A open to Managers."""
    admin = await headers_for(client, "admin")
    a = await add_project(client, admin, "Alpha", start_date="2026-10-01")
    b = await add_project(client, admin, "Bravo", start_date="2026-10-01")
    dana = await employee(client, admin, "Dana Levi", "50.0000")
    omer = await employee(client, admin, "Omer Cohen", "40.0000")
    await worked(client, admin, a, 5, dana)
    await worked(client, admin, b, 6, dana)
    await worked(client, admin, a, 7, omer)
    await client.put(
        project_managers(a["id"]), json={"user_ids": [str(seed_users["manager"].id)]}, headers=admin
    )
    await client.patch(project_pay(a["id"]), json={"managers_see_pay": True}, headers=admin)
    return {"admin": admin, "a": a, "b": b, "dana": dana, "omer": omer}


def flat(body: Any) -> dict[tuple[str, str], dict[str, Any]]:
    """Each (group name, row name) and its row."""
    return {(g["name"], r["name"]): r for g in body["groups"] for r in g["rows"]}


async def test_by_employee_and_by_project_give_the_same_hours(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    world = await setup(async_client, seed_users)
    admin = world["admin"]

    by_employee = (
        await async_client.get(HOURS, params={**OCTOBER, "group_by": "employee"}, headers=admin)
    ).json()
    by_project = (
        await async_client.get(HOURS, params={**OCTOBER, "group_by": "project"}, headers=admin)
    ).json()

    assert {(g["name"], g["hours"], g["shifts"]) for g in by_employee["groups"]} == {
        ("Dana Levi", "16.00", 2),
        ("Omer Cohen", "8.00", 1),
    }
    assert {(g["name"], g["hours"], g["shifts"]) for g in by_project["groups"]} == {
        ("Alpha", "16.00", 2),
        ("Bravo", "8.00", 1),
    }
    assert by_employee["hours"] == by_project["hours"] == "24.00"
    assert flat(by_employee)[("Dana Levi", "Bravo")]["hours"] == "8.00"


async def test_an_admin_sees_pay_on_every_project_with_pay(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    world = await setup(async_client, seed_users)

    body = (
        await async_client.get(
            HOURS, params={**OCTOBER, "group_by": "project"}, headers=world["admin"]
        )
    ).json()

    rows = flat(body)
    assert rows[("Alpha", "Dana Levi")]["pay"] == "400.00"
    assert rows[("Alpha", "Omer Cohen")]["pay"] == "320.00"
    assert rows[("Bravo", "Dana Levi")]["pay"] == "400.00"
    assert body["pay"] == "1120.00"


async def test_a_manager_sees_pay_only_where_assigned_and_open(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    await setup(async_client, seed_users)
    manager = await headers_for(async_client, "manager")

    body = (
        await async_client.get(HOURS, params={**OCTOBER, "group_by": "project"}, headers=manager)
    ).json()

    groups = {g["name"]: g for g in body["groups"]}
    assert groups["Alpha"]["pay"] == "720.00"
    assert "pay" not in groups["Bravo"]
    assert all("pay" not in r for r in groups["Bravo"]["rows"])
    assert groups["Bravo"]["hours"] == "8.00"
    assert body["pay"] == "720.00"


async def test_a_manager_not_assigned_or_not_open_sees_no_pay(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    world = await setup(async_client, seed_users)
    admin = world["admin"]
    await async_client.patch(
        project_pay(world["a"]["id"]), json={"managers_see_pay": False}, headers=admin
    )
    manager = await headers_for(async_client, "manager")

    closed = (
        await async_client.get(HOURS, params={**OCTOBER, "group_by": "employee"}, headers=manager)
    ).json()

    assert "pay" not in closed
    assert all("pay" not in g for g in closed["groups"])
    assert closed["hours"] == "24.00"


async def test_a_demoted_manager_sees_no_pay(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    world = await setup(async_client, seed_users)
    manager = await headers_for(async_client, "manager")
    await async_client.patch(
        f"/api/v1/employees/{seed_users['manager'].id}",
        json={"role": "employee"},
        headers=world["admin"],
    )

    answer = await async_client.get(
        HOURS, params={**OCTOBER, "group_by": "project"}, headers=manager
    )

    assert answer.status_code == 403


async def test_a_project_without_pay_shows_hours_only_and_missing_rates_are_counted(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    world = await setup(async_client, seed_users)
    admin = world["admin"]
    await async_client.patch(
        project_pay(world["b"]["id"]), json={"pay_enabled": False}, headers=admin
    )
    await async_client.patch(
        f"/api/v1/employees/{world['omer']}", json={"default_hourly_rate": None}, headers=admin
    )

    body = (
        await async_client.get(HOURS, params={**OCTOBER, "group_by": "project"}, headers=admin)
    ).json()

    rows = flat(body)
    assert "pay" not in rows[("Bravo", "Dana Levi")]
    # Alpha's pay may be seen: Omer's row shows the pay of its lines with a rate (none) and
    # counts the line without one; it is not left out, which would look like hidden pay.
    assert rows[("Alpha", "Omer Cohen")]["pay"] == "0.00"
    assert rows[("Alpha", "Omer Cohen")]["shifts_missing_rate"] == 1
    assert body["pay"] == "400.00"
    assert body["shifts_missing_rate"] == 1


@pytest.mark.parametrize(
    "params",
    [
        {"from": "2026-10-10", "to": "2026-10-09", "group_by": "employee"},
        {"from": "2025-10-01", "to": "2026-10-02", "group_by": "employee"},
    ],
)
async def test_a_backwards_or_too_long_range_is_refused(
    async_client: AsyncClient, seed_users: dict[str, User], params: dict[str, str]
) -> None:
    answer = await async_client.get(
        HOURS, params=params, headers=await headers_for(async_client, "admin")
    )

    assert answer.status_code == 422
    assert answer.json()["code"] == "reports.range_invalid"


@pytest.mark.parametrize("params", [{"from": "2026-10-01"}, {"group_by": "site", **OCTOBER}])
async def test_the_range_and_grouping_are_required(
    async_client: AsyncClient, seed_users: dict[str, User], params: dict[str, str]
) -> None:
    answer = await async_client.get(
        HOURS, params=params, headers=await headers_for(async_client, "admin")
    )

    assert answer.status_code == 422


@pytest.mark.parametrize("role", ["employee", "client"])
async def test_employees_and_clients_are_refused(
    async_client: AsyncClient, seed_users: dict[str, User], role: str
) -> None:
    answer = await async_client.get(
        HOURS,
        params={**OCTOBER, "group_by": "employee"},
        headers=await headers_for(async_client, role),
    )

    assert answer.status_code == 403


async def test_missing_rates_are_counted_only_where_pay_is_shown(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    world = await setup(async_client, seed_users)
    await async_client.patch(
        f"/api/v1/employees/{world['dana']}",
        json={"default_hourly_rate": None},
        headers=world["admin"],
    )
    manager = await headers_for(async_client, "manager")

    body = (
        await async_client.get(HOURS, params={**OCTOBER, "group_by": "project"}, headers=manager)
    ).json()

    groups = {g["name"]: g for g in body["groups"]}
    assert groups["Alpha"]["shifts_missing_rate"] == 1  # Dana on Alpha, which the Manager sees
    assert groups["Bravo"]["shifts_missing_rate"] == 0  # hidden: nothing about its rates shows
    assert body["shifts_missing_rate"] == 1
