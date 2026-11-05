"""What the review of PR 7 found outside the reports' own figures (005 T040).

Rates outside reports (FR-022a), a demoted Manager's assignments (FR-022) and the pay of a deleted
project (FR-007).

Covers: 005 FR-007, FR-022, FR-022a; the review of PR 7.
"""

from typing import Any

from httpx import AsyncClient

from hds.models.user import User
from tests.integration.test_hours_report import OCTOBER, setup
from tests.pay_support import ORG_PAY, SPECIAL_DAYS, project_managers
from tests.report_support import HOURS, worked
from tests.roster_support import headers_for

SHIFTS = "/api/v1/shifts"


async def roster_rates(client: AsyncClient, headers: dict[str, str]) -> dict[str, Any]:
    """Each project id on the October roster and the shift rates the reader is given for it."""
    answer = await client.get(SHIFTS, params={**OCTOBER, "limit": 200}, headers=headers)
    assert answer.status_code == 200, answer.text
    rates: dict[str, Any] = {}
    for item in answer.json()["items"]:
        rates.setdefault(item["project_id"], set()).add(item["pay_rate"])
    return rates


async def test_a_manager_gets_a_shifts_rate_only_where_they_may_see_its_pay(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    world = await setup(async_client, seed_users)
    admin = world["admin"]
    await worked(async_client, admin, world["a"], 12, world["dana"], pay_rate="70.0000")
    await worked(async_client, admin, world["b"], 13, world["dana"], pay_rate="70.0000")
    a, b = world["a"]["id"], world["b"]["id"]

    as_admin = await roster_rates(async_client, admin)
    as_manager = await roster_rates(async_client, await headers_for(async_client, "manager"))

    assert as_admin[a] == {None, "70.0000"} and as_admin[b] == {None, "70.0000"}
    assert as_manager[a] == {None, "70.0000"}  # assigned, pay on and open to Managers
    assert as_manager[b] == {None}  # not open: the rate would give the pay the report hides


async def test_a_manager_never_gets_another_persons_default_rate(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    world = await setup(async_client, seed_users)
    manager = await headers_for(async_client, "manager")
    own_id = str(seed_users["manager"].id)

    listed = (await async_client.get("/api/v1/employees", headers=manager)).json()["items"]
    one = (await async_client.get(f"/api/v1/employees/{world['dana']}", headers=manager)).json()
    by_admin = (
        await async_client.get(f"/api/v1/employees/{world['dana']}", headers=world["admin"])
    ).json()

    assert all(u["default_hourly_rate"] is None for u in listed if u["id"] != own_id)
    assert one["default_hourly_rate"] is None
    assert by_admin["default_hourly_rate"] == "50.0000"


async def test_a_demoted_manager_promoted_again_sees_no_pay_until_reassigned(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    world = await setup(async_client, seed_users)
    admin, a = world["admin"], world["a"]["id"]
    path = f"/api/v1/employees/{seed_users['manager'].id}"
    for role in ("employee", "manager"):
        changed = await async_client.patch(path, json={"role": role}, headers=admin)
        assert changed.status_code == 200, changed.text

    members = (await async_client.get(project_managers(a), headers=admin)).json()
    body = (
        await async_client.get(
            HOURS,
            params={**OCTOBER, "group_by": "project"},
            headers=await headers_for(async_client, "manager"),
        )
    ).json()

    assert str(seed_users["manager"].id) not in str(members)
    assert "pay" not in body


async def test_deleting_a_project_fixes_its_pay(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    """Later changes to the defaults and the organisation's days never reach a deleted project."""
    world = await setup(async_client, seed_users)
    admin, b = world["admin"], world["b"]["id"]
    deleted = await async_client.delete(f"/api/v1/projects/{b}", headers=admin)
    assert deleted.status_code == 204, deleted.text

    changed = await async_client.patch(ORG_PAY, json={"day_normal_minutes": 300}, headers=admin)
    assert changed.status_code == 200, changed.text
    day = await async_client.post(
        SPECIAL_DAYS,
        json={"day": "2026-10-06", "name": "Election day", "rate_pct": "200.00"},
        headers=admin,
    )
    assert day.status_code == 201, day.text

    body = (
        await async_client.get(HOURS, params={**OCTOBER, "group_by": "project"}, headers=admin)
    ).json()

    groups = {g["name"]: g for g in body["groups"]}
    assert groups["Bravo"]["pay"] == "400.00"  # Dana, 8 hours at 50, as on the day it was deleted


async def test_a_manager_reading_their_own_record_gets_their_own_rate(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    """FR-022a: a person always receives their own default rate, through the employee list too."""
    admin = await headers_for(async_client, "admin")
    own = f"/api/v1/employees/{seed_users['manager'].id}"
    set_rate = await async_client.patch(own, json={"default_hourly_rate": "61.0000"}, headers=admin)
    assert set_rate.status_code == 200, set_rate.text

    answer = await async_client.get(own, headers=await headers_for(async_client, "manager"))

    assert answer.json()["default_hourly_rate"] == "61.0000"
