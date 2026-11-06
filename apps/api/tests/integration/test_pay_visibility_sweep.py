"""No reader ever receives a pay figure they may not see (005 T025).

Eight projects, one for every combination of pay on or off, open to Managers or not, and the Manager
assigned or not, each with one ended shift worked by the seeded Employee. Every report and pay view
is read as every role, and every pay figure that comes back is checked against the rule.

Covers: 005 FR-019, FR-022, FR-025, FR-028, FR-031; SC-004.
"""

import itertools
from typing import Any

import pytest
from httpx import AsyncClient

from hds.models.user import User
from tests.integration.test_my_pay import MY_PAY, set_rate
from tests.pay_support import project_managers, project_pay
from tests.report_support import HOURS, WORK_LOG, worked
from tests.roster_support import add_project, headers_for

OCTOBER = {"from": "2026-10-01", "to": "2026-10-31"}
COMBINATIONS = list(itertools.product([True, False], repeat=3))
# Weekdays of October 2026 before the clocks change, so every shift pays 8 hours at 100%.
WEEKDAYS = [1, 4, 5, 6, 7, 8, 11, 12]


async def world(client: AsyncClient, seed_users: dict[str, User]) -> dict[str, tuple[bool, ...]]:
    """Project name -> (pay on, open to Managers, Manager assigned)."""
    admin = await headers_for(client, "admin")
    await set_rate(client, admin, seed_users["employee"], "50.0000")
    made: dict[str, tuple[bool, ...]] = {}
    for n, (pay_on, open_, assigned) in enumerate(COMBINATIONS):
        name = f"P{n} pay={pay_on} open={open_} assigned={assigned}"
        project = await add_project(client, admin, name, start_date="2026-10-01")
        await client.patch(
            project_pay(project["id"]),
            json={"pay_enabled": pay_on, "managers_see_pay": open_},
            headers=admin,
        )
        if assigned:
            await client.put(
                project_managers(project["id"]),
                json={"user_ids": [str(seed_users["manager"].id)]},
                headers=admin,
            )
        await worked(client, admin, project, WEEKDAYS[n], seed_users["employee"].id)
        made[name] = (pay_on, open_, assigned)
    return made


def may_see(role: str, pay_on: bool, open_: bool, assigned: bool) -> bool:
    """The rule of FR-022 and FR-028."""
    if role == "admin":
        return pay_on
    return role == "manager" and pay_on and open_ and assigned


def pay_keys(value: Any) -> int:
    """How many ``pay`` keys appear anywhere in a JSON value."""
    if isinstance(value, dict):
        return ("pay" in value) + sum(pay_keys(v) for v in value.values())
    if isinstance(value, list):
        return sum(pay_keys(v) for v in value)
    return 0


@pytest.mark.parametrize("role", ["admin", "manager"])
async def test_the_hours_report_shows_pay_exactly_where_allowed(
    async_client: AsyncClient, seed_users: dict[str, User], role: str
) -> None:
    made = await world(async_client, seed_users)
    headers = await headers_for(async_client, role)

    for group_by in ("project", "employee"):
        body = (
            await async_client.get(HOURS, params={**OCTOBER, "group_by": group_by}, headers=headers)
        ).json()
        rows = (
            [(g["name"], g) for g in body["groups"]]
            if group_by == "project"
            else [(r["name"], r) for g in body["groups"] for r in g["rows"]]
        )
        assert {name for name, _ in rows} == set(made)
        for name, row in rows:
            assert ("pay" in row) is may_see(role, *made[name]), (group_by, name, row)
        allowed = sum(may_see(role, *flags) for flags in made.values())
        assert (body.get("pay") is not None) is (allowed > 0)
        if allowed:
            assert body["pay"] == f"{allowed * 400:.2f}"


@pytest.mark.parametrize("role", ["admin", "manager"])
async def test_the_work_log_never_shows_pay(
    async_client: AsyncClient, seed_users: dict[str, User], role: str
) -> None:
    await world(async_client, seed_users)
    headers = await headers_for(async_client, role)
    site_projects = (await async_client.get("/api/v1/projects", headers=headers)).json()["items"]

    for project in site_projects:
        body = (
            await async_client.get(
                WORK_LOG, params={"project_id": project["id"], "to": "2026-10-31"}, headers=headers
            )
        ).json()
        assert pay_keys(body) == 0, body


@pytest.mark.parametrize("role", ["employee", "client"])
async def test_employees_and_clients_get_no_report(
    async_client: AsyncClient, seed_users: dict[str, User], role: str
) -> None:
    await world(async_client, seed_users)
    headers = await headers_for(async_client, role)

    hours = await async_client.get(
        HOURS, params={**OCTOBER, "group_by": "employee"}, headers=headers
    )
    log = await async_client.get(
        WORK_LOG,
        params={"site_id": "00000000-0000-0000-0000-000000000000", **OCTOBER},
        headers=headers,
    )

    assert (hours.status_code, log.status_code) == (403, 403)
    assert pay_keys(hours.json()) == pay_keys(log.json()) == 0


async def test_an_employee_sees_their_own_pay_only_where_the_project_has_pay(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    made = await world(async_client, seed_users)

    body = (
        await async_client.get(
            MY_PAY, params={"month": "2026-10"}, headers=await headers_for(async_client, "employee")
        )
    ).json()

    assert len(body["lines"]) == len(made)
    for line in body["lines"]:
        assert ("pay" in line) is made[line["project_name"]][0], line
    for role in ("manager", "admin"):
        theirs = (
            await async_client.get(
                MY_PAY, params={"month": "2026-10"}, headers=await headers_for(async_client, role)
            )
        ).json()
        assert theirs["lines"] == [], role
