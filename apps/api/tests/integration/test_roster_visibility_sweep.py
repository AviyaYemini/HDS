"""No read of availability or the roster leaks another employee's data or a draft (T060).

Every Part 2 read, and every read of 004 (staffing needs, the scheduling limits and schedule jobs,
whose proposals name employees), is listed here from the application's OpenAPI document, so a new
read fails until it is added. Each is called as an Employee, a Client and an employee of another
organisation, and the answer must hold nothing they may not see.

Covers: 003 SC-104, FR-109, FR-122 to FR-125; 004 FR-022, FR-027.
"""

from datetime import date
from typing import Any

import pytest
from httpx import AsyncClient

from hds.main import app
from hds.models.user import User
from tests.integration.conftest import OtherOrg
from tests.roster_support import (
    add_project,
    add_shift,
    assign,
    body,
    constraints,
    headers_for,
    open_window,
)
from tests.schedule_support import ask, work

PART2_READS = {
    "GET /api/v1/availability-windows",
    "GET /api/v1/employees/{id}/constraints",
    "GET /api/v1/shift-types",
    "GET /api/v1/projects/{id}/shift-types",
    "GET /api/v1/shifts",
    "GET /me/shifts",
    "GET /me/shifts/{id}",
    "GET /api/v1/projects/{id}/requirements",
    "GET /api/v1/organisation/scheduling",
    "GET /api/v1/schedule-jobs",
    "GET /api/v1/schedule-jobs/{id}",
}
PART2_TAGS = {
    "availability",
    "shift types",
    "shifts",
    "my roster",
    "staffing needs",
    "scheduling settings",
    "schedule jobs",
}


def test_every_part_2_read_is_in_the_sweep() -> None:
    found = set()
    for path, item in app.openapi()["paths"].items():
        op = item.get("get")
        if op is not None and set(op.get("tags", [])) & PART2_TAGS:
            found.add(f"GET {path}")
    assert found == PART2_READS


@pytest.fixture
async def world(
    async_client: AsyncClient, seed_users: dict[str, User], other_org: OtherOrg
) -> dict[str, Any]:
    """Ann shares a published shift with her manager; the manager has another; Ann has a draft."""
    admin = await headers_for(async_client, "admin")
    manager = await headers_for(async_client, "manager")
    project = await add_project(async_client, admin)
    ann, boss = seed_users["employee"].id, seed_users["manager"].id
    shared = (await add_shift(async_client, admin, project, date(2027, 6, 7), "night", 2))["shift"]
    theirs = (await add_shift(async_client, admin, project, date(2027, 6, 8)))["shift"]
    for shift, person in ((shared, ann), (shared, boss), (theirs, boss)):
        await assign(async_client, admin, shift["id"], person)
    await async_client.post(
        f"/api/v1/projects/{project['id']}/publish",
        json={"from": "2027-06-07", "to": "2027-06-08"},
        headers=admin,
    )
    draft = (await add_shift(async_client, admin, project, date(2027, 6, 9)))["shift"]
    await assign(async_client, admin, draft["id"], ann)
    await open_window(async_client, manager)
    boss_entry = await async_client.post(constraints(boss), json=body(), headers=manager)
    assert boss_entry.status_code == 201, boss_entry.text
    job = await ask(async_client, manager)  # its proposal names Ann and her manager
    assert job.status_code == 202, job.text
    await work()
    return {
        "job": job.json()["id"],
        "project": project,
        "ann": ann,
        "boss": boss,
        "theirs": theirs["id"],
        "draft": draft["id"],
        "boss_entry": boss_entry.json()["id"],
        "other_org": other_org,
    }


def leaked(answer_text: str, path: str, secrets: list[Any]) -> list[str]:
    """The secrets in an answer, apart from ids the caller sent in the path, which a 404 repeats."""
    return [str(s) for s in secrets if str(s) not in path and str(s) in answer_text]


def paths(w: dict[str, Any], me: Any) -> dict[str, str]:
    """Every Part 2 read, with ids filled in: another employee's where a path takes one."""
    return {
        "GET /api/v1/availability-windows": "/api/v1/availability-windows",
        "GET /api/v1/employees/{id}/constraints": f"/api/v1/employees/{w['boss']}/constraints",
        "GET /api/v1/shift-types": "/api/v1/shift-types",
        "GET /api/v1/projects/{id}/shift-types": (
            f"/api/v1/projects/{w['project']['id']}/shift-types"
        ),
        "GET /api/v1/shifts": "/api/v1/shifts",
        "GET /me/shifts": "/me/shifts",
        # The manager's own shift, which Ann is not on: no more hers than another organisation's.
        "GET /me/shifts/{id}": f"/me/shifts/{w['theirs']}",
        "GET /api/v1/projects/{id}/requirements": (
            f"/api/v1/projects/{w['project']['id']}/requirements"
        ),
        "GET /api/v1/organisation/scheduling": "/api/v1/organisation/scheduling",
        "GET /api/v1/schedule-jobs": "/api/v1/schedule-jobs",
        "GET /api/v1/schedule-jobs/{id}": f"/api/v1/schedule-jobs/{w['job']}",
    }


# What an Employee gets from each read of another employee's or the roster's data.
EMPLOYEE = {
    "GET /api/v1/availability-windows": 200,
    "GET /api/v1/employees/{id}/constraints": 404,
    "GET /api/v1/shift-types": 200,
    "GET /api/v1/projects/{id}/shift-types": 403,
    "GET /api/v1/shifts": 403,
    "GET /me/shifts": 200,
    "GET /me/shifts/{id}": 404,
    "GET /api/v1/projects/{id}/requirements": 403,
    "GET /api/v1/organisation/scheduling": 403,
    "GET /api/v1/schedule-jobs": 403,
    "GET /api/v1/schedule-jobs/{id}": 403,
}


@pytest.mark.parametrize("key", sorted(PART2_READS))
async def test_an_employee_sees_nothing_of_anyone_else(
    key: str, async_client: AsyncClient, world: dict[str, Any]
) -> None:
    employee = await headers_for(async_client, "employee")

    path = paths(world, world["ann"])[key]
    answer = await async_client.get(path, headers=employee)

    assert answer.status_code == EMPLOYEE[key], answer.text
    secrets = [world["boss"], world["theirs"], world["draft"], world["boss_entry"]]
    assert leaked(answer.text, path, secrets) == [], key


@pytest.mark.parametrize("key", sorted(PART2_READS))
async def test_a_client_sees_nothing_at_all(
    key: str, async_client: AsyncClient, world: dict[str, Any]
) -> None:
    client = await headers_for(async_client, "client")

    answer = await async_client.get(paths(world, world["ann"])[key], headers=client)

    assert answer.status_code == 403, answer.text


@pytest.mark.parametrize("key", sorted(PART2_READS))
async def test_another_organisations_employee_sees_nothing_of_this_one(
    key: str, async_client: AsyncClient, world: dict[str, Any]
) -> None:
    outsider = await async_client.post(
        "/auth/login", json={"email": "employee@other.test", "password": "Dev1234!"}
    )
    headers = {"Authorization": f"Bearer {outsider.json()['access_token']}"}

    path = paths(world, world["ann"])[key]
    answer = await async_client.get(path, headers=headers)

    assert answer.status_code in (200, 403, 404), answer.text
    secrets = [
        world["ann"],
        world["boss"],
        world["theirs"],
        world["draft"],
        world["boss_entry"],
        world["project"]["id"],
    ]
    assert leaked(answer.text, path, secrets) == [], key
