"""An employee sees their own published shifts and nothing else of the roster (T059).

Covers: 003 FR-118, FR-122, FR-123, FR-124; US7; EMP-1.
"""

from datetime import date
from typing import Any

import pytest
from httpx import AsyncClient

from hds.main import app
from hds.models.user import User
from tests.roster_support import add_project, add_shift, assign, headers_for

MINE = "/me/shifts"


@pytest.fixture
async def roster(async_client: AsyncClient, seed_users: dict[str, User]) -> dict[str, Any]:
    """Ann and her manager on one published shift; Ann on a draft; the manager alone on another."""
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin)
    ann, boss = seed_users["employee"].id, seed_users["manager"].id
    shared = (await add_shift(async_client, admin, project, date(2027, 6, 7), "night", 2))["shift"]
    theirs = (await add_shift(async_client, admin, project, date(2027, 6, 8)))["shift"]
    await assign(async_client, admin, shared["id"], ann)
    await assign(async_client, admin, shared["id"], boss)
    await assign(async_client, admin, theirs["id"], boss)
    await async_client.post(
        f"/api/v1/projects/{project['id']}/publish",
        json={"from": "2027-06-07", "to": "2027-06-08"},
        headers=admin,
    )
    draft = (await add_shift(async_client, admin, project, date(2027, 6, 9)))["shift"]
    await assign(async_client, admin, draft["id"], ann)
    return {"project": project, "shared": shared, "theirs": theirs, "draft": draft}


async def test_an_employee_sees_only_their_own_published_shifts(
    async_client: AsyncClient, roster: dict[str, Any]
) -> None:
    employee = await headers_for(async_client, "employee")

    listed = await async_client.get(MINE, headers=employee)

    assert listed.status_code == 200, listed.text
    [mine] = listed.json()["items"]
    assert mine["id"] == roster["shared"]["id"]
    assert (mine["starts_at"], mine["ends_at"]) == (
        roster["shared"]["starts_at"],
        roster["shared"]["ends_at"],
    )
    assert mine["timezone"] == "Asia/Jerusalem"
    assert mine["project"]["name"] == "Night Watch"
    assert mine["site"]["name"] == "Night Watch site"
    assert mine["shift_type"] == "night"


async def test_the_answer_names_no_co_worker_and_no_headcount(
    async_client: AsyncClient, roster: dict[str, Any], seed_users: dict[str, User]
) -> None:
    employee = await headers_for(async_client, "employee")

    text = (await async_client.get(MINE, headers=employee)).text

    assert str(seed_users["manager"].id) not in text
    assert "assignments" not in text and "headcount" not in text and "pay_rate" not in text


def test_the_published_schema_has_no_field_about_other_people() -> None:
    schemas = app.openapi()["components"]["schemas"]
    fields = set(schemas["MyShift"]["properties"])

    assert fields == {"id", "starts_at", "ends_at", "timezone", "shift_type", "project", "site"}


async def test_a_manager_sees_their_own_shifts_there_too(
    async_client: AsyncClient, roster: dict[str, Any]
) -> None:
    manager = await headers_for(async_client, "manager")

    listed = await async_client.get(MINE, headers=manager)

    assert sorted(i["id"] for i in listed.json()["items"]) == sorted(
        [roster["shared"]["id"], roster["theirs"]["id"]]
    )


async def test_the_list_is_limited_to_a_date_range(
    async_client: AsyncClient, roster: dict[str, Any]
) -> None:
    manager = await headers_for(async_client, "manager")

    listed = await async_client.get(
        MINE, params={"from": "2027-06-08", "to": "2027-06-08"}, headers=manager
    )

    assert [i["id"] for i in listed.json()["items"]] == [roster["theirs"]["id"]]


async def test_a_change_to_a_published_shift_shows_at_once(
    async_client: AsyncClient, roster: dict[str, Any]
) -> None:
    admin = await headers_for(async_client, "admin")
    employee = await headers_for(async_client, "employee")
    await async_client.patch(
        f"/api/v1/shifts/{roster['shared']['id']}",
        json={"ends_at": "2027-06-08T05:00:00Z"},
        headers=admin,
    )

    [mine] = (await async_client.get(MINE, headers=employee)).json()["items"]

    assert mine["ends_at"].startswith("2027-06-08T05:00:00")


async def test_a_client_sees_no_roster(async_client: AsyncClient, roster: dict[str, Any]) -> None:
    client = await headers_for(async_client, "client")

    assert (await async_client.get(MINE, headers=client)).status_code == 403


async def test_a_deleted_projects_shifts_are_no_longer_shown(
    async_client: AsyncClient, roster: dict[str, Any]
) -> None:
    admin = await headers_for(async_client, "admin")
    employee = await headers_for(async_client, "employee")
    await async_client.delete(f"/api/v1/projects/{roster['project']['id']}", headers=admin)

    listed = await async_client.get(MINE, headers=employee)

    assert listed.json()["items"] == []


@pytest.mark.parametrize("path", [MINE, "/api/v1/shifts"])
async def test_a_range_that_ends_before_it_starts_is_refused(
    path: str, async_client: AsyncClient, roster: dict[str, Any]
) -> None:
    admin = await headers_for(async_client, "admin")

    refused = await async_client.get(
        path, params={"from": "2027-06-30", "to": "2027-06-01"}, headers=admin
    )

    assert refused.status_code == 422
    assert refused.json()["code"] == "shifts.date_range_invalid"


@pytest.mark.parametrize(
    "params", [{"to": "9999-12-31"}, {"from": "0001-01-01"}], ids=["last-day", "first-day"]
)
async def test_the_first_and_last_calendar_days_are_open_ends(
    params: dict[str, str], async_client: AsyncClient, roster: dict[str, Any]
) -> None:
    admin = await headers_for(async_client, "admin")

    for path in (MINE, "/api/v1/shifts"):
        answer = await async_client.get(path, params=params, headers=admin)
        assert answer.status_code == 200, f"{path}: {answer.text}"
