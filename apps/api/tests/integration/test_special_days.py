"""Special days of the organisation and of one project (005 T010).

Covers: 005 FR-003, FR-008, FR-008a, FR-031, FR-032; US3 acceptance scenario 9; the edge case of
deleting an organisation day a project overrides.
"""

from typing import Any

import pytest
from httpx import AsyncClient

from hds.models.user import User
from tests.integration.conftest import OtherOrg
from tests.pay_support import SPECIAL_DAYS, end_project, project_days
from tests.roster_support import add_project, headers_for

YOM_KIPPUR = {"day": "2027-09-11", "name": "Yom Kippur"}
ELECTION = {"day": "2027-10-26", "name": "Election day", "rate_pct": "200.00"}


async def add_day(client: AsyncClient, headers: dict[str, str], body: dict[str, Any]) -> Any:
    answer = await client.post(SPECIAL_DAYS, json=body, headers=headers)
    assert answer.status_code == 201, answer.text
    return answer.json()


async def test_an_admin_keeps_the_organisations_days(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")

    kippur = await add_day(async_client, admin, YOM_KIPPUR)
    election = await add_day(async_client, admin, ELECTION)
    changed = await async_client.patch(
        f"{SPECIAL_DAYS}/{kippur['id']}", json={"rate_pct": "175.00"}, headers=admin
    )
    deleted = await async_client.delete(f"{SPECIAL_DAYS}/{election['id']}", headers=admin)

    assert kippur["rate_pct"] == "150.00" and kippur["project_id"] is None
    assert changed.json()["rate_pct"] == "175.00"
    assert deleted.status_code == 204
    listed = await async_client.get(
        SPECIAL_DAYS, params={"year": 2027}, headers=await headers_for(async_client, "manager")
    )
    assert [(d["day"], d["name"], d["rate_pct"]) for d in listed.json()["items"]] == [
        ("2027-09-11", "Yom Kippur", "175.00")
    ]


async def test_the_year_filter_keeps_only_that_years_days(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    await add_day(async_client, admin, YOM_KIPPUR)
    await add_day(async_client, admin, {"day": "2028-09-30", "name": "Yom Kippur"})

    listed = await async_client.get(SPECIAL_DAYS, params={"year": 2028}, headers=admin)

    assert [d["day"] for d in listed.json()["items"]] == ["2028-09-30"]


async def test_a_second_organisation_day_on_one_date_is_refused(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    await add_day(async_client, admin, YOM_KIPPUR)

    again = await async_client.post(
        SPECIAL_DAYS, json={**YOM_KIPPUR, "name": "Again"}, headers=admin
    )

    assert again.status_code == 409
    assert again.json()["code"] == "special_days.duplicate"


@pytest.mark.parametrize(
    "bad",
    [
        {**YOM_KIPPUR, "rate_pct": "99.99"},
        {**YOM_KIPPUR, "rate_pct": "300.01"},
        {**YOM_KIPPUR, "rate_pct": 200},
        {**YOM_KIPPUR, "name": "   "},
        {"day": "11/09/2027", "name": "Yom Kippur"},
    ],
)
async def test_a_bad_day_is_refused(
    async_client: AsyncClient, seed_users: dict[str, User], bad: dict[str, Any]
) -> None:
    admin = await headers_for(async_client, "admin")

    answer = await async_client.post(SPECIAL_DAYS, json=bad, headers=admin)

    assert answer.status_code == 422, answer.text


async def test_a_project_adds_its_own_day_and_overrides_the_organisations(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin)
    other = await add_project(async_client, admin, "Other")
    await add_day(async_client, admin, ELECTION)
    url = project_days(project["id"])

    local = await async_client.post(
        url, json={"day": "2027-04-01", "name": "Site holiday"}, headers=admin
    )
    override = await async_client.post(url, json={**ELECTION, "rate_pct": "150.00"}, headers=admin)

    assert local.status_code == 201, local.text
    assert override.status_code == 201, override.text
    assert local.json()["project_id"] == project["id"]
    mine = (await async_client.get(url, headers=admin)).json()["items"]
    theirs = (await async_client.get(project_days(other["id"]), headers=admin)).json()["items"]
    assert [(d["day"], d["rate_pct"], d["source"]) for d in mine] == [
        ("2027-04-01", "150.00", "project"),
        ("2027-10-26", "150.00", "project"),
    ]
    assert [(d["day"], d["rate_pct"], d["source"]) for d in theirs] == [
        ("2027-10-26", "200.00", "organisation")
    ]


async def test_a_second_project_day_on_one_date_is_refused(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin)
    url = project_days(project["id"])
    await async_client.post(url, json=YOM_KIPPUR, headers=admin)

    again = await async_client.post(url, json=YOM_KIPPUR, headers=admin)

    assert again.status_code == 409
    assert again.json()["code"] == "special_days.duplicate"


async def test_deleting_an_organisation_day_keeps_a_projects_override(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin)
    day = await add_day(async_client, admin, ELECTION)
    await async_client.post(
        project_days(project["id"]), json={**ELECTION, "rate_pct": "180.00"}, headers=admin
    )

    await async_client.delete(f"{SPECIAL_DAYS}/{day['id']}", headers=admin)

    mine = (await async_client.get(project_days(project["id"]), headers=admin)).json()["items"]
    assert [(d["day"], d["rate_pct"], d["source"]) for d in mine] == [
        ("2027-10-26", "180.00", "project")
    ]


async def test_an_ended_projects_days_cannot_change(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin)
    url = project_days(project["id"])
    own = (await async_client.post(url, json=YOM_KIPPUR, headers=admin)).json()
    await end_project(async_client, admin, project["id"])

    added = await async_client.post(url, json=ELECTION, headers=admin)
    changed = await async_client.patch(
        f"{SPECIAL_DAYS}/{own['id']}", json={"rate_pct": "200.00"}, headers=admin
    )
    deleted = await async_client.delete(f"{SPECIAL_DAYS}/{own['id']}", headers=admin)

    assert [a.status_code for a in (added, changed, deleted)] == [409, 409, 409]
    assert {a.json()["code"] for a in (added, changed, deleted)} == {"pay.project_ended"}


async def test_a_manager_reads_the_days_but_cannot_change_them(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    manager = await headers_for(async_client, "manager")
    project = await add_project(async_client, admin)
    day = await add_day(async_client, admin, YOM_KIPPUR)

    refused = [
        await async_client.post(SPECIAL_DAYS, json=ELECTION, headers=manager),
        await async_client.patch(
            f"{SPECIAL_DAYS}/{day['id']}", json={"name": "x"}, headers=manager
        ),
        await async_client.delete(f"{SPECIAL_DAYS}/{day['id']}", headers=manager),
        await async_client.post(project_days(project["id"]), json=ELECTION, headers=manager),
    ]
    read = [
        await async_client.get(SPECIAL_DAYS, headers=manager),
        await async_client.get(project_days(project["id"]), headers=manager),
    ]

    assert [r.status_code for r in refused] == [403, 403, 403, 403]
    assert [r.status_code for r in read] == [200, 200]


@pytest.mark.parametrize("role", ["employee", "client"])
async def test_employees_and_clients_are_refused(
    async_client: AsyncClient, seed_users: dict[str, User], role: str
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin)
    headers = await headers_for(async_client, role)

    answers = [
        await async_client.get(SPECIAL_DAYS, headers=headers),
        await async_client.post(SPECIAL_DAYS, json=YOM_KIPPUR, headers=headers),
        await async_client.get(project_days(project["id"]), headers=headers),
    ]

    assert [a.status_code for a in answers] == [403, 403, 403]


async def test_another_organisations_project_or_day_is_not_found(
    async_client: AsyncClient, seed_users: dict[str, User], other_org: OtherOrg
) -> None:
    admin = await headers_for(async_client, "admin")
    assert other_org.project is not None
    foreign = project_days(str(other_org.project.id))

    answers = [
        await async_client.get(foreign, headers=admin),
        await async_client.post(foreign, json=YOM_KIPPUR, headers=admin),
        await async_client.patch(
            f"{SPECIAL_DAYS}/00000000-0000-0000-0000-000000000001",
            json={"name": "x"},
            headers=admin,
        ),
    ]

    assert [a.status_code for a in answers] == [404, 404, 404]
