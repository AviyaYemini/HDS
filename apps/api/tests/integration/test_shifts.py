"""An Admin creates, changes and deletes shifts; Managers read the roster (T051, T053).

Covers: 003 FR-112 to FR-114, FR-118 to FR-120; US6.
"""

from datetime import UTC, date, datetime
from typing import Any

import pytest
from httpx import AsyncClient

from hds.models.user import User
from tests.roster_support import SHIFTS, add_project, add_shift, assign, headers_for

MONDAY = date(2027, 6, 7)


@pytest.fixture
async def admin(async_client: AsyncClient, seed_users: dict[str, User]) -> dict[str, str]:
    return await headers_for(async_client, "admin")


@pytest.fixture
async def project(async_client: AsyncClient, admin: dict[str, str]) -> dict[str, Any]:
    return await add_project(async_client, admin)


async def test_a_shift_takes_its_types_local_times_at_the_site(
    async_client: AsyncClient, admin: dict[str, str], project: dict[str, Any]
) -> None:
    """Night is 23:00 to 07:00 in Israel, which is UTC+3 in June."""
    created = await add_shift(async_client, admin, project, MONDAY, "night")
    shift = created["shift"]

    assert shift["status"] == "draft"
    assert shift["timezone"] == "Asia/Jerusalem"
    assert datetime.fromisoformat(shift["starts_at"]) == datetime(2027, 6, 7, 20, tzinfo=UTC)
    assert datetime.fromisoformat(shift["ends_at"]) == datetime(2027, 6, 8, 4, tzinfo=UTC)
    assert shift["assignments"] == []
    assert [w["code"] for w in created["warnings"]] == ["warnings.under_headcount"]


async def test_times_may_be_given_instead_of_the_types(
    async_client: AsyncClient, admin: dict[str, str], project: dict[str, Any]
) -> None:
    created = await add_shift(
        async_client,
        admin,
        project,
        MONDAY,
        starts_at="2027-06-07T05:00:00Z",
        ends_at="2027-06-07T09:30:00Z",
    )

    assert created["shift"]["starts_at"].startswith("2027-06-07T05:00:00")
    assert created["shift"]["ends_at"].startswith("2027-06-07T09:30:00")


async def test_a_shift_that_ends_before_it_starts_is_refused(
    async_client: AsyncClient, admin: dict[str, str], project: dict[str, Any]
) -> None:
    refused = await async_client.post(
        SHIFTS,
        json={
            "project_id": project["id"],
            "shift_type_id": project["types"]["morning"],
            "date": MONDAY.isoformat(),
            "headcount": 1,
            "starts_at": "2027-06-07T09:00:00Z",
            "ends_at": "2027-06-07T08:00:00Z",
        },
        headers=admin,
    )

    assert refused.status_code == 422
    assert refused.json()["code"] == "shifts.times_invalid"


async def test_a_type_the_project_does_not_use_is_refused(
    async_client: AsyncClient, admin: dict[str, str], project: dict[str, Any]
) -> None:
    await async_client.put(
        f"/api/v1/projects/{project['id']}/shift-types",
        json={"ids": [project["types"]["night"]]},
        headers=admin,
    )

    refused = await async_client.post(
        SHIFTS,
        json={
            "project_id": project["id"],
            "shift_type_id": project["types"]["morning"],
            "date": MONDAY.isoformat(),
            "headcount": 1,
        },
        headers=admin,
    )

    assert refused.status_code == 422
    assert refused.json()["code"] == "shifts.type_not_in_project"


@pytest.mark.parametrize(
    "extra", [{"headcount": 0}, {"pay_rate": "0"}, {"pay_rate": 12.5}], ids=str
)
async def test_bad_headcount_or_pay_rate_is_refused(
    extra: dict[str, Any], async_client: AsyncClient, admin: dict[str, str], project: dict[str, Any]
) -> None:
    refused = await async_client.post(
        SHIFTS,
        json={
            "project_id": project["id"],
            "shift_type_id": project["types"]["morning"],
            "date": MONDAY.isoformat(),
            "headcount": 1,
            **extra,
        },
        headers=admin,
    )

    assert refused.status_code == 422


async def test_an_admin_changes_and_deletes_a_shift_and_its_assignments(
    async_client: AsyncClient,
    admin: dict[str, str],
    project: dict[str, Any],
    seed_users: dict[str, User],
) -> None:
    shift = (await add_shift(async_client, admin, project, MONDAY))["shift"]
    await assign(async_client, admin, shift["id"], seed_users["employee"].id)
    url = f"{SHIFTS}/{shift['id']}"

    changed = await async_client.patch(
        url,
        json={"headcount": 2, "pay_rate": "55.5000", "ends_at": "2027-06-07T13:00:00Z"},
        headers=admin,
    )
    removed = await async_client.delete(url, headers=admin)

    assert changed.status_code == 200, changed.text
    body = changed.json()["shift"]
    assert (body["headcount"], body["pay_rate"]) == (2, "55.5000")
    assert body["ends_at"].startswith("2027-06-07T13:00:00")
    assert [w["code"] for w in changed.json()["warnings"]] == ["warnings.under_headcount"]
    assert removed.status_code == 204
    listed = await async_client.get(SHIFTS, headers=admin)
    assert listed.json()["items"] == []


async def test_a_manager_reads_another_projects_roster_but_cannot_change_it(
    async_client: AsyncClient,
    admin: dict[str, str],
    project: dict[str, Any],
    seed_users: dict[str, User],
) -> None:
    shift = (await add_shift(async_client, admin, project, MONDAY))["shift"]
    await assign(async_client, admin, shift["id"], seed_users["employee"].id)
    manager = await headers_for(async_client, "manager")

    listed = await async_client.get(SHIFTS, headers=manager)
    attempts = [
        await async_client.post(
            SHIFTS,
            json={
                "project_id": project["id"],
                "shift_type_id": project["types"]["morning"],
                "date": MONDAY.isoformat(),
                "headcount": 1,
            },
            headers=manager,
        ),
        await async_client.patch(f"{SHIFTS}/{shift['id']}", json={"headcount": 3}, headers=manager),
        await async_client.delete(f"{SHIFTS}/{shift['id']}", headers=manager),
        await assign(async_client, manager, shift["id"], seed_users["manager"].id),
    ]

    assert listed.status_code == 200
    [item] = listed.json()["items"]
    assert [a["user_id"] for a in item["assignments"]] == [str(seed_users["employee"].id)]
    # Since 007 (FR-028) a Manager changes only the rosters of projects they manage.
    for refused in attempts:
        assert refused.status_code == 403, refused.text
        assert refused.json()["code"] == "roster.project_not_managed"


@pytest.mark.parametrize("role", ["employee", "client"])
async def test_employees_and_clients_cannot_read_the_roster(
    role: str, async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    headers = await headers_for(async_client, role)

    assert (await async_client.get(SHIFTS, headers=headers)).status_code == 403


async def test_the_roster_is_filtered_and_paged(
    async_client: AsyncClient, admin: dict[str, str], project: dict[str, Any]
) -> None:
    other = await add_project(async_client, admin, "Day Watch")
    for day in (7, 8, 9):
        await add_shift(async_client, admin, project, date(2027, 6, day))
    await add_shift(async_client, admin, other, date(2027, 6, 8))

    first = await async_client.get(
        SHIFTS,
        params={"from": "2027-06-07", "to": "2027-06-09", "project_id": project["id"], "limit": 2},
        headers=admin,
    )
    second = await async_client.get(
        SHIFTS,
        params={
            "from": "2027-06-07",
            "to": "2027-06-09",
            "project_id": project["id"],
            "limit": 2,
            "cursor": first.json()["next_cursor"],
        },
        headers=admin,
    )
    narrow = await async_client.get(
        SHIFTS, params={"from": "2027-06-08", "to": "2027-06-08"}, headers=admin
    )

    assert len(first.json()["items"]) == 2 and first.json()["next_cursor"]
    assert len(second.json()["items"]) == 1 and second.json()["next_cursor"] is None
    assert len(narrow.json()["items"]) == 2


async def test_changing_a_types_times_leaves_its_shifts(
    async_client: AsyncClient, admin: dict[str, str], project: dict[str, Any]
) -> None:
    shift = (await add_shift(async_client, admin, project, MONDAY))["shift"]

    await async_client.patch(
        f"/api/v1/shift-types/{project['types']['morning']}",
        json={"starts_at_local": "06:00", "ends_at_local": "14:00"},
        headers=admin,
    )

    [stored] = (await async_client.get(SHIFTS, headers=admin)).json()["items"]
    assert (stored["starts_at"], stored["ends_at"]) == (shift["starts_at"], shift["ends_at"])


@pytest.mark.parametrize("rate", ["0.0000", "-5.0000"])
async def test_a_pay_rate_must_be_positive(
    rate: str, async_client: AsyncClient, admin: dict[str, str], project: dict[str, Any]
) -> None:
    shift = (await add_shift(async_client, admin, project, MONDAY))["shift"]

    created = await async_client.post(
        SHIFTS,
        json={
            "project_id": project["id"],
            "shift_type_id": project["types"]["morning"],
            "date": MONDAY.isoformat(),
            "headcount": 1,
            "pay_rate": rate,
        },
        headers=admin,
    )
    changed = await async_client.patch(
        f"{SHIFTS}/{shift['id']}", json={"pay_rate": rate}, headers=admin
    )

    for refused in (created, changed):
        assert refused.status_code == 422, refused.text
        assert refused.json()["code"] in ("shifts.pay_rate_invalid", "validation.request_invalid")
