"""Each person's own times in a shift through the API (007 FR-026, FR-027, clarification Q3).

Covers: assigning with own times, changing and clearing them, the 4-hour reach, the roster read
showing each person's times, My shifts showing the employee's own, and pay priced on them.
"""

from datetime import datetime, timedelta

from httpx import AsyncClient

from hds.models.user import User
from tests.roster_support import FIRST, SHIFTS, add_project, add_shift, headers_for


def _iso(moment: datetime) -> str:
    return moment.isoformat()


async def test_a_half_shift_and_extra_hours_are_stored_and_read_back(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin)
    shift = (await add_shift(async_client, admin, project, FIRST, headcount=2))["shift"]
    start = datetime.fromisoformat(shift["starts_at"])
    end = datetime.fromisoformat(shift["ends_at"])

    half = await async_client.post(
        f"{SHIFTS}/{shift['id']}/assignments",
        json={
            "user_id": str(seed_users["employee"].id),
            "starts_at": _iso(start),
            "ends_at": _iso(start + timedelta(hours=4)),
        },
        headers=admin,
    )
    assert half.status_code == 201, half.text
    assert half.json()["assignment"]["own_times"] is True
    extra = await async_client.post(
        f"{SHIFTS}/{shift['id']}/assignments",
        json={
            "user_id": str(seed_users["manager"].id),
            "starts_at": _iso(start),
            "ends_at": _iso(end + timedelta(hours=2)),
        },
        headers=admin,
    )
    assert extra.status_code == 201, extra.text

    [item] = (
        await async_client.get(SHIFTS, params={"project_id": project["id"]}, headers=admin)
    ).json()["items"]
    spans = {a["user_id"]: (a["starts_at"], a["ends_at"]) for a in item["assignments"]}
    assert datetime.fromisoformat(spans[str(seed_users["employee"].id)][1]) == start + timedelta(
        hours=4
    )
    extra_end = datetime.fromisoformat(spans[str(seed_users["manager"].id)][1])
    assert extra_end == end + timedelta(hours=2)

    cleared = await async_client.patch(
        f"/api/v1/assignments/{half.json()['assignment']['id']}",
        json={"starts_at": None, "ends_at": None},
        headers=admin,
    )
    assert cleared.status_code == 200, cleared.text
    assert cleared.json()["assignment"]["own_times"] is False


async def test_times_past_four_hours_are_refused(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin)
    shift = (await add_shift(async_client, admin, project, FIRST))["shift"]
    end = datetime.fromisoformat(shift["ends_at"])
    start = datetime.fromisoformat(shift["starts_at"])
    refused = await async_client.post(
        f"{SHIFTS}/{shift['id']}/assignments",
        json={
            "user_id": str(seed_users["employee"].id),
            "starts_at": _iso(start),
            "ends_at": _iso(end + timedelta(hours=5)),
        },
        headers=admin,
    )
    assert refused.status_code == 422
    assert refused.json()["code"] == "assignment.times_out_of_range"


async def test_pay_is_priced_on_the_persons_own_times(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    """A half shift pays half: My pay prices the person's own span, not the shift's (FR-027)."""
    admin = await headers_for(async_client, "admin")
    employee = seed_users["employee"]
    rate = await async_client.patch(
        f"/api/v1/employees/{employee.id}", json={"default_hourly_rate": "50.0000"}, headers=admin
    )
    assert rate.status_code == 200, rate.text
    project = await add_project(async_client, admin)
    # 08:00 to 16:00 in Israel on Monday 5 October 2026; the employee works 08:00 to 12:00.
    created = await async_client.post(
        SHIFTS,
        json={
            "project_id": project["id"],
            "shift_type_id": project["types"]["morning"],
            "date": "2026-10-05",
            "starts_at": "2026-10-05T05:00:00Z",
            "ends_at": "2026-10-05T13:00:00Z",
            "headcount": 1,
        },
        headers=admin,
    )
    assert created.status_code == 201, created.text
    shift = created.json()["shift"]
    half = await async_client.post(
        f"{SHIFTS}/{shift['id']}/assignments",
        json={
            "user_id": str(employee.id),
            "starts_at": "2026-10-05T05:00:00Z",
            "ends_at": "2026-10-05T09:00:00Z",
        },
        headers=admin,
    )
    assert half.status_code == 201, half.text
    published = await async_client.post(
        f"/api/v1/projects/{project['id']}/publish",
        json={"from": "2026-10-05", "to": "2026-10-05"},
        headers=admin,
    )
    assert published.status_code == 200, published.text

    pay = await async_client.get(
        "/me/pay", params={"month": "2026-10"}, headers=await headers_for(async_client, "employee")
    )
    assert pay.status_code == 200, pay.text
    [line] = pay.json()["lines"]
    assert (line["hours"], line["pay"]) == ("4.00", "200.00")


async def test_moving_a_shift_moves_its_peoples_own_times(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    """R83: own times are instants, so a shift moved a day takes them along."""
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin)
    shift = (await add_shift(async_client, admin, project, FIRST))["shift"]
    start = datetime.fromisoformat(shift["starts_at"])
    end = datetime.fromisoformat(shift["ends_at"])
    half = await async_client.post(
        f"{SHIFTS}/{shift['id']}/assignments",
        json={
            "user_id": str(seed_users["employee"].id),
            "starts_at": _iso(start),
            "ends_at": _iso(start + timedelta(hours=4)),
        },
        headers=admin,
    )
    assert half.status_code == 201, half.text
    day = timedelta(days=1)
    moved = await async_client.patch(
        f"{SHIFTS}/{shift['id']}",
        json={"starts_at": _iso(start + day), "ends_at": _iso(end + day)},
        headers=admin,
    )
    assert moved.status_code == 200, moved.text
    [item] = (
        await async_client.get(SHIFTS, params={"project_id": project["id"]}, headers=admin)
    ).json()["items"]
    [person] = item["assignments"]
    assert datetime.fromisoformat(person["starts_at"]) == start + day
    assert datetime.fromisoformat(person["ends_at"]) == start + day + timedelta(hours=4)
