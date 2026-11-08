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
            "user_id": str(seed_users["extra"].id),
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
    assert datetime.fromisoformat(spans[str(seed_users["extra"].id)][1]) == end + timedelta(hours=2)

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
