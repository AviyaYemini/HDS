"""Two people changing one shift never silently overwrite each other (007 FR-041).

Covers: every shift read carries an ``etag``; a change with the current one works and answers a
new one; a change with a stale one is refused with 412 ``roster.changed`` and changes nothing; a
change without ``If-Match`` behaves as before 007; assignment changes make the etag stale too.
"""

from httpx import AsyncClient

from hds.models.user import User
from tests.roster_support import FIRST, SHIFTS, add_project, add_shift, assign, headers_for


async def test_a_stale_if_match_is_refused_and_a_current_one_works(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin)
    shift = (await add_shift(async_client, admin, project, FIRST))["shift"]
    first_tag = shift["etag"]

    changed = await async_client.patch(
        f"{SHIFTS}/{shift['id']}", json={"headcount": 2}, headers={**admin, "If-Match": first_tag}
    )
    assert changed.status_code == 200, changed.text
    assert changed.json()["shift"]["etag"] != first_tag

    stale = await async_client.patch(
        f"{SHIFTS}/{shift['id']}", json={"headcount": 3}, headers={**admin, "If-Match": first_tag}
    )
    assert stale.status_code == 412
    assert stale.json()["code"] == "roster.changed"
    [item] = (
        await async_client.get(SHIFTS, params={"project_id": project["id"]}, headers=admin)
    ).json()["items"]
    assert item["headcount"] == 2

    without = await async_client.patch(
        f"{SHIFTS}/{shift['id']}", json={"headcount": 4}, headers=admin
    )
    assert without.status_code == 200  # no If-Match: as before 007


async def test_an_assignment_change_makes_the_shifts_etag_stale(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin)
    shift = (await add_shift(async_client, admin, project, FIRST, headcount=2))["shift"]
    assigned = await assign(async_client, admin, shift["id"], seed_users["employee"].id)
    assert assigned.status_code == 201
    assert assigned.json()["shift_etag"] != shift["etag"]
    stale = await async_client.post(
        f"{SHIFTS}/{shift['id']}/assignments",
        json={"user_id": str(seed_users["extra"].id)},
        headers={**admin, "If-Match": shift["etag"]},
    )
    assert stale.status_code == 412
