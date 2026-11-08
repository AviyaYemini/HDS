"""The contact card through the API: every role reads it, only an Admin changes it.

Covers: 007 FR-012 (C6, task T027).
"""

from httpx import AsyncClient

from hds.models.user import User
from tests.roster_support import headers_for

URL = "/api/v1/organisation/contact"
WEEK = [{"weekday": d, "opens": "08:00", "closes": "17:00"} for d in range(5)]


async def test_an_admin_sets_the_card_and_every_role_reads_it(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    changed = await async_client.patch(
        URL,
        json={"phone": "03-1234567", "email": "Office@HDS.test", "opening_hours": WEEK},
        headers=admin,
    )
    assert changed.status_code == 200, changed.text
    for role in ("employee", "manager", "admin", "client"):
        read = await async_client.get(URL, headers=await headers_for(async_client, role))
        assert read.status_code == 200, role
        assert read.json() == {
            "phone": "03-1234567",
            "email": "office@hds.test",
            "opening_hours": WEEK,
        }


async def test_null_clears_a_field_and_an_omitted_one_stays(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    await async_client.patch(
        URL, json={"phone": "03-1234567", "email": "office@hds.test"}, headers=admin
    )
    cleared = await async_client.patch(URL, json={"phone": None}, headers=admin)
    assert cleared.json()["phone"] is None
    assert cleared.json()["email"] == "office@hds.test"


async def test_only_an_admin_changes_it(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    for role in ("employee", "manager", "client"):
        response = await async_client.patch(
            URL, json={"phone": "1"}, headers=await headers_for(async_client, role)
        )
        assert response.status_code == 403, role


async def test_bad_opening_hours_are_refused(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    twice = [WEEK[0], WEEK[0]]
    backwards = [{"weekday": 1, "opens": "17:00", "closes": "08:00"}]
    # Found by the contract suite's fuzzing, which skips this operation's valid-data check (R20).
    no_time_open = [{"weekday": 5, "opens": "00:00", "closes": "00:00"}]
    for hours in (twice, backwards, no_time_open):
        response = await async_client.patch(URL, json={"opening_hours": hours}, headers=admin)
        assert response.status_code == 422, hours
        assert response.json()["code"] == "organisation.opening_hours_invalid", hours
    bad_time = [{"weekday": 1, "opens": "25:00", "closes": "26:00"}]
    response = await async_client.patch(URL, json={"opening_hours": bad_time}, headers=admin)
    assert response.status_code == 422
    # Valid hours right after are kept: the refusals changed nothing.
    response = await async_client.patch(URL, json={"opening_hours": WEEK}, headers=admin)
    assert response.status_code == 200
    assert response.json()["opening_hours"] == WEEK
