"""Saved views through the API (007 FR-043, FR-049, task T082).

Covers: a user saves, lists and removes their views of a screen; names are unique per screen; nobody
sees or removes another user's.
"""

from httpx import AsyncClient

from hds.models.user import User
from tests.roster_support import headers_for

URL = "/me/saved-views"


async def test_a_user_keeps_their_own_views(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    manager = await headers_for(async_client, "manager")
    made = await async_client.post(
        URL,
        json={"screen": "staff", "name": "Active", "query": "status=active&sort=name"},
        headers=manager,
    )
    assert made.status_code == 201, made.text
    twice = await async_client.post(
        URL, json={"screen": "staff", "name": "Active", "query": "x"}, headers=manager
    )
    assert twice.status_code == 409
    listed = await async_client.get(URL, params={"screen": "staff"}, headers=manager)
    assert [v["name"] for v in listed.json()["items"]] == ["Active"]

    admin = await headers_for(async_client, "admin")
    assert (await async_client.get(URL, params={"screen": "staff"}, headers=admin)).json()[
        "items"
    ] == []
    stolen = await async_client.delete(f"{URL}/{made.json()['id']}", headers=admin)
    assert stolen.status_code == 404
    removed = await async_client.delete(f"{URL}/{made.json()['id']}", headers=manager)
    assert removed.status_code == 204


async def test_a_client_has_no_saved_views(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    response = await async_client.get(
        URL, params={"screen": "staff"}, headers=await headers_for(async_client, "client")
    )
    assert response.status_code == 403
