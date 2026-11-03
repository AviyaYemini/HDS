"""A NUL in a search term is refused at the edge, like in every other text input (Group C).

PostgreSQL text cannot hold NUL, so the term reached the database and answered 500. The fuzzer found
this, at random, on the contract run.

Covers: no spec requirement; guards Constitution V (the schema says what the API answers).
"""

from httpx import AsyncClient

from hds.models.user import User

PASSWORD = "Dev1234!"


async def headers_for(client: AsyncClient) -> dict[str, str]:
    response = await client.post(
        "/auth/login", json={"email": "manager@hds.test", "password": PASSWORD}
    )
    assert response.status_code == 200, response.text
    return {"Authorization": f"Bearer {response.json()['access_token']}"}


async def test_a_nul_in_the_search_term_is_refused(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    refused = await async_client.get(
        "/api/v1/employees", params={"search": "a\x00b"}, headers=await headers_for(async_client)
    )

    assert refused.status_code == 422
    assert refused.json()["code"] == "validation.request_invalid"


async def test_an_ordinary_search_term_still_works(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    found = await async_client.get(
        "/api/v1/employees", params={"search": "Manager"}, headers=await headers_for(async_client)
    )

    assert found.status_code == 200
    assert [row["email"] for row in found.json()["items"]] == ["manager@hds.test"]
