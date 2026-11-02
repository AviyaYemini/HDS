"""Two requests creating one address at once get 201 and 409, never a validation error (Group D).

Hashing now happens between the duplicate check and the insert, so the two can overlap for as long
as the hashing queue takes. The loser used to reach the unique index and answer 422, which the
operations do not document.

Covers: FR-222, FR-215.
"""

from collections.abc import Awaitable, Callable
from typing import Any

import pytest
from httpx import AsyncClient

from hds.models.user import User

PASSWORD = "Dev1234!"
NEW_PASSWORD = "A-long-password-1"
Race = Callable[..., Awaitable[list[Any]]]


async def admin_headers(client: AsyncClient) -> dict[str, str]:
    response = await client.post(
        "/auth/login", json={"email": "admin@hds.test", "password": PASSWORD}
    )
    assert response.status_code == 200, response.text
    return {"Authorization": f"Bearer {response.json()['access_token']}"}


@pytest.mark.parametrize("round_number", range(3))
async def test_two_creates_of_one_address_answer_201_and_409(
    async_client: AsyncClient, seed_users: dict[str, User], race: Race, round_number: int
) -> None:
    headers = await admin_headers(async_client)
    body = {
        "email": f"both{round_number}@hds.test",
        "full_name": "Both",
        "initial_password": NEW_PASSWORD,
    }

    async def create() -> Any:
        return await async_client.post("/api/v1/employees", json=body, headers=headers)

    results = await race(create, create)

    assert sorted(r.status_code for r in results) == [201, 409], [r.text for r in results]
    refused = next(r for r in results if r.status_code == 409)
    assert refused.json()["code"] == "users.duplicate_email"
