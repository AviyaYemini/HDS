"""Two requests creating one address at once get 201 and 409, never a validation error (T388).

Hashing now happens between the duplicate check and the insert, so the two can overlap for as long
as the hashing queue takes. The loser used to reach the unique index and answer 422, which the
operations do not document.

Covers: FR-244, FR-222, FR-215.
"""

from collections.abc import Awaitable, Callable
from typing import Any

from httpx import AsyncClient

from hds.models.user import User

PASSWORD = "Dev1234!"
NEW_PASSWORD = "A-long-password-1"
Race = Callable[..., Awaitable[list[Any]]]


def assert_one_winner(results: list[Any], round_number: int) -> None:
    statuses = sorted(r.status_code for r in results)
    assert statuses == [201, 409], f"round {round_number}: {[r.text for r in results]}"
    assert next(r for r in results if r.status_code == 409).json()["code"] == (
        "users.duplicate_email"
    )


async def admin_headers(client: AsyncClient) -> dict[str, str]:
    response = await client.post(
        "/auth/login", json={"email": "admin@hds.test", "password": PASSWORD}
    )
    assert response.status_code == 200, response.text
    return {"Authorization": f"Bearer {response.json()['access_token']}"}


ROUNDS = 50


async def test_two_creates_of_one_address_answer_201_and_409(
    async_client: AsyncClient, seed_users: dict[str, User], race: Race
) -> None:
    headers = await admin_headers(async_client)

    for round_number in range(ROUNDS):
        body = {
            "email": f"both{round_number}@hds.test",
            "full_name": "Both",
            "initial_password": NEW_PASSWORD,
        }

        async def create(payload: dict[str, str] = body) -> Any:
            return await async_client.post("/api/v1/employees", json=payload, headers=headers)

        assert_one_winner(await race(create, create), round_number)


async def test_two_portal_users_for_one_address_answer_201_and_409(
    async_client: AsyncClient, seed_users: dict[str, User], race: Race
) -> None:
    headers = await admin_headers(async_client)
    client = await async_client.post(
        "/api/v1/clients",
        json={"name": "Portal Ltd", "contract_start": "2026-01-01"},
        headers=headers,
    )
    assert client.status_code == 201, client.text
    client_id = client.json()["id"]

    for round_number in range(ROUNDS):
        body = {
            "email": f"portal{round_number}@hds.test",
            "full_name": "Portal",
            "initial_password": NEW_PASSWORD,
        }

        async def create(payload: dict[str, str] = body) -> Any:
            return await async_client.post(
                f"/api/v1/clients/{client_id}/users", json=payload, headers=headers
            )

        assert_one_winner(await race(create, create), round_number)
