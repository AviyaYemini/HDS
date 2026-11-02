"""Only real IANA time zones are stored for a site (T294, FR-217, SC-209).

A bad zone name would be accepted today and fail later, when a shift is shown or scheduled in the
site's local time. Through the real application on PostgreSQL: an invalid name is refused with 422
``sites.timezone_invalid`` on create and on edit, and nothing is stored or changed.

Covers: FR-217, SC-209.
"""

import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hds.models.user import User

BAD = ["Mars/Base", "", "../etc/passwd", "israel time", "UTC\\x00"]


async def manager_headers(client: AsyncClient) -> dict[str, str]:
    response = await client.post(
        "/auth/login", json={"email": "manager@hds.test", "password": "Dev1234!"}
    )
    assert response.status_code == 200, response.text
    return {"Authorization": f"Bearer {response.json()['access_token']}"}


async def site_rows(db: AsyncSession) -> list[tuple[str, str]]:
    await db.rollback()
    return [
        tuple(r) for r in (await db.execute(text("SELECT name, timezone FROM sites ORDER BY 1")))
    ]  # type: ignore[misc]


@pytest.mark.parametrize("zone", BAD)
async def test_creating_a_site_with_an_invalid_zone_is_refused_and_stores_nothing(
    zone: str, async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    headers = await manager_headers(async_client)

    response = await async_client.post(
        "/api/v1/sites",
        json={"name": "Gate", "address": "1 Example St", "timezone": zone},
        headers=headers,
    )

    assert response.status_code == 422
    assert response.json()["code"] in {"sites.timezone_invalid", "validation.request_invalid"}
    assert await site_rows(test_db) == []


@pytest.mark.parametrize("zone", ["Mars/Base", "../etc/passwd", "israel time"])
async def test_the_service_refuses_what_the_request_type_lets_through_with_its_own_code(
    zone: str, async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    headers = await manager_headers(async_client)

    response = await async_client.post(
        "/api/v1/sites",
        json={"name": "Gate", "address": "1 Example St", "timezone": zone},
        headers=headers,
    )

    assert response.status_code == 422
    assert response.json()["code"] == "sites.timezone_invalid"


@pytest.mark.parametrize("zone", ["Mars/Base", "../etc/passwd", "israel time"])
async def test_editing_a_site_to_an_invalid_zone_is_refused_and_the_zone_is_unchanged(
    zone: str, async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    headers = await manager_headers(async_client)
    created = await async_client.post(
        "/api/v1/sites",
        json={"name": "Gate", "address": "1 Example St", "timezone": "Asia/Jerusalem"},
        headers=headers,
    )
    assert created.status_code == 201, created.text

    response = await async_client.patch(
        f"/api/v1/sites/{created.json()['id']}", json={"timezone": zone}, headers=headers
    )

    assert response.status_code == 422
    assert response.json()["code"] == "sites.timezone_invalid"
    assert await site_rows(test_db) == [("Gate", "Asia/Jerusalem")]


async def test_a_real_zone_is_stored_as_given(
    async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    headers = await manager_headers(async_client)

    created = await async_client.post(
        "/api/v1/sites",
        json={"name": "Gate", "address": "1 Example St", "timezone": "Asia/Kolkata"},
        headers=headers,
    )

    assert created.status_code == 201
    assert await site_rows(test_db) == [("Gate", "Asia/Kolkata")]
