"""A Client-role account with no client sees no projects (T388, F18).

An empty ``client_id`` meant "no filter", so such an account saw every project of the organisation.
The schema allows the empty value on purpose, so the rule has to hold in the service.

Covers: FR-237.
"""

from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hds.models.user import User

PASSWORD = "Dev1234!"


async def headers_for(client: AsyncClient, email: str) -> dict[str, str]:
    response = await client.post("/auth/login", json={"email": email, "password": PASSWORD})
    assert response.status_code == 200, response.text
    return {"Authorization": f"Bearer {response.json()['access_token']}"}


async def add_project(client: AsyncClient, headers: dict[str, str]) -> Any:
    client_row = await client.post(
        "/api/v1/clients",
        json={"name": "Test Corp", "contract_start": "2026-01-01"},
        headers=headers,
    )
    site = await client.post(
        "/api/v1/sites",
        json={"name": "HQ", "address": "1 HQ St", "timezone": "UTC"},
        headers=headers,
    )
    project = await client.post(
        "/api/v1/projects",
        json={
            "client_id": client_row.json()["id"],
            "site_id": site.json()["id"],
            "name": "Night Watch",
            "type": "security",
            "billing_rate": "75.0000",
            "start_date": "2026-01-01",
        },
        headers=headers,
    )
    assert project.status_code == 201, project.text
    return project.json()


@pytest.fixture
async def portal(
    async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> dict[str, str]:
    """Sign in the seeded Client account, stored with no client, as the rule is about."""
    await test_db.execute(
        text("UPDATE users SET client_id = NULL WHERE id = :u"), {"u": seed_users["client"].id}
    )
    await test_db.commit()
    return await headers_for(async_client, "client@hds.test")


async def test_the_project_list_is_empty(
    async_client: AsyncClient, seed_users: dict[str, User], portal: dict[str, str]
) -> None:
    project = await add_project(async_client, await headers_for(async_client, "manager@hds.test"))

    response = await async_client.get("/api/v1/projects", headers=portal)
    filtered = await async_client.get(
        "/api/v1/projects", params={"client_id": project["client_id"]}, headers=portal
    )

    assert response.status_code == 200
    assert response.json()["items"] == []
    assert filtered.json()["items"] == []


async def test_a_project_answers_404(
    async_client: AsyncClient, seed_users: dict[str, User], portal: dict[str, str]
) -> None:
    project = await add_project(async_client, await headers_for(async_client, "manager@hds.test"))

    response = await async_client.get(f"/api/v1/projects/{project['id']}", headers=portal)

    assert response.status_code == 404


async def test_staff_still_see_the_project(
    async_client: AsyncClient, seed_users: dict[str, User], portal: dict[str, str]
) -> None:
    headers = await headers_for(async_client, "manager@hds.test")
    project = await add_project(async_client, headers)

    response = await async_client.get("/api/v1/projects", headers=headers)

    assert [row["id"] for row in response.json()["items"]] == [project["id"]]
