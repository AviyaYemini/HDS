"""Integration tests for clients, sites and projects (US4, T090).

Covers: FR-009a, FR-021, FR-023, FR-025, FR-026, FR-028.
"""

import pytest
from httpx import AsyncClient

from hds.models.organisation import Organisation
from hds.models.user import User


async def _login(client: AsyncClient, email: str, password: str = "Dev1234!") -> str:
    r = await client.post(
        "/auth/login",
        json={"email": email, "password": password},
    )
    assert r.status_code == 200, r.text
    return r.json()["access_token"]


@pytest.mark.asyncio
async def test_create_client(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    seed_org: Organisation,
) -> None:
    token = await _login(async_client, "manager@hds.test")
    r = await async_client.post(
        "/api/v1/clients",
        json={"name": "Acme Security", "contract_start": "2026-01-01"},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert r.status_code == 201
    assert r.json()["name"] == "Acme Security"


@pytest.mark.asyncio
async def test_create_site(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    seed_org: Organisation,
) -> None:
    token = await _login(async_client, "manager@hds.test")
    r = await async_client.post(
        "/api/v1/sites",
        json={"name": "Main Gate", "address": "1 Example St", "timezone": "UTC"},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert r.status_code == 201
    assert r.json()["name"] == "Main Gate"


@pytest.mark.asyncio
async def test_create_project_valid_billing_rate(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    seed_org: Organisation,
) -> None:
    token = await _login(async_client, "manager@hds.test")

    # Create client and site first
    client_r = await async_client.post(
        "/api/v1/clients",
        json={"name": "Test Corp", "contract_start": "2026-01-01"},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert client_r.status_code == 201
    client_id = client_r.json()["id"]

    site_r = await async_client.post(
        "/api/v1/sites",
        json={"name": "HQ", "address": "1 HQ St", "timezone": "UTC"},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert site_r.status_code == 201
    site_id = site_r.json()["id"]

    r = await async_client.post(
        "/api/v1/projects",
        json={
            "client_id": client_id,
            "site_id": site_id,
            "name": "Night Watch",
            "type": "security",
            "billing_rate": "75.0000",
            "start_date": "2026-01-01",
        },
        headers={"Authorization": f"Bearer {token}"},
    )
    assert r.status_code == 201


@pytest.mark.asyncio
async def test_create_project_zero_billing_rate_returns_422(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    seed_org: Organisation,
) -> None:
    token = await _login(async_client, "manager@hds.test")

    client_r = await async_client.post(
        "/api/v1/clients",
        json={"name": "Bad Corp", "contract_start": "2026-01-01"},
        headers={"Authorization": f"Bearer {token}"},
    )
    client_id = client_r.json()["id"]
    site_r = await async_client.post(
        "/api/v1/sites",
        json={"name": "Bad Site", "address": "1 Bad St", "timezone": "UTC"},
        headers={"Authorization": f"Bearer {token}"},
    )
    site_id = site_r.json()["id"]

    r = await async_client.post(
        "/api/v1/projects",
        json={
            "client_id": client_id,
            "site_id": site_id,
            "name": "Zero Rate",
            "type": "security",
            "billing_rate": "0",
            "start_date": "2026-01-01",
        },
        headers={"Authorization": f"Bearer {token}"},
    )
    assert r.status_code == 422
    assert r.json()["code"] == "projects.billing_rate_invalid"


@pytest.mark.asyncio
async def test_project_status_transitions(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    seed_org: Organisation,
) -> None:
    token = await _login(async_client, "manager@hds.test")

    client_r = await async_client.post(
        "/api/v1/clients",
        json={"name": "Status Corp", "contract_start": "2026-01-01"},
        headers={"Authorization": f"Bearer {token}"},
    )
    client_id = client_r.json()["id"]
    site_r = await async_client.post(
        "/api/v1/sites",
        json={"name": "Status Site", "address": "1 St", "timezone": "UTC"},
        headers={"Authorization": f"Bearer {token}"},
    )
    site_id = site_r.json()["id"]

    proj_r = await async_client.post(
        "/api/v1/projects",
        json={
            "client_id": client_id,
            "site_id": site_id,
            "name": "Status Proj",
            "type": "security",
            "billing_rate": "50.0000",
            "start_date": "2026-01-01",
        },
        headers={"Authorization": f"Bearer {token}"},
    )
    project_id = proj_r.json()["id"]

    # active → ended
    r = await async_client.patch(
        f"/api/v1/projects/{project_id}",
        json={"status": "ended"},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert r.status_code == 200
    assert r.json()["status"] == "ended"

    # ended → active
    r2 = await async_client.patch(
        f"/api/v1/projects/{project_id}",
        json={"status": "active"},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert r2.status_code == 200
    assert r2.json()["status"] == "active"


@pytest.mark.asyncio
async def test_delete_client_with_existing_project_returns_409(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    seed_org: Organisation,
) -> None:
    token = await _login(async_client, "admin@hds.test")

    client_r = await async_client.post(
        "/api/v1/clients",
        json={"name": "Blocked Corp", "contract_start": "2026-01-01"},
        headers={"Authorization": f"Bearer {token}"},
    )
    client_id = client_r.json()["id"]
    site_r = await async_client.post(
        "/api/v1/sites",
        json={"name": "Blocked Site", "address": "1 Blocked St", "timezone": "UTC"},
        headers={"Authorization": f"Bearer {token}"},
    )
    site_id = site_r.json()["id"]

    await async_client.post(
        "/api/v1/projects",
        json={
            "client_id": client_id,
            "site_id": site_id,
            "name": "Linked Project",
            "type": "security",
            "billing_rate": "60.0000",
            "start_date": "2026-01-01",
        },
        headers={"Authorization": f"Bearer {token}"},
    )

    r = await async_client.delete(
        f"/api/v1/clients/{client_id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert r.status_code == 409
    assert r.json()["code"] == "clients.has_projects"
