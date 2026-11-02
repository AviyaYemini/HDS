"""Integration tests for the client portal: a Client sees only their own projects (US5, T096).

Covers: FR-010, SC-004.
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
    return str(r.json()["access_token"])


@pytest.mark.asyncio
async def test_client_can_list_own_projects(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    seed_org: Organisation,
) -> None:
    """Client can list projects — scope enforced by service (own client_id only)."""
    token = await _login(async_client, "client@hds.test")
    r = await async_client.get(
        "/api/v1/projects",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert r.status_code == 200
    # All returned projects must belong to this client's client_id
    client_user = seed_users["client"]
    for project in r.json()["items"]:
        assert project["client_id"] == str(client_user.client_id)


@pytest.mark.asyncio
async def test_client_cannot_post_projects(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    seed_org: Organisation,
) -> None:
    token = await _login(async_client, "client@hds.test")
    r = await async_client.post(
        "/api/v1/projects",
        json={},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert r.status_code == 403


@pytest.mark.asyncio
async def test_client_cannot_list_employees(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    seed_org: Organisation,
) -> None:
    token = await _login(async_client, "client@hds.test")
    r = await async_client.get(
        "/api/v1/employees",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert r.status_code == 403


@pytest.mark.asyncio
async def test_client_cannot_list_clients(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    seed_org: Organisation,
) -> None:
    token = await _login(async_client, "client@hds.test")
    r = await async_client.get(
        "/api/v1/clients",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert r.status_code == 403
