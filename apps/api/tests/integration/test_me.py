"""Integration tests for /me profile endpoints (US2)."""

import pytest
from httpx import AsyncClient

from hds.models.organisation import Organisation
from hds.models.user import User


async def _login(client: AsyncClient, email: str, password: str = "Dev1234!") -> str:
    response = await client.post(
        "/auth/login",
        json={"email": email, "password": password},
    )
    assert response.status_code == 200, response.text
    return response.json()["access_token"]


@pytest.mark.asyncio
async def test_employee_can_update_phone(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    seed_org: Organisation,
) -> None:
    token = await _login(async_client, "employee@hds.test")
    response = await async_client.patch(
        "/me",
        json={"phone": "+972501234567"},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 200
    assert response.json()["phone"] == "+972501234567"


@pytest.mark.asyncio
async def test_employee_cannot_change_role(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    seed_org: Organisation,
) -> None:
    token = await _login(async_client, "employee@hds.test")
    response = await async_client.patch(
        "/me",
        json={"role": "manager"},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_employee_cannot_access_other_employee_profile(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    seed_org: Organisation,
) -> None:
    token = await _login(async_client, "employee@hds.test")
    manager = seed_users["manager"]
    response = await async_client.get(
        f"/api/v1/employees/{manager.id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    # 403 if route exists; 404 if not yet registered (T067)
    assert response.status_code in (403, 404)
