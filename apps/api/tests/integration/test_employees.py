"""Integration tests for employee management endpoints (US3)."""

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
async def test_create_employee_success(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    seed_org: Organisation,
) -> None:
    token = await _login(async_client, "manager@hds.test")
    r = await async_client.post(
        "/api/v1/employees",
        json={
            "email": "newstaff@hds.test",
            "full_name": "New Staff",
            "role": "employee",
            "initial_password": "Dev1234!Dev1234!",
        },
        headers={"Authorization": f"Bearer {token}"},
    )
    assert r.status_code == 201
    assert r.json()["email"] == "newstaff@hds.test"


@pytest.mark.asyncio
async def test_create_employee_duplicate_email_returns_409(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    seed_org: Organisation,
) -> None:
    token = await _login(async_client, "manager@hds.test")
    # employee@hds.test already seeded
    r = await async_client.post(
        "/api/v1/employees",
        json={
            "email": "employee@hds.test",
            "full_name": "Dup",
            "role": "employee",
            "initial_password": "Dev1234!Dev1234!",
        },
        headers={"Authorization": f"Bearer {token}"},
    )
    assert r.status_code == 409
    assert r.json()["code"] == "users.duplicate_email"


@pytest.mark.asyncio
async def test_non_admin_promoting_to_admin_returns_403(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    seed_org: Organisation,
) -> None:
    token = await _login(async_client, "manager@hds.test")
    employee = seed_users["employee"]
    r = await async_client.patch(
        f"/api/v1/employees/{employee.id}",
        json={"role": "admin"},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert r.status_code == 403
    assert r.json()["code"] == "users.admin_required"


@pytest.mark.asyncio
async def test_deactivate_employee(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    seed_org: Organisation,
) -> None:
    token = await _login(async_client, "manager@hds.test")
    employee = seed_users["employee"]
    r = await async_client.post(
        f"/api/v1/employees/{employee.id}/deactivate",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert r.status_code == 200
    assert r.json()["is_active"] is False


@pytest.mark.asyncio
async def test_last_admin_deactivation_blocked(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    seed_org: Organisation,
) -> None:
    token = await _login(async_client, "admin@hds.test")
    admin = seed_users["admin"]
    r = await async_client.delete(
        f"/api/v1/employees/{admin.id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert r.status_code == 409
    assert r.json()["code"] == "users.last_admin"


@pytest.mark.asyncio
async def test_put_employee_groups(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    seed_org: Organisation,
) -> None:
    token = await _login(async_client, "manager@hds.test")
    # Create a group first
    gr = await async_client.post(
        "/api/v1/groups",
        json={"name": "Night Shift"},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert gr.status_code == 201
    group_id = gr.json()["id"]

    employee = seed_users["employee"]
    r = await async_client.put(
        f"/api/v1/employees/{employee.id}/groups",
        json={"ids": [group_id]},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert r.status_code == 204

    # Verify
    r2 = await async_client.get(
        f"/api/v1/employees/{employee.id}/groups",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert r2.status_code == 200
    assert any(g["id"] == group_id for g in r2.json())
