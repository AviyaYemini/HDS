"""Integration tests for role-based access boundaries.

These tests verify that each seeded role can sign in and that role-based
endpoint restrictions are enforced.
"""

import pytest
from httpx import AsyncClient

from hds.models.organisation import Organisation
from hds.models.user import User


@pytest.mark.asyncio
async def test_employee_can_sign_in(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    seed_org: Organisation,
) -> None:
    response = await async_client.post(
        "/auth/login",
        json={
            "email": "employee@hds.test",
            "password": "Dev1234!",
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert "access_token" in body
    assert "refresh_token" in body


@pytest.mark.asyncio
async def test_manager_can_sign_in(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    seed_org: Organisation,
) -> None:
    response = await async_client.post(
        "/auth/login",
        json={
            "email": "manager@hds.test",
            "password": "Dev1234!",
        },
    )
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_admin_can_sign_in(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    seed_org: Organisation,
) -> None:
    response = await async_client.post(
        "/auth/login",
        json={
            "email": "admin@hds.test",
            "password": "Dev1234!",
        },
    )
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_client_can_sign_in(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    seed_org: Organisation,
) -> None:
    response = await async_client.post(
        "/auth/login",
        json={
            "email": "client@hds.test",
            "password": "Dev1234!",
        },
    )
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_employee_cannot_list_employees(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    seed_org: Organisation,
) -> None:
    login = await async_client.post(
        "/auth/login",
        json={
            "email": "employee@hds.test",
            "password": "Dev1234!",
        },
    )
    token = login.json()["access_token"]

    response = await async_client.get(
        "/api/v1/employees",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_client_cannot_list_employees(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    seed_org: Organisation,
) -> None:
    login = await async_client.post(
        "/auth/login",
        json={
            "email": "client@hds.test",
            "password": "Dev1234!",
        },
    )
    token = login.json()["access_token"]

    response = await async_client.get(
        "/api/v1/employees",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_manager_can_list_employees(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    seed_org: Organisation,
) -> None:
    """Manager reaches the employees endpoint (strict 200, T132)."""
    login = await async_client.post(
        "/auth/login",
        json={
            "email": "manager@hds.test",
            "password": "Dev1234!",
        },
    )
    token = login.json()["access_token"]

    response = await async_client.get(
        "/api/v1/employees",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 200
