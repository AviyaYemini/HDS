"""Integration tests for invitation and password-reset flows (US6, T105).

Covers: FR-006.
"""

import pytest
from httpx import AsyncClient

from hds.models.user import User
from tests.conftest import OutboxMailSender


async def _login(client: AsyncClient, email: str, password: str = "Dev1234!") -> str:
    r = await client.post("/auth/login", json={"email": email, "password": password})
    assert r.status_code == 200, r.text
    return r.json()["access_token"]


@pytest.mark.asyncio
async def test_invite_returns_202_and_mails_token(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
) -> None:
    token = await _login(async_client, "manager@hds.test")
    r = await async_client.post(
        "/auth/invite",
        json={"email": "invited@hds.test", "role": "employee"},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert r.status_code == 202
    assert r.content == b""
    assert outbox.last_token_for("invited@hds.test")


@pytest.mark.asyncio
async def test_employee_cannot_invite(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
) -> None:
    token = await _login(async_client, "employee@hds.test")
    r = await async_client.post(
        "/auth/invite",
        json={"email": "x@hds.test", "role": "employee"},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert r.status_code == 403
    assert outbox.messages == []


@pytest.mark.asyncio
async def test_accept_invitation_allows_signin(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
) -> None:
    mgr_token = await _login(async_client, "manager@hds.test")
    inv = await async_client.post(
        "/auth/invite",
        json={"email": "newuser2@hds.test", "role": "employee"},
        headers={"Authorization": f"Bearer {mgr_token}"},
    )
    assert inv.status_code == 202
    raw_token = outbox.last_token_for("newuser2@hds.test")

    r = await async_client.post(
        "/auth/invite/accept",
        json={"token": raw_token, "password": "NewPass123456!"},
    )
    assert r.status_code == 204

    r2 = await async_client.post(
        "/auth/login",
        json={"email": "newuser2@hds.test", "password": "NewPass123456!"},
    )
    assert r2.status_code == 200


@pytest.mark.asyncio
async def test_accept_invitation_twice_returns_401(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
) -> None:
    mgr_token = await _login(async_client, "manager@hds.test")
    await async_client.post(
        "/auth/invite",
        json={"email": "twiceuser@hds.test", "role": "employee"},
        headers={"Authorization": f"Bearer {mgr_token}"},
    )
    raw_token = outbox.last_token_for("twiceuser@hds.test")

    await async_client.post(
        "/auth/invite/accept",
        json={"token": raw_token, "password": "NewPass123456!"},
    )
    r = await async_client.post(
        "/auth/invite/accept",
        json={"token": raw_token, "password": "AnotherPass123456!"},
    )
    assert r.status_code == 401
    assert r.json()["code"] == "auth.token_used"


@pytest.mark.asyncio
async def test_accept_invitation_short_password_rejected(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
) -> None:
    r = await async_client.post(
        "/auth/invite/accept",
        json={"token": "whatever", "password": "short"},
    )
    assert r.status_code == 422


@pytest.mark.asyncio
async def test_password_reset_request_is_silent_for_unknown_email(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
) -> None:
    r = await async_client.post("/auth/password-reset/request", json={"email": "nobody@hds.test"})
    assert r.status_code == 202
    assert r.content == b""
    assert outbox.messages == []


@pytest.mark.asyncio
async def test_password_reset_flow(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
) -> None:
    r = await async_client.post("/auth/password-reset/request", json={"email": "employee@hds.test"})
    assert r.status_code == 202
    raw_token = outbox.last_token_for("employee@hds.test")

    r2 = await async_client.post(
        "/auth/password-reset/confirm",
        json={"token": raw_token, "new_password": "BrandNewPass123!"},
    )
    assert r2.status_code == 204

    r3 = await async_client.post(
        "/auth/login",
        json={"email": "employee@hds.test", "password": "BrandNewPass123!"},
    )
    assert r3.status_code == 200
