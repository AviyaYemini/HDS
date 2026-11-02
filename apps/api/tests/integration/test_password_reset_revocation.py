"""T136: a password reset revokes every refresh token of the user.

Covers: FR-006, NFR-005.
"""

import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hds.models.user import User
from tests.conftest import OutboxMailSender


async def _login(client: AsyncClient, email: str, password: str) -> str:
    response = await client.post("/auth/login", json={"email": email, "password": password})
    assert response.status_code == 200, response.text
    return str(response.json()["refresh_token"])


@pytest.mark.asyncio
async def test_password_reset_revokes_all_refresh_tokens(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
) -> None:
    email = "employee@hds.test"
    session_a = await _login(async_client, email, "Dev1234!")  # two devices
    session_b = await _login(async_client, email, "Dev1234!")
    other_user_session = await _login(async_client, "manager@hds.test", "Dev1234!")

    await async_client.post("/auth/password-reset/request", json={"email": email})
    confirm = await async_client.post(
        "/auth/password-reset/confirm",
        json={"token": outbox.last_token_for(email), "new_password": "BrandNewPass123!"},
    )
    assert confirm.status_code == 204, confirm.text

    for old in (session_a, session_b):
        response = await async_client.post("/auth/refresh", json={"refresh_token": old})
        assert response.status_code == 401, response.text

    unrevoked = await test_db.scalar(
        text("SELECT count(*) FROM refresh_tokens WHERE user_id = :u AND revoked_at IS NULL"),
        {"u": seed_users["employee"].id},
    )
    assert unrevoked == 0

    # Other users are unaffected.
    other = await async_client.post("/auth/refresh", json={"refresh_token": other_user_session})
    assert other.status_code == 200, other.text
