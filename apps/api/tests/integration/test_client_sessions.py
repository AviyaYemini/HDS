"""Portal users lose their sessions when their client goes, and stay out (T311, F02).

Deactivating an employee ends their sessions; a portal user must get the same. Otherwise a refresh
token issued before the client was deactivated works again the moment the user is reactivated, and
a Manager could bring a portal user back under a client that no longer exists.

Covers: FR-225.
"""

import hashlib
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.security import generate_refresh_token
from hds.models.user import User

PASSWORD = "Dev1234!"
PORTAL_PASSWORD = "A-long-password-1"
PORTAL_EMAIL = "portal.user@hds.test"


async def sign_in(client: AsyncClient, email: str, password: str = PASSWORD) -> dict[str, Any]:
    response = await client.post("/auth/login", json={"email": email, "password": password})
    assert response.status_code == 200, response.text
    body: dict[str, Any] = response.json()
    return body


async def headers_for(client: AsyncClient, email: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {(await sign_in(client, email))['access_token']}"}


async def portal_user(client: AsyncClient, admin: dict[str, str]) -> tuple[str, str]:
    """Create a client with one portal user; return ``(client id, user id)``."""
    created = await client.post(
        "/api/v1/clients", json={"name": "Acme", "contract_start": "2026-01-01"}, headers=admin
    )
    assert created.status_code == 201, created.text
    client_id = created.json()["id"]
    user = await client.post(
        f"/api/v1/clients/{client_id}/users",
        json={
            "email": PORTAL_EMAIL,
            "full_name": "Portal",
            "initial_password": PORTAL_PASSWORD,
        },
        headers=admin,
    )
    assert user.status_code == 201, user.text
    return client_id, user.json()["id"]


async def add_link(db: AsyncSession, user_id: str) -> str:
    """Give the user an unused reset link; return its raw token."""
    raw, token_hash = generate_refresh_token()
    await db.execute(
        text(
            "INSERT INTO invitation_tokens (id, user_id, token_hash, purpose, expires_at) "
            "VALUES (:i, :u, :h, 'reset', :x)"
        ),
        {
            "i": uuid.uuid4(),
            "u": uuid.UUID(user_id),
            "h": token_hash,
            "x": datetime.now(UTC) + timedelta(hours=1),
        },
    )
    await db.commit()
    return raw


async def token_state(db: AsyncSession, raw: str) -> tuple[bool, str | None]:
    await db.rollback()
    row = (
        await db.execute(
            text(
                "SELECT revoked_at IS NOT NULL, revoked_reason FROM refresh_tokens "
                "WHERE token_hash = :h"
            ),
            {"h": hashlib.sha256(raw.encode()).hexdigest()},
        )
    ).one()
    return bool(row[0]), row[1]


async def refresh_status(client: AsyncClient, raw: str) -> int:
    response = await client.post("/auth/refresh", json={"refresh_token": raw})
    return response.status_code


async def end_portal_access(
    how: str, client: AsyncClient, client_id: str, user_id: str, admin: dict[str, str]
) -> None:
    if how == "deactivate":
        done = await client.patch(
            f"/api/v1/clients/{client_id}", json={"is_active": False}, headers=admin
        )
    elif how == "delete":
        done = await client.delete(f"/api/v1/clients/{client_id}", headers=admin)
    else:
        done = await client.delete(f"/api/v1/clients/{client_id}/users/{user_id}", headers=admin)
    assert done.status_code in (200, 204), done.text


@pytest.mark.parametrize("how", ["deactivate", "delete", "remove"])
async def test_ending_portal_access_ends_the_sessions_and_cancels_the_links(
    how: str, async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin@hds.test")
    client_id, user_id = await portal_user(async_client, admin)
    session = await sign_in(async_client, PORTAL_EMAIL, PORTAL_PASSWORD)
    link = await add_link(test_db, user_id)

    await end_portal_access(how, async_client, client_id, user_id, admin)

    assert await token_state(test_db, session["refresh_token"]) == (True, "account")
    redeemed = await async_client.post(
        "/auth/password-reset/confirm", json={"token": link, "new_password": "Another-pass-1"}
    )
    assert redeemed.status_code == 401
    assert redeemed.json()["code"] == "auth.token_used"


async def test_an_old_token_stays_refused_after_the_user_is_reactivated(
    async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin@hds.test")
    client_id, user_id = await portal_user(async_client, admin)
    session = await sign_in(async_client, PORTAL_EMAIL, PORTAL_PASSWORD)
    await end_portal_access("deactivate", async_client, client_id, user_id, admin)
    back_on = await async_client.patch(
        f"/api/v1/clients/{client_id}", json={"is_active": True}, headers=admin
    )
    assert back_on.status_code == 200, back_on.text

    reactivated = await async_client.post(f"/api/v1/employees/{user_id}/reactivate", headers=admin)

    assert reactivated.status_code == 200, reactivated.text
    assert await refresh_status(async_client, session["refresh_token"]) == 401


@pytest.mark.parametrize("how", ["deactivate", "delete"])
async def test_a_portal_user_of_a_gone_client_cannot_be_reactivated(
    how: str, async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin@hds.test")
    client_id, user_id = await portal_user(async_client, admin)
    await end_portal_access(how, async_client, client_id, user_id, admin)

    refused = await async_client.post(f"/api/v1/employees/{user_id}/reactivate", headers=admin)

    assert refused.status_code == 409, refused.text
    assert refused.json()["code"] == "users.client_inactive"
    await test_db.rollback()
    active = await test_db.scalar(
        text("SELECT is_active FROM users WHERE id = :u"), {"u": uuid.UUID(user_id)}
    )
    assert active is False


async def test_a_portal_user_cannot_be_created_under_an_inactive_client(
    async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin@hds.test")
    client_id, user_id = await portal_user(async_client, admin)
    await end_portal_access("deactivate", async_client, client_id, user_id, admin)

    refused = await async_client.post(
        f"/api/v1/clients/{client_id}/users",
        json={
            "email": "second.portal@hds.test",
            "full_name": "Second",
            "initial_password": PORTAL_PASSWORD,
        },
        headers=admin,
    )

    assert refused.status_code == 409, refused.text
    assert refused.json()["code"] == "users.client_inactive"
    await test_db.rollback()
    count = await test_db.scalar(
        text("SELECT count(*) FROM users WHERE email = 'second.portal@hds.test'")
    )
    assert count == 0
