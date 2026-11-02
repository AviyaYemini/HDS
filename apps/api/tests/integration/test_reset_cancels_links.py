"""A completed password reset ends the account's older links and its lockout (T305, F01, F11).

Someone holding an earlier link must lose it the moment the owner finishes a reset, otherwise they
keep control of the account for the rest of the link's lifetime. The owner has also just proved
control of the mailbox, so a lock from earlier guessing no longer stands.

Covers: FR-224, FR-232.
"""

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.config import get_settings
from hds.core.security import generate_refresh_token
from hds.models.user import User
from tests.conftest import OutboxMailSender

EMAIL = "employee@hds.test"
PASSWORD = "Dev1234!"
NEWER_PASSWORD = "Newer-password-1"
OLDER_PASSWORD = "Older-password-1"


async def sign_in(client: AsyncClient, password: str) -> int:
    response = await client.post("/auth/login", json={"email": EMAIL, "password": password})
    return response.status_code


async def request_reset(client: AsyncClient, outbox: OutboxMailSender) -> str:
    response = await client.post("/auth/password-reset/request", json={"email": EMAIL})
    assert response.status_code == 202
    return outbox.last_token_for(EMAIL)


async def confirm(client: AsyncClient, token: str, password: str) -> Any:
    return await client.post(
        "/auth/password-reset/confirm", json={"token": token, "new_password": password}
    )


async def add_link(db: AsyncSession, user_id: uuid.UUID, purpose: str) -> tuple[str, str]:
    """Store an unused link directly, as one issued before this rule would be."""
    raw, token_hash = generate_refresh_token()
    await db.execute(
        text(
            "INSERT INTO invitation_tokens (id, user_id, token_hash, purpose, expires_at) "
            "VALUES (:i, :u, :h, :p, :x)"
        ),
        {
            "i": uuid.uuid4(),
            "u": user_id,
            "h": token_hash,
            "p": purpose,
            "x": datetime.now(UTC) + timedelta(hours=1),
        },
    )
    await db.commit()
    return raw, token_hash


async def unused(db: AsyncSession, token_hash: str) -> bool:
    await db.rollback()
    row = await db.execute(
        text("SELECT used_at IS NULL FROM invitation_tokens WHERE token_hash = :h"),
        {"h": token_hash},
    )
    return bool(row.scalar_one())


async def test_a_new_request_cancels_the_earlier_reset_link(
    async_client: AsyncClient, seed_users: dict[str, User], outbox: OutboxMailSender
) -> None:
    older = await request_reset(async_client, outbox)
    newer = await request_reset(async_client, outbox)

    assert (await confirm(async_client, newer, NEWER_PASSWORD)).status_code == 204

    refused = await confirm(async_client, older, OLDER_PASSWORD)
    assert refused.status_code == 401
    assert refused.json()["code"] == "auth.token_used"
    assert await sign_in(async_client, OLDER_PASSWORD) == 401
    assert await sign_in(async_client, NEWER_PASSWORD) == 200


async def test_the_earlier_reset_link_stops_working_as_soon_as_a_new_one_is_requested(
    async_client: AsyncClient, seed_users: dict[str, User], outbox: OutboxMailSender
) -> None:
    older = await request_reset(async_client, outbox)
    await request_reset(async_client, outbox)

    refused = await confirm(async_client, older, OLDER_PASSWORD)  # no reset has completed yet

    assert refused.status_code == 401
    assert refused.json()["code"] == "auth.token_used"
    assert await sign_in(async_client, PASSWORD) == 200


async def test_a_completed_reset_ends_every_other_outstanding_link(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
) -> None:
    user_id = seed_users["employee"].id
    link = await request_reset(async_client, outbox)
    other_reset, _ = await add_link(test_db, user_id, "reset")
    other_invite, _ = await add_link(test_db, user_id, "invite")

    assert (await confirm(async_client, link, NEWER_PASSWORD)).status_code == 204

    refused = await confirm(async_client, other_reset, OLDER_PASSWORD)
    assert refused.status_code == 401
    assert refused.json()["code"] == "auth.token_used"
    refused = await async_client.post(
        "/auth/invite/accept", json={"token": other_invite, "password": OLDER_PASSWORD}
    )
    assert refused.status_code == 401
    assert refused.json()["code"] == "auth.token_used"
    assert await sign_in(async_client, NEWER_PASSWORD) == 200


async def test_a_new_request_leaves_a_different_kind_of_link_alone(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
) -> None:
    _, invite_hash = await add_link(test_db, seed_users["employee"].id, "invite")

    await request_reset(async_client, outbox)

    assert await unused(test_db, invite_hash), "a reset request cancelled an invitation link"


async def test_a_completed_reset_clears_the_lockout(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
) -> None:
    for _ in range(get_settings().LOGIN_MAX_FAILURES):
        assert await sign_in(async_client, "wrong-password") == 401
    assert await sign_in(async_client, PASSWORD) == 423
    link = await request_reset(async_client, outbox)

    assert (await confirm(async_client, link, NEWER_PASSWORD)).status_code == 204

    await test_db.rollback()
    row = (
        await test_db.execute(
            text("SELECT failed_login_attempts, locked_until FROM users WHERE email = :e"),
            {"e": EMAIL},
        )
    ).one()
    assert tuple(row) == (0, None)
    assert await sign_in(async_client, NEWER_PASSWORD) == 200
