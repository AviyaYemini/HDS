"""Records past their expiry are removed, and nothing younger is (T322, F08).

Cleanup runs only after a successful sign-in or refresh: a failed sign-in writes nothing but its
failure count. A spent or cancelled link inside its lifetime is kept, because cancelling marks a
link used and its answer must stay ``auth.token_used``.

Covers: FR-209, FR-224.
"""

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.security import generate_refresh_token
from hds.models.user import User
from tests.conftest import OutboxMailSender

EMAIL = "employee@hds.test"
PASSWORD = "Dev1234!"
NOW = datetime.now(UTC)


async def add_token(db: AsyncSession, user_id: uuid.UUID, expires: datetime) -> str:
    raw, token_hash = generate_refresh_token()
    await db.execute(
        text(
            "INSERT INTO refresh_tokens (id, user_id, token_hash, expires_at) "
            "VALUES (:i, :u, :h, :x)"
        ),
        {"i": uuid.uuid4(), "u": user_id, "h": token_hash, "x": expires},
    )
    await db.commit()
    return token_hash


async def add_link(
    db: AsyncSession, user_id: uuid.UUID, expires: datetime, *, used: bool = False
) -> tuple[str, str]:
    raw, token_hash = generate_refresh_token()
    await db.execute(
        text(
            "INSERT INTO invitation_tokens (id, user_id, token_hash, purpose, expires_at, used_at) "
            "VALUES (:i, :u, :h, 'reset', :x, :at)"
        ),
        {
            "i": uuid.uuid4(),
            "u": user_id,
            "h": token_hash,
            "x": expires,
            "at": NOW if used else None,
        },
    )
    await db.commit()
    return raw, token_hash


async def exists(db: AsyncSession, table: str, token_hash: str) -> bool:
    await db.rollback()
    count = await db.scalar(
        text(f"SELECT count(*) FROM {table} WHERE token_hash = :h"),  # noqa: S608
        {"h": token_hash},
    )
    return bool(count)


async def sign_in(client: AsyncClient, password: str = PASSWORD) -> int:
    response = await client.post("/auth/login", json={"email": EMAIL, "password": password})
    return response.status_code


@pytest.mark.parametrize("how", ["sign_in", "refresh"])
async def test_a_successful_sign_in_or_refresh_removes_expired_records(
    how: str, async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    user_id = seed_users["employee"].id
    other_user = seed_users["manager"].id
    session = await async_client.post("/auth/login", json={"email": EMAIL, "password": PASSWORD})
    old_token = await add_token(test_db, user_id, NOW - timedelta(days=1))
    other_token = await add_token(test_db, other_user, NOW - timedelta(days=1))
    _, other_link = await add_link(test_db, other_user, NOW - timedelta(hours=1))
    live_token = await add_token(test_db, user_id, NOW + timedelta(days=1))
    _, old_link = await add_link(test_db, user_id, NOW - timedelta(hours=1))
    _, live_link = await add_link(test_db, user_id, NOW + timedelta(hours=1))

    if how == "sign_in":
        assert await sign_in(async_client) == 200
    else:
        renewed = await async_client.post(
            "/auth/refresh", json={"refresh_token": session.json()["refresh_token"]}
        )
        assert renewed.status_code == 200, renewed.text

    assert not await exists(test_db, "refresh_tokens", old_token)
    assert not await exists(test_db, "invitation_tokens", old_link)
    assert await exists(test_db, "refresh_tokens", live_token)
    assert await exists(test_db, "invitation_tokens", live_link)
    # Only the caller's own records go.
    assert await exists(test_db, "refresh_tokens", other_token)
    assert await exists(test_db, "invitation_tokens", other_link)


async def test_a_failed_sign_in_removes_nothing(
    async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    user_id = seed_users["employee"].id
    old_token = await add_token(test_db, user_id, NOW - timedelta(days=1))
    _, old_link = await add_link(test_db, user_id, NOW - timedelta(hours=1))

    assert await sign_in(async_client, "wrong-password") == 401

    assert await exists(test_db, "refresh_tokens", old_token)
    assert await exists(test_db, "invitation_tokens", old_link)


async def test_a_spent_link_inside_its_lifetime_is_kept_and_answers_used(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
) -> None:
    user_id = seed_users["employee"].id
    raw, token_hash = await add_link(test_db, user_id, NOW + timedelta(hours=1), used=True)

    assert await sign_in(async_client) == 200

    assert await exists(test_db, "invitation_tokens", token_hash)
    answer = await async_client.post(
        "/auth/password-reset/confirm", json={"token": raw, "new_password": "A-long-password-1"}
    )
    assert answer.status_code == 401
    assert answer.json()["code"] == "auth.token_used"
