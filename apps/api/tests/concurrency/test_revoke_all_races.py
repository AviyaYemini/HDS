"""Nothing issued before or during "end every session" stays usable (T327, F09).

A refresh inserts its new token in an uncommitted transaction, so a password reset, sign-out
everywhere, theft response or account change running at that moment cannot see the row, and the new
token would stay live for days. A sign-in that verified the old password has the same problem. Both
now serialise on the user's row. The refresh tests widen the window with a short pause after the
insert, so the old behaviour fails every time rather than now and then. The stale-read tests are
deterministic: the competing change happens exactly between the check and the lock.

Covers: FR-230.
"""

import asyncio
import hashlib
import uuid
from collections.abc import Awaitable, Callable, Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.security import generate_refresh_token
from hds.models.user import User
from hds.repositories.refresh_token import RefreshTokenRepository
from hds.repositories.user import UserRepository

ROUNDS = 50
PAUSE = 0.02  # seconds between the new token's insert and its commit
EMAIL = "employee@hds.test"
PASSWORD = "Dev1234!"
Race = Callable[..., Awaitable[list[Any]]]


@pytest.fixture
def slow_insert(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Hold each new refresh token uncommitted for a moment, where the old code was exposed."""
    real = RefreshTokenRepository.create

    async def create(self: RefreshTokenRepository, *args: Any, **kwargs: Any) -> Any:
        token = await real(self, *args, **kwargs)
        await asyncio.sleep(PAUSE)
        return token

    monkeypatch.setattr(RefreshTokenRepository, "create", create)
    yield


async def add_token(
    db: AsyncSession,
    user_id: uuid.UUID,
    *,
    revoked: bool = False,
) -> str:
    """Store a refresh token: live, or rotated an hour ago. Returns the raw value."""
    raw, token_hash = generate_refresh_token()
    await db.execute(
        text(
            "INSERT INTO refresh_tokens "
            "(id, user_id, token_hash, expires_at, revoked_at, revoked_reason) "
            "VALUES (:i, :u, :h, :x, :r, :why)"
        ),
        {
            "i": uuid.uuid4(),
            "u": user_id,
            "h": token_hash,
            "x": datetime.now(UTC) + timedelta(days=7),
            "r": datetime.now(UTC) - timedelta(hours=1) if revoked else None,
            "why": "rotated" if revoked else None,
        },
    )
    await db.commit()
    return raw


async def add_reset_link(db: AsyncSession, user_id: uuid.UUID) -> str:
    raw, token_hash = generate_refresh_token()
    await db.execute(
        text(
            "INSERT INTO invitation_tokens (id, user_id, token_hash, purpose, expires_at) "
            "VALUES (:i, :u, :h, 'reset', :x)"
        ),
        {
            "i": uuid.uuid4(),
            "u": user_id,
            "h": token_hash,
            "x": datetime.now(UTC) + timedelta(hours=1),
        },
    )
    await db.commit()
    return raw


async def live_tokens(db: AsyncSession, user_id: uuid.UUID) -> int:
    await db.rollback()
    count = await db.scalar(
        text("SELECT count(*) FROM refresh_tokens WHERE user_id = :u AND revoked_at IS NULL"),
        {"u": user_id},
    )
    await db.commit()
    return int(count or 0)


async def access_headers(client: AsyncClient, email: str, password: str = PASSWORD) -> Any:
    response = await client.post("/auth/login", json={"email": email, "password": password})
    assert response.status_code == 200, response.text
    return {"Authorization": f"Bearer {response.json()['access_token']}"}


async def clear_tokens(db: AsyncSession, user_id: uuid.UUID) -> None:
    await db.execute(text("DELETE FROM refresh_tokens WHERE user_id = :u"), {"u": user_id})
    await db.commit()


async def run_rounds(
    other: Callable[[int], Awaitable[Callable[[], Awaitable[Any]]]],
    client: AsyncClient,
    db: AsyncSession,
    user_id: uuid.UUID,
    race: Race,
) -> None:
    """Race a refresh against ``other`` each round; afterwards no token of the user may be live."""
    for n in range(ROUNDS):
        await clear_tokens(db, user_id)
        raw = await add_token(db, user_id)
        action = await other(n)

        async def refresh() -> Any:
            return await client.post("/auth/refresh", json={"refresh_token": raw})

        results = await race(refresh, action)

        assert not any(isinstance(r, BaseException) for r in results), results
        assert await live_tokens(db, user_id) == 0, f"round {n}: a session survived"


async def test_a_refresh_cannot_outlive_a_password_reset(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    race: Race,
    slow_insert: None,
) -> None:
    user_id = seed_users["employee"].id

    async def other(n: int) -> Callable[[], Awaitable[Any]]:
        link = await add_reset_link(test_db, user_id)

        async def reset() -> Any:
            return await async_client.post(
                "/auth/password-reset/confirm",
                json={"token": link, "new_password": f"A-long-password-{n}"},
            )

        return reset

    await run_rounds(other, async_client, test_db, user_id, race)


async def test_a_refresh_cannot_outlive_sign_out_everywhere(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    race: Race,
    slow_insert: None,
) -> None:
    user_id = seed_users["employee"].id
    headers = await access_headers(async_client, EMAIL)

    async def other(n: int) -> Callable[[], Awaitable[Any]]:
        async def everywhere() -> Any:
            return await async_client.post("/auth/logout-all", headers=headers)

        return everywhere

    await run_rounds(other, async_client, test_db, user_id, race)


async def test_a_refresh_cannot_outlive_the_theft_response(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    race: Race,
    slow_insert: None,
) -> None:
    user_id = seed_users["employee"].id

    async def other(n: int) -> Callable[[], Awaitable[Any]]:
        stolen = await add_token(test_db, user_id, revoked=True)

        async def replay() -> Any:
            await asyncio.sleep(PAUSE / 2)  # land inside the refresh's window
            return await async_client.post("/auth/refresh", json={"refresh_token": stolen})

        return replay

    await run_rounds(other, async_client, test_db, user_id, race)


async def test_a_refresh_cannot_outlive_an_account_change(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    race: Race,
    slow_insert: None,
) -> None:
    user_id = seed_users["employee"].id
    admin = await access_headers(async_client, "admin@hds.test")

    async def other(n: int) -> Callable[[], Awaitable[Any]]:
        role = "manager" if n % 2 == 0 else "employee"

        async def change() -> Any:
            return await async_client.patch(
                f"/api/v1/employees/{user_id}", json={"role": role}, headers=admin
            )

        return change

    await run_rounds(other, async_client, test_db, user_id, race)


async def test_a_sign_in_with_the_old_password_racing_a_reset_leaves_no_session(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    race: Race,
    slow_insert: None,
) -> None:
    user_id = seed_users["employee"].id
    password = PASSWORD
    for n in range(ROUNDS // 5):
        await clear_tokens(test_db, user_id)
        link = await add_reset_link(test_db, user_id)
        new_password = f"A-long-password-{n}"

        async def sign_in(old: str = password) -> Any:
            return await async_client.post("/auth/login", json={"email": EMAIL, "password": old})

        async def reset(new: str = new_password, token: str = link) -> Any:
            return await async_client.post(
                "/auth/password-reset/confirm", json={"token": token, "new_password": new}
            )

        await race(sign_in, reset)

        assert await live_tokens(test_db, user_id) == 0, f"round {n}: a session survived"
        password = new_password


async def test_a_sign_in_is_refused_when_a_reset_finishes_after_its_password_check(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Deterministic: the reset completes exactly between the password check and the lock."""
    user_id = seed_users["employee"].id
    link = await add_reset_link(test_db, user_id)
    real = UserRepository.lock_user
    fired = {"done": False}

    async def lock_user(self: UserRepository, target: uuid.UUID) -> None:
        if not fired["done"]:
            fired["done"] = True
            reset = await async_client.post(
                "/auth/password-reset/confirm",
                json={"token": link, "new_password": "A-long-password-1"},
            )
            assert reset.status_code == 204, reset.text
        await real(self, target)

    monkeypatch.setattr(UserRepository, "lock_user", lock_user)

    refused = await async_client.post("/auth/login", json={"email": EMAIL, "password": PASSWORD})

    assert refused.status_code == 401, refused.text
    assert refused.json()["code"] == "auth.invalid_credentials"
    assert await live_tokens(test_db, user_id) == 0


async def test_a_refresh_reads_the_account_after_taking_the_lock(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Deterministic: the account is deactivated while the refresh waits for the lock."""
    user_id = seed_users["employee"].id
    raw = await add_token(test_db, user_id)
    real = UserRepository.lock_user

    async def lock_user(self: UserRepository, target: uuid.UUID) -> None:
        await test_db.execute(
            text("UPDATE users SET is_active = false WHERE id = :u"), {"u": target}
        )
        await test_db.commit()
        await real(self, target)

    monkeypatch.setattr(UserRepository, "lock_user", lock_user)

    refused = await async_client.post("/auth/refresh", json={"refresh_token": raw})

    assert refused.status_code == 401, refused.text
    assert refused.json()["code"] == "auth.invalid_token"
    row = await test_db.scalar(
        text("SELECT revoked_at IS NULL FROM refresh_tokens WHERE token_hash = :h"),
        {"h": hashlib.sha256(raw.encode()).hexdigest()},
    )
    assert row is True  # refused before the token was spent
