"""Sign-in locks after exactly N wrong passwords, and the count restarts when the lock ends (T274).

Run on PostgreSQL through the real application for every limit from 2 to 10. A user who fails N-1
times can still sign in; one who fails N times is refused as locked even with the right password;
and once the lock has expired the user again has the full N attempts (SC-206). The limit is changed
per test through the settings object the application reads.

Covers: FR-212, FR-213, SC-206.
"""

import pytest
from httpx import AsyncClient, Response
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.config import get_settings
from hds.models.user import User

EMAIL = "employee@hds.test"
PASSWORD = "Dev1234!"
LIMITS = list(range(2, 11))


@pytest.fixture
def limit(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> int:
    n: int = request.param
    monkeypatch.setattr(get_settings(), "LOGIN_MAX_FAILURES", n)
    return n


async def attempt(client: AsyncClient, password: str) -> Response:
    return await client.post("/auth/login", json={"email": EMAIL, "password": password})


async def state(db: AsyncSession) -> tuple[int, bool]:
    """``(consecutive failures, locked)`` as stored."""
    await db.rollback()
    row = (
        await db.execute(
            text(
                "SELECT failed_login_attempts, locked_until IS NOT NULL FROM users WHERE email = :e"
            ),
            {"e": EMAIL},
        )
    ).one()
    return int(row[0]), bool(row[1])


async def expire_the_lock(db: AsyncSession) -> None:
    await db.execute(
        text("UPDATE users SET locked_until = now() - interval '1 minute' WHERE email = :e"),
        {"e": EMAIL},
    )
    await db.commit()


@pytest.mark.parametrize("limit", LIMITS, indirect=True)
async def test_one_failure_fewer_than_the_limit_still_allows_sign_in(
    limit: int, async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    for _ in range(limit - 1):
        assert (await attempt(async_client, "wrong")).status_code == 401

    assert await state(test_db) == (limit - 1, False)
    assert (await attempt(async_client, PASSWORD)).status_code == 200
    assert await state(test_db) == (0, False)  # a success clears the count


@pytest.mark.parametrize("limit", LIMITS, indirect=True)
async def test_the_limit_locks_the_account_even_for_the_right_password(
    limit: int, async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    for _ in range(limit):
        assert (await attempt(async_client, "wrong")).status_code == 401  # the answer is unchanged

    assert await state(test_db) == (limit, True)
    locked = await attempt(async_client, PASSWORD)
    assert locked.status_code == 423
    assert locked.json()["code"] == "auth.account_locked"


@pytest.mark.parametrize("limit", LIMITS, indirect=True)
async def test_after_the_lock_expires_the_user_has_the_full_limit_again(
    limit: int, async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    for _ in range(limit):
        await attempt(async_client, "wrong")
    await expire_the_lock(test_db)

    first = await attempt(async_client, "wrong")  # one wrong password after the cooldown

    assert first.status_code == 401
    assert await state(test_db) == (1, limit == 1)  # the count restarted at zero, then one
    for _ in range(limit - 2):  # up to limit-1 failures in all: still not locked
        assert (await attempt(async_client, "wrong")).status_code == 401
    assert await state(test_db) == (limit - 1, False)
    assert (await attempt(async_client, "wrong")).status_code == 401  # the N-th new failure
    assert await state(test_db) == (limit, True)
    assert (await attempt(async_client, PASSWORD)).status_code == 423


@pytest.mark.parametrize("limit", [3], indirect=True)
async def test_a_good_sign_in_after_expiry_leaves_no_lock_or_count(
    limit: int, async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    for _ in range(limit):
        await attempt(async_client, "wrong")
    await expire_the_lock(test_db)

    assert (await attempt(async_client, PASSWORD)).status_code == 200
    assert await state(test_db) == (0, False)
