"""Every address locks the same way, and failures from one network are limited (T352).

An address with no account used to have nothing to count against, so it answered 401 for ever while
a registered one locked, and anyone could tell them apart. Now both follow one rule and give the
same answers. Separately, failures from one network address are counted in a fixed window. A success
clears the address count but never the network count, or a holder of one valid account could keep
guessing other addresses.

Covers: FR-221, SC-210.
"""

import hashlib
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from httpx import AsyncClient, Response
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core import security
from hds.core.config import get_settings
from hds.models.user import User
from hds.repositories.login_throttle import LoginThrottleRepository
from hds.repositories.user import UserRepository
from hds.services.throttle import hash_key

KNOWN = "employee@hds.test"
UNKNOWN = "nobody@hds.test"
PASSWORD = "Dev1234!"
LIMIT = 3
NETWORK_LIMIT = 6
VOLATILE = {"date", "x-request-id"}


@pytest.fixture(autouse=True)
def limits(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(get_settings(), "LOGIN_MAX_FAILURES", LIMIT)
    monkeypatch.setattr(get_settings(), "LOGIN_RATE_LIMIT_PER_NETWORK", 0)


@pytest.fixture
def network_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(get_settings(), "LOGIN_RATE_LIMIT_PER_NETWORK", NETWORK_LIMIT)
    monkeypatch.setattr(get_settings(), "LOGIN_MAX_FAILURES", 1000)


@pytest.fixture
def verifications(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[str]]:
    calls: list[str] = []

    class Counting:
        def __init__(self, real: Any) -> None:
            self._real = real

        def verify(self, hashed: str, plain: str) -> bool:
            calls.append(hashed)
            return bool(self._real.verify(hashed, plain))

        def __getattr__(self, name: str) -> Any:
            return getattr(self._real, name)

    monkeypatch.setattr(security, "_ph", Counting(security._ph))
    yield calls


async def attempt(client: AsyncClient, email: str, password: str = "not-it") -> Response:
    return await client.post("/auth/login", json={"email": email, "password": password})


def shape(response: Response) -> dict[str, Any]:
    body = response.json()
    body.pop("request_id", None)
    body.pop("detail", None)  # compared separately: a lock names its own end time
    headers = {k: v for k, v in response.headers.items() if k.lower() not in VOLATILE}
    return {"status": response.status_code, "body": body, "headers": headers}


def key(value: str) -> str:
    return hash_key(value, get_settings().throttle_key)


async def row(db: AsyncSession, kind: str, value: str) -> Any:
    await db.rollback()
    result = await db.execute(
        text(
            "SELECT failures, locked_until, updated_at FROM login_throttle "
            "WHERE kind = :k AND key_hash = :h"
        ),
        {"k": kind, "h": key(value)},
    )
    return result.one_or_none()


async def test_a_registered_and_an_unregistered_address_answer_alike_at_every_step(
    async_client: AsyncClient, seed_users: dict[str, User], verifications: list[str]
) -> None:
    for step in range(LIMIT + 2):
        before = len(verifications)
        known = await attempt(async_client, KNOWN)
        known_work = len(verifications) - before
        before = len(verifications)
        unknown = await attempt(async_client, UNKNOWN.upper())
        unknown_work = len(verifications) - before

        assert shape(known) == shape(unknown), f"step {step}"
        assert known_work == unknown_work, f"step {step}"
        expected = 401 if step < LIMIT else 423
        assert known.status_code == expected, f"step {step}"
        if expected == 423:
            assert unknown.json()["code"] == "auth.account_locked"
            assert "locked until" in unknown.json()["detail"]
            assert known_work == 0


async def test_while_locked_the_right_password_is_refused_and_not_verified(
    async_client: AsyncClient, seed_users: dict[str, User], verifications: list[str]
) -> None:
    for _ in range(LIMIT):
        await attempt(async_client, KNOWN)
    before = len(verifications)

    locked = await attempt(async_client, KNOWN, PASSWORD)

    assert locked.status_code == 423
    assert len(verifications) == before


async def test_a_lock_set_during_the_password_check_refuses_the_right_password(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Guesses overlap, so a correct one can pass the first check before the lock is set."""
    real = UserRepository.lock_user

    async def lock_user(self: UserRepository, target: uuid.UUID) -> None:
        await test_db.execute(
            text("UPDATE users SET locked_until = :t WHERE id = :u"),
            {"t": datetime.now(UTC) + timedelta(minutes=15), "u": target},
        )
        await test_db.commit()
        await real(self, target)

    monkeypatch.setattr(UserRepository, "lock_user", lock_user)

    refused = await attempt(async_client, KNOWN, PASSWORD)

    assert refused.status_code == 423, refused.text
    assert refused.json()["code"] == "auth.account_locked"
    await test_db.rollback()
    sessions = await test_db.scalar(text("SELECT count(*) FROM refresh_tokens"))
    assert sessions == 0


async def test_an_expired_lock_restarts_the_count_for_an_unregistered_address(
    async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    for _ in range(LIMIT):
        await attempt(async_client, UNKNOWN)
    assert (await attempt(async_client, UNKNOWN)).status_code == 423
    await test_db.execute(
        text("UPDATE login_throttle SET locked_until = now() - interval '1 minute'")
    )
    await test_db.commit()

    assert (await attempt(async_client, UNKNOWN)).status_code == 401
    stored = await row(test_db, "address", UNKNOWN)
    assert (stored.failures, stored.locked_until) == (1, None)
    assert (await attempt(async_client, UNKNOWN)).status_code == 401
    assert (await attempt(async_client, UNKNOWN)).status_code == 401
    assert (await attempt(async_client, UNKNOWN)).status_code == 423


async def test_nothing_is_written_before_the_password_check_after_a_cooldown(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
) -> None:
    """A write first would make ``release_connection`` refuse, and the sign-in would be a 500."""
    await test_db.execute(
        text(
            "UPDATE users SET failed_login_attempts = :n, "
            "locked_until = now() - interval '1 minute' WHERE email = :e"
        ),
        {"n": LIMIT, "e": KNOWN},
    )
    await test_db.commit()

    answer = await attempt(async_client, KNOWN, PASSWORD)

    assert answer.status_code == 200, answer.text


async def test_a_success_clears_the_address_count(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    for _ in range(LIMIT - 1):
        assert (await attempt(async_client, KNOWN)).status_code == 401
    assert (await attempt(async_client, KNOWN, PASSWORD)).status_code == 200

    for _ in range(LIMIT - 1):
        assert (await attempt(async_client, KNOWN)).status_code == 401
    assert (await attempt(async_client, KNOWN, PASSWORD)).status_code == 200


async def test_the_network_limit_refuses_every_address_before_any_lookup(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    network_limit: None,
    verifications: list[str],
) -> None:
    for n in range(NETWORK_LIMIT):
        assert (await attempt(async_client, f"guess{n}@hds.test")).status_code == 401
    before = len(verifications)

    for email in (KNOWN, UNKNOWN):
        refused = await attempt(async_client, email, PASSWORD)
        assert refused.status_code == 429, refused.text
        assert refused.json()["code"] == "auth.rate_limited"
        assert int(refused.headers["Retry-After"]) >= 1
    assert len(verifications) == before


async def test_failures_spread_across_two_windows_do_not_add_up(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    network_limit: None,
) -> None:
    for n in range(NETWORK_LIMIT - 1):
        await attempt(async_client, f"guess{n}@hds.test")
    await test_db.execute(
        text("UPDATE login_throttle SET window_started_at = now() - interval '1 day'")
    )
    await test_db.commit()

    for n in range(NETWORK_LIMIT - 1):
        answer = await attempt(async_client, f"late{n}@hds.test")
        assert answer.status_code == 401, f"failure {n} of the second window"
    stored = await row(test_db, "network", "127.0.0.1")
    assert stored.failures == NETWORK_LIMIT - 1  # only the second window's failures count
    assert stored.locked_until is None


async def test_a_success_does_not_clear_the_network_count(
    async_client: AsyncClient, seed_users: dict[str, User], network_limit: None
) -> None:
    for n in range(NETWORK_LIMIT - 1):
        await attempt(async_client, f"guess{n}@hds.test")
    assert (await attempt(async_client, KNOWN, PASSWORD)).status_code == 200

    assert (await attempt(async_client, UNKNOWN)).status_code == 401  # reaches the limit
    assert (await attempt(async_client, UNKNOWN)).status_code == 429


async def test_a_limit_of_zero_turns_the_network_limit_off(
    async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    for n in range(LIMIT * 3):
        assert (await attempt(async_client, f"guess{n}@hds.test")).status_code == 401
    await test_db.rollback()
    count = await test_db.scalar(text("SELECT count(*) FROM login_throttle WHERE kind = 'network'"))
    assert count == 0


async def test_only_hashes_are_stored(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    network_limit: None,
) -> None:
    await attempt(async_client, UNKNOWN)
    await test_db.rollback()
    rows = (await test_db.execute(text("SELECT kind, key_hash FROM login_throttle"))).all()
    assert {r.kind for r in rows} == {"address", "network"}
    assert all(len(r.key_hash) == 64 and "@" not in r.key_hash for r in rows)
    plain = hashlib.sha256(UNKNOWN.encode()).hexdigest()
    assert plain not in {r.key_hash for r in rows}  # a copy of the table alone cannot confirm it
    assert (await row(test_db, "address", UNKNOWN)) is not None


async def add_row(
    db: AsyncSession,
    value: str,
    *,
    updated: timedelta,
    locked: timedelta | None = None,
    kind: str = "address",
) -> None:
    now = datetime.now(UTC)
    await db.execute(
        text(
            "INSERT INTO login_throttle (kind, key_hash, failures, locked_until, updated_at) "
            "VALUES (:k, :h, 1, :l, :u)"
        ),
        {
            "k": kind,
            "h": key(value),
            "l": None if locked is None else now + locked,
            "u": now + updated,
        },
    )
    await db.commit()


async def test_each_failure_removes_stale_unlocked_rows_and_never_a_locked_one(
    async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    await add_row(test_db, "stale", updated=-timedelta(days=2))
    await add_row(
        test_db,
        "lock-ran-out",
        updated=-timedelta(days=2),
        locked=-timedelta(days=1),
        kind="network",
    )
    await add_row(test_db, "still-locked", updated=-timedelta(days=2), locked=timedelta(hours=1))
    await add_row(test_db, "recent", updated=-timedelta(hours=1))

    await attempt(async_client, UNKNOWN)

    assert await row(test_db, "address", "stale") is None
    assert await row(test_db, "network", "lock-ran-out") is None
    assert await row(test_db, "address", "still-locked") is not None
    assert await row(test_db, "address", "recent") is not None


async def test_a_batch_clean_removes_at_most_its_limit(
    test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    for n in range(3):
        await add_row(test_db, f"stale{n}", updated=-timedelta(days=2))
    now = datetime.now(UTC)

    await LoginThrottleRepository(test_db).purge_stale(now - timedelta(hours=24), now, limit=2)
    await test_db.commit()

    left = await test_db.scalar(text("SELECT count(*) FROM login_throttle"))
    assert left == 1


async def test_a_recent_failure_keeps_an_old_row_alive(
    async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    """The upsert must move ``updated_at``; ``onupdate`` does not apply to ON CONFLICT DO UPDATE."""
    await add_row(test_db, UNKNOWN, updated=-timedelta(days=3))

    await attempt(async_client, UNKNOWN)  # a second failure on the old row

    stored = await row(test_db, "address", UNKNOWN)
    assert stored.failures == 2
    assert datetime.now(UTC) - stored.updated_at < timedelta(minutes=1)

    await attempt(async_client, "someone.else@hds.test")  # runs the batch clean
    assert await row(test_db, "address", UNKNOWN) is not None
