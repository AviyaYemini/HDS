"""Reset requests are limited per network, so one client cannot queue work for every address (T425).

The per-address limit does nothing against one client trying many addresses, and each request queues
a job that uses a database connection. Over the limit the answer stays 202 and nothing is queued.
One warning is logged, because behind a proxy with a wrong proxy count every user shares one address
and this would silently stop reset mail for everyone.

Covers: FR-247.
"""

import logging
from typing import Any

import pytest
from httpx import AsyncClient, Response
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.config import get_settings
from hds.models.user import User
from hds.repositories.login_throttle import RESET
from hds.services.invitation import RESET_NETWORK_PREFIX
from hds.services.throttle import hash_key
from tests.conftest import OutboxMailSender

KNOWN = "employee@hds.test"
LIMIT = 3
PEER = "127.0.0.1"
OTHER = "198.51.100.7"
WARNING = "reset request network limit reached"


@pytest.fixture(autouse=True)
def limit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(get_settings(), "RESET_REQUEST_LIMIT_PER_NETWORK", LIMIT)


async def ask(client: AsyncClient, email: str, **headers: str) -> Response:
    return await client.post(
        "/auth/password-reset/request", json={"email": email}, headers=headers or None
    )


def mails(outbox: OutboxMailSender) -> int:
    return len(outbox.messages)


def key(network: str) -> str:
    return hash_key(f"{RESET_NETWORK_PREFIX}{network}", get_settings().throttle_key)


async def count(db: AsyncSession, network: str) -> Any:
    await db.rollback()
    result = await db.execute(
        text("SELECT failures FROM login_throttle WHERE kind = :k AND key_hash = :h"),
        {"k": RESET, "h": key(network)},
    )
    return result.scalar_one_or_none()


async def test_the_last_within_the_limit_sends_and_the_next_does_not(
    async_client: AsyncClient, seed_users: dict[str, User], outbox: OutboxMailSender
) -> None:
    for sent in range(1, LIMIT + 1):
        assert (await ask(async_client, KNOWN)).status_code == 202
        assert mails(outbox) == sent

    for _ in range(3):
        assert (await ask(async_client, KNOWN)).status_code == 202
        assert mails(outbox) == LIMIT


async def test_it_counts_every_address_from_one_network_together(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
) -> None:
    for number in range(LIMIT):
        await ask(async_client, f"nobody{number}@hds.test")
    assert await count(test_db, PEER) == LIMIT

    await ask(async_client, KNOWN)

    assert mails(outbox) == 0


async def test_another_network_is_not_affected(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(get_settings(), "TRUSTED_PROXY_COUNT", 1)
    for _ in range(LIMIT + 1):
        await ask(async_client, "nobody@hds.test", **{"X-Forwarded-For": "203.0.113.9"})

    await ask(async_client, KNOWN, **{"X-Forwarded-For": OTHER})

    assert mails(outbox) == 1


async def test_the_key_has_its_prefix_so_it_cannot_meet_an_address_count(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
) -> None:
    await ask(async_client, KNOWN)

    await test_db.rollback()
    rows = await test_db.execute(text("SELECT key_hash FROM login_throttle WHERE kind = 'reset'"))
    assert [row[0] for row in rows.all()] == [key(PEER)]


async def test_one_warning_per_window_with_no_address_in_it(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.WARNING, logger="hds.invitation"):
        for _ in range(LIMIT):
            await ask(async_client, KNOWN)
        assert caplog.records == []

        for _ in range(4):
            await ask(async_client, KNOWN)

    messages = [r.getMessage() for r in caplog.records]
    assert len(messages) == 1
    assert WARNING in messages[0]
    assert "TRUSTED_PROXY_COUNT" in messages[0]
    assert KNOWN not in messages[0]
    assert PEER not in messages[0]


async def test_a_limit_of_zero_turns_it_off(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(get_settings(), "RESET_REQUEST_LIMIT_PER_NETWORK", 0)

    for _ in range(LIMIT + 2):
        await ask(async_client, KNOWN)

    assert mails(outbox) == LIMIT + 2
    assert await count(test_db, PEER) is None


async def test_the_count_restarts_after_the_window(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
) -> None:
    for _ in range(LIMIT + 1):
        await ask(async_client, KNOWN)
    await test_db.execute(
        text(
            "UPDATE login_throttle SET window_started_at = window_started_at - interval '61 min', "
            "locked_until = locked_until - interval '61 min'"
        )
    )
    await test_db.commit()

    await ask(async_client, KNOWN)

    assert mails(outbox) == LIMIT + 1
    assert await count(test_db, PEER) == 1


async def test_over_the_limit_no_address_is_counted_so_its_allowance_is_not_used_up(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(get_settings(), "RESET_REQUEST_LIMIT_PER_ADDRESS", 5)
    for number in range(LIMIT + 1):
        await ask(async_client, f"nobody{number}@hds.test")

    for _ in range(6):
        await ask(async_client, KNOWN)

    crossing = f"nobody{LIMIT}@hds.test"  # the request that went over is not counted either
    hashes = [hash_key(value, get_settings().throttle_key) for value in (KNOWN, crossing)]
    await test_db.rollback()
    rows = await test_db.execute(
        text("SELECT count(*) FROM login_throttle WHERE key_hash = ANY(:h)"), {"h": hashes}
    )
    assert rows.scalar_one() == 0


async def test_over_the_limit_a_request_only_reads_so_a_flood_takes_no_lock(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
) -> None:
    for _ in range(LIMIT + 1):
        await ask(async_client, KNOWN)
    assert await count(test_db, PEER) == LIMIT + 1

    for _ in range(5):
        await ask(async_client, KNOWN)

    assert await count(test_db, PEER) == LIMIT + 1
