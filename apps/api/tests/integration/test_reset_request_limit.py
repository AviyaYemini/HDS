"""Reset requests are limited per address, so nobody can fill someone's mailbox (T417).

The request needs no proof of anything, so without a limit anyone could fill someone's mailbox. Over
the limit the answer stays 202, because any other answer would say the address was being asked
about, and no job is queued. A request cancels no link, so nothing is lost by asking.

Covers: FR-247, FR-221, FR-224.
"""

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from httpx import AsyncClient, Response
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.config import get_settings
from hds.models.user import User
from hds.repositories.login_throttle import RESET
from hds.repositories.user import UserRepository
from hds.services.throttle import hash_key
from tests.conftest import OutboxMailSender

KNOWN = "employee@hds.test"
UNKNOWN = "nobody@hds.test"
LIMIT = 5
VOLATILE = {"date", "x-request-id"}


@pytest.fixture(autouse=True)
def limit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(get_settings(), "RESET_REQUEST_LIMIT_PER_ADDRESS", LIMIT)


async def ask(client: AsyncClient, email: str) -> Response:
    return await client.post("/auth/password-reset/request", json={"email": email})


def shape(response: Response) -> dict[str, Any]:
    headers = {k: v for k, v in response.headers.items() if k.lower() not in VOLATILE}
    return {"status": response.status_code, "body": response.content, "headers": headers}


def mails(outbox: OutboxMailSender, to: str) -> int:
    return sum(1 for address, _, _ in outbox.messages if address == to)


def key(value: str) -> str:
    return hash_key(value, get_settings().throttle_key)


async def count(db: AsyncSession, value: str) -> int | None:
    await db.rollback()
    result = await db.execute(
        text("SELECT failures FROM login_throttle WHERE kind = :k AND key_hash = :h"),
        {"k": RESET, "h": key(value)},
    )
    return result.scalar_one_or_none()


async def add_row(
    db: AsyncSession, name: str, updated: timedelta, locked: timedelta | None = None
) -> None:
    now = datetime.now(UTC)
    await db.execute(
        text(
            "INSERT INTO login_throttle "
            "(kind, key_hash, failures, window_started_at, locked_until, updated_at) "
            "VALUES ('reset', :h, 6, :u, :l, :u)"
        ),
        {"h": key(name), "u": now + updated, "l": now + locked if locked is not None else None},
    )
    await db.commit()


async def row_exists(db: AsyncSession, name: str) -> bool:
    return await count(db, name) is not None


async def confirm(client: AsyncClient, token: str) -> Response:
    return await client.post(
        "/auth/password-reset/confirm",
        json={"token": token, "new_password": "Newer-password-1"},
    )


async def test_the_fifth_of_five_still_sends_and_the_sixth_does_not(
    async_client: AsyncClient, seed_users: dict[str, User], outbox: OutboxMailSender
) -> None:
    for sent in range(1, LIMIT + 1):
        assert (await ask(async_client, KNOWN)).status_code == 202
        assert mails(outbox, KNOWN) == sent

    for _ in range(3):
        assert (await ask(async_client, KNOWN)).status_code == 202
        assert mails(outbox, KNOWN) == LIMIT


async def test_requests_never_take_away_a_link_already_mailed(
    async_client: AsyncClient, seed_users: dict[str, User], outbox: OutboxMailSender
) -> None:
    await ask(async_client, KNOWN)
    first = outbox.last_token_for(KNOWN)

    for _ in range(LIMIT + 3):
        await ask(async_client, KNOWN)

    assert (await confirm(async_client, first)).status_code == 204


async def test_over_the_limit_no_job_is_queued(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real = UserRepository.get_by_email
    lookups: list[str] = []

    async def spy(self: UserRepository, email: str) -> Any:
        lookups.append(email)
        return await real(self, email)

    monkeypatch.setattr(UserRepository, "get_by_email", spy)

    for _ in range(LIMIT + 3):
        assert (await ask(async_client, UNKNOWN)).status_code == 202

    assert lookups == [UNKNOWN] * LIMIT


async def test_a_registered_and_an_unregistered_address_answer_alike_at_every_step(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
) -> None:
    for step in range(1, LIMIT + 3):
        known = await ask(async_client, KNOWN)
        unknown = await ask(async_client, UNKNOWN)

        assert shape(known) == shape(unknown), f"step {step}"
        assert await count(test_db, KNOWN) == await count(test_db, UNKNOWN) == step


async def test_any_letter_case_counts_as_one_address(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
) -> None:
    await ask(async_client, KNOWN)
    await ask(async_client, KNOWN.upper())
    await ask(async_client, "Employee@HDS.test")

    assert await count(test_db, KNOWN) == 3


async def test_the_count_restarts_after_the_window(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
) -> None:
    for _ in range(LIMIT + 1):
        await ask(async_client, KNOWN)
    assert mails(outbox, KNOWN) == LIMIT

    await test_db.execute(
        text(
            "UPDATE login_throttle SET window_started_at = window_started_at - interval '61 min', "
            "locked_until = locked_until - interval '61 min' WHERE kind = :k AND key_hash = :h"
        ),
        {"k": RESET, "h": key(KNOWN)},
    )
    await test_db.commit()

    await ask(async_client, KNOWN)

    assert mails(outbox, KNOWN) == LIMIT + 1
    assert await count(test_db, KNOWN) == 1


async def test_a_limit_of_zero_turns_it_off(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(get_settings(), "RESET_REQUEST_LIMIT_PER_ADDRESS", 0)

    for _ in range(LIMIT + 2):
        await ask(async_client, KNOWN)

    assert mails(outbox, KNOWN) == LIMIT + 2
    assert await count(test_db, KNOWN) is None


async def test_no_address_appears_in_the_table(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
) -> None:
    await ask(async_client, KNOWN)
    await ask(async_client, UNKNOWN)

    await test_db.rollback()
    rows = await test_db.execute(text("SELECT kind, key_hash FROM login_throttle"))
    stored = rows.all()
    assert {kind for kind, _ in stored} == {RESET}
    assert {value for _, value in stored} == {key(KNOWN), key(UNKNOWN)}


async def test_each_request_removes_stale_rows_and_never_a_live_lock(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
) -> None:
    await add_row(test_db, "stale", updated=-timedelta(days=2))
    await add_row(test_db, "ran-out", updated=-timedelta(days=2), locked=-timedelta(days=2))
    await add_row(test_db, "live", updated=-timedelta(days=2), locked=timedelta(minutes=5))
    await add_row(test_db, "recent", updated=-timedelta(hours=1))

    await ask(async_client, UNKNOWN)

    assert not await row_exists(test_db, "stale")
    assert not await row_exists(test_db, "ran-out")
    assert await row_exists(test_db, "live")
    assert await row_exists(test_db, "recent")


async def test_requests_for_random_addresses_do_not_grow_the_table_without_bound(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
) -> None:
    for number in range(120):
        await add_row(test_db, f"old{number}", updated=-timedelta(days=2))

    for _ in range(2):
        await ask(async_client, f"{uuid.uuid4()}@hds.test")

    await test_db.rollback()
    left = await test_db.execute(text("SELECT count(*) FROM login_throttle"))
    assert left.scalar_one() == 120 - 2 * 50 + 2
