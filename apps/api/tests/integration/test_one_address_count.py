"""One failure count for every address, kept in one place under one rule (T429).

A registered address used to be counted on its account row, which never expired, and an unknown one
in the throttle table, whose rows go after a day. A caller who failed, waited a day and failed again
could tell them apart. Both now live in the throttle table and restart after a day without a
failure.

Covers: FR-221, FR-232.
"""

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from httpx import AsyncClient, Response
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.config import get_settings
from hds.models.user import User
from hds.repositories.user import UserRepository
from hds.services.throttle import SignInThrottle
from tests.address_count import address_key, address_state, set_address_state
from tests.conftest import OutboxMailSender

KNOWN = "employee@hds.test"
UNKNOWN = "nobody@hds.test"
PASSWORD = "Dev1234!"
LIMIT = 3
VOLATILE = {"date", "x-request-id"}


@pytest.fixture(autouse=True)
def limits(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(get_settings(), "LOGIN_MAX_FAILURES", LIMIT)


async def attempt(client: AsyncClient, email: str, password: str = "not-it") -> Response:
    return await client.post("/auth/login", json={"email": email, "password": password})


def shape(response: Response) -> dict[str, Any]:
    body = response.json()
    body.pop("request_id", None)
    body.pop("detail", None)
    headers = {k: v for k, v in response.headers.items() if k.lower() not in VOLATILE}
    return {"status": response.status_code, "body": body, "headers": headers}


async def age_the_count(db: AsyncSession, email: str, hours: int) -> None:
    await db.execute(
        text(
            "UPDATE login_throttle SET updated_at = now() - make_interval(hours => :h) "
            "WHERE kind = 'address' AND key_hash = :k"
        ),
        {"h": hours, "k": address_key(email)},
    )
    await db.commit()


async def account_row(db: AsyncSession, email: str) -> dict[str, Any]:
    """Every column of the account row, as stored."""
    await db.rollback()  # read what is committed, not what this session saw earlier
    row = await db.execute(text("SELECT * FROM users WHERE email = :e"), {"e": email})
    return dict(row.mappings().one())


async def test_a_registered_address_is_counted_in_the_throttle_table_only(
    async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    before = await account_row(test_db, KNOWN)

    await attempt(async_client, KNOWN)
    await attempt(async_client, KNOWN)

    assert await address_state(test_db, KNOWN) == (2, False)
    # Failed sign-ins write nothing to the account row; migration 019 dropped its old count and
    # lock columns, so the throttle table is the only place a count can live.
    assert await account_row(test_db, KNOWN) == before


@pytest.mark.parametrize("email", [KNOWN, UNKNOWN], ids=["registered", "unknown"])
async def test_the_count_restarts_after_a_day_without_a_failure(
    email: str, async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    for _ in range(LIMIT - 1):
        await attempt(async_client, email)
    await age_the_count(test_db, email, 25)

    answer = await attempt(async_client, email)

    assert answer.status_code == 401
    assert await address_state(test_db, email) == (1, False)


async def test_a_count_from_less_than_a_day_ago_still_adds_up(
    async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    for _ in range(LIMIT - 1):
        await attempt(async_client, KNOWN)
    await age_the_count(test_db, KNOWN, 23)

    assert (await attempt(async_client, KNOWN)).status_code == 401
    assert (await attempt(async_client, KNOWN)).status_code == 423


async def test_waiting_a_day_tells_nothing_apart(
    async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    """The probe from the report: fail, wait longer than the retention, fail again."""
    for email in (KNOWN, UNKNOWN):
        for _ in range(LIMIT - 1):
            await attempt(async_client, email)
        await age_the_count(test_db, email, 25)

    for step in range(LIMIT + 1):
        known = await attempt(async_client, KNOWN)
        unknown = await attempt(async_client, UNKNOWN)
        assert shape(known) == shape(unknown), f"step {step}"


async def test_a_success_clears_the_count(
    async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    await attempt(async_client, KNOWN)

    assert (await attempt(async_client, KNOWN, PASSWORD)).status_code == 200
    assert await address_state(test_db, KNOWN) == (0, False)


async def test_a_completed_reset_clears_the_count_and_the_lock(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
) -> None:
    for _ in range(LIMIT):
        await attempt(async_client, KNOWN)
    assert (await attempt(async_client, KNOWN, PASSWORD)).status_code == 423
    await async_client.post("/auth/password-reset/request", json={"email": KNOWN})
    link = outbox.last_token_for(KNOWN)

    confirmed = await async_client.post(
        "/auth/password-reset/confirm",
        json={"token": link, "new_password": "A-long-password-1"},
    )

    assert confirmed.status_code == 204
    assert await address_state(test_db, KNOWN) == (0, False)
    assert (await attempt(async_client, KNOWN, "A-long-password-1")).status_code == 200


async def test_a_lock_set_while_the_password_was_checked_is_seen_in_the_one_store(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real = UserRepository.lock_user

    async def lock_user(self: UserRepository, target: Any) -> None:
        await set_address_state(test_db, KNOWN, LIMIT, datetime.now(UTC) + timedelta(minutes=15))
        await real(self, target)

    monkeypatch.setattr(UserRepository, "lock_user", lock_user)

    refused = await attempt(async_client, KNOWN, PASSWORD)

    assert refused.status_code == 423, refused.text


async def test_a_lock_set_by_a_guess_during_a_success_is_not_erased(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A guess can lock the address between the success's re-check and its clean-up."""
    real = SignInThrottle.address_locked_until
    calls = {"n": 0}

    async def locked_until(self: SignInThrottle, address: str, now: datetime) -> Any:
        calls["n"] += 1
        answer = await real(self, address, now)
        if calls["n"] == 2:  # the re-check after the user lock has just passed
            await set_address_state(
                test_db, KNOWN, LIMIT, datetime.now(UTC) + timedelta(minutes=15)
            )
        return answer

    monkeypatch.setattr(SignInThrottle, "address_locked_until", locked_until)

    answer = await attempt(async_client, KNOWN, PASSWORD)

    assert answer.status_code == 423, answer.text
    assert await address_state(test_db, KNOWN) == (LIMIT, True)
