"""The reset request limit holds when requests arrive together (T417 follow-up).

The shared test configuration turns the limit off, so the other race tests never run the count that
production runs by default. Here it is on: parallel requests for one address cannot send more mail
than the limit, and the count does not deadlock with the paths that hold the same user's row.

Covers: FR-246, FR-247.
"""

from collections.abc import Awaitable, Callable
from typing import Any

import pytest
from httpx import AsyncClient

from hds.core.config import get_settings
from hds.models.user import User
from tests.conftest import OutboxMailSender

EMAIL = "employee@hds.test"
Race = Callable[..., Awaitable[list[Any]]]


def statuses(results: list[Any]) -> list[Any]:
    return [getattr(r, "status_code", repr(r)[:200]) for r in results]


async def test_parallel_requests_send_no_more_mail_than_the_limit(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
    race: Race,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    limit = 3
    monkeypatch.setattr(get_settings(), "RESET_REQUEST_LIMIT_PER_ADDRESS", limit)

    async def request() -> Any:
        return await async_client.post("/auth/password-reset/request", json={"email": EMAIL})

    results = await race(*[request for _ in range(10)])

    assert statuses(results) == [202] * 10
    assert sum(1 for to, _, _ in outbox.messages if to == EMAIL) == limit


async def test_a_counted_request_never_deadlocks_with_a_failed_sign_in(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
    race: Race,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = get_settings()
    monkeypatch.setattr(settings, "RESET_REQUEST_LIMIT_PER_ADDRESS", 100_000)
    monkeypatch.setattr(settings, "LOGIN_RATE_LIMIT_PER_NETWORK", 100_000)
    monkeypatch.setattr(settings, "LOGIN_MAX_FAILURES", 100_000)

    async def request() -> Any:
        return await async_client.post("/auth/password-reset/request", json={"email": EMAIL})

    async def sign_in() -> Any:
        return await async_client.post(
            "/auth/login",
            json={"email": EMAIL, "password": "not-it"},
            headers={"X-Forwarded-For": "203.0.113.9"},
        )

    for round_number in range(30):
        outcomes = statuses(await race(request, sign_in))
        assert outcomes == [202, 401], f"round {round_number}"
