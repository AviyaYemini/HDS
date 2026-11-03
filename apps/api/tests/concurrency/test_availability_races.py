"""Two windows or two clashing entries made at once leave exactly one (T042, T045).

Covers: 003 FR-104, FR-106; research R5, R6.
"""

from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from httpx import AsyncClient

from hds.models.user import User
from tests.roster_support import FIRST, WINDOWS, body, constraints, headers_for, open_window

ROUNDS = 20


def outcome(result: Any) -> str:
    """A status code, or the exception a request raised instead of answering."""
    status = getattr(result, "status_code", None)
    return str(status) if status is not None else repr(result)[:300]


async def test_two_overlapping_windows_at_once_leave_one(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    race: Callable[..., Awaitable[list[Any]]],
) -> None:
    manager = await headers_for(async_client, "manager")
    admin = await headers_for(async_client, "admin")

    for n in range(ROUNDS):
        first = FIRST + timedelta(days=30 * (n + 1))
        payload = {
            "starts_on": first.isoformat(),
            "ends_on": (first + timedelta(days=6)).isoformat(),
            "deadline": datetime.now(UTC).isoformat(),
        }

        async def create(headers: dict[str, str]) -> Any:
            return await async_client.post(WINDOWS, json=payload, headers=headers)

        results = await race(lambda: create(manager), lambda: create(admin))

        assert sorted(outcome(r) for r in results) == ["201", "409"], f"round {n}: {results}"


async def test_two_clashing_entries_at_once_leave_one(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    race: Callable[..., Awaitable[list[Any]]],
) -> None:
    manager = await headers_for(async_client, "manager")
    await open_window(async_client, manager, FIRST, FIRST + timedelta(days=ROUNDS))
    employee = await headers_for(async_client, "employee")
    url = constraints(seed_users["employee"].id)

    for n in range(ROUNDS):
        day = FIRST + timedelta(days=n)

        async def add(kind: str) -> Any:
            return await async_client.post(url, json=body(kind, day), headers=employee)

        results = await race(lambda: add("blocked"), lambda: add("open"))

        assert sorted(outcome(r) for r in results) == ["201", "409"], f"round {n}: {results}"


async def test_opening_a_window_and_adding_an_entry_at_once_never_deadlock(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    race: Callable[..., Awaitable[list[Any]]],
) -> None:
    """The window's organisation lock must not wait on the foreign-key checks of an entry."""
    manager = await headers_for(async_client, "manager")
    await open_window(async_client, manager, FIRST, FIRST + timedelta(days=ROUNDS))
    url = constraints(seed_users["manager"].id)

    for n in range(ROUNDS):
        later = FIRST + timedelta(days=60 + 10 * n)
        payload = {
            "starts_on": later.isoformat(),
            "ends_on": (later + timedelta(days=6)).isoformat(),
            "deadline": datetime.now(UTC).isoformat(),
        }

        async def window() -> Any:
            return await async_client.post(WINDOWS, json=payload, headers=manager)

        async def entry() -> Any:
            return await async_client.post(
                url, json=body("blocked", FIRST + timedelta(days=n)), headers=manager
            )

        results = await race(window, entry)

        assert sorted(outcome(r) for r in results) == ["201", "201"], f"round {n}: {results}"
