"""Shift type changes made at once stay consistent (003 Group H review).

Covers: 003 FR-110, FR-111.
"""

from collections.abc import Awaitable, Callable
from typing import Any

from httpx import AsyncClient

from hds.models.user import User
from tests.integration.test_shift_types import TYPES, add_project, types_by_name
from tests.roster_support import headers_for

ROUNDS = 20


def outcome(result: Any) -> str:
    status = getattr(result, "status_code", None)
    return str(status) if status is not None else repr(result)[:300]


async def test_two_time_changes_at_once_keep_crossing_midnight_true_to_the_times(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    race: Callable[..., Awaitable[list[Any]]],
) -> None:
    manager = await headers_for(async_client, "manager")
    admin = await headers_for(async_client, "admin")
    url = f"{TYPES}/{(await types_by_name(async_client, manager))['morning']['id']}"

    for n in range(ROUNDS):
        reset = await async_client.patch(
            url, json={"starts_at_local": "07:00", "ends_at_local": "15:00"}, headers=admin
        )
        assert reset.status_code == 200, reset.text

        async def end_early() -> Any:
            return await async_client.patch(url, json={"ends_at_local": "06:00"}, headers=manager)

        async def start_early() -> Any:
            return await async_client.patch(url, json={"starts_at_local": "05:00"}, headers=admin)

        results = await race(end_early, start_early)

        assert sorted(outcome(r) for r in results) == ["200", "200"], f"round {n}: {results}"
        stored = (await types_by_name(async_client, manager))["morning"]
        crosses = stored["ends_at_local"] <= stored["starts_at_local"]
        assert stored["crosses_midnight"] is crosses, f"round {n}: {stored}"


async def test_two_replacements_of_one_projects_set_at_once_both_succeed(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    race: Callable[..., Awaitable[list[Any]]],
) -> None:
    manager = await headers_for(async_client, "manager")
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, manager)
    night = (await types_by_name(async_client, manager))["night"]["id"]
    url = f"/api/v1/projects/{project['id']}/shift-types"

    for n in range(ROUNDS):

        async def put(headers: dict[str, str]) -> Any:
            return await async_client.put(url, json={"ids": [night]}, headers=headers)

        results = await race(lambda: put(manager), lambda: put(admin))

        assert sorted(outcome(r) for r in results) == ["200", "200"], f"round {n}: {results}"
