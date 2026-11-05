"""Staffing needs changed at once stay whole and consistent (004 T012).

Covers: 004 FR-001, FR-002.
"""

from collections.abc import Awaitable, Callable
from typing import Any

from httpx import AsyncClient

from hds.models.user import User
from tests.roster_support import add_project, headers_for

ROUNDS = 20


def outcome(result: Any) -> str:
    status = getattr(result, "status_code", None)
    return str(status) if status is not None else repr(result)[:300]


async def test_two_replacements_at_once_leave_exactly_one_complete_set(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    race: Callable[..., Awaitable[list[Any]]],
) -> None:
    manager = await headers_for(async_client, "manager")
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, manager)
    url = f"/api/v1/projects/{project['id']}/requirements"
    types = project["types"]
    nights = [{"shift_type_id": types["night"], "weekday": d, "headcount": 2} for d in range(7)]
    mornings = [{"shift_type_id": types["morning"], "weekday": d, "headcount": 1} for d in range(5)]

    for n in range(ROUNDS):
        results = await race(
            lambda: async_client.put(url, json={"items": nights}, headers=manager),
            lambda: async_client.put(url, json={"items": mornings}, headers=admin),
        )

        assert sorted(outcome(r) for r in results) == ["200", "200"], f"round {n}: {results}"
        stored = (await async_client.get(url, headers=manager)).json()["items"]
        kinds = {i["shift_type_id"] for i in stored}
        assert kinds in ({types["night"]}, {types["morning"]}), f"round {n}: {stored}"
        assert len(stored) == (7 if kinds == {types["night"]} else 5), f"round {n}: {stored}"


async def test_a_need_never_outlives_its_type_leaving_the_project(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    race: Callable[..., Awaitable[list[Any]]],
) -> None:
    manager = await headers_for(async_client, "manager")
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, manager)
    types = project["types"]
    needs_url = f"/api/v1/projects/{project['id']}/requirements"
    types_url = f"/api/v1/projects/{project['id']}/shift-types"
    night = [{"shift_type_id": types["night"], "weekday": 0, "headcount": 1}]

    for n in range(ROUNDS):
        reset = await async_client.put(types_url, json={"ids": list(types.values())}, headers=admin)
        assert reset.status_code == 200, reset.text

        results = await race(
            lambda: async_client.put(needs_url, json={"items": night}, headers=manager),
            lambda: async_client.put(types_url, json={"ids": [types["morning"]]}, headers=admin),
        )

        assert all(outcome(r) in ("200", "422") for r in results), f"round {n}: {results}"
        stored = (await async_client.get(needs_url, headers=manager)).json()["items"]
        used = {
            t["id"] for t in (await async_client.get(types_url, headers=manager)).json()["items"]
        }
        assert {i["shift_type_id"] for i in stored} <= used, f"round {n}: {stored}, {used}"


async def test_a_need_never_outlives_its_type_being_retired(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    race: Callable[..., Awaitable[list[Any]]],
) -> None:
    manager = await headers_for(async_client, "manager")
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, manager)
    types = project["types"]
    needs_url = f"/api/v1/projects/{project['id']}/requirements"
    type_url = f"/api/v1/shift-types/{types['night']}"
    night = [{"shift_type_id": types["night"], "weekday": 0, "headcount": 1}]

    for n in range(ROUNDS):
        await async_client.patch(type_url, json={"is_active": True}, headers=admin)
        await async_client.put(
            f"/api/v1/projects/{project['id']}/shift-types",
            json={"ids": list(types.values())},
            headers=admin,
        )

        results = await race(
            lambda: async_client.put(needs_url, json={"items": night}, headers=manager),
            lambda: async_client.patch(type_url, json={"is_active": False}, headers=admin),
        )

        assert all(outcome(r) in ("200", "422") for r in results), f"round {n}: {results}"
        stored = (await async_client.get(needs_url, headers=manager)).json()["items"]
        assert stored == [], f"round {n}: a need on a retired type: {stored}"


async def test_retiring_a_type_with_a_week_of_needs_never_deadlocks_a_replacement(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    race: Callable[..., Awaitable[list[Any]]],
) -> None:
    """Review finding: the two deletes locked shared rows in opposite orders and could deadlock."""
    manager = await headers_for(async_client, "manager")
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, manager)
    types = project["types"]
    needs_url = f"/api/v1/projects/{project['id']}/requirements"
    type_url = f"/api/v1/shift-types/{types['night']}"
    week = [
        {"shift_type_id": types["night"], "weekday": d, "headcount": 1}
        for d in (6, 2, 4, 0, 5, 1, 3)
    ]
    mornings = [{"shift_type_id": types["morning"], "weekday": 0, "headcount": 1}]

    for n in range(ROUNDS):
        await async_client.patch(type_url, json={"is_active": True}, headers=admin)
        await async_client.put(
            f"/api/v1/projects/{project['id']}/shift-types",
            json={"ids": list(types.values())},
            headers=admin,
        )
        seeded = await async_client.put(needs_url, json={"items": week}, headers=admin)
        assert seeded.status_code == 200, seeded.text

        results = await race(
            lambda: async_client.put(needs_url, json={"items": mornings}, headers=manager),
            lambda: async_client.patch(type_url, json={"is_active": False}, headers=admin),
        )

        assert sorted(outcome(r) for r in results) == ["200", "200"], f"round {n}: {results}"
