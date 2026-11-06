"""Ending a project at the same moment as a change to its pay settings, days or managers (005 T012).

Ending a project freezes its settings and copies the organisation's special days into it. A change
racing it must land wholly before the freeze, or be refused (the project's own settings and days) or
leave the frozen copy alone (the organisation's), never half of each.

Covers: 005 FR-006a, FR-007, FR-008a, FR-021.
"""

from collections.abc import Awaitable, Callable
from typing import Any

from httpx import AsyncClient

from hds.models.user import User
from tests.pay_support import ORG_PAY, SPECIAL_DAYS, pay_settings, project_days, project_managers
from tests.roster_support import add_project, headers_for

ROUNDS = 10


def outcome(result: Any) -> str:
    status = getattr(result, "status_code", None)
    return str(status) if status is not None else repr(result)[:300]


async def ending(client: AsyncClient, headers: dict[str, str], project_id: str) -> Any:
    return await client.patch(
        f"/api/v1/projects/{project_id}", json={"status": "ended"}, headers=headers
    )


async def test_an_override_racing_the_end_is_frozen_or_refused(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    race: Callable[..., Awaitable[list[Any]]],
) -> None:
    admin = await headers_for(async_client, "admin")
    manager = await headers_for(async_client, "manager")

    for n in range(ROUNDS):
        project = await add_project(async_client, admin, f"Race {n}")
        url = f"/api/v1/projects/{project['id']}/pay-settings"

        ended, changed = await race(
            lambda: ending(async_client, manager, project["id"]),
            lambda: async_client.patch(
                url, json={"settings": {"overtime1_pct": "140.00"}}, headers=admin
            ),
        )

        assert outcome(ended) == "200", f"round {n}: {ended}"
        assert outcome(changed) in ("200", "409"), f"round {n}: {changed}"
        frozen = await pay_settings(async_client, admin, project["id"])
        expected = "140.00" if outcome(changed) == "200" else "125.00"
        assert frozen["settings"]["overtime1_pct"] == expected, f"round {n}: {frozen}"
        assert len(frozen["own"]) == 9, f"round {n}: {frozen}"


async def test_a_default_change_racing_the_end_is_frozen_whole(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    race: Callable[..., Awaitable[list[Any]]],
) -> None:
    admin = await headers_for(async_client, "admin")

    for n in range(ROUNDS):
        await async_client.patch(
            ORG_PAY, json={"overtime1_pct": "125.00", "overtime2_pct": "150.00"}, headers=admin
        )
        project = await add_project(async_client, admin, f"Race {n}")

        results = await race(
            lambda: ending(async_client, admin, project["id"]),
            lambda: async_client.patch(
                ORG_PAY, json={"overtime1_pct": "130.00", "overtime2_pct": "160.00"}, headers=admin
            ),
        )

        assert [outcome(r) for r in results] == ["200", "200"], f"round {n}: {results}"
        frozen = (await pay_settings(async_client, admin, project["id"]))["settings"]
        pair = (frozen["overtime1_pct"], frozen["overtime2_pct"])
        assert pair in (("125.00", "150.00"), ("130.00", "160.00")), f"round {n}: {pair}"
        await async_client.patch(ORG_PAY, json={"overtime1_pct": "199.00"}, headers=admin)
        later = (await pay_settings(async_client, admin, project["id"]))["settings"]
        assert later["overtime1_pct"] == frozen["overtime1_pct"], f"round {n}: {later}"


async def test_a_day_changed_while_the_project_ends_is_never_lost(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    race: Callable[..., Awaitable[list[Any]]],
) -> None:
    admin = await headers_for(async_client, "admin")

    for n in range(ROUNDS):
        day = await async_client.post(
            SPECIAL_DAYS, json={"day": f"2027-09-{n + 1:02d}", "name": "Holiday"}, headers=admin
        )
        assert day.status_code == 201, day.text
        project = await add_project(async_client, admin, f"Race {n}")
        own_url = project_days(project["id"])
        own = await async_client.post(
            own_url, json={"day": f"2027-08-{n + 1:02d}", "name": "Local"}, headers=admin
        )

        results = await race(
            lambda: ending(async_client, admin, project["id"]),
            lambda: async_client.patch(
                f"{SPECIAL_DAYS}/{day.json()['id']}", json={"rate_pct": "200.00"}, headers=admin
            ),
            lambda: async_client.patch(
                f"{SPECIAL_DAYS}/{own.json()['id']}", json={"rate_pct": "250.00"}, headers=admin
            ),
        )

        assert outcome(results[0]) == "200", (
            f"round {n}: {[getattr(r, 'text', r) for r in results]}"
        )
        assert outcome(results[1]) == "200", f"round {n}: {results}"
        assert outcome(results[2]) in ("200", "409"), f"round {n}: {results}"
        days = (await async_client.get(own_url, headers=admin)).json()["items"]
        rates = {d["day"]: d["rate_pct"] for d in days}
        assert rates.get(f"2027-09-{n + 1:02d}") in ("150.00", "200.00"), f"round {n}: {days}"
        local = "250.00" if outcome(results[2]) == "200" else "150.00"
        assert rates.get(f"2027-08-{n + 1:02d}") == local, f"round {n}: {days}"
        assert {d["source"] for d in days} == {"project"}, f"round {n}: {days}"


async def test_two_replacements_of_the_managers_leave_one_set(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    race: Callable[..., Awaitable[list[Any]]],
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin)
    url = project_managers(project["id"])
    one = [str(seed_users["manager"].id)]
    other = [str(seed_users["admin"].id)]

    for n in range(ROUNDS * 2):
        results = await race(
            lambda: async_client.put(url, json={"user_ids": one}, headers=admin),
            lambda: async_client.put(url, json={"user_ids": other}, headers=admin),
        )

        assert [outcome(r) for r in results] == ["200", "200"], f"round {n}: {results}"
        stored = [m["id"] for m in (await async_client.get(url, headers=admin)).json()["items"]]
        assert stored in (one, other), f"round {n}: {stored}"


async def test_ending_with_other_changes_racing_another_end_never_deadlocks(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    race: Callable[..., Awaitable[list[Any]]],
) -> None:
    """A rename sent with the end locks the organisation first, as every ender does (review)."""
    admin = await headers_for(async_client, "admin")

    for n in range(ROUNDS):
        project = await add_project(async_client, admin, f"Race {n}")
        url = f"/api/v1/projects/{project['id']}"

        results = await race(
            lambda: async_client.patch(
                url, json={"name": f"Renamed {n}", "status": "ended"}, headers=admin
            ),
            lambda: async_client.patch(url, json={"status": "ended"}, headers=admin),
        )

        assert [outcome(r) for r in results] == ["200", "200"], f"round {n}: {results}"
