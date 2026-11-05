"""The organisation's minimum rest and weekly maximum (004 T011).

Covers: 004 FR-026, FR-027; US7.
"""

from typing import Any

import pytest
from httpx import AsyncClient

from hds.models.user import User
from tests.roster_support import FIRST, add_project, add_shift, assign, codes, headers_for

SETTINGS = "/api/v1/organisation/scheduling"


async def test_the_defaults_are_eleven_and_forty_eight_hours(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    answer = await async_client.get(SETTINGS, headers=await headers_for(async_client, "manager"))
    assert answer.status_code == 200
    assert answer.json() == {"min_rest_hours": 11, "max_weekly_hours": 48}


async def test_an_admin_changes_the_limits_and_manual_warnings_follow(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin)
    first = (await add_shift(async_client, admin, project, FIRST, "morning"))["shift"]
    later = (await add_shift(async_client, admin, project, FIRST, "night"))["shift"]
    employee = seed_users["employee"].id
    await assign(async_client, admin, first["id"], employee)

    clean = await assign(async_client, admin, later["id"], employee)  # 8 hours of rest
    assert "warnings.rest_too_short" in codes(clean.json())
    await async_client.delete(
        f"/api/v1/assignments/{clean.json()['assignment']['id']}", headers=admin
    )

    changed = await async_client.patch(SETTINGS, json={"min_rest_hours": 8}, headers=admin)
    assert changed.status_code == 200, changed.text
    assert changed.json() == {"min_rest_hours": 8, "max_weekly_hours": 48}

    again = await assign(async_client, admin, later["id"], employee)
    assert "warnings.rest_too_short" not in codes(again.json())


async def test_a_manager_reads_but_cannot_change_the_limits(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    manager = await headers_for(async_client, "manager")

    answer = await async_client.patch(SETTINGS, json={"max_weekly_hours": 40}, headers=manager)

    assert answer.status_code == 403
    assert (await async_client.get(SETTINGS, headers=manager)).json()["max_weekly_hours"] == 48


@pytest.mark.parametrize(
    "bad",
    [
        {"min_rest_hours": 0},
        {"min_rest_hours": 25},
        {"max_weekly_hours": 0},
        {"max_weekly_hours": 169},
        {"min_rest_hours": "eleven"},
        {"min_rest_hours": "11"},  # a string is not an integer, even when it reads as one
        {"max_weekly_hours": True},  # PR 6 CI finding: lax ints read true as 1
        {"max_weekly_hours": 40.5},
        {"time_limit": 30},
    ],
)
async def test_out_of_range_or_unknown_values_are_refused(
    async_client: AsyncClient, seed_users: dict[str, User], bad: dict[str, Any]
) -> None:
    admin = await headers_for(async_client, "admin")

    answer = await async_client.patch(SETTINGS, json=bad, headers=admin)

    assert answer.status_code == 422
    assert answer.json()["code"] == "validation.request_invalid"
    assert (await async_client.get(SETTINGS, headers=admin)).json() == {
        "min_rest_hours": 11,
        "max_weekly_hours": 48,
    }


@pytest.mark.parametrize("role", ["employee", "client"])
async def test_employees_and_clients_cannot_read_or_change_the_limits(
    async_client: AsyncClient, seed_users: dict[str, User], role: str
) -> None:
    headers = await headers_for(async_client, role)

    assert (await async_client.get(SETTINGS, headers=headers)).status_code == 403
    assert (
        await async_client.patch(SETTINGS, json={"min_rest_hours": 9}, headers=headers)
    ).status_code == 403


async def test_the_bounds_are_inclusive(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    for body in (
        {"min_rest_hours": 1, "max_weekly_hours": 1},
        {"min_rest_hours": 24, "max_weekly_hours": 168},
    ):
        answer = await async_client.patch(SETTINGS, json=body, headers=admin)
        assert answer.status_code == 200, answer.text
        assert answer.json() == body


async def test_a_whole_number_sent_as_a_float_is_accepted(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    """JSON's 40.0 is the integer 40, as the published schema says."""
    admin = await headers_for(async_client, "admin")

    answer = await async_client.patch(SETTINGS, json={"max_weekly_hours": 40.0}, headers=admin)

    assert answer.status_code == 200, answer.text
    assert answer.json()["max_weekly_hours"] == 40
