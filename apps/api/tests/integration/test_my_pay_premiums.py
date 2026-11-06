"""Premiums through the API: worked examples, project settings and special days (005 T019).

Rate 50 and the defaults; the site is in Israel, UTC+3 until 2026-10-25. 2026-10-05 is a Monday and
2026-10-09 a Friday.

Covers: 005 FR-006a, FR-007, FR-008a, FR-009 to FR-016; US3 acceptance scenarios 1 to 9; SC-003.
"""

from typing import Any

import pytest
from httpx import AsyncClient

from hds.models.user import User
from tests.integration.test_my_pay import MY_PAY, published_shift, set_rate
from tests.pay_support import ORG_PAY, SPECIAL_DAYS, end_project, project_days, project_pay
from tests.roster_support import add_project, headers_for


async def october(client: AsyncClient) -> Any:
    answer = await client.get(
        MY_PAY, params={"month": "2026-10"}, headers=await headers_for(client, "employee")
    )
    assert answer.status_code == 200, answer.text
    return answer.json()


@pytest.mark.parametrize(
    ("starts_at", "ends_at", "pay"),
    [
        ("2026-10-05T03:00:00Z", "2026-10-05T13:00:00Z", "525.00"),  # 10-hour weekday day
        ("2026-10-10T03:00:00Z", "2026-10-10T11:00:00Z", "600.00"),  # Saturday 06:00 to 14:00
        ("2026-10-05T19:00:00Z", "2026-10-06T03:00:00Z", "412.50"),  # weekday night
        ("2026-10-09T19:00:00Z", "2026-10-10T03:00:00Z", "612.50"),  # Friday night
    ],
    ids=["long-day", "saturday", "night", "friday-night"],
)
async def test_the_specs_worked_examples(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    starts_at: str,
    ends_at: str,
    pay: str,
) -> None:
    admin = await headers_for(async_client, "admin")
    employee = seed_users["employee"]
    await set_rate(async_client, admin, employee, "50.0000")
    project = await add_project(async_client, admin)

    await published_shift(async_client, admin, project, starts_at, ends_at, employee.id)

    assert (await october(async_client))["pay"] == pay


async def test_a_projects_own_setting_and_day_are_used(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    employee = seed_users["employee"]
    await set_rate(async_client, admin, employee, "50.0000")
    project = await add_project(async_client, admin)
    await async_client.post(
        SPECIAL_DAYS,
        json={"day": "2026-10-07", "name": "Election day", "rate_pct": "200.00"},
        headers=admin,
    )
    await async_client.post(
        project_days(project["id"]),
        json={"day": "2026-10-07", "name": "Election day", "rate_pct": "150.00"},
        headers=admin,
    )
    await async_client.patch(
        project_pay(project["id"]), json={"settings": {"day_normal_minutes": 540}}, headers=admin
    )
    # Monday 06:00 to 16:00: 9 hours normal, 1 at 125% -> 450 + 62.5.
    await published_shift(
        async_client, admin, project, "2026-10-05T03:00:00Z", "2026-10-05T13:00:00Z", employee.id
    )
    # Wednesday 06:00 to 14:00 on the project's 150% day -> 600.
    await published_shift(
        async_client, admin, project, "2026-10-07T03:00:00Z", "2026-10-07T11:00:00Z", employee.id
    )

    body = await october(async_client)

    assert [line["pay"] for line in body["lines"]] == ["512.50", "600.00"]
    assert body["pay"] == "1112.50"


async def test_a_default_change_reaches_an_active_projects_past_shifts_only(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    employee = seed_users["employee"]
    await set_rate(async_client, admin, employee, "50.0000")
    active = await add_project(async_client, admin, "Active")
    ended = await add_project(async_client, admin, "Ended")
    for project in (active, ended):
        await published_shift(
            async_client,
            admin,
            project,
            "2026-10-05T03:00:00Z" if project is active else "2026-10-06T03:00:00Z",
            "2026-10-05T13:00:00Z" if project is active else "2026-10-06T13:00:00Z",
            employee.id,
        )
    await end_project(async_client, admin, ended["id"])

    await async_client.patch(ORG_PAY, json={"overtime1_pct": "150.00"}, headers=admin)

    pays = {line["project_name"]: line["pay"] for line in (await october(async_client))["lines"]}
    assert pays == {"Active": "550.00", "Ended": "525.00"}
