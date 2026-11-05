"""An employee's gross pay earned so far in a month (005 T018).

Shifts are made in October 2026, which has ended, and in June 2027, which has not begun; which
shifts of the current month have ended is a matter of the clock, so the unit tests of ``PayService``
cover it with a fixed ``now``. The site is in Israel; 2026-10-05 is a Monday.

Covers: 005 FR-004, FR-017 to FR-020, FR-029, FR-031; US1 acceptance scenarios 1 to 7.
"""

from typing import Any

import pytest
from httpx import AsyncClient

from hds.models.user import User
from tests.pay_support import project_pay
from tests.roster_support import SHIFTS, add_project, assign, headers_for

MY_PAY = "/me/pay"


async def published_shift(
    client: AsyncClient,
    admin: dict[str, str],
    project: dict[str, Any],
    starts_at: str,
    ends_at: str,
    user_id: Any,
    **extra: Any,
) -> dict[str, Any]:
    """A published shift with one employee on it, at explicit UTC times."""
    created = await client.post(
        SHIFTS,
        json={
            "project_id": project["id"],
            "shift_type_id": project["types"]["morning"],
            "date": starts_at[:10],
            "starts_at": starts_at,
            "ends_at": ends_at,
            "headcount": 1,
            **extra,
        },
        headers=admin,
    )
    assert created.status_code == 201, created.text
    shift: dict[str, Any] = created.json()["shift"]
    assigned = await assign(client, admin, shift["id"], user_id)
    assert assigned.status_code == 201, assigned.text
    published = await client.post(
        f"/api/v1/projects/{project['id']}/publish",
        json={"from": starts_at[:10], "to": starts_at[:10]},
        headers=admin,
    )
    assert published.status_code == 200, published.text
    return shift


async def set_rate(
    client: AsyncClient, admin: dict[str, str], user: User, rate: str | None
) -> None:
    answer = await client.patch(
        f"/api/v1/employees/{user.id}", json={"default_hourly_rate": rate}, headers=admin
    )
    assert answer.status_code == 200, answer.text


async def test_an_employee_sees_each_ended_shift_and_the_total(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    employee = seed_users["employee"]
    await set_rate(async_client, admin, employee, "50.0000")
    project = await add_project(async_client, admin)
    # 08:00 to 16:00 and 07:00 to 15:00 in Israel (UTC+3 in October until the 25th).
    first = await published_shift(
        async_client, admin, project, "2026-10-05T05:00:00Z", "2026-10-05T13:00:00Z", employee.id
    )
    await published_shift(
        async_client,
        admin,
        project,
        "2026-10-06T04:00:00Z",
        "2026-10-06T12:00:00Z",
        employee.id,
        pay_rate="60.0000",
    )

    answer = await async_client.get(
        MY_PAY, params={"month": "2026-10"}, headers=await headers_for(async_client, "employee")
    )

    assert answer.status_code == 200, answer.text
    body = answer.json()
    assert body["month"] == "2026-10"
    assert [(line["date"], line["hours"], line["pay"]) for line in body["lines"]] == [
        ("2026-10-05", "8.00", "400.00"),
        ("2026-10-06", "8.00", "480.00"),
    ]
    line = body["lines"][0]
    assert line["shift_id"] == first["id"]
    assert line["project_name"] == project["name"]
    assert line["base_rate"] == "50.0000"
    assert line["parts"] == [{"rate_pct": "100.00", "hours": "8.00"}]
    assert (body["hours"], body["pay"], body["shifts_missing_rate"]) == ("16.00", "880.00", 0)


async def test_premiums_are_applied(async_client: AsyncClient, seed_users: dict[str, User]) -> None:
    admin = await headers_for(async_client, "admin")
    employee = seed_users["employee"]
    await set_rate(async_client, admin, employee, "50.0000")
    project = await add_project(async_client, admin)
    # Friday 2026-10-09 22:00 to Saturday 06:00 in Israel: 7 hours at 150% and 1 at 175%.
    await published_shift(
        async_client, admin, project, "2026-10-09T19:00:00Z", "2026-10-10T03:00:00Z", employee.id
    )

    body = (
        await async_client.get(
            MY_PAY, params={"month": "2026-10"}, headers=await headers_for(async_client, "employee")
        )
    ).json()

    assert body["lines"][0]["parts"] == [
        {"rate_pct": "150.00", "hours": "7.00"},
        {"rate_pct": "175.00", "hours": "1.00"},
    ]
    assert body["pay"] == "612.50"


async def test_draft_shifts_and_months_not_yet_worked_are_not_counted(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    employee = seed_users["employee"]
    project = await add_project(async_client, admin)
    draft = await async_client.post(
        SHIFTS,
        json={
            "project_id": project["id"],
            "shift_type_id": project["types"]["morning"],
            "date": "2026-10-07",
            "headcount": 1,
        },
        headers=admin,
    )
    await assign(async_client, admin, draft.json()["shift"]["id"], employee.id)
    await published_shift(
        async_client, admin, project, "2027-06-07T04:00:00Z", "2027-06-07T12:00:00Z", employee.id
    )
    headers = await headers_for(async_client, "employee")

    october = (await async_client.get(MY_PAY, params={"month": "2026-10"}, headers=headers)).json()
    june = (await async_client.get(MY_PAY, params={"month": "2027-06"}, headers=headers)).json()

    assert (october["lines"], october["pay"], october["hours"]) == ([], "0.00", "0.00")
    assert june["lines"] == []


async def test_a_project_without_pay_shows_hours_but_no_pay(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    employee = seed_users["employee"]
    await set_rate(async_client, admin, employee, "50.0000")
    project = await add_project(async_client, admin)
    await async_client.patch(project_pay(project["id"]), json={"pay_enabled": False}, headers=admin)
    await published_shift(
        async_client, admin, project, "2026-10-05T05:00:00Z", "2026-10-05T13:00:00Z", employee.id
    )

    body = (
        await async_client.get(
            MY_PAY, params={"month": "2026-10"}, headers=await headers_for(async_client, "employee")
        )
    ).json()

    line = body["lines"][0]
    assert line["pay_enabled"] is False
    assert "pay" not in line
    assert (line["hours"], body["hours"], body["pay"]) == ("8.00", "8.00", "0.00")


async def test_no_rate_is_missing_never_zero(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    employee = seed_users["employee"]
    await set_rate(async_client, admin, employee, None)
    project = await add_project(async_client, admin)
    await published_shift(
        async_client, admin, project, "2026-10-05T05:00:00Z", "2026-10-05T13:00:00Z", employee.id
    )

    body = (
        await async_client.get(
            MY_PAY, params={"month": "2026-10"}, headers=await headers_for(async_client, "employee")
        )
    ).json()

    line = body["lines"][0]
    assert line["rate_missing"] is True
    assert line["base_rate"] is None
    assert "pay" not in line
    assert (body["pay"], body["shifts_missing_rate"]) == ("0.00", 1)


async def test_a_shift_belongs_to_the_month_it_starts_in_at_its_site(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    employee = seed_users["employee"]
    await set_rate(async_client, admin, employee, "50.0000")
    project = await add_project(async_client, admin)
    # 2026-09-30 22:00 to 2026-10-01 06:00 in Israel: September's.
    await published_shift(
        async_client, admin, project, "2026-09-30T19:00:00Z", "2026-10-01T03:00:00Z", employee.id
    )
    headers = await headers_for(async_client, "employee")

    september = (
        await async_client.get(MY_PAY, params={"month": "2026-09"}, headers=headers)
    ).json()
    october = (await async_client.get(MY_PAY, params={"month": "2026-10"}, headers=headers)).json()

    assert [line["date"] for line in september["lines"]] == ["2026-09-30"]
    assert october["lines"] == []


async def test_each_person_sees_only_their_own_pay(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin)
    await published_shift(
        async_client,
        admin,
        project,
        "2026-10-05T05:00:00Z",
        "2026-10-05T13:00:00Z",
        seed_users["employee"].id,
    )

    for role in ("manager", "admin"):
        mine = await async_client.get(
            MY_PAY, params={"month": "2026-10"}, headers=await headers_for(async_client, role)
        )
        assert mine.status_code == 200
        assert mine.json()["lines"] == [], role


@pytest.mark.parametrize("month", ["2026-13", "2026-1", "26-10", "october", "2026-10-01"])
async def test_a_bad_month_is_refused(
    async_client: AsyncClient, seed_users: dict[str, User], month: str
) -> None:
    answer = await async_client.get(
        MY_PAY, params={"month": month}, headers=await headers_for(async_client, "employee")
    )

    assert answer.status_code == 422


@pytest.mark.parametrize("month", ["0001-01", "9999-12"])
async def test_months_at_the_ends_of_the_calendar_are_empty_not_errors(
    async_client: AsyncClient, seed_users: dict[str, User], month: str
) -> None:
    answer = await async_client.get(
        MY_PAY, params={"month": month}, headers=await headers_for(async_client, "employee")
    )

    assert answer.status_code == 200, answer.text
    assert answer.json()["lines"] == []


async def test_the_month_defaults_to_the_current_one(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    answer = await async_client.get(MY_PAY, headers=await headers_for(async_client, "employee"))

    assert answer.status_code == 200
    assert len(answer.json()["month"]) == 7


async def test_a_client_is_refused(async_client: AsyncClient, seed_users: dict[str, User]) -> None:
    answer = await async_client.get(MY_PAY, headers=await headers_for(async_client, "client"))

    assert answer.status_code == 403
