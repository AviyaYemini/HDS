"""Assignments answer with every rule they break and are never refused for one (T052).

Covers: 003 FR-115 to FR-117, SC-103; US6.
"""

from datetime import date, timedelta
from typing import Any

import pytest
from httpx import AsyncClient

from hds.models.user import User
from tests.roster_support import (
    SHIFTS,
    add_project,
    add_shift,
    assign,
    body,
    codes,
    constraints,
    headers_for,
)

MONDAY = date(2027, 6, 7)


@pytest.fixture
async def admin(async_client: AsyncClient, seed_users: dict[str, User]) -> dict[str, str]:
    return await headers_for(async_client, "admin")


@pytest.fixture
async def project(async_client: AsyncClient, admin: dict[str, str]) -> dict[str, Any]:
    return await add_project(async_client, admin)


@pytest.fixture
def ann(seed_users: dict[str, User]) -> Any:
    return seed_users["employee"].id


async def test_a_clean_assignment_has_no_warning(
    async_client: AsyncClient, admin: dict[str, str], project: dict[str, Any], ann: Any
) -> None:
    shift = (await add_shift(async_client, admin, project, MONDAY))["shift"]

    created = await assign(async_client, admin, shift["id"], ann)

    assert created.status_code == 201, created.text
    assert created.json()["assignment"]["user_id"] == str(ann)
    assert created.json()["warnings"] == []


async def test_too_little_rest_is_reported_and_the_assignment_is_kept(
    async_client: AsyncClient, admin: dict[str, str], project: dict[str, Any], ann: Any
) -> None:
    night = (await add_shift(async_client, admin, project, MONDAY, "night"))["shift"]
    morning = (await add_shift(async_client, admin, project, MONDAY + timedelta(days=1)))["shift"]
    await assign(async_client, admin, night["id"], ann)

    created = await assign(async_client, admin, morning["id"], ann)

    assert created.status_code == 201, created.text
    [warning] = created.json()["warnings"]
    assert warning["code"] == "warnings.rest_too_short"
    assert warning["employee_id"] == str(ann)
    assert set(warning["shift_ids"]) == {night["id"], morning["id"]}
    assert warning["detail"]


async def test_overlap_is_found_across_projects_and_in_drafts(
    async_client: AsyncClient, admin: dict[str, str], project: dict[str, Any], ann: Any
) -> None:
    other = await add_project(async_client, admin, "Day Watch")
    here = (await add_shift(async_client, admin, project, MONDAY))["shift"]
    there = (await add_shift(async_client, admin, other, MONDAY))["shift"]
    await assign(async_client, admin, there["id"], ann)

    assert codes((await assign(async_client, admin, here["id"], ann)).json()) == [
        "warnings.overlap"
    ]


async def test_too_many_weekly_hours_are_reported(
    async_client: AsyncClient, admin: dict[str, str], project: dict[str, Any], ann: Any
) -> None:
    # Sunday 6 to Saturday 12 June 2027: seven 8-hour mornings, 56 hours against 48.
    for day in range(6, 13):
        shift = (await add_shift(async_client, admin, project, date(2027, 6, day)))["shift"]
        answer = (await assign(async_client, admin, shift["id"], ann)).json()

    assert codes(answer) == ["warnings.weekly_hours_exceeded"]


@pytest.mark.parametrize(
    ("entry", "code"),
    [
        (body("blocked", MONDAY), "warnings.blocked"),
        (body("absence", MONDAY, absence_kind="sick"), "warnings.absence"),
        (body("close", MONDAY), "warnings.close_preference"),
    ],
    ids=["blocked", "absence", "close"],
)
async def test_availability_entries_are_reported(
    entry: dict[str, Any],
    code: str,
    async_client: AsyncClient,
    admin: dict[str, str],
    project: dict[str, Any],
    ann: Any,
) -> None:
    stored = await async_client.post(constraints(ann), json=entry, headers=admin)
    assert stored.status_code == 201, stored.text
    shift = (await add_shift(async_client, admin, project, MONDAY))["shift"]

    assert codes((await assign(async_client, admin, shift["id"], ann)).json()) == [code]


async def test_an_open_preference_is_not_a_warning(
    async_client: AsyncClient, admin: dict[str, str], project: dict[str, Any], ann: Any
) -> None:
    await async_client.post(constraints(ann), json=body("open", MONDAY), headers=admin)
    shift = (await add_shift(async_client, admin, project, MONDAY))["shift"]

    assert (await assign(async_client, admin, shift["id"], ann)).json()["warnings"] == []


async def test_more_people_than_the_headcount_is_reported(
    async_client: AsyncClient,
    admin: dict[str, str],
    project: dict[str, Any],
    seed_users: dict[str, User],
) -> None:
    shift = (await add_shift(async_client, admin, project, MONDAY))["shift"]
    await assign(async_client, admin, shift["id"], seed_users["employee"].id)

    over = await assign(async_client, admin, shift["id"], seed_users["manager"].id)

    assert over.status_code == 201
    assert codes(over.json()) == ["warnings.over_headcount"]


async def test_removing_an_assignment_reports_the_shortfall(
    async_client: AsyncClient, admin: dict[str, str], project: dict[str, Any], ann: Any
) -> None:
    shift = (await add_shift(async_client, admin, project, MONDAY))["shift"]
    created = (await assign(async_client, admin, shift["id"], ann)).json()

    removed = await async_client.delete(
        f"/api/v1/assignments/{created['assignment']['id']}", headers=admin
    )

    assert removed.status_code == 200, removed.text
    assert codes(removed.json()) == ["warnings.under_headcount"]


async def test_the_same_employee_twice_is_refused(
    async_client: AsyncClient, admin: dict[str, str], project: dict[str, Any], ann: Any
) -> None:
    shift = (await add_shift(async_client, admin, project, MONDAY, headcount=2))["shift"]
    await assign(async_client, admin, shift["id"], ann)

    again = await assign(async_client, admin, shift["id"], ann)

    assert again.status_code == 409
    assert again.json()["code"] == "assignments.duplicate"


async def test_an_inactive_employee_cannot_be_assigned(
    async_client: AsyncClient, admin: dict[str, str], project: dict[str, Any], ann: Any
) -> None:
    await async_client.post(f"/api/v1/employees/{ann}/deactivate", headers=admin)
    shift = (await add_shift(async_client, admin, project, MONDAY))["shift"]

    refused = await assign(async_client, admin, shift["id"], ann)

    assert refused.status_code == 409
    assert refused.json()["code"] == "assignments.employee_inactive"


async def test_an_employee_deactivated_later_is_reported_on_their_shifts(
    async_client: AsyncClient, admin: dict[str, str], project: dict[str, Any], ann: Any
) -> None:
    shift = (await add_shift(async_client, admin, project, MONDAY))["shift"]
    await assign(async_client, admin, shift["id"], ann)
    await async_client.post(f"/api/v1/employees/{ann}/deactivate", headers=admin)

    changed = await async_client.patch(
        f"{SHIFTS}/{shift['id']}", json={"headcount": 1}, headers=admin
    )

    assert codes(changed.json()) == ["warnings.employee_inactive"]


async def test_a_client_account_or_a_missing_employee_cannot_be_assigned(
    async_client: AsyncClient,
    admin: dict[str, str],
    project: dict[str, Any],
    seed_users: dict[str, User],
) -> None:
    shift = (await add_shift(async_client, admin, project, MONDAY))["shift"]

    client = await assign(async_client, admin, shift["id"], seed_users["client"].id)
    missing = await assign(async_client, admin, shift["id"], "00000000-0000-4000-8000-000000000000")

    assert (client.status_code, missing.status_code) == (404, 404)


async def test_a_deleted_employee_left_on_a_shift_is_reported_as_inactive(
    async_client: AsyncClient, admin: dict[str, str], project: dict[str, Any], ann: Any
) -> None:
    """Deleting an employee keeps their assignments; the roster must still be editable."""
    shift = (await add_shift(async_client, admin, project, MONDAY))["shift"]
    await assign(async_client, admin, shift["id"], ann)
    assert (await async_client.delete(f"/api/v1/employees/{ann}", headers=admin)).status_code == 204

    changed = await async_client.patch(
        f"{SHIFTS}/{shift['id']}", json={"headcount": 1}, headers=admin
    )

    assert changed.status_code == 200, changed.text
    assert codes(changed.json()) == ["warnings.employee_inactive"]


async def test_removing_an_assignment_twice_answers_404_the_second_time(
    async_client: AsyncClient, admin: dict[str, str], project: dict[str, Any], ann: Any
) -> None:
    shift = (await add_shift(async_client, admin, project, MONDAY))["shift"]
    created = (await assign(async_client, admin, shift["id"], ann)).json()
    url = f"/api/v1/assignments/{created['assignment']['id']}"

    first = await async_client.delete(url, headers=admin)
    second = await async_client.delete(url, headers=admin)

    assert (first.status_code, second.status_code) == (200, 404)
