"""Availability submissions through the API (007 FR-014, FR-015, task T049).

Covers the status moving Not submitted → Draft → Submitted → Draft → Locked, "counted as
unavailable" after a missed deadline, the refusal to submit late, that nobody sees another
employee's status, and that a manual assignment of someone who missed the deadline warns
``warnings.not_submitted``.
"""

from typing import Any

from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from hds.models.user import User
from tests.roster_support import (
    FIRST,
    add_project,
    add_shift,
    assign,
    body,
    close_window,
    codes,
    constraints,
    headers_for,
    open_window,
)

URL = "/me/availability-submissions"


async def status(client: AsyncClient, headers: dict[str, str], window: dict[str, Any]) -> Any:
    response = await client.get(URL, params={"window_id": window["id"]}, headers=headers)
    assert response.status_code == 200, response.text
    return response.json()


async def test_the_status_moves_from_not_submitted_to_locked(
    async_client: AsyncClient, seed_users: dict[str, User], test_db: AsyncSession
) -> None:
    manager = await headers_for(async_client, "manager")
    employee = await headers_for(async_client, "employee")
    window = await open_window(async_client, manager)
    me = seed_users["employee"]

    assert (await status(async_client, employee, window))["status"] == "not_submitted"

    added = await async_client.post(constraints(me.id), json=body("open"), headers=employee)
    assert added.status_code == 201, added.text
    assert (await status(async_client, employee, window))["status"] == "draft"

    submitted = await async_client.post(URL, json={"window_id": window["id"]}, headers=employee)
    assert submitted.status_code == 200, submitted.text
    assert submitted.json()["status"] == "submitted"
    assert submitted.json()["submitted_at"] is not None

    second = await async_client.post(
        constraints(me.id), json=body("close", FIRST.replace(day=8)), headers=employee
    )
    assert second.status_code == 201, second.text
    assert (await status(async_client, employee, window))["status"] == "draft"

    await close_window(test_db, window["id"])
    locked = await status(async_client, employee, window)
    assert locked["status"] == "locked"
    assert locked["counted_unavailable"] is False  # they had submitted

    late = await async_client.post(URL, json={"window_id": window["id"]}, headers=employee)
    assert late.status_code == 409
    assert late.json()["code"] == "availability.window_closed"


async def test_missing_the_deadline_counts_unavailable_and_a_manual_assignment_warns(
    async_client: AsyncClient, seed_users: dict[str, User], test_db: AsyncSession
) -> None:
    admin = await headers_for(async_client, "admin")
    window = await open_window(async_client, admin)
    await close_window(test_db, window["id"])
    employee = await headers_for(async_client, "employee")
    assert (await status(async_client, employee, window))["counted_unavailable"] is True

    project = await add_project(async_client, admin)
    shift = await add_shift(async_client, admin, project, FIRST)
    answer = await assign(async_client, admin, shift["id"], seed_users["employee"].id)
    assert answer.status_code == 201, answer.text
    assert "warnings.not_submitted" in codes(answer.json())


async def test_a_client_has_no_availability_status(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    window = await open_window(async_client, await headers_for(async_client, "manager"))
    response = await async_client.get(
        URL, params={"window_id": window["id"]}, headers=await headers_for(async_client, "client")
    )
    assert response.status_code == 403


async def test_an_admin_submits_on_behalf_of_someone_who_missed_the_deadline(
    async_client: AsyncClient, seed_users: dict[str, User], test_db: AsyncSession
) -> None:
    admin = await headers_for(async_client, "admin")
    window = await open_window(async_client, admin)
    await close_window(test_db, window["id"])
    me = seed_users["employee"]
    employee = await headers_for(async_client, "employee")

    # An Admin's hand edit alone does not create a submission (reviewer note, 2026-11-07).
    edited = await async_client.post(constraints(me.id), json=body("open"), headers=admin)
    assert edited.status_code == 201, edited.text
    assert (await status(async_client, employee, window))["counted_unavailable"] is True

    url = f"/api/v1/employees/{me.id}/availability-submissions"
    refused = await async_client.post(
        url, json={"window_id": window["id"]}, headers=await headers_for(async_client, "manager")
    )
    assert refused.status_code == 403
    submitted = await async_client.post(url, json={"window_id": window["id"]}, headers=admin)
    assert submitted.status_code == 200, submitted.text
    assert submitted.json()["submitted_by"] == str(seed_users["admin"].id)
    after = await status(async_client, employee, window)
    assert (after["status"], after["counted_unavailable"]) == ("locked", False)
