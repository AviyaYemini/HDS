"""Accepting and discarding a proposal (004 T020).

Covers: 004 FR-023 to FR-025, FR-027, SC-007, SC-009; US3, US6.
"""

from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hds.models.user import User
from tests.integration.conftest import OtherOrg
from tests.roster_support import add_project, add_shift, assign, body, constraints, headers_for
from tests.schedule_support import JOBS, SUNDAY, ask, staffed_project, work

THURSDAY = SUNDAY + timedelta(days=4)


async def proposed(client: AsyncClient, headers: dict[str, str], **extra: Any) -> dict[str, Any]:
    """Ask for Sunday to Thursday, run the worker, and return the finished job."""
    job = (await ask(client, headers, SUNDAY, THURSDAY, **extra)).json()
    await work()
    done: dict[str, Any] = (await client.get(f"{JOBS}/{job['id']}", headers=headers)).json()
    assert done["status"] == "succeeded", done
    return done


async def roster(client: AsyncClient, headers: dict[str, str]) -> list[dict[str, Any]]:
    answer = await client.get("/api/v1/shifts?from=2027-06-06&to=2027-06-12", headers=headers)
    assert answer.status_code == 200, answer.text
    items: list[dict[str, Any]] = answer.json()["items"]
    return items


@pytest.mark.parametrize("role", ["manager", "admin"])
async def test_accepting_stores_the_proposal_as_drafts_and_publishes_nothing(
    async_client: AsyncClient, seed_users: dict[str, User], test_db: AsyncSession, role: str
) -> None:
    headers = await headers_for(async_client, role)
    await staffed_project(async_client, headers)
    job = await proposed(async_client, headers)

    answer = await async_client.post(f"{JOBS}/{job['id']}/accept", headers=headers)

    assert answer.status_code == 200, answer.text
    assert answer.json() == {
        "shifts_created": 5,
        "shifts_updated": 0,
        "assignments_added": 5,
        "assignments_removed": 0,
    }
    shifts = await roster(async_client, headers)
    assert len(shifts) == 5 and all(s["status"] == "draft" for s in shifts)
    assert all(
        [a["user_id"] for a in s["assignments"]] == [str(seed_users["employee"].id)] for s in shifts
    )
    sources = await test_db.execute(text("SELECT DISTINCT source, assigned_by FROM assignments"))
    assert [tuple(r) for r in sources] == [("generated", seed_users[role].id)]
    employee = await headers_for(async_client, "employee")
    assert (await async_client.get("/me/shifts", headers=employee)).json()["items"] == []
    read = (await async_client.get(f"{JOBS}/{job['id']}", headers=headers)).json()
    assert read["proposal_state"] == "accepted"


async def test_an_existing_draft_is_updated_in_place(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await staffed_project(async_client, admin)
    monday = (await add_shift(async_client, admin, project, SUNDAY + timedelta(days=1)))["shift"]
    job = await proposed(async_client, admin)

    answer = await async_client.post(f"{JOBS}/{job['id']}/accept", headers=admin)

    assert answer.json()["shifts_updated"] == 1 and answer.json()["shifts_created"] == 4
    # Monday's morning starts 07:00 in Israel, 04:00 UTC the same date.
    on_monday = [
        s for s in await roster(async_client, admin) if s["starts_at"].startswith("2027-06-07")
    ]
    assert [s["id"] for s in on_monday] == [monday["id"]]
    assert len(on_monday[0]["assignments"]) == 1


async def test_accepting_twice_is_refused(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    manager = await headers_for(async_client, "manager")
    await staffed_project(async_client, manager)
    job = await proposed(async_client, manager)
    await async_client.post(f"{JOBS}/{job['id']}/accept", headers=manager)

    again = await async_client.post(f"{JOBS}/{job['id']}/accept", headers=manager)

    assert again.status_code == 409
    assert again.json()["code"] == "schedule.proposal_closed"
    assert len(await roster(async_client, manager)) == 5


async def test_a_job_that_has_not_succeeded_cannot_be_accepted_or_discarded(
    async_client: AsyncClient, seed_users: dict[str, User], test_db: AsyncSession
) -> None:
    manager = await headers_for(async_client, "manager")
    await staffed_project(async_client, manager)  # a run needs a project of theirs (T108)
    queued = (await ask(async_client, manager)).json()
    for action in ("accept", "discard"):
        answer = await async_client.post(f"{JOBS}/{queued['id']}/{action}", headers=manager)
        assert answer.status_code == 409
        assert answer.json()["code"] == "schedule.not_ready"
    await test_db.execute(
        text("UPDATE schedule_jobs SET status = 'failed', error_code = 'jobs.solver_failed'")
    )
    await test_db.commit()
    answer = await async_client.post(f"{JOBS}/{queued['id']}/accept", headers=manager)
    assert answer.json()["code"] == "schedule.not_ready"


Change = Callable[[AsyncClient, dict[str, Any], dict[str, User]], Awaitable[Any]]


async def new_absence(c: AsyncClient, w: dict[str, Any], u: dict[str, User]) -> Any:
    return await c.post(
        constraints(u["employee"].id),
        json=body("absence", SUNDAY + timedelta(days=2), absence_kind="sick"),
        headers=w["admin"],
    )


async def shift_nearby(c: AsyncClient, w: dict[str, Any], u: dict[str, User]) -> Any:
    other = await add_project(c, w["admin"], "Elsewhere")
    made = (await add_shift(c, w["admin"], other, SUNDAY + timedelta(days=5)))["shift"]
    return await assign(c, w["admin"], made["id"], u["employee"].id)


async def deactivated(c: AsyncClient, w: dict[str, Any], u: dict[str, User]) -> Any:
    return await c.post(f"/api/v1/employees/{u['employee'].id}/deactivate", headers=w["admin"])


async def new_need(c: AsyncClient, w: dict[str, Any], u: dict[str, User]) -> Any:
    types = w["project"]["types"]
    return await c.put(
        f"/api/v1/projects/{w['project']['id']}/requirements",
        json={"items": [{"shift_type_id": types["morning"], "weekday": 1, "headcount": 2}]},
        headers=w["admin"],
    )


async def fewer_types(c: AsyncClient, w: dict[str, Any], u: dict[str, User]) -> Any:
    types = w["project"]["types"]
    return await c.put(
        f"/api/v1/projects/{w['project']['id']}/shift-types",
        json={"ids": [types["morning"], types["night"]]},
        headers=w["admin"],
    )


async def new_limits(c: AsyncClient, w: dict[str, Any], u: dict[str, User]) -> Any:
    return await c.patch(
        "/api/v1/organisation/scheduling", json={"max_weekly_hours": 40}, headers=w["admin"]
    )


@pytest.mark.parametrize(
    "change", [new_absence, shift_nearby, deactivated, new_need, fewer_types, new_limits]
)
async def test_any_change_the_proposal_used_makes_it_out_of_date(
    async_client: AsyncClient, seed_users: dict[str, User], change: Change
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await staffed_project(async_client, admin)
    job = await proposed(async_client, admin)
    before = await roster(async_client, admin)
    made = await change(async_client, {"admin": admin, "project": project}, seed_users)
    assert made.status_code in (200, 201, 204), made.text
    after_change = await roster(async_client, admin)

    answer = await async_client.post(f"{JOBS}/{job['id']}/accept", headers=admin)

    assert answer.status_code == 409, answer.text
    assert answer.json()["code"] == "schedule.proposal_out_of_date"
    assert await roster(async_client, admin) == after_change
    assert len(after_change) - len(before) <= 1  # only the change itself, if it was a shift


async def test_a_discarded_proposal_cannot_be_accepted(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    manager = await headers_for(async_client, "manager")
    await staffed_project(async_client, manager)
    job = await proposed(async_client, manager)

    discarded = await async_client.post(f"{JOBS}/{job['id']}/discard", headers=manager)
    accept = await async_client.post(f"{JOBS}/{job['id']}/accept", headers=manager)
    twice = await async_client.post(f"{JOBS}/{job['id']}/discard", headers=manager)

    assert discarded.status_code == 200
    assert discarded.json()["proposal_state"] == "discarded"
    assert (accept.status_code, accept.json()["code"]) == (409, "schedule.proposal_closed")
    assert (twice.status_code, twice.json()["code"]) == (409, "schedule.proposal_closed")
    assert await roster(async_client, manager) == []


async def test_a_proposal_left_for_seven_days_expires(
    async_client: AsyncClient, seed_users: dict[str, User], test_db: AsyncSession
) -> None:
    manager = await headers_for(async_client, "manager")
    await staffed_project(async_client, manager)
    job = await proposed(async_client, manager)
    await test_db.execute(
        text("UPDATE schedule_jobs SET finished_at = :t WHERE id = :i"),
        {"t": datetime.now(UTC) - timedelta(days=7, minutes=1), "i": job["id"]},
    )
    await test_db.commit()

    read = (await async_client.get(f"{JOBS}/{job['id']}", headers=manager)).json()
    accept = await async_client.post(f"{JOBS}/{job['id']}/accept", headers=manager)

    assert read["proposal_state"] == "expired"
    assert (accept.status_code, accept.json()["code"]) == (409, "schedule.proposal_closed")


@pytest.mark.parametrize("role", ["employee", "client"])
async def test_employees_and_clients_cannot_accept_or_discard(
    async_client: AsyncClient, seed_users: dict[str, User], role: str
) -> None:
    manager = await headers_for(async_client, "manager")
    await staffed_project(async_client, manager)
    job = await proposed(async_client, manager)
    headers = await headers_for(async_client, role)

    for action in ("accept", "discard"):
        answer = await async_client.post(f"{JOBS}/{job['id']}/{action}", headers=headers)
        assert answer.status_code == 403
    assert await roster(async_client, manager) == []


async def test_another_organisations_job_is_not_found(
    async_client: AsyncClient, seed_users: dict[str, User], other_org: OtherOrg
) -> None:
    manager = await headers_for(async_client, "manager")
    assert other_org.job is not None
    for action in ("accept", "discard"):
        answer = await async_client.post(f"{JOBS}/{other_org.job.id}/{action}", headers=manager)
        assert answer.status_code == 404


async def test_generating_again_after_accepting_proposes_no_change(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    """SC-007: a roster accepted as it was proposed is proposed again unchanged."""
    manager = await headers_for(async_client, "manager")
    await staffed_project(async_client, manager)
    first = await proposed(async_client, manager)
    await async_client.post(f"{JOBS}/{first['id']}/accept", headers=manager)

    again = await proposed(async_client, manager)

    assert again["proposal"]["changes"] == {"added": [], "removed": []}
    assert all(s["shift_id"] for s in again["proposal"]["slots"])  # the drafts are the slots now


async def test_someone_the_proposal_takes_off_a_draft_is_removed(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    """The person on Monday's draft is now absent that day, so the proposal takes them off it."""
    admin = await headers_for(async_client, "admin")
    project = await staffed_project(async_client, admin)
    monday = (await add_shift(async_client, admin, project, SUNDAY + timedelta(days=1)))["shift"]
    employee = seed_users["employee"].id
    await assign(async_client, admin, monday["id"], employee)
    away = await async_client.post(
        constraints(employee),
        json=body("absence", SUNDAY + timedelta(days=1), absence_kind="leave"),
        headers=admin,
    )
    assert away.status_code == 201, away.text
    job = await proposed(async_client, admin)

    answer = await async_client.post(f"{JOBS}/{job['id']}/accept", headers=admin)

    assert answer.json()["assignments_removed"] == 1
    (draft,) = [s for s in await roster(async_client, admin) if s["id"] == monday["id"]]
    assert draft["assignments"] == []
