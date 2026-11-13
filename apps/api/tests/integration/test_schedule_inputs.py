"""Building a roster problem from what is stored (004 T016).

Covers: 004 FR-006, FR-007, FR-016, FR-019, FR-024; research G9.
"""

import uuid
from datetime import date, timedelta
from typing import Any

from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.database import get_session_factory
from hds.main import app
from hds.models.user import User
from hds.services.schedule_inputs import Built
from hds.wiring import input_service
from tests.roster_support import (
    add_project,
    add_shift,
    assign,
    body,
    constraints,
    headers_for,
    open_window,
)
from tests.schedule_support import SUNDAY, staffed_project

SATURDAY = SUNDAY + timedelta(days=6)


async def built(org_id: uuid.UUID, ids: list[str] | None = None) -> Built:
    """The problem for the week of ``SUNDAY``, read in a session of its own."""
    factory = app.dependency_overrides[get_session_factory]()
    async with factory() as session:
        return await input_service(session).build(
            org_id, SUNDAY, SATURDAY, [uuid.UUID(i) for i in ids or []]
        )


def days(b: Built) -> list[date]:
    return sorted(s.day for s in b.problem.slots)


async def test_slots_come_from_the_needs_on_each_date_of_the_project(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    manager = await headers_for(async_client, "manager")
    await staffed_project(async_client, manager, needs={"morning": 1, "night": 2})

    b = await built(seed_users["manager"].organisation_id)

    assert len(b.problem.slots) == 14
    nights = [s for s in b.problem.slots if s.night]
    assert len(nights) == 7 and all(s.headcount == 2 for s in nights)
    assert all(s.shift_id is None for s in b.problem.slots)


async def test_a_project_gets_slots_only_on_its_own_dates(
    async_client: AsyncClient, seed_users: dict[str, User], test_db: AsyncSession
) -> None:
    manager = await headers_for(async_client, "manager")
    project = await staffed_project(async_client, manager)
    # A project's dates are set when it is made; set them here for a project that starts and ends
    # inside the week.
    await test_db.execute(
        text("UPDATE projects SET start_date = :s, end_date = :e WHERE id = :p"),
        {"s": date(2027, 6, 8), "e": date(2027, 6, 10), "p": project["id"]},
    )
    await test_db.commit()

    b = await built(seed_users["manager"].organisation_id)

    assert days(b) == [date(2027, 6, 8), date(2027, 6, 9), date(2027, 6, 10)]


async def test_drafts_are_used_as_slots_and_published_shifts_stay_fixed(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await staffed_project(async_client, admin, needs={"morning": 1})
    monday = (await add_shift(async_client, admin, project, SUNDAY + timedelta(1), headcount=3))[
        "shift"
    ]
    extra = (await add_shift(async_client, admin, project, SUNDAY + timedelta(2), "afternoon"))[
        "shift"
    ]
    friday = (await add_shift(async_client, admin, project, SUNDAY + timedelta(5)))["shift"]
    await assign(async_client, admin, friday["id"], seed_users["employee"].id)
    published = await async_client.post(
        f"/api/v1/projects/{project['id']}/publish",
        json={"from": "2027-06-11", "to": "2027-06-11"},
        headers=admin,
    )
    assert published.status_code == 200, published.text
    saturday = (await add_shift(async_client, admin, project, SATURDAY))["shift"]
    await async_client.post(
        f"/api/v1/projects/{project['id']}/publish",
        json={"from": "2027-06-12", "to": "2027-06-12"},
        headers=admin,
    )

    b = await built(seed_users["admin"].organisation_id)

    by_shift = {s.shift_id: s for s in b.problem.slots if s.shift_id}
    assert by_shift[uuid.UUID(monday["id"])].headcount == 3  # the draft's own headcount
    assert uuid.UUID(extra["id"]) in by_shift  # a draft with no need is still a slot
    assert days(b).count(SUNDAY + timedelta(1)) == 1  # the draft stands in for the need
    assert days(b).count(SUNDAY + timedelta(2)) == 2  # Tuesday's need and the afternoon draft
    assert SUNDAY + timedelta(5) not in days(b) and SATURDAY not in days(b)
    fixed = {s.id for s in b.problem.fixed}
    assert uuid.UUID(friday["id"]) in fixed
    assert [p.shift_id for p in b.short_published] == [uuid.UUID(saturday["id"])]


async def test_the_surroundings_reach_the_weeks_touched_plus_the_minimum_rest(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    await staffed_project(async_client, admin)
    other = await add_project(async_client, admin, "Other")
    employee = seed_users["employee"].id
    near = (await add_shift(async_client, admin, other, SUNDAY - timedelta(1), "afternoon"))[
        "shift"
    ]
    far = (await add_shift(async_client, admin, other, SUNDAY - timedelta(3)))["shift"]
    for shift in (near, far):
        await assign(async_client, admin, shift["id"], employee)

    b = await built(seed_users["admin"].organisation_id)

    fixed = {s.id for s in b.problem.fixed}
    # Ends Saturday 23:00, an hour before the range: outside it, but within the 11-hour rest.
    assert uuid.UUID(near["id"]) in fixed
    assert uuid.UUID(far["id"]) not in fixed


async def test_candidates_are_employees_and_staff_with_an_open_entry(
    async_client: AsyncClient, seed_users: dict[str, User], test_db: AsyncSession
) -> None:
    admin = await headers_for(async_client, "admin")
    manager = await headers_for(async_client, "manager")
    project = await staffed_project(async_client, admin)
    await open_window(async_client, manager)
    opened = await async_client.post(
        constraints(seed_users["manager"].id), json=body("open"), headers=manager
    )
    assert opened.status_code == 201, opened.text
    draft = (await add_shift(async_client, admin, project, SUNDAY + timedelta(2)))["shift"]
    await assign(async_client, admin, draft["id"], seed_users["admin"].id)

    b = await built(seed_users["admin"].organisation_id)

    ids = {c.id for c in b.problem.candidates}
    assert ids == {seed_users["employee"].id, seed_users["manager"].id}
    (slot,) = [s for s in b.problem.slots if s.shift_id == uuid.UUID(draft["id"])]
    assert slot.kept == (seed_users["admin"].id,)  # an Admin with no open entry stays on it
    assert b.no_availability == (seed_users["employee"].id,)

    await test_db.execute(
        text("UPDATE users SET is_active = false WHERE id = :u"), {"u": seed_users["employee"].id}
    )
    await test_db.commit()
    assert seed_users["employee"].id not in {
        c.id for c in (await built(seed_users["admin"].organisation_id)).problem.candidates
    }


async def test_the_fingerprint_is_stable_and_follows_every_input(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await staffed_project(async_client, admin)
    org = seed_users["admin"].organisation_id
    employee = seed_users["employee"].id
    # An entry already, so the next one changes the entries alone, not who has none.
    already = await async_client.post(
        constraints(employee),
        json=body("absence", SUNDAY + timedelta(1), absence_kind="leave"),
        headers=admin,
    )
    assert already.status_code == 201, already.text
    first = (await built(org)).fingerprint
    assert (await built(org)).fingerprint == first

    async def step(change: Any) -> str:
        await change()
        return (await built(org)).fingerprint

    seen = {first}
    changes = [
        lambda: async_client.post(
            constraints(employee), json=body("absence", absence_kind="sick"), headers=admin
        ),
        lambda: add_shift(async_client, admin, project, SUNDAY + timedelta(3), "night"),
        lambda: async_client.put(
            f"/api/v1/projects/{project['id']}/requirements",
            json={
                "items": [
                    {"shift_type_id": project["types"]["morning"], "weekday": 1, "headcount": 2}
                ]
            },
            headers=admin,
        ),
        lambda: async_client.patch(
            "/api/v1/organisation/scheduling", json={"min_rest_hours": 10}, headers=admin
        ),
        lambda: async_client.put(
            f"/api/v1/projects/{project['id']}/shift-types",
            json={"ids": [project["types"]["morning"], project["types"]["night"]]},
            headers=admin,
        ),
        # A temporary worker's engagement is an input, and a shorter one too (CodeRabbit, PR 28).
        lambda: async_client.patch(
            f"/api/v1/employees/{employee}",
            json={
                "employment_type": "temporary",
                "engaged_from": SUNDAY.isoformat(),
                "engaged_until": (SUNDAY + timedelta(30)).isoformat(),
                "project_ids": [project["id"]],
            },
            headers=admin,
        ),
        lambda: async_client.patch(
            f"/api/v1/employees/{employee}",
            json={"engaged_until": (SUNDAY + timedelta(20)).isoformat()},
            headers=admin,
        ),
        lambda: async_client.post(f"/api/v1/employees/{employee}/deactivate", headers=admin),
    ]
    for change in changes:
        now = await step(change)
        assert now not in seen
        seen.add(now)
