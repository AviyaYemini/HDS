"""Moving, swapping and noting roster edits (008 T042f, T042g; the owner's requests of 2026-11-09).

Covers: who is busy at a shift's time, named only as far as the editor may see; a transfer and a
swap in one step that keep the assignments' ids and drop their overrides; a note shown on the
employee's own shift page; the alert choice recorded with each edit for feature 010; and the role
rules: a Manager of both projects or an Admin may move; a Manager of one project is refused; an
Employee and a Client are refused; another organisation's shift or assignment is not found.
"""

import uuid
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hds.models.user import User
from tests.pay_support import project_managers
from tests.report_support import employee
from tests.roster_support import FIRST, SHIFTS, add_project, add_shift, assign, headers_for

ASSIGNMENTS = "/api/v1/assignments"


async def managed(client: AsyncClient, admin: dict[str, str], user: User, name: str) -> Any:
    project = await add_project(client, admin, name)
    put = await client.put(
        project_managers(project["id"]), json={"user_ids": [str(user.id)]}, headers=admin
    )
    assert put.status_code == 200, put.text
    return project


async def placed(client: AsyncClient, headers: dict[str, str], shift: Any, user: Any) -> str:
    made = await assign(client, headers, shift["id"], user)
    assert made.status_code == 201, made.text
    return str(made.json()["assignment"]["id"])


async def test_busy_names_the_other_shift_only_where_the_editor_may_edit(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    manager = await headers_for(async_client, "manager")
    here = await managed(async_client, admin, seed_users["manager"], "Here")
    theirs = await managed(async_client, admin, seed_users["manager"], "Theirs too")
    elsewhere = await add_project(async_client, admin, "Not theirs")
    shift = (await add_shift(async_client, admin, here, FIRST))["shift"]
    visible = (await add_shift(async_client, admin, theirs, FIRST))["shift"]
    hidden = (await add_shift(async_client, admin, elsewhere, FIRST))["shift"]
    ann = await employee(async_client, admin, "Ann Busy")
    ben = await employee(async_client, admin, "Ben Busy")
    await placed(async_client, admin, visible, ann)
    await placed(async_client, admin, hidden, ben)

    answer = await async_client.get(f"{SHIFTS}/{shift['id']}/busy", headers=manager)

    assert answer.status_code == 200, answer.text
    by = {i["user_id"]: i for i in answer.json()["items"]}
    assert by[ann]["movable"] is True and by[ann]["shift_id"] == visible["id"]
    assert by[ben]["movable"] is False
    assert (by[ben]["shift_id"], by[ben]["project_id"], by[ben]["starts_at"]) == (None, None, None)
    assert hidden["id"] not in answer.text and elsewhere["id"] not in answer.text


async def test_a_transfer_moves_the_person_keeping_the_assignment(
    async_client: AsyncClient, seed_users: dict[str, User], test_db: AsyncSession
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin, "Moves")
    first = (await add_shift(async_client, admin, project, FIRST))["shift"]
    second = (await add_shift(async_client, admin, project, FIRST, "afternoon"))["shift"]
    person = await employee(async_client, admin, "Mover")
    moving = await placed(async_client, admin, first, person)
    await test_db.execute(
        text(
            "INSERT INTO rule_overrides (id, organization_id, assignment_id, warning_code, reason,"
            " created_by) SELECT :i, organization_id, id, 'warnings.overlap', 'old', :u"
            " FROM assignments WHERE id = :a"
        ),
        {"i": uuid.uuid4(), "a": moving, "u": seed_users["admin"].id},
    )
    await test_db.commit()

    answer = await async_client.post(
        f"{ASSIGNMENTS}/{moving}/move",
        json={"to_shift_id": second["id"], "note": "Cover the afternoon", "alert": "affected"},
        headers=admin,
    )

    assert answer.status_code == 200, answer.text
    assert set(answer.json()["shift_etags"]) == {first["id"], second["id"]}
    row = (
        await test_db.execute(
            text("SELECT shift_id::text, note FROM assignments WHERE id = :a"), {"a": moving}
        )
    ).one()
    assert tuple(row) == (second["id"], "Cover the afternoon")
    left = await test_db.execute(
        text("SELECT count(*) FROM rule_overrides WHERE assignment_id = :a"), {"a": moving}
    )
    assert left.scalar_one() == 0
    event = (
        await test_db.execute(
            text(
                "SELECT kind, alert, note, affected::text FROM roster_edit_events"
                " WHERE shift_id = :s ORDER BY created_at DESC LIMIT 1"
            ),
            {"s": second["id"]},
        )
    ).one()
    assert tuple(event[:3]) == ("moved", "affected", "Cover the afternoon")
    assert person in event[3]


async def test_a_swap_trades_two_people_in_one_step(
    async_client: AsyncClient, seed_users: dict[str, User], test_db: AsyncSession
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin, "Swaps")
    first = (await add_shift(async_client, admin, project, FIRST))["shift"]
    second = (await add_shift(async_client, admin, project, FIRST, "afternoon"))["shift"]
    ann = await employee(async_client, admin, "Ann Swap")
    ben = await employee(async_client, admin, "Ben Swap")
    a = await placed(async_client, admin, first, ann)
    b = await placed(async_client, admin, second, ben)

    answer = await async_client.post(
        f"{ASSIGNMENTS}/{a}/move", json={"to_shift_id": second["id"], "swap_with": b}, headers=admin
    )

    assert answer.status_code == 200, answer.text
    rows = await test_db.execute(
        text("SELECT id::text, shift_id::text FROM assignments WHERE id IN (:a, :b)"),
        {"a": a, "b": b},
    )
    assert dict(rows.tuples().all()) == {a: second["id"], b: first["id"]}
    back = await async_client.post(
        f"{ASSIGNMENTS}/{a}/move", json={"to_shift_id": first["id"], "swap_with": b}, headers=admin
    )
    assert back.status_code == 200, back.text


async def test_a_move_that_would_put_someone_on_a_shift_twice_is_refused(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin, "Twice")
    first = (await add_shift(async_client, admin, project, FIRST, headcount=2))["shift"]
    second = (await add_shift(async_client, admin, project, FIRST, "afternoon"))["shift"]
    person = await employee(async_client, admin, "Twice Over")
    a = await placed(async_client, admin, first, person)
    await placed(async_client, admin, second, person)

    twice = await async_client.post(
        f"{ASSIGNMENTS}/{a}/move", json={"to_shift_id": second["id"]}, headers=admin
    )
    same = await async_client.post(
        f"{ASSIGNMENTS}/{a}/move", json={"to_shift_id": first["id"]}, headers=admin
    )

    assert (twice.status_code, twice.json()["code"]) == (409, "assignments.duplicate")
    assert (same.status_code, same.json()["code"]) == (409, "assignments.move_same_shift")


async def test_a_manager_of_only_one_of_the_shifts_cannot_move_between_them(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    manager = await headers_for(async_client, "manager")
    mine = await managed(async_client, admin, seed_users["manager"], "Mine")
    other = await add_project(async_client, admin, "Other")
    first = (await add_shift(async_client, admin, mine, FIRST))["shift"]
    second = (await add_shift(async_client, admin, other, FIRST))["shift"]
    a = await placed(async_client, admin, first, await employee(async_client, admin, "Held"))

    answer = await async_client.post(
        f"{ASSIGNMENTS}/{a}/move", json={"to_shift_id": second["id"]}, headers=manager
    )

    assert answer.status_code == 403, answer.text
    assert answer.json()["code"] == "roster.project_not_managed"


async def test_a_note_shows_on_the_employees_own_shift_and_the_alert_is_recorded(
    async_client: AsyncClient, seed_users: dict[str, User], test_db: AsyncSession
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin, "Noted")
    shift = (await add_shift(async_client, admin, project, FIRST))["shift"]
    worker = seed_users["employee"]
    made = await async_client.post(
        f"{SHIFTS}/{shift['id']}/assignments",
        json={"user_id": str(worker.id), "note": "Bring the keys", "alert": "everyone"},
        headers=admin,
    )
    assert made.status_code == 201, made.text
    published = await async_client.post(
        f"/api/v1/projects/{project['id']}/publish",
        json={"from": FIRST.isoformat(), "to": FIRST.isoformat()},
        headers=admin,
    )
    assert published.status_code == 200, published.text

    mine = await async_client.get(
        f"/me/shifts/{shift['id']}", headers=await headers_for(async_client, "employee")
    )
    removed = await async_client.delete(
        f"{ASSIGNMENTS}/{made.json()['assignment']['id']}",
        params={"alert": "none"},
        headers=admin,
    )

    assert mine.status_code == 200, mine.text
    assert mine.json()["note"] == "Bring the keys"
    assert removed.status_code == 200, removed.text
    kinds = await test_db.execute(
        text(
            "SELECT kind, alert FROM roster_edit_events WHERE project_id = :p ORDER BY created_at"
        ),
        {"p": project["id"]},
    )
    assert list(kinds.tuples()) == [("assigned", "everyone"), ("unassigned", "none")]


@pytest.mark.parametrize("role", ["employee", "client"])
async def test_employees_and_clients_cannot_see_who_is_busy_or_move(
    async_client: AsyncClient, seed_users: dict[str, User], role: str
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin, "Gate")
    shift = (await add_shift(async_client, admin, project, FIRST))["shift"]
    a = await placed(async_client, admin, shift, await employee(async_client, admin, "Gated"))
    headers = await headers_for(async_client, role)

    answers = [
        await async_client.get(f"{SHIFTS}/{shift['id']}/busy", headers=headers),
        await async_client.post(
            f"{ASSIGNMENTS}/{a}/move", json={"to_shift_id": shift["id"]}, headers=headers
        ),
    ]

    assert [x.status_code for x in answers] == [403, 403]


async def test_a_shift_or_assignment_not_in_the_organisation_is_not_found(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    """Another organisation's real records are swept in test_cross_organisation_sweep.py."""
    admin = await headers_for(async_client, "admin")
    theirs = uuid.uuid4()

    answers = [
        await async_client.get(f"{SHIFTS}/{theirs}/busy", headers=admin),
        await async_client.post(
            f"{ASSIGNMENTS}/{theirs}/move", json={"to_shift_id": str(theirs)}, headers=admin
        ),
    ]

    assert [x.status_code for x in answers] == [404, 404]


async def test_an_inactive_person_is_never_moved_onto_a_shift(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    """R41: a move keeps assign's rule: no one inactive is placed."""
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin, "Inactive move")
    first = (await add_shift(async_client, admin, project, FIRST))["shift"]
    second = (await add_shift(async_client, admin, project, FIRST, "afternoon"))["shift"]
    person = await employee(async_client, admin, "Leaving Soon")
    a = await placed(async_client, admin, first, person)
    off = await async_client.post(f"/api/v1/employees/{person}/deactivate", headers=admin)
    assert off.status_code == 200, off.text

    answer = await async_client.post(
        f"{ASSIGNMENTS}/{a}/move", json={"to_shift_id": second["id"]}, headers=admin
    )

    assert (answer.status_code, answer.json()["code"]) == (409, "assignments.employee_inactive")
