"""The roster tools through the API (007 FR-034 to FR-040, task T064).

Covers: an override needs a warning the assignment has, and closes it in the publish review; the
review counts what publish then publishes, and publish records one event per person; copy week and
templates repeat a week, with a dry run that changes nothing; bulk edit touches drafts only; the
submission tracker lists everyone's status.
"""

from datetime import date, timedelta
from typing import Any

from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hds.models.user import User
from tests.roster_support import (
    FIRST,
    SHIFTS,
    add_project,
    add_shift,
    assign,
    headers_for,
    open_window,
)

NEXT = FIRST + timedelta(days=7)  # FIRST is a Sunday


async def staffed(
    client: AsyncClient, admin: dict[str, str], project: dict[str, Any], who: User, day: date
) -> Any:
    shift = (await add_shift(client, admin, project, day))["shift"]
    answer = await assign(client, admin, shift["id"], who.id)
    assert answer.status_code == 201, answer.text
    return shift, answer.json()


async def test_an_override_closes_its_warning_in_the_publish_review(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin)
    # Two overlapping shifts for one employee: each carries warnings.overlap.
    _, first = await staffed(async_client, admin, project, seed_users["employee"], FIRST)
    second_shift = (await add_shift(async_client, admin, project, FIRST, kind="morning"))["shift"]
    second = await assign(async_client, admin, second_shift["id"], seed_users["employee"].id)
    assert "warnings.overlap" in [w["code"] for w in second.json()["warnings"]]
    url = f"/api/v1/projects/{project['id']}/publish-preview"
    params = {"from": FIRST.isoformat(), "to": FIRST.isoformat()}
    before = (await async_client.get(url, params=params, headers=admin)).json()
    assert before["shifts"] == 2 and before["people"] == 1 and before["open_warnings"] >= 1

    wrong = await async_client.post(
        f"/api/v1/assignments/{first['assignment']['id']}/overrides",
        json={"warning_code": "warnings.absence", "reason": "x"},
        headers=admin,
    )
    assert wrong.status_code == 422
    for answer in (first, second.json()):
        made = await async_client.post(
            f"/api/v1/assignments/{answer['assignment']['id']}/overrides",
            json={"warning_code": "warnings.overlap", "reason": "Short handover, agreed with Dana"},
            headers=admin,
        )
        assert made.status_code == 201, made.text
    after = (await async_client.get(url, params=params, headers=admin)).json()
    assert after["open_warnings"] < before["open_warnings"]


async def test_publish_records_one_event_per_person_and_matches_the_review(
    async_client: AsyncClient, seed_users: dict[str, User], test_db: AsyncSession
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin)
    await staffed(async_client, admin, project, seed_users["employee"], FIRST)
    await staffed(async_client, admin, project, seed_users["employee"], FIRST + timedelta(days=1))
    await staffed(async_client, admin, project, seed_users["manager"], FIRST + timedelta(days=2))
    params = {"from": FIRST.isoformat(), "to": (FIRST + timedelta(days=6)).isoformat()}
    review = (
        await async_client.get(
            f"/api/v1/projects/{project['id']}/publish-preview", params=params, headers=admin
        )
    ).json()
    published = await async_client.post(
        f"/api/v1/projects/{project['id']}/publish", json={**params, "notify": False}, headers=admin
    )
    assert published.json()["published"] == review["shifts"] == 3
    rows = (
        await test_db.execute(
            text(
                "SELECT shift_count, notify FROM publish_events "
                "WHERE project_id = :p ORDER BY shift_count"
            ),
            {"p": project["id"]},
        )
    ).all()
    assert [tuple(r) for r in rows] == [(1, False), (2, False)]


async def test_copy_week_and_a_template_repeat_a_week_and_a_dry_run_changes_nothing(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin)
    await staffed(async_client, admin, project, seed_users["employee"], FIRST + timedelta(days=1))
    copy = f"/api/v1/projects/{project['id']}/copy-week"
    body = {"from_week": FIRST.isoformat(), "to_week": NEXT.isoformat()}

    dry = await async_client.post(copy, json={**body, "dry_run": True}, headers=admin)
    assert dry.status_code == 200, dry.text
    assert [c["action"] for c in dry.json()["changes"]] == ["create"]
    listed = await async_client.get(
        SHIFTS, params={"project_id": project["id"], "from": NEXT.isoformat()}, headers=admin
    )
    assert listed.json()["items"] == []

    done = await async_client.post(copy, json=body, headers=admin)
    assert done.status_code == 200, done.text
    [copied] = (
        await async_client.get(
            SHIFTS, params={"project_id": project["id"], "from": NEXT.isoformat()}, headers=admin
        )
    ).json()["items"]
    assert [a["user_id"] for a in copied["assignments"]] == [str(seed_users["employee"].id)]

    saved = await async_client.post(
        f"/api/v1/projects/{project['id']}/week-templates",
        json={"name": "Usual week", "week": FIRST.isoformat()},
        headers=admin,
    )
    assert saved.status_code == 201, saved.text
    again = await async_client.post(
        f"/api/v1/projects/{project['id']}/week-templates/{saved.json()['id']}/apply",
        json={"week": NEXT.isoformat(), "on_conflict": "overwrite", "dry_run": True},
        headers=admin,
    )
    assert [c["action"] for c in again.json()["changes"]] == ["replace"]
    not_sunday = await async_client.post(
        copy, json={**body, "to_week": (NEXT + timedelta(days=1)).isoformat()}, headers=admin
    )
    assert not_sunday.status_code == 422


async def test_bulk_edit_touches_drafts_only(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin)
    draft = (await add_shift(async_client, admin, project, FIRST))["shift"]
    published = (await add_shift(async_client, admin, project, FIRST + timedelta(days=1)))["shift"]
    day = (FIRST + timedelta(days=1)).isoformat()
    await async_client.post(
        f"/api/v1/projects/{project['id']}/publish", json={"from": day, "to": day}, headers=admin
    )
    url = f"/api/v1/projects/{project['id']}/roster/bulk"
    week = {"from": FIRST.isoformat(), "to": (FIRST + timedelta(days=6)).isoformat()}
    preview = await async_client.post(
        url, json={**week, "headcount": 3, "dry_run": True}, headers=admin
    )
    assert [c["shift_id"] for c in preview.json()["changes"]] == [draft["id"]]
    await async_client.post(url, json={**week, "headcount": 3}, headers=admin)
    items = {
        s["id"]: s
        for s in (
            await async_client.get(SHIFTS, params={"project_id": project["id"]}, headers=admin)
        ).json()["items"]
    }
    assert (items[draft["id"]]["headcount"], items[published["id"]]["headcount"]) == (3, 1)
    neither = await async_client.post(url, json=week, headers=admin)
    assert neither.status_code == 422


async def test_the_tracker_lists_everyones_status(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    manager = await headers_for(async_client, "manager")
    window = await open_window(async_client, manager)
    employee = await headers_for(async_client, "employee")
    await async_client.post(
        "/me/availability-submissions", json={"window_id": window["id"]}, headers=employee
    )
    listed = await async_client.get(
        f"/api/v1/availability-windows/{window['id']}/submissions", headers=manager
    )
    assert listed.status_code == 200, listed.text
    status = {i["user_id"]: i["status"] for i in listed.json()["items"]}
    assert status[str(seed_users["employee"].id)] == "submitted"
    assert status[str(seed_users["manager"].id)] == "not_submitted"
    assert str(seed_users["client"].id) not in status
    refused = await async_client.get(
        f"/api/v1/availability-windows/{window['id']}/submissions", headers=employee
    )
    assert refused.status_code == 403


async def test_a_template_naming_someone_since_deactivated_still_applies(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    """R86: they are left off, with a warning, in the dry run and the apply alike."""
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin)
    leaver = seed_users["manager"]
    await staffed(async_client, admin, project, leaver, FIRST + timedelta(days=1))
    saved = await async_client.post(
        f"/api/v1/projects/{project['id']}/week-templates",
        json={"name": "With a leaver", "week": FIRST.isoformat()},
        headers=admin,
    )
    assert saved.status_code == 201, saved.text
    off = await async_client.post(f"/api/v1/employees/{leaver.id}/deactivate", headers=admin)
    assert off.status_code in (200, 204), off.text
    apply = f"/api/v1/projects/{project['id']}/week-templates/{saved.json()['id']}/apply"
    for dry_run in (True, False):
        answer = await async_client.post(
            apply, json={"week": NEXT.isoformat(), "dry_run": dry_run}, headers=admin
        )
        assert answer.status_code == 200, answer.text
        assert [c["people"] for c in answer.json()["changes"]] == [0]
        assert "assignments.employee_inactive" in [w["code"] for w in answer.json()["warnings"]]


async def test_dates_at_the_edge_of_the_calendar_are_a_bad_range_not_a_crash(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    """R87: 422, never 500, for a range that runs off the end of the calendar."""
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin)
    preview = await async_client.get(
        f"/api/v1/projects/{project['id']}/publish-preview",
        params={"from": "2026-01-01", "to": "9999-12-31"},
        headers=admin,
    )
    assert preview.status_code == 422, preview.text
    assert preview.json()["code"] == "shifts.date_range_invalid"
