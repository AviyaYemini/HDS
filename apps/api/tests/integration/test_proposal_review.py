"""Reviewing, partly accepting and editing a proposal (008 T007 to T009).

Covers: 008 FR-001 to FR-012, FR-030, FR-035; US1, US2.
"""

from datetime import timedelta
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hds.models.user import User
from tests.integration.conftest import OtherOrg
from tests.report_support import employee
from tests.roster_support import add_project, add_shift, assign, body, constraints, headers_for
from tests.schedule_support import JOBS, SUNDAY, ask, staffed_project, work

THURSDAY = SUNDAY + timedelta(days=4)
WEDNESDAY = SUNDAY + timedelta(days=3)


async def proposed(client: AsyncClient, headers: dict[str, str]) -> dict[str, Any]:
    """Sunday to Thursday, one morning a day, solved; the finished job."""
    job = (await ask(client, headers, SUNDAY, THURSDAY)).json()
    await work()
    done: dict[str, Any] = (await client.get(f"{JOBS}/{job['id']}", headers=headers)).json()
    assert done["status"] == "succeeded", done
    return done


async def review(client: AsyncClient, headers: dict[str, str], job: dict[str, Any]) -> Any:
    answer = await client.get(f"{JOBS}/{job['id']}/proposal", headers=headers)
    assert answer.status_code == 200, answer.text
    return answer


def slot_on(view: dict[str, Any], day: Any) -> dict[str, Any]:
    found: dict[str, Any] = next(s for s in view["slots"] if s["date"] == day.isoformat())
    return found


async def roster_count(test_db: AsyncSession) -> int:
    return int((await test_db.execute(text("SELECT count(*) FROM shifts"))).scalar_one())


async def test_the_review_marks_every_assignment_and_counts_them(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    await staffed_project(async_client, admin)
    job = await proposed(async_client, admin)

    answer = await review(async_client, admin, job)

    view = answer.json()
    assert answer.headers["etag"] == '"0"' and view["version"] == 0
    assert view["counts"] == {"added": 5, "changed": 0, "removed": 0, "unchanged": 0}
    assert all(c["filled"] == 1 and c["needed"] == 1 for c in view["coverage"])
    assert len(view["coverage"]) == 5 and view["settled"] is False
    first = slot_on(view, SUNDAY)
    assert first["marks"] == [
        {
            "mark": "added",
            "user_id": str(seed_users["employee"].id),
            "replaces": None,
            "accepted": False,
        }
    ]
    assert first["people"][0]["by"] == "solver"


@pytest.mark.parametrize("role", ["manager", "admin"])
async def test_one_day_is_accepted_and_the_rest_stays_open(
    async_client: AsyncClient, seed_users: dict[str, User], test_db: AsyncSession, role: str
) -> None:
    headers = await headers_for(async_client, role)
    await staffed_project(async_client, headers)
    job = await proposed(async_client, headers)

    answer = await async_client.post(
        f"{JOBS}/{job['id']}/accept",
        json={"days": [SUNDAY.isoformat()]},
        headers={**headers, "If-Match": '"0"'},
    )

    assert answer.status_code == 200, answer.text
    assert answer.json() == {
        "shifts_created": 1,
        "shifts_updated": 0,
        "assignments_added": 1,
        "assignments_removed": 0,
        "version": 1,
        "state": "open",
    }
    assert await roster_count(test_db) == 1
    view = (await review(async_client, headers, job)).json()
    assert slot_on(view, SUNDAY)["marks"][0]["accepted"] is True
    assert slot_on(view, THURSDAY)["marks"][0]["accepted"] is False
    assert view["state"] == "open" and view["version"] == 1

    rest = await async_client.post(f"{JOBS}/{job['id']}/discard", headers=headers)

    assert rest.status_code == 200, rest.text
    assert rest.json()["proposal_state"] == "partly_accepted"
    assert await roster_count(test_db) == 1
    kinds = await test_db.execute(text("SELECT kind FROM schedule_job_events ORDER BY seq"))
    # The run's own steps first, then the decisions (008 FR-030).
    assert [k for (k,) in kinds] == ["created", "finished", "accepted", "discarded"]


async def test_the_rest_accepted_afterwards_closes_the_proposal(
    async_client: AsyncClient, seed_users: dict[str, User], test_db: AsyncSession
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await staffed_project(async_client, admin)
    job = await proposed(async_client, admin)
    await async_client.post(
        f"{JOBS}/{job['id']}/accept", json={"days": [SUNDAY.isoformat()]}, headers=admin
    )

    answer = await async_client.post(
        f"{JOBS}/{job['id']}/accept", json={"project_ids": [project["id"]]}, headers=admin
    )

    assert answer.status_code == 200, answer.text
    assert answer.json()["state"] == "accepted" and answer.json()["shifts_created"] == 4
    assert await roster_count(test_db) == 5
    again = await async_client.post(f"{JOBS}/{job['id']}/accept", headers=admin)
    assert again.json()["code"] == "schedule.proposal_closed"


async def test_a_stale_version_is_refused_and_changes_nothing(
    async_client: AsyncClient, seed_users: dict[str, User], test_db: AsyncSession
) -> None:
    admin = await headers_for(async_client, "admin")
    await staffed_project(async_client, admin)
    job = await proposed(async_client, admin)

    for path in ("accept", "discard", "edits/undo"):
        answer = await async_client.post(
            f"{JOBS}/{job['id']}/{path}", headers={**admin, "If-Match": '"7"'}
        )
        assert answer.status_code == 412, (path, answer.text)
        assert answer.json()["code"] == "schedule.version_stale"
    assert await roster_count(test_db) == 0


async def test_a_change_on_one_day_leaves_the_other_days_acceptable(
    async_client: AsyncClient, seed_users: dict[str, User], test_db: AsyncSession
) -> None:
    admin = await headers_for(async_client, "admin")
    await staffed_project(async_client, admin)
    job = await proposed(async_client, admin)
    tuesday = SUNDAY + timedelta(days=2)
    absent = await async_client.post(
        constraints(seed_users["employee"].id),
        json=body("absence", tuesday, absence_kind="sick"),
        headers=admin,
    )
    assert absent.status_code == 201, absent.text

    whole = await async_client.post(f"{JOBS}/{job['id']}/accept", headers=admin)
    assert whole.json()["code"] == "schedule.proposal_out_of_date"

    sunday = await async_client.post(
        f"{JOBS}/{job['id']}/accept", json={"days": [SUNDAY.isoformat()]}, headers=admin
    )
    assert sunday.status_code == 200, sunday.text

    refused = await async_client.post(
        f"{JOBS}/{job['id']}/accept", json={"days": [tuesday.isoformat()]}, headers=admin
    )
    assert refused.status_code == 409, refused.text
    problem = refused.json()
    assert problem["code"] == "schedule.part_out_of_date"
    assert [(e["date"], e["code"]) for e in problem["errors"]] == [
        (tuesday.isoformat(), "warnings.absence")
    ]
    assert await roster_count(test_db) == 1


async def test_a_rule_break_needs_a_reason_which_becomes_an_override(
    async_client: AsyncClient, seed_users: dict[str, User], test_db: AsyncSession
) -> None:
    admin = await headers_for(async_client, "admin")
    await staffed_project(async_client, admin)
    job = await proposed(async_client, admin)
    dana = await employee(async_client, admin, "Dana Levi")
    blocked = await async_client.post(
        constraints(dana), json=body("blocked", WEDNESDAY), headers=admin
    )
    assert blocked.status_code == 201, blocked.text
    key = slot_on((await review(async_client, admin, job)).json(), WEDNESDAY)["key"]
    edit = {"op": "assign", "key": key, "user_id": dana}

    refused = await async_client.post(f"{JOBS}/{job['id']}/edits", json=edit, headers=admin)

    assert refused.status_code == 422, refused.text
    assert refused.json()["code"] == "schedule.override_reason_required"
    assert {e["code"] for e in refused.json()["errors"]} >= {"warnings.blocked"}

    kept = await async_client.post(
        f"{JOBS}/{job['id']}/edits",
        json={**edit, "override_reason": "Dana offered to cover"},
        headers={**admin, "If-Match": '"0"'},
    )

    assert kept.status_code == 200, kept.text
    answer = kept.json()
    assert kept.headers["etag"] == '"1"'
    placed = slot_on(answer["proposal"], WEDNESDAY)["people"]
    dana_there = next(p for p in placed if p["user_id"] == dana)
    assert dana_there["by"] == "person"
    assert "warnings.blocked" in dana_there["override"]["codes"]
    assert any(w["code"] == "warnings.blocked" and w["excused"] for w in answer["warnings"])

    accepted = await async_client.post(
        f"{JOBS}/{job['id']}/accept", json={"days": [WEDNESDAY.isoformat()]}, headers=admin
    )
    assert accepted.status_code == 200, accepted.text
    reasons = await test_db.execute(text("SELECT warning_code, reason FROM rule_overrides"))
    assert ("warnings.blocked", "Dana offered to cover") in [tuple(r) for r in reasons]
    sources = await test_db.execute(
        text("SELECT source FROM assignments WHERE user_id = :u"), {"u": dana}
    )
    assert [s for (s,) in sources] == ["manual"]


async def test_edits_are_undone_one_at_a_time(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    await staffed_project(async_client, admin)
    job = await proposed(async_client, admin)
    me = str(seed_users["employee"].id)
    view = (await review(async_client, admin, job)).json()
    monday, tuesday = (slot_on(view, SUNDAY + timedelta(days=d))["key"] for d in (1, 2))
    await async_client.post(
        f"{JOBS}/{job['id']}/edits",
        json={"op": "unassign", "key": monday, "user_id": me},
        headers=admin,
    )
    await async_client.post(
        f"{JOBS}/{job['id']}/edits",
        json={"op": "unassign", "key": tuesday, "user_id": me},
        headers=admin,
    )

    first = await async_client.post(f"{JOBS}/{job['id']}/edits/undo", headers=admin)

    assert first.status_code == 200, first.text
    after = first.json()
    assert [p["user_id"] for p in slot_on(after, SUNDAY + timedelta(days=2))["people"]] == [me]
    assert slot_on(after, SUNDAY + timedelta(days=1))["people"] == []
    second = await async_client.post(f"{JOBS}/{job['id']}/edits/undo", headers=admin)
    assert slot_on(second.json(), SUNDAY + timedelta(days=1))["people"][0]["user_id"] == me
    third = await async_client.post(f"{JOBS}/{job['id']}/edits/undo", headers=admin)
    assert third.status_code == 409 and third.json()["code"] == "schedule.nothing_to_undo"


async def test_two_edits_on_one_version_the_second_is_stale(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    await staffed_project(async_client, admin)
    job = await proposed(async_client, admin)
    me = str(seed_users["employee"].id)
    view = (await review(async_client, admin, job)).json()
    keys = [s["key"] for s in view["slots"]]
    sent = {**admin, "If-Match": '"0"'}

    one = await async_client.post(
        f"{JOBS}/{job['id']}/edits",
        json={"op": "unassign", "key": keys[0], "user_id": me},
        headers=sent,
    )
    two = await async_client.post(
        f"{JOBS}/{job['id']}/edits",
        json={"op": "unassign", "key": keys[1], "user_id": me},
        headers=sent,
    )

    assert one.status_code == 200 and two.status_code == 412


async def test_an_inactive_employee_cannot_be_placed(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    await staffed_project(async_client, admin)
    job = await proposed(async_client, admin)
    dana = await employee(async_client, admin, "Dana Levi")
    await async_client.post(f"/api/v1/employees/{dana}/deactivate", headers=admin)
    key = (await review(async_client, admin, job)).json()["slots"][0]["key"]

    answer = await async_client.post(
        f"{JOBS}/{job['id']}/edits",
        json={"op": "assign", "key": key, "user_id": dana, "override_reason": "anyway"},
        headers=admin,
    )

    assert answer.status_code == 422 and answer.json()["code"] == "warnings.employee_inactive"


async def test_a_shift_with_its_part_accepted_is_edited_on_the_roster(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    await staffed_project(async_client, admin)
    job = await proposed(async_client, admin)
    me = str(seed_users["employee"].id)
    await async_client.post(
        f"{JOBS}/{job['id']}/accept", json={"days": [SUNDAY.isoformat()]}, headers=admin
    )
    key = slot_on((await review(async_client, admin, job)).json(), SUNDAY)["key"]

    answer = await async_client.post(
        f"{JOBS}/{job['id']}/edits",
        json={"op": "unassign", "key": key, "user_id": me},
        headers=admin,
    )

    assert answer.status_code == 409 and answer.json()["code"] == "schedule.part_accepted"
    shown = slot_on((await review(async_client, admin, job)).json(), SUNDAY)
    assert shown["part_accepted"] is True  # the client reads this, never guesses it (R8)


async def test_an_edit_that_does_not_fit_the_proposal_is_refused(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    await staffed_project(async_client, admin)
    job = await proposed(async_client, admin)
    me = str(seed_users["employee"].id)
    key = (await review(async_client, admin, job)).json()["slots"][0]["key"]

    for edit in (
        {"op": "assign", "key": key, "user_id": me},
        {"op": "unassign", "key": "need:nowhere", "user_id": me},
        {"op": "move", "key": key, "user_id": me, "to_key": key},
    ):
        answer = await async_client.post(f"{JOBS}/{job['id']}/edits", json=edit, headers=admin)
        assert answer.status_code == 422, (edit, answer.text)
        assert answer.json()["code"] == "schedule.edit_invalid"


async def test_a_manager_of_only_some_of_the_run_cannot_review_or_edit_it(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    await staffed_project(async_client, admin, "Ours")
    await staffed_project(async_client, admin, "Theirs", managed=False)
    job = await proposed(async_client, admin)
    manager = await headers_for(async_client, "manager")
    me = str(seed_users["employee"].id)
    key = (await review(async_client, admin, job)).json()["slots"][0]["key"]

    read = await async_client.get(f"{JOBS}/{job['id']}/proposal", headers=manager)
    edit = await async_client.post(
        f"{JOBS}/{job['id']}/edits",
        json={"op": "unassign", "key": key, "user_id": me},
        headers=manager,
    )
    undo = await async_client.post(f"{JOBS}/{job['id']}/edits/undo", headers=manager)

    for answer in (read, edit, undo):
        assert answer.status_code == 403, answer.text
        assert answer.json()["code"] == "roster.project_not_managed"


@pytest.mark.parametrize("role", ["employee", "client"])
async def test_employees_and_clients_cannot_review_or_edit(
    async_client: AsyncClient, seed_users: dict[str, User], role: str
) -> None:
    admin = await headers_for(async_client, "admin")
    await staffed_project(async_client, admin)
    job = await proposed(async_client, admin)
    key = (await review(async_client, admin, job)).json()["slots"][0]["key"]
    headers = await headers_for(async_client, role)
    me = str(seed_users["employee"].id)

    answers = [
        await async_client.get(f"{JOBS}/{job['id']}/proposal", headers=headers),
        await async_client.post(
            f"{JOBS}/{job['id']}/edits",
            json={"op": "unassign", "key": key, "user_id": me},
            headers=headers,
        ),
        await async_client.post(f"{JOBS}/{job['id']}/edits/undo", headers=headers),
    ]

    assert [a.status_code for a in answers] == [403, 403, 403]


async def test_another_organisations_proposal_is_not_found(
    async_client: AsyncClient, seed_users: dict[str, User], other_org: OtherOrg
) -> None:
    admin = await headers_for(async_client, "admin")
    assert other_org.job is not None
    theirs = other_org.job.id

    answers = [
        await async_client.get(f"{JOBS}/{theirs}/proposal", headers=admin),
        await async_client.post(f"{JOBS}/{theirs}/edits/undo", headers=admin),
    ]

    assert [a.status_code for a in answers] == [404, 404]


async def test_a_proposal_with_nothing_to_change_is_accepted_and_closed(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    await staffed_project(async_client, admin)
    first = await proposed(async_client, admin)
    await async_client.post(f"{JOBS}/{first['id']}/accept", headers=admin)
    same = await proposed(async_client, admin)
    assert (await review(async_client, admin, same)).json()["counts"]["added"] == 0

    answer = await async_client.post(f"{JOBS}/{same['id']}/accept", headers=admin)
    named = await async_client.post(
        f"{JOBS}/{first['id']}/accept", json={"days": [SUNDAY.isoformat()]}, headers=admin
    )

    assert answer.status_code == 200, answer.text
    assert answer.json()["state"] == "accepted" and answer.json()["shifts_created"] == 0
    assert named.json()["code"] == "schedule.proposal_closed"


async def test_one_more_than_the_headcount_needs_a_reason(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    await staffed_project(async_client, admin)
    job = await proposed(async_client, admin)
    dana = await employee(async_client, admin, "Dana Levi")
    key = (await review(async_client, admin, job)).json()["slots"][0]["key"]

    answer = await async_client.post(
        f"{JOBS}/{job['id']}/edits",
        json={"op": "assign", "key": key, "user_id": dana},
        headers=admin,
    )

    assert answer.status_code == 422, answer.text
    assert [e["code"] for e in answer.json()["errors"]] == ["warnings.over_headcount"]


async def test_an_edit_checks_the_persons_shifts_in_projects_outside_the_run(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    await staffed_project(async_client, admin)
    job = await proposed(async_client, admin)
    dana = await employee(async_client, admin, "Dana Levi")
    other = await add_project(async_client, admin, "Elsewhere")
    made = (await add_shift(async_client, admin, other, WEDNESDAY))["shift"]
    assert (await assign(async_client, admin, made["id"], dana)).status_code == 201
    view = (await review(async_client, admin, job)).json()
    key = slot_on(view, WEDNESDAY)["key"]
    me = str(seed_users["employee"].id)
    await async_client.post(
        f"{JOBS}/{job['id']}/edits",
        json={"op": "unassign", "key": key, "user_id": me},
        headers=admin,
    )

    answer = await async_client.post(
        f"{JOBS}/{job['id']}/edits",
        json={"op": "assign", "key": key, "user_id": dana},
        headers=admin,
    )

    assert answer.status_code == 422, answer.text
    assert "warnings.overlap" in [e["code"] for e in answer.json()["errors"]]


async def test_an_empty_body_is_the_whole_proposal_and_still_refused_out_of_date(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    """R1: ``{}`` names nothing, as no body does, so an untouched stale proposal is refused."""
    admin = await headers_for(async_client, "admin")
    await staffed_project(async_client, admin)
    job = await proposed(async_client, admin)
    await async_client.post(
        constraints(seed_users["employee"].id),
        json=body("absence", SUNDAY + timedelta(days=2), absence_kind="sick"),
        headers=admin,
    )

    answer = await async_client.post(f"{JOBS}/{job['id']}/accept", json={}, headers=admin)

    assert answer.status_code == 409 and answer.json()["code"] == "schedule.proposal_out_of_date"


async def test_someone_deactivated_since_the_run_never_reaches_the_roster(
    async_client: AsyncClient, seed_users: dict[str, User], test_db: AsyncSession
) -> None:
    """R2: a part placing a person deactivated since is refused, naming why."""
    admin = await headers_for(async_client, "admin")
    await staffed_project(async_client, admin)
    job = await proposed(async_client, admin)
    await async_client.post(
        f"/api/v1/employees/{seed_users['employee'].id}/deactivate", headers=admin
    )

    answer = await async_client.post(
        f"{JOBS}/{job['id']}/accept", json={"days": [SUNDAY.isoformat()]}, headers=admin
    )

    assert answer.status_code == 409, answer.text
    assert "warnings.employee_inactive" in [e.get("code") for e in answer.json()["errors"]]
    assert await roster_count(test_db) == 0


async def test_a_deleted_account_cannot_be_placed(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    """R3: a soft-deleted employee is not staff any more."""
    admin = await headers_for(async_client, "admin")
    await staffed_project(async_client, admin)
    job = await proposed(async_client, admin)
    dana = await employee(async_client, admin, "Dana Levi")
    assert (
        await async_client.delete(f"/api/v1/employees/{dana}", headers=admin)
    ).status_code == 204
    key = (await review(async_client, admin, job)).json()["slots"][0]["key"]

    answer = await async_client.post(
        f"{JOBS}/{job['id']}/edits",
        json={"op": "assign", "key": key, "user_id": dana, "override_reason": "anyway"},
        headers=admin,
    )

    assert answer.status_code == 404, answer.text


async def test_a_manager_placed_by_hand_is_judged_by_all_their_shifts(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    """R4: a Manager with no open entry is no candidate, yet their other shifts still count."""
    admin = await headers_for(async_client, "admin")
    await staffed_project(async_client, admin)
    job = await proposed(async_client, admin)
    other = await add_project(async_client, admin, "Elsewhere")
    made = (await add_shift(async_client, admin, other, WEDNESDAY))["shift"]
    manager = str(seed_users["manager"].id)
    assert (await assign(async_client, admin, made["id"], manager)).status_code == 201
    key = slot_on((await review(async_client, admin, job)).json(), WEDNESDAY)["key"]

    answer = await async_client.post(
        f"{JOBS}/{job['id']}/edits",
        json={"op": "assign", "key": key, "user_id": manager},
        headers=admin,
    )

    assert answer.status_code == 422, answer.text
    assert "warnings.overlap" in [e["code"] for e in answer.json()["errors"]]


async def test_a_need_whose_headcount_changed_is_out_of_date(
    async_client: AsyncClient, seed_users: dict[str, User], test_db: AsyncSession
) -> None:
    """R5: the run's headcount is never written over a need changed since."""
    admin = await headers_for(async_client, "admin")
    project = await staffed_project(async_client, admin)
    job = await proposed(async_client, admin)
    items = [
        {"shift_type_id": project["types"]["morning"], "weekday": d, "headcount": 2}
        for d in range(7)
    ]
    changed = await async_client.put(
        f"/api/v1/projects/{project['id']}/requirements", json={"items": items}, headers=admin
    )
    assert changed.status_code == 200, changed.text

    answer = await async_client.post(
        f"{JOBS}/{job['id']}/accept", json={"days": [SUNDAY.isoformat()]}, headers=admin
    )

    assert answer.status_code == 409, answer.text
    assert [e["cause"] for e in answer.json()["errors"]] == ["changed"]
    assert await roster_count(test_db) == 0


async def test_a_manager_placed_by_hand_then_deactivated_is_refused_on_a_matching_fingerprint(
    async_client: AsyncClient, seed_users: dict[str, User], test_db: AsyncSession
) -> None:
    """R14: the fingerprint leaves out non-candidates, so an edited copy is always judged."""
    admin = await headers_for(async_client, "admin")
    await staffed_project(async_client, admin)
    job = await proposed(async_client, admin)
    manager = str(seed_users["manager"].id)
    key = slot_on((await review(async_client, admin, job)).json(), WEDNESDAY)["key"]
    placed = await async_client.post(
        f"{JOBS}/{job['id']}/edits",
        json={"op": "assign", "key": key, "user_id": manager, "override_reason": "covering"},
        headers=admin,
    )
    assert placed.status_code == 200, placed.text
    gone = await async_client.post(f"/api/v1/employees/{manager}/deactivate", headers=admin)
    assert gone.status_code == 200, gone.text

    answer = await async_client.post(f"{JOBS}/{job['id']}/accept", headers=admin)

    assert answer.status_code == 409, answer.text
    assert "warnings.employee_inactive" in [e.get("code") for e in answer.json()["errors"]]
    assert await roster_count(test_db) == 0


async def test_naming_one_person_of_a_changed_pair_takes_the_person_replaced(
    async_client: AsyncClient, seed_users: dict[str, User], test_db: AsyncSession
) -> None:
    """R15: accepting the newcomer alone never leaves both on a full draft."""
    admin = await headers_for(async_client, "admin")
    project = await staffed_project(async_client, admin)
    made = (await add_shift(async_client, admin, project, WEDNESDAY))["shift"]
    worker = str(seed_users["employee"].id)
    assert (await assign(async_client, admin, made["id"], worker)).status_code == 201
    job = await proposed(async_client, admin)
    dana = await employee(async_client, admin, "Dana Levi")
    key = f"draft:{made['id']}"
    swapped = await async_client.post(
        f"{JOBS}/{job['id']}/edits",
        json={"op": "unassign", "key": key, "user_id": worker},
        headers=admin,
    )
    assert swapped.status_code == 200, swapped.text
    added = await async_client.post(
        f"{JOBS}/{job['id']}/edits",
        json={"op": "assign", "key": key, "user_id": dana, "override_reason": "covering"},
        headers=admin,
    )
    assert added.status_code == 200, added.text

    answer = await async_client.post(
        f"{JOBS}/{job['id']}/accept",
        json={"assignments": [{"key": key, "user_id": dana}]},
        headers=admin,
    )

    assert answer.status_code == 200, answer.text
    on = await test_db.execute(
        text("SELECT user_id::text FROM assignments WHERE shift_id = :s"), {"s": made["id"]}
    )
    assert [u for (u,) in on] == [dana]


async def test_a_draft_whose_times_moved_is_out_of_date(
    async_client: AsyncClient, seed_users: dict[str, User], test_db: AsyncSession
) -> None:
    """R16: the same people on a draft at other times is a changed draft."""
    admin = await headers_for(async_client, "admin")
    project = await staffed_project(async_client, admin)
    made = (await add_shift(async_client, admin, project, WEDNESDAY))["shift"]
    worker = str(seed_users["employee"].id)
    assert (await assign(async_client, admin, made["id"], worker)).status_code == 201
    job = await proposed(async_client, admin)
    key = f"draft:{made['id']}"
    dropped = await async_client.post(
        f"{JOBS}/{job['id']}/edits",
        json={"op": "unassign", "key": key, "user_id": worker},
        headers=admin,
    )
    assert dropped.status_code == 200, dropped.text
    await test_db.execute(
        text(
            "UPDATE shifts SET starts_at = starts_at + interval '1 hour', "
            "ends_at = ends_at + interval '1 hour' WHERE id = :s"
        ),
        {"s": made["id"]},
    )
    await test_db.commit()

    answer = await async_client.post(
        f"{JOBS}/{job['id']}/accept", json={"days": [WEDNESDAY.isoformat()]}, headers=admin
    )

    assert answer.status_code == 409, answer.text
    assert [e["cause"] for e in answer.json()["errors"]] == ["changed"]
