"""Best so far, Stop and the run's record, through the worker and the API (008 T045, T046).

Covers: 008 FR-027 to FR-031, SC-007, SC-009, FR-035. The worker writes a run's best so far while
it works and reads the stop flag; Stop with a result ends stopped with a proposal, Stop before any
result ends cancelled with none; Stop on a queued run cancels it at once and on a finished one is
409; the record lists created, finished, edits, overrides, accepts and discards in order with
who; Managers of only part of a run, Employees and Clients are refused, another organisation's
run is not found. The solve is a stand-in that waits on the stop flag, so the tests are steady.
"""

import asyncio
import uuid
from dataclasses import replace
from datetime import timedelta
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hds import worker
from hds.models.user import User
from hds.scheduling import generate
from tests.integration.conftest import OtherOrg
from tests.report_support import employee
from tests.roster_support import body, constraints, headers_for
from tests.schedule_support import JOBS, SUNDAY, ask, staffed_project, work

THURSDAY = SUNDAY + timedelta(days=4)


def waiting_solve(report_first: bool) -> Any:
    """A solve that reports progress (or not), then waits for Stop, then returns a real result."""

    async def solve(
        claim: Any, built: Any, limit: Any, progress: Any = None, should_stop: Any = None
    ) -> Any:
        if report_first and progress is not None:
            progress(1, sum(s.places for s in built.problem.slots))
        for _ in range(200):
            if should_stop is not None and should_stop():
                break
            await asyncio.sleep(0.02)
        result = await asyncio.to_thread(generate, built.problem, solver="greedy", time_limit=limit)
        return replace(result, stopped=True, optimal=False)

    return solve


async def queued(client: AsyncClient, headers: dict[str, str]) -> str:
    answer = await ask(client, headers, SUNDAY, THURSDAY)
    assert answer.status_code == 202, answer.text
    return str(answer.json()["id"])


async def ask_stop_soon(test_db: AsyncSession, job: str) -> None:
    """Set the stop flag the moment the worker has claimed the job."""
    for _ in range(200):
        await asyncio.sleep(0.02)
        state = (
            await test_db.execute(
                text("SELECT status FROM schedule_jobs WHERE id = :j"), {"j": job}
            )
        ).scalar_one()
        await test_db.commit()
        if state == "running":
            await test_db.execute(
                text("UPDATE schedule_jobs SET stop_requested_at = now() WHERE id = :j"), {"j": job}
            )
            await test_db.commit()
            return


async def test_stop_with_a_result_ends_stopped_with_its_proposal_and_the_progress_written(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    test_db: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    admin = await headers_for(async_client, "admin")
    await staffed_project(async_client, admin)
    job = await queued(async_client, admin)
    monkeypatch.setattr(worker, "FOLLOW_SECONDS", 0.05)
    monkeypatch.setattr(worker, "solve", waiting_solve(report_first=True))

    await asyncio.gather(work(), ask_stop_soon(test_db, job))
    done = (await async_client.get(f"{JOBS}/{job}", headers=admin)).json()

    assert done["status"] == "succeeded", done
    assert done["ended"] == "stopped" and done["stop_requested"] is True
    assert done["proposal"] is not None
    assert done["progress"]["needed"] == 5 and done["progress"]["filled"] is not None
    assert done["optimal"] is False


async def test_stop_before_any_result_cancels_with_no_proposal(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    test_db: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    admin = await headers_for(async_client, "admin")
    await staffed_project(async_client, admin)
    job = await queued(async_client, admin)
    monkeypatch.setattr(worker, "FOLLOW_SECONDS", 0.05)
    monkeypatch.setattr(worker, "solve", waiting_solve(report_first=False))

    await asyncio.gather(work(), ask_stop_soon(test_db, job))
    done = (await async_client.get(f"{JOBS}/{job}", headers=admin)).json()

    assert (done["status"], done["ended"], done["proposal"]) == ("cancelled", "cancelled", None)


async def test_stop_on_a_queued_run_cancels_it_and_on_a_finished_one_is_refused(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    await staffed_project(async_client, admin)
    waiting = await queued(async_client, admin)

    stopped = await async_client.post(f"{JOBS}/{waiting}/stop", headers=admin)

    assert stopped.status_code == 200, stopped.text
    assert (stopped.json()["status"], stopped.json()["ended"]) == ("cancelled", "cancelled")
    again = await async_client.post(f"{JOBS}/{waiting}/stop", headers=admin)
    assert (again.status_code, again.json()["code"]) == (409, "schedule.not_running")


async def test_the_record_lists_each_step_in_order_with_who(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    await staffed_project(async_client, admin)
    dana = await employee(async_client, admin, "Dana Record")
    blocked = await async_client.post(
        constraints(dana), json=body("blocked", SUNDAY), headers=admin
    )
    assert blocked.status_code == 201, blocked.text
    job = await queued(async_client, admin)
    await work()
    view = (await async_client.get(f"{JOBS}/{job}/proposal", headers=admin)).json()
    sunday = next(s for s in view["slots"] if s["date"] == SUNDAY.isoformat())
    edited = await async_client.post(
        f"{JOBS}/{job}/edits",
        json={"op": "assign", "key": sunday["key"], "user_id": dana, "override_reason": "asked to"},
        headers=admin,
    )
    assert edited.status_code == 200, edited.text
    accepted = await async_client.post(
        f"{JOBS}/{job}/accept", json={"days": [SUNDAY.isoformat()]}, headers=admin
    )
    assert accepted.status_code == 200, accepted.text
    discarded = await async_client.post(f"{JOBS}/{job}/discard", headers=admin)
    assert discarded.status_code == 200, discarded.text

    first = await async_client.get(f"{JOBS}/{job}/record", params={"limit": 3}, headers=admin)
    rest = await async_client.get(
        f"{JOBS}/{job}/record", params={"after": first.json()["next_after"]}, headers=admin
    )

    items = first.json()["items"] + rest.json()["items"]
    assert [i["kind"] for i in items] == [
        "created",
        "finished",
        "edited",
        "overridden",
        "accepted",
        "discarded",
    ]
    assert [i["seq"] for i in items] == sorted(i["seq"] for i in items)
    admin_id = str(seed_users["admin"].id)
    assert [i["actor_id"] for i in items if i["kind"] != "finished"] == [admin_id] * 5
    assert next(i for i in items if i["kind"] == "overridden")["body"]["reason"] == "asked to"


async def test_a_manager_of_only_part_of_the_run_cannot_stop_or_read_its_record(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    manager = await headers_for(async_client, "manager")
    await staffed_project(async_client, admin)
    await staffed_project(async_client, admin, "Not Theirs", managed=False)
    job = await queued(async_client, admin)

    answers = [
        await async_client.post(f"{JOBS}/{job}/stop", headers=manager),
        await async_client.get(f"{JOBS}/{job}/record", headers=manager),
    ]

    assert [(a.status_code, a.json()["code"]) for a in answers] == [
        (403, "roster.project_not_managed")
    ] * 2


@pytest.mark.parametrize("role", ["employee", "client"])
async def test_employees_and_clients_cannot_stop_or_read_a_record(
    async_client: AsyncClient, seed_users: dict[str, User], role: str
) -> None:
    admin = await headers_for(async_client, "admin")
    await staffed_project(async_client, admin)
    job = await queued(async_client, admin)
    headers = await headers_for(async_client, role)

    answers = [
        await async_client.post(f"{JOBS}/{job}/stop", headers=headers),
        await async_client.get(f"{JOBS}/{job}/record", headers=headers),
    ]

    assert [a.status_code for a in answers] == [403, 403]


async def test_another_organisations_run_is_not_found(
    async_client: AsyncClient, seed_users: dict[str, User], other_org: OtherOrg
) -> None:
    admin = await headers_for(async_client, "admin")
    assert other_org.job is not None
    theirs = other_org.job.id

    answers = [
        await async_client.post(f"{JOBS}/{theirs}/stop", headers=admin),
        await async_client.get(f"{JOBS}/{theirs}/record", headers=admin),
        await async_client.post(f"{JOBS}/{uuid.uuid4()}/stop", headers=admin),
    ]

    assert [a.status_code for a in answers] == [404, 404, 404]
