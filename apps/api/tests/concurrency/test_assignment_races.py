"""Assignments made at once: one employee once per shift, one key one write (T054).

Covers: 003 FR-115, FR-115a; research R8.
"""

import asyncio
from collections.abc import Awaitable, Callable
from datetime import date
from typing import Any

from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hds.models.user import User
from tests.roster_support import add_project, add_shift, assign, headers_for

ROUNDS = 15


def outcome(result: Any) -> str:
    status = getattr(result, "status_code", None)
    return str(status) if status is not None else repr(result)[:300]


async def test_two_identical_assignments_at_once_leave_one(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    race: Callable[..., Awaitable[list[Any]]],
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin)
    ann = seed_users["employee"].id

    for n in range(ROUNDS):
        shift = (await add_shift(async_client, admin, project, date(2027, 6, 1 + n)))["shift"]

        results = await race(
            lambda: assign(async_client, admin, shift["id"], ann),
            lambda: assign(async_client, admin, shift["id"], ann),
        )

        assert sorted(outcome(r) for r in results) == ["201", "409"], f"round {n}: {results}"


async def test_two_requests_with_one_key_at_once_write_once_and_answer_alike(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    race: Callable[..., Awaitable[list[Any]]],
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin)
    ann = seed_users["employee"].id

    for n in range(ROUNDS):
        shift = (await add_shift(async_client, admin, project, date(2027, 6, 1 + n)))["shift"]
        key = f"race-{n}"

        results = await race(
            lambda: assign(async_client, admin, shift["id"], ann, key=key),
            lambda: assign(async_client, admin, shift["id"], ann, key=key),
        )

        assert [outcome(r) for r in results] == ["201", "201"], f"round {n}: {results}"
        assert results[0].json() == results[1].json(), f"round {n}"
    await test_db.rollback()
    count = (await test_db.execute(text("SELECT count(*) FROM assignments"))).scalar_one()
    assert count == ROUNDS


async def test_two_clashing_assignments_of_one_employee_at_once_report_the_clash(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    race: Callable[..., Awaitable[list[Any]]],
) -> None:
    """Assignments of one employee run one after the other, so the second sees the first."""
    admin = await headers_for(async_client, "admin")
    first_project = await add_project(async_client, admin)
    second_project = await add_project(async_client, admin, "Day Watch")
    ann = seed_users["employee"].id

    for n in range(ROUNDS):
        day = date(2027, 7, 1 + n)
        a = (await add_shift(async_client, admin, first_project, day))["shift"]
        b = (await add_shift(async_client, admin, second_project, day))["shift"]

        results = await race(
            lambda: assign(async_client, admin, a["id"], ann),
            lambda: assign(async_client, admin, b["id"], ann),
        )

        assert [outcome(r) for r in results] == ["201", "201"], f"round {n}: {results}"
        reported = [w["code"] for r in results for w in r.json()["warnings"]]
        assert "warnings.overlap" in reported, f"round {n}: neither answer saw the clash"


async def test_an_assignment_waits_for_a_deactivation_in_progress_and_is_refused(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    session_pair: tuple[AsyncSession, AsyncSession],
) -> None:
    """The employee's row is locked before ``is_active`` is read, so a stale read cannot pass.

    Deactivating someone who is already assigned is allowed (they stay, with a warning); what
    must not happen is an assignment that read the employee as active while a deactivation was
    being committed.
    """
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin)
    shift = (await add_shift(async_client, admin, project, date(2027, 8, 2)))["shift"]
    holder, _ = session_pair
    await holder.execute(
        text("UPDATE users SET is_active = false WHERE id = :u"), {"u": seed_users["employee"].id}
    )

    pending = asyncio.create_task(
        assign(async_client, admin, shift["id"], seed_users["employee"].id)
    )
    await asyncio.sleep(0.5)
    waited = not pending.done()
    await holder.commit()
    answer = await asyncio.wait_for(pending, 10)

    assert waited, "the assignment did not wait for the deactivation in progress"
    assert answer.status_code == 409, answer.text
    assert answer.json()["code"] == "assignments.employee_inactive"


async def test_two_removals_of_one_assignment_at_once_answer_200_and_404(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    race: Callable[..., Awaitable[list[Any]]],
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin)
    ann = seed_users["employee"].id

    for n in range(ROUNDS):
        shift = (await add_shift(async_client, admin, project, date(2027, 9, 1 + n)))["shift"]
        created = (await assign(async_client, admin, shift["id"], ann)).json()
        url = f"/api/v1/assignments/{created['assignment']['id']}"

        results = await race(
            lambda: async_client.delete(url, headers=admin),
            lambda: async_client.delete(url, headers=admin),
        )

        assert sorted(outcome(r) for r in results) == ["200", "404"], f"round {n}: {results}"
