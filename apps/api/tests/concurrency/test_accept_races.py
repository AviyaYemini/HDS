"""Accepting a proposal at the same moment as something else (004 T021).

Covers: 004 FR-024, FR-025.
"""

from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any

from httpx import AsyncClient

from hds.models.user import User
from tests.roster_support import body, constraints, headers_for
from tests.schedule_support import JOBS, SUNDAY, ask, staffed_project, work

ROUNDS = 8


def outcome(result: Any) -> str:
    status = getattr(result, "status_code", None)
    if status is None:
        return repr(result)[:300]
    code = result.json().get("code") if status >= 400 else None
    return f"{status} {code}" if code else str(status)


async def fresh_proposal(client: AsyncClient, headers: dict[str, str]) -> str:
    """Clear the week's drafts, then propose Sunday to Thursday again; the job's id."""
    for shift in (
        await client.get("/api/v1/shifts?from=2027-06-06&to=2027-06-12", headers=headers)
    ).json()["items"]:
        await client.delete(f"/api/v1/shifts/{shift['id']}", headers=headers)
    job = (await ask(client, headers, SUNDAY, SUNDAY + timedelta(days=4))).json()
    await work()
    return str(job["id"])


async def test_two_accepts_at_once_store_the_proposal_once(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    race: Callable[..., Awaitable[list[Any]]],
) -> None:
    admin = await headers_for(async_client, "admin")
    manager = await headers_for(async_client, "manager")
    await staffed_project(async_client, admin)

    for n in range(ROUNDS):
        job = await fresh_proposal(async_client, admin)
        url = f"{JOBS}/{job}/accept"

        results = await race(
            lambda: async_client.post(url, headers=admin),
            lambda: async_client.post(url, headers=manager),
        )

        assert sorted(outcome(r) for r in results) == [
            "200",
            "409 schedule.proposal_closed",
        ], f"round {n}: {results}"
        shifts = (
            await async_client.get("/api/v1/shifts?from=2027-06-06&to=2027-06-12", headers=admin)
        ).json()["items"]
        assert len(shifts) == 5, f"round {n}: {len(shifts)} shifts"


async def test_an_accept_racing_an_absence_is_either_first_or_out_of_date(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    race: Callable[..., Awaitable[list[Any]]],
) -> None:
    admin = await headers_for(async_client, "admin")
    await staffed_project(async_client, admin)
    employee = seed_users["employee"].id

    for n in range(ROUNDS):
        for entry in (await async_client.get(constraints(employee), headers=admin)).json()["items"]:
            await async_client.delete(f"/api/v1/constraints/{entry['id']}", headers=admin)
        job = await fresh_proposal(async_client, admin)
        sick = body("absence", SUNDAY + timedelta(days=n % 5), absence_kind="sick")

        accept, absence = await race(
            lambda: async_client.post(f"{JOBS}/{job}/accept", headers=admin),
            lambda: async_client.post(constraints(employee), json=sick, headers=admin),
        )

        assert outcome(absence) == "201", f"round {n}: {absence}"
        assert outcome(accept) in ("200", "409 schedule.proposal_out_of_date"), (
            f"round {n}: {accept}"
        )


async def test_an_accept_racing_a_publish_never_touches_a_published_shift(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    race: Callable[..., Awaitable[list[Any]]],
) -> None:
    """Review finding: publishing could slip in between accept's check and its writes."""
    admin = await headers_for(async_client, "admin")
    project = await staffed_project(async_client, admin)
    monday = SUNDAY + timedelta(days=1)

    for n in range(ROUNDS):
        await fresh_proposal(async_client, admin)  # clears the week's drafts
        await async_client.post(
            "/api/v1/shifts",
            json={
                "project_id": project["id"],
                "shift_type_id": project["types"]["afternoon"],
                "date": monday.isoformat(),
                "headcount": 1,
            },
            headers=admin,
        )
        job = (await ask(async_client, admin, SUNDAY, SUNDAY + timedelta(days=4))).json()["id"]
        await work()

        accept, publish = await race(
            lambda: async_client.post(f"{JOBS}/{job}/accept", headers=admin),
            lambda: async_client.post(
                f"/api/v1/projects/{project['id']}/publish",
                json={"from": monday.isoformat(), "to": monday.isoformat()},
                headers=admin,
            ),
        )

        assert outcome(publish) == "200", f"round {n}: {publish}"
        assert outcome(accept) in ("200", "409 schedule.proposal_out_of_date"), (
            f"round {n}: {accept}"
        )
        if outcome(accept) == "200":
            # Accepted first: its drafts were in place before publishing saw them.
            continue
        shifts = (
            await async_client.get("/api/v1/shifts?from=2027-06-06&to=2027-06-12", headers=admin)
        ).json()["items"]
        published = [s for s in shifts if s["status"] == "published"]
        assert all(s["assignments"] == [] for s in published), f"round {n}: {published}"
