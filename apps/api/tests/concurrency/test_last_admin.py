"""The last active Admin survives two Admins acting on each other at the same moment (T270, SC-205).

An organisation with exactly two Admins is the dangerous case: each request counts two active
Admins, concludes the other one may go, and both succeed, leaving none. Each round puts the two
Admins back, then has each act on the other at once: deactivation in one test, demotion to Manager
in the other. At most one action may succeed and an active Admin must remain every time.

Covers: FR-211, SC-205.
"""

import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any

from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.security import hash_password
from hds.models.user import User, UserRole

ROUNDS = 100
PASSWORD = "Dev1234!"


async def second_admin(db: AsyncSession, first: User) -> User:
    other = User(
        id=uuid.uuid4(),
        organisation_id=first.organisation_id,
        email="admin2@hds.test",
        password_hash=hash_password(PASSWORD),
        full_name="Second Admin",
        role=UserRole.admin,
        activated_at=datetime.now(UTC),
    )
    db.add(other)
    await db.commit()
    return other


async def headers_for(client: AsyncClient, email: str) -> dict[str, str]:
    response = await client.post("/auth/login", json={"email": email, "password": PASSWORD})
    assert response.status_code == 200, response.text
    return {"Authorization": f"Bearer {response.json()['access_token']}"}


async def put_both_admins_back(db: AsyncSession, ids: list[uuid.UUID]) -> None:
    await db.execute(
        text(
            "UPDATE users SET is_active = true, role = CAST('admin' AS user_role) "
            "WHERE id = ANY(:ids)"
        ),
        {"ids": ids},
    )
    await db.commit()


async def active_admins(db: AsyncSession, ids: list[uuid.UUID]) -> int:
    result = await db.execute(
        text(
            "SELECT count(*) FROM users WHERE id = ANY(:ids) AND is_active "
            "AND role = CAST('admin' AS user_role) AND NOT is_deleted"
        ),
        {"ids": ids},
    )
    return int(result.scalar_one())


async def run_rounds(
    request: Callable[[AsyncClient, uuid.UUID, dict[str, str]], Awaitable[Any]],
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    race: Callable[..., Awaitable[list[Any]]],
) -> None:
    first = seed_users["admin"]
    other = await second_admin(test_db, first)
    ids = [first.id, other.id]
    headers = {
        first.id: await headers_for(async_client, first.email),
        other.id: await headers_for(async_client, other.email),
    }

    def acts_on(actor: User, target: User) -> Callable[[], Awaitable[Any]]:
        async def go() -> Any:
            return await request(async_client, target.id, headers[actor.id])

        return go

    for n in range(ROUNDS):
        await put_both_admins_back(test_db, ids)

        results = await race(acts_on(first, other), acts_on(other, first))

        statuses = [getattr(r, "status_code", repr(r)) for r in results]
        assert statuses.count(200) <= 1, f"round {n}: both Admins were removed ({statuses})"
        assert await active_admins(test_db, ids) >= 1, f"round {n}: no active Admin left"
        # Nothing but a clean refusal may come back for the one that lost.
        assert all(s in (200, 401, 403, 409) for s in statuses), f"round {n}: {statuses}"


async def test_two_admins_deactivating_each_other_never_leave_none(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    race: Callable[..., Awaitable[list[Any]]],
) -> None:
    async def deactivate(client: AsyncClient, target: uuid.UUID, headers: dict[str, str]) -> Any:
        return await client.post(f"/api/v1/employees/{target}/deactivate", headers=headers)

    await run_rounds(deactivate, async_client, test_db, seed_users, race)


async def test_two_admins_demoting_each_other_never_leave_none(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    race: Callable[..., Awaitable[list[Any]]],
) -> None:
    async def demote(client: AsyncClient, target: uuid.UUID, headers: dict[str, str]) -> Any:
        return await client.patch(
            f"/api/v1/employees/{target}", json={"role": "manager"}, headers=headers
        )

    await run_rounds(demote, async_client, test_db, seed_users, race)
