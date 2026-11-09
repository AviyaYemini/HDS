"""Two simultaneous date changes to one client cannot together store a contract that ends first.

Since the start date became editable (003 Group A), one request may move the start later while
another moves the end earlier. Each pair passes alone against the stored row; both together break
the rule. The row is therefore locked before the pair is judged, and every path that touches both
the client and its portal users takes the client row first, so the new lock cannot deadlock.

Covers: 003 FR-002, FR-225.
"""

import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, date, datetime
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.security import hash_password
from hds.models.client import Client
from hds.models.organisation import Organisation
from hds.models.user import User, UserRole
from hds.repositories.client import ClientRepository

ROUNDS = 30
LOCK_ROUNDS = 100
PORTAL_USERS = 8
PASSWORD = "Dev1234!"


async def test_a_new_start_and_a_new_end_sent_at_once_never_store_a_broken_pair(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    race: Callable[..., Awaitable[list[Any]]],
) -> None:
    client = Client(
        id=uuid.uuid4(),
        organisation_id=seed_users["admin"].organisation_id,
        name="Acme",
        contract_start=date(2026, 2, 1),
    )
    test_db.add(client)
    await test_db.commit()
    client_id = client.id
    login = await async_client.post(
        "/auth/login", json={"email": "admin@hds.test", "password": PASSWORD}
    )
    headers = {"Authorization": f"Bearer {login.json()['access_token']}"}
    url = f"/api/v1/clients/{client_id}"

    async def move_start() -> Any:
        return await async_client.patch(url, json={"contract_start": "2026-06-01"}, headers=headers)

    async def move_end() -> Any:
        return await async_client.patch(url, json={"contract_end": "2026-03-01"}, headers=headers)

    for n in range(ROUNDS):
        await test_db.execute(
            text(
                "UPDATE clients SET contract_start = '2026-02-01', contract_end = '2026-07-01' "
                "WHERE id = :c"
            ),
            {"c": client_id},
        )
        await test_db.commit()

        results = await race(move_start, move_end)

        statuses = sorted(getattr(r, "status_code", repr(r)) for r in results)
        stored = (
            await test_db.execute(
                text("SELECT contract_start, contract_end FROM clients WHERE id = :c"),
                {"c": client_id},
            )
        ).one()
        await test_db.commit()
        assert stored.contract_end >= stored.contract_start, f"round {n}: {tuple(stored)}"
        assert statuses == [200, 422], f"round {n}: {statuses}"


@pytest.mark.parametrize("other", ["deactivate", "delete"])
async def test_a_date_change_that_deactivates_never_deadlocks_with_a_deactivation_or_delete(
    other: str,
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    race: Callable[..., Awaitable[list[Any]]],
) -> None:
    org_id = seed_users["admin"].organisation_id
    client = Client(
        id=uuid.uuid4(), organisation_id=org_id, name="Acme", contract_start=date(2026, 2, 1)
    )
    test_db.add(client)
    await test_db.flush()
    client_id = client.id
    secret = hash_password(PASSWORD)
    for n in range(PORTAL_USERS):
        test_db.add(
            User(
                id=uuid.uuid4(),
                organisation_id=org_id,
                client_id=client_id,
                email=f"portal{n}@hds.test",
                password_hash=secret,
                full_name="Portal",
                role=UserRole.client,
                activated_at=datetime.now(UTC),
            )
        )
    await test_db.commit()
    login = await async_client.post(
        "/auth/login", json={"email": "admin@hds.test", "password": PASSWORD}
    )
    headers = {"Authorization": f"Bearer {login.json()['access_token']}"}
    url = f"/api/v1/clients/{client_id}"

    async def end_and_deactivate() -> Any:
        body = {"contract_end": "2026-12-31", "is_active": False}
        return await async_client.patch(url, json=body, headers=headers)

    async def deactivate() -> Any:
        return await async_client.patch(url, json={"is_active": False}, headers=headers)

    async def delete() -> Any:
        return await async_client.delete(url, headers=headers)

    rival = delete if other == "delete" else deactivate
    for n in range(LOCK_ROUNDS):
        await test_db.execute(
            text("UPDATE clients SET is_active = true, is_deleted = false WHERE id = :c"),
            {"c": client_id},
        )
        await test_db.execute(
            text("UPDATE users SET is_active = true WHERE client_id = :c"), {"c": client_id}
        )
        await test_db.commit()

        results = await race(end_and_deactivate, rival)

        statuses = [getattr(r, "status_code", repr(r)) for r in results]
        assert all(s in (200, 204, 404) for s in statuses), f"round {n}: {statuses}"


async def test_a_locked_read_returns_the_latest_dates_even_if_the_session_holds_older_ones(
    session_pair: tuple[AsyncSession, AsyncSession],
) -> None:
    """A rule judged under the lock sees the committed row, not what the session read before."""
    first, second = session_pair
    org = Organisation(id=uuid.uuid4(), name="Org", timezone="UTC")
    first.add(org)
    await first.flush()
    client = Client(
        id=uuid.uuid4(), organisation_id=org.id, name="Acme", contract_start=date(2026, 2, 1)
    )
    first.add(client)
    await first.commit()
    client_id, org_id = client.id, org.id
    repo = ClientRepository(first)
    assert (await repo.get_by_id(client_id, org_id)) is not None  # now held by the session

    await second.execute(
        text("UPDATE clients SET contract_start = '2026-06-01' WHERE id = :c"), {"c": client_id}
    )
    await second.commit()

    locked = await repo.get_by_id(client_id, org_id, for_update=True)
    seen = None if locked is None else locked.contract_start
    await first.rollback()

    assert seen == date(2026, 6, 1)
