"""Two simultaneous changes to one client do not deadlock on its portal users (T313, F02).

Ending a client's portal sessions touches several user rows. If two requests took them in different
orders each could wait for a row the other holds, and PostgreSQL would abort one with a deadlock.
The rows are therefore always taken in ascending id order.

Covers: FR-225.
"""

import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any

from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.security import hash_password
from hds.models.client import Client
from hds.models.user import User, UserRole

ROUNDS = 50
PORTAL_USERS = 8
PASSWORD = "Dev1234!"


async def test_two_simultaneous_client_deactivations_never_deadlock(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    race: Callable[..., Awaitable[list[Any]]],
) -> None:
    org_id = seed_users["admin"].organization_id
    client = Client(
        id=uuid.uuid4(),
        organization_id=org_id,
        name="Acme",
        contract_start=datetime.now(UTC).date(),
    )
    test_db.add(client)
    await test_db.flush()
    client_id = client.id
    secret = hash_password(PASSWORD)
    for n in range(PORTAL_USERS):
        test_db.add(
            User(
                id=uuid.uuid4(),
                organization_id=org_id,
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

    async def deactivate() -> Any:
        return await async_client.patch(
            f"/api/v1/clients/{client_id}", json={"is_active": False}, headers=headers
        )

    for n in range(ROUNDS):
        await test_db.execute(
            text("UPDATE clients SET is_active = true WHERE id = :c"), {"c": client_id}
        )
        await test_db.execute(
            text("UPDATE users SET is_active = true WHERE client_id = :c"), {"c": client_id}
        )
        await test_db.commit()

        results = await race(deactivate, deactivate)

        statuses = [getattr(r, "status_code", repr(r)) for r in results]
        assert statuses == [200, 200], f"round {n}: {statuses}"
