"""A link being redeemed and a staff action on the same account never deadlock (T413).

Every path that touches both a user and that user's links takes the user's row first. When one path
spent the link and then updated the user while another updated the user and then cancelled the
link, PostgreSQL aborted one of them with a deadlock, which reached the caller as a server error.
Each test races the two sides for many rounds and fails on any 5xx or raised database error. Which
side wins does not matter here; other tests cover what each outcome must leave behind.

Covers: FR-246, FR-228.
"""

import logging
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.security import generate_refresh_token, hash_password
from hds.models.client import Client
from hds.models.user import User, UserRole
from tests.conftest import OutboxMailSender

ROUNDS = 100
PASSWORD = "Dev1234!"
NEW_PASSWORD = "A-long-password-1"
EMAIL = "employee@hds.test"
Race = Callable[..., Awaitable[list[Any]]]


async def add_link(db: AsyncSession, user_id: uuid.UUID, purpose: str) -> str:
    raw, token_hash = generate_refresh_token()
    await db.execute(
        text(
            "INSERT INTO invitation_tokens (id, user_id, token_hash, purpose, expires_at) "
            "VALUES (:i, :u, :h, :p, :x)"
        ),
        {
            "i": uuid.uuid4(),
            "u": user_id,
            "h": token_hash,
            "p": purpose,
            "x": datetime.now(UTC) + timedelta(hours=1),
        },
    )
    await db.commit()
    return raw


async def add_pending(db: AsyncSession, org_id: uuid.UUID, email: str) -> uuid.UUID:
    user_id = uuid.uuid4()
    await db.execute(
        text(
            "INSERT INTO users (id, organization_id, email, password_hash, full_name, role, "
            "is_active) VALUES (:i, :o, :e, 'h', 'Pending', CAST('employee' AS user_role), false)"
        ),
        {"i": user_id, "o": org_id, "e": email},
    )
    await db.commit()
    return user_id


async def admin_headers(client: AsyncClient) -> dict[str, str]:
    response = await client.post(
        "/auth/login", json={"email": "admin@hds.test", "password": PASSWORD}
    )
    assert response.status_code == 200, response.text
    return {"Authorization": f"Bearer {response.json()['access_token']}"}


async def add_active(db: AsyncSession, org_id: uuid.UUID, email: str) -> uuid.UUID:
    user_id = uuid.uuid4()
    await db.execute(
        text(
            "INSERT INTO users (id, organization_id, email, password_hash, full_name, role, "
            "is_active, activated_at) VALUES (:i, :o, :e, 'h', 'Active', "
            "CAST('employee' AS user_role), true, now())"
        ),
        {"i": user_id, "o": org_id, "e": email},
    )
    await db.commit()
    return user_id


def assert_no_failed_job(caplog: pytest.LogCaptureFixture, round_number: int) -> None:
    """A reset job runs after the answer, so its failure shows only in the log."""
    failures = [r.getMessage() for r in caplog.records if r.name == "hds.database"]
    assert failures == [], f"round {round_number}: {failures}"


def assert_no_server_error(results: list[Any], round_number: int) -> None:
    statuses = [getattr(r, "status_code", repr(r)[:200]) for r in results]
    assert all(isinstance(s, int) and s < 500 for s in statuses), (
        f"round {round_number}: {statuses}"
    )


async def test_a_reset_and_a_deactivation_of_one_account_never_deadlock(
    async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User], race: Race
) -> None:
    user_id = seed_users["employee"].id
    headers = await admin_headers(async_client)

    async def deactivate() -> Any:
        return await async_client.post(f"/api/v1/employees/{user_id}/deactivate", headers=headers)

    for n in range(ROUNDS):
        await test_db.execute(
            text("UPDATE users SET is_active = true WHERE id = :u"), {"u": user_id}
        )
        await test_db.commit()
        link = await add_link(test_db, user_id, "reset")

        async def reset(token: str = link) -> Any:
            return await async_client.post(
                "/auth/password-reset/confirm",
                json={"token": token, "new_password": NEW_PASSWORD},
            )

        assert_no_server_error(await race(reset, deactivate), n)


async def test_accepting_an_invitation_and_deleting_the_account_never_deadlock(
    async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User], race: Race
) -> None:
    org_id = seed_users["admin"].organization_id
    headers = await admin_headers(async_client)

    for n in range(ROUNDS):
        user_id = await add_pending(test_db, org_id, f"delete{n}@hds.test")
        link = await add_link(test_db, user_id, "invite")

        async def accept(token: str = link) -> Any:
            return await async_client.post(
                "/auth/invite/accept", json={"token": token, "password": NEW_PASSWORD}
            )

        async def delete(target: uuid.UUID = user_id) -> Any:
            return await async_client.delete(f"/api/v1/employees/{target}", headers=headers)

        assert_no_server_error(await race(accept, delete), n)


async def test_accepting_an_invitation_and_a_re_invitation_never_deadlock(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
    race: Race,
) -> None:
    org_id = seed_users["admin"].organization_id
    headers = await admin_headers(async_client)

    for n in range(ROUNDS):
        email = f"invitee{n}@hds.test"
        user_id = await add_pending(test_db, org_id, email)
        link = await add_link(test_db, user_id, "invite")
        role = "manager" if n % 2 == 0 else "employee"  # with and without a role change

        async def accept(token: str = link) -> Any:
            return await async_client.post(
                "/auth/invite/accept", json={"token": token, "password": NEW_PASSWORD}
            )

        async def invite(address: str = email, new_role: str = role) -> Any:
            return await async_client.post(
                "/auth/invite", json={"email": address, "role": new_role}, headers=headers
            )

        assert_no_server_error(await race(accept, invite), n)


async def test_a_reset_request_and_a_reset_confirmation_never_deadlock(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
    race: Race,
) -> None:
    user_id = seed_users["employee"].id

    async def request() -> Any:
        return await async_client.post("/auth/password-reset/request", json={"email": EMAIL})

    for n in range(ROUNDS):
        link = await add_link(test_db, user_id, "reset")

        async def confirm(token: str = link) -> Any:
            return await async_client.post(
                "/auth/password-reset/confirm",
                json={"token": token, "new_password": NEW_PASSWORD},
            )

        assert_no_server_error(await race(confirm, request), n)


async def test_a_reset_and_a_client_deactivation_never_deadlock(
    async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User], race: Race
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
    user_id = uuid.uuid4()
    test_db.add(
        User(
            id=user_id,
            organization_id=org_id,
            client_id=client_id,
            email="portal@hds.test",
            password_hash=hash_password(PASSWORD),
            full_name="Portal",
            role=UserRole.client,
            activated_at=datetime.now(UTC),
        )
    )
    await test_db.commit()
    headers = await admin_headers(async_client)

    async def deactivate() -> Any:
        return await async_client.patch(
            f"/api/v1/clients/{client_id}", json={"is_active": False}, headers=headers
        )

    for n in range(ROUNDS):
        await test_db.execute(
            text("UPDATE clients SET is_active = true WHERE id = :c"), {"c": client_id}
        )
        await test_db.execute(
            text("UPDATE users SET is_active = true WHERE id = :u"), {"u": user_id}
        )
        await test_db.commit()
        link = await add_link(test_db, user_id, "reset")

        async def reset(token: str = link) -> Any:
            return await async_client.post(
                "/auth/password-reset/confirm",
                json={"token": token, "new_password": NEW_PASSWORD},
            )

        assert_no_server_error(await race(reset, deactivate), n)


async def test_a_reset_job_and_a_deactivation_never_deadlock(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
    race: Race,
    caplog: pytest.LogCaptureFixture,
) -> None:
    user_id = seed_users["employee"].id
    headers = await admin_headers(async_client)

    async def request() -> Any:
        return await async_client.post("/auth/password-reset/request", json={"email": EMAIL})

    async def deactivate() -> Any:
        return await async_client.post(f"/api/v1/employees/{user_id}/deactivate", headers=headers)

    with caplog.at_level(logging.ERROR, logger="hds.database"):
        for n in range(ROUNDS):
            await test_db.execute(
                text("UPDATE users SET is_active = true WHERE id = :u"), {"u": user_id}
            )
            await test_db.commit()
            assert_no_server_error(await race(request, deactivate), n)
            assert_no_failed_job(caplog, n)


async def test_a_reset_job_and_deleting_the_account_never_deadlock(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
    race: Race,
    caplog: pytest.LogCaptureFixture,
) -> None:
    org_id = seed_users["admin"].organization_id
    headers = await admin_headers(async_client)

    with caplog.at_level(logging.ERROR, logger="hds.database"):
        for n in range(ROUNDS):
            email = f"gone{n}@hds.test"
            user_id = await add_active(test_db, org_id, email)

            async def request(address: str = email) -> Any:
                return await async_client.post(
                    "/auth/password-reset/request", json={"email": address}
                )

            async def delete(target: uuid.UUID = user_id) -> Any:
                return await async_client.delete(f"/api/v1/employees/{target}", headers=headers)

            assert_no_server_error(await race(request, delete), n)
            assert_no_failed_job(caplog, n)


async def test_a_reset_job_and_a_reset_confirmation_never_deadlock(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
    race: Race,
    caplog: pytest.LogCaptureFixture,
) -> None:
    user_id = seed_users["employee"].id

    async def request() -> Any:
        return await async_client.post("/auth/password-reset/request", json={"email": EMAIL})

    with caplog.at_level(logging.ERROR, logger="hds.database"):
        for n in range(ROUNDS):
            link = await add_link(test_db, user_id, "reset")

            async def confirm(token: str = link) -> Any:
                return await async_client.post(
                    "/auth/password-reset/confirm",
                    json={"token": token, "new_password": NEW_PASSWORD},
                )

            assert_no_server_error(await race(confirm, request), n)
            assert_no_failed_job(caplog, n)
