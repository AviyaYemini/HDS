"""A pending account cannot be deactivated, so it can never be activated by staff (T317, F03).

Reactivating a pending invitee is refused, but deactivating first used to stamp ``activated_at``,
after which reactivation succeeded: staff could activate an account that never accepted its
invitation, and the invitee could no longer accept or be re-invited.

Covers: FR-226, FR-220.
"""

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.security import generate_refresh_token, hash_password
from hds.models.client import Client
from hds.models.user import User, UserRole
from tests.conftest import OutboxMailSender

PASSWORD = "Dev1234!"
INVITEE = "invitee@hds.test"


async def headers_for(client: AsyncClient, email: str = "admin@hds.test") -> dict[str, str]:
    response = await client.post("/auth/login", json={"email": email, "password": PASSWORD})
    assert response.status_code == 200, response.text
    return {"Authorization": f"Bearer {response.json()['access_token']}"}


async def invite(client: AsyncClient, headers: dict[str, str]) -> None:
    sent = await client.post(
        "/auth/invite", json={"email": INVITEE, "role": "employee"}, headers=headers
    )
    assert sent.status_code == 202, sent.text


async def state(db: AsyncSession, email: str) -> Any:
    await db.rollback()
    row = await db.execute(
        text("SELECT id, is_active, activated_at FROM users WHERE email = :e"), {"e": email}
    )
    return row.one()


async def updated_at(db: AsyncSession, email: str) -> datetime:
    await db.rollback()
    value = await db.scalar(text("SELECT updated_at FROM users WHERE email = :e"), {"e": email})
    assert isinstance(value, datetime)
    return value


async def test_deactivating_a_pending_account_is_refused_and_changes_nothing(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
) -> None:
    headers = await headers_for(async_client)
    await invite(async_client, headers)
    account = await state(test_db, INVITEE)

    refused = await async_client.post(f"/api/v1/employees/{account.id}/deactivate", headers=headers)

    assert refused.status_code == 409, refused.text
    assert refused.json()["code"] == "users.pending_cannot_deactivate"
    after = await state(test_db, INVITEE)
    assert (after.is_active, after.activated_at) == (False, None)
    accepted = await async_client.post(
        "/auth/invite/accept",
        json={"token": outbox.last_token_for(INVITEE), "password": "A-long-password-1"},
    )
    assert accepted.status_code == 204, accepted.text  # the invitation still works


async def test_deactivate_then_reactivate_never_activates_a_pending_account(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
) -> None:
    headers = await headers_for(async_client)
    await invite(async_client, headers)
    account = await state(test_db, INVITEE)

    for _ in range(2):
        await async_client.post(f"/api/v1/employees/{account.id}/deactivate", headers=headers)
        await async_client.post(f"/api/v1/employees/{account.id}/reactivate", headers=headers)

    after = await state(test_db, INVITEE)
    assert (after.is_active, after.activated_at) == (False, None)


async def add_portal_users(db: AsyncSession, org_id: uuid.UUID) -> tuple[uuid.UUID, str]:
    """A client with one active portal user and one pending one holding a link."""
    client = Client(
        id=uuid.uuid4(),
        organization_id=org_id,
        name="Acme",
        contract_start=datetime.now(UTC).date(),
    )
    db.add(client)
    await db.flush()
    client_id = client.id
    db.add(
        User(
            id=uuid.uuid4(),
            organization_id=org_id,
            client_id=client_id,
            email="active.portal@hds.test",
            password_hash=hash_password(PASSWORD),
            full_name="Active",
            role=UserRole.client,
            activated_at=datetime.now(UTC),
        )
    )
    pending = User(
        id=uuid.uuid4(),
        organization_id=org_id,
        client_id=client_id,
        email="pending.portal@hds.test",
        password_hash=hash_password(PASSWORD),
        full_name="Pending",
        role=UserRole.client,
        is_active=False,
    )
    db.add(pending)
    await db.flush()
    raw, token_hash = generate_refresh_token()
    await db.execute(
        text(
            "INSERT INTO invitation_tokens (id, user_id, token_hash, purpose, expires_at) "
            "VALUES (:i, :u, :h, 'invite', :x)"
        ),
        {
            "i": uuid.uuid4(),
            "u": pending.id,
            "h": token_hash,
            "x": datetime.now(UTC) + timedelta(days=1),
        },
    )
    await db.commit()
    return client_id, raw


async def test_deactivating_a_client_skips_its_pending_portal_users(
    async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    client_id, link = await add_portal_users(test_db, seed_users["admin"].organization_id)
    headers = await headers_for(async_client)

    before = await updated_at(test_db, "pending.portal@hds.test")

    done = await async_client.patch(
        f"/api/v1/clients/{client_id}", json={"is_active": False}, headers=headers
    )

    assert done.status_code == 200, done.text
    assert (await state(test_db, "active.portal@hds.test")).is_active is False
    pending = await state(test_db, "pending.portal@hds.test")
    assert (pending.is_active, pending.activated_at) == (False, None)
    assert await updated_at(test_db, "pending.portal@hds.test") == before  # the row was not touched
    accepted = await async_client.post(
        "/auth/invite/accept", json={"token": link, "password": "A-long-password-1"}
    )
    assert accepted.status_code == 204, accepted.text  # the link was left alone
