"""A change either fully happens or does not happen at all (T265, FR-210, SC-204).

Each test lets an action run through the real application on PostgreSQL, makes its LAST step fail,
and then reads the database through another connection: nothing the earlier steps did may be
visible. The same actions are then repeated without the failure, to show the failure left them
usable (a link not spent can still be spent).

Covers: FR-210.
"""

import hashlib
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.security import generate_refresh_token, hash_password
from hds.models.client import Client
from hds.models.user import User, UserRole
from hds.repositories.client import ClientRepository
from hds.repositories.refresh_token import RefreshTokenRepository
from tests.conftest import OutboxMailSender

PASSWORD = "Dev1234!"


class InjectedFailureError(Exception):
    """The failure the tests cause on purpose, so no real error can be mistaken for it."""


async def explode(*_args: Any, **_kwargs: Any) -> None:
    raise InjectedFailureError


async def sign_in(client: AsyncClient, email: str) -> dict[str, Any]:
    response = await client.post("/auth/login", json={"email": email, "password": PASSWORD})
    assert response.status_code == 200, response.text
    body: dict[str, Any] = response.json()
    return body


async def scalar(db: AsyncSession, sql: str, **params: Any) -> Any:
    """Read through the test's own connection, which sees only what was committed."""
    await db.rollback()
    return (await db.execute(text(sql), params)).scalar_one()


def hashed(raw: str) -> str:
    return hashlib.sha256(raw.encode()).hexdigest()


@pytest.fixture
def last_step_fails(monkeypatch: pytest.MonkeyPatch) -> Iterator[pytest.MonkeyPatch]:
    yield monkeypatch


async def test_a_failed_refresh_keeps_the_old_token_and_adds_no_new_one(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    last_step_fails: pytest.MonkeyPatch,
) -> None:
    session = await sign_in(async_client, "employee@hds.test")
    tokens_before = await scalar(test_db, "SELECT count(*) FROM refresh_tokens")
    last_step_fails.setattr(RefreshTokenRepository, "create", explode)  # the last step

    with pytest.raises(InjectedFailureError):
        await async_client.post("/auth/refresh", json={"refresh_token": session["refresh_token"]})

    revoked = await scalar(
        test_db,
        "SELECT revoked_at IS NOT NULL FROM refresh_tokens WHERE token_hash = :h",
        h=hashed(session["refresh_token"]),
    )
    assert revoked is False, "the old token was revoked although no new one was issued"
    assert await scalar(test_db, "SELECT count(*) FROM refresh_tokens") == tokens_before
    last_step_fails.undo()
    retry = await async_client.post(
        "/auth/refresh", json={"refresh_token": session["refresh_token"]}
    )
    assert retry.status_code == 200  # the user can simply try again


async def test_a_failed_password_reset_keeps_the_password_the_link_and_the_sessions(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
    last_step_fails: pytest.MonkeyPatch,
) -> None:
    session = await sign_in(async_client, "employee@hds.test")
    await async_client.post("/auth/password-reset/request", json={"email": "employee@hds.test"})
    link = outbox.last_token_for("employee@hds.test")
    old_hash = await scalar(
        test_db, "SELECT password_hash FROM users WHERE email = :e", e="employee@hds.test"
    )
    last_step_fails.setattr(RefreshTokenRepository, "revoke_all_for_user", explode)

    with pytest.raises(InjectedFailureError):
        await async_client.post(
            "/auth/password-reset/confirm", json={"token": link, "new_password": "A-new-password-1"}
        )

    assert (
        await scalar(
            test_db, "SELECT password_hash FROM users WHERE email = :e", e="employee@hds.test"
        )
        == old_hash
    ), "the password changed although the reset failed"
    assert (
        await scalar(
            test_db,
            "SELECT used_at IS NULL FROM invitation_tokens WHERE token_hash = :h",
            h=hashed(link),
        )
        is True
    ), "the link was spent although the reset failed"
    assert (
        await scalar(
            test_db,
            "SELECT revoked_at IS NULL FROM refresh_tokens WHERE token_hash = :h",
            h=hashed(session["refresh_token"]),
        )
        is True
    )
    last_step_fails.undo()
    again = await async_client.post(
        "/auth/password-reset/confirm", json={"token": link, "new_password": "A-new-password-1"}
    )
    assert again.status_code == 204  # the same link still works


async def add_portal_user_with_pending_link(
    db: AsyncSession, org_id: uuid.UUID
) -> tuple[Client, User, str]:
    """A client with an active portal user who also holds an unused password-reset link."""
    client = Client(
        id=uuid.uuid4(),
        organisation_id=org_id,
        name="Portal Client",
        contract_start=datetime.now(UTC).date(),
    )
    db.add(client)
    await db.flush()
    portal = User(
        id=uuid.uuid4(),
        organisation_id=org_id,
        client_id=client.id,
        email="portal@hds.test",
        password_hash=hash_password(PASSWORD),
        full_name="Portal",
        role=UserRole.client,
        activated_at=datetime.now(UTC),
    )
    db.add(portal)
    await db.flush()
    raw, token_hash = generate_refresh_token()
    await db.execute(
        text(
            "INSERT INTO invitation_tokens (id, user_id, token_hash, purpose, expires_at) "
            "VALUES (:i, :u, :h, 'reset', :x)"
        ),
        {
            "i": uuid.uuid4(),
            "u": portal.id,
            "h": token_hash,
            "x": datetime.now(UTC) + timedelta(hours=1),
        },
    )
    await db.commit()
    return client, portal, token_hash


async def test_a_failed_client_deactivation_leaves_the_portal_users_and_their_links_alone(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    last_step_fails: pytest.MonkeyPatch,
) -> None:
    client, portal, link_hash = await add_portal_user_with_pending_link(
        test_db, seed_users["admin"].organisation_id
    )
    portal_id, client_id = portal.id, client.id
    admin = await sign_in(async_client, "admin@hds.test")
    headers = {"Authorization": f"Bearer {admin['access_token']}"}
    last_step_fails.setattr(ClientRepository, "update", explode)  # after the portal users' step

    with pytest.raises(InjectedFailureError):
        await async_client.patch(
            f"/api/v1/clients/{client_id}", json={"is_active": False}, headers=headers
        )

    assert await scalar(test_db, "SELECT is_active FROM users WHERE id = :u", u=portal_id) is True
    assert (
        await scalar(
            test_db,
            "SELECT used_at IS NULL FROM invitation_tokens WHERE token_hash = :h",
            h=link_hash,
        )
        is True
    ), "the portal user's link was cancelled although the change failed"
    assert await scalar(test_db, "SELECT is_active FROM clients WHERE id = :c", c=client_id) is True


async def test_a_failed_role_change_keeps_the_role_and_the_sessions(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    last_step_fails: pytest.MonkeyPatch,
) -> None:
    session = await sign_in(async_client, "employee@hds.test")
    admin = await sign_in(async_client, "admin@hds.test")
    headers = {"Authorization": f"Bearer {admin['access_token']}"}
    last_step_fails.setattr(RefreshTokenRepository, "revoke_all_for_user", explode)

    with pytest.raises(InjectedFailureError):
        await async_client.patch(
            f"/api/v1/employees/{seed_users['employee'].id}",
            json={"role": "manager"},
            headers=headers,
        )

    role = await scalar(
        test_db, "SELECT role::text FROM users WHERE id = :u", u=seed_users["employee"].id
    )
    assert role == "employee", "the role changed although the change failed"
    assert (
        await scalar(
            test_db,
            "SELECT revoked_at IS NULL FROM refresh_tokens WHERE token_hash = :h",
            h=hashed(session["refresh_token"]),
        )
        is True
    )
