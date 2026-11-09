"""An invitation completes only a pending account (T235, SC-204, spec Story 2).

The paths from the code review, run through the real application on PostgreSQL: a leftover link
must not bring a deactivated person back, replace an active person's password or end their
sessions, and must not reach into another organisation. Where a rule is enforced twice (links are
cancelled when an account changes state, and redemption also checks the account's state), a test
removes the first guard by hand so the second one is shown to hold on its own.

Covers: FR-205, FR-206, FR-207, SC-204.
"""

import hashlib
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from httpx import AsyncClient, Response
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hds.models.user import User
from tests.conftest import OutboxMailSender
from tests.integration.conftest import OtherOrg

NEW_PASSWORD = "A-fresh-password-1"


async def login(client: AsyncClient, email: str, password: str = "Dev1234!") -> dict[str, Any]:
    response = await client.post("/auth/login", json={"email": email, "password": password})
    assert response.status_code == 200, response.text
    body: dict[str, Any] = response.json()
    return body


async def auth(client: AsyncClient, email: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {(await login(client, email))['access_token']}"}


async def invite(
    client: AsyncClient, headers: dict[str, str], email: str, role: str = "employee"
) -> Response:
    return await client.post("/auth/invite", json={"email": email, "role": role}, headers=headers)


async def row(db: AsyncSession, email: str) -> Any:
    """The account as stored, read fresh (never from the session's identity map)."""
    result = await db.execute(
        text(
            "SELECT id, organisation_id, role::text AS role, is_active, activated_at, "
            "password_hash FROM users WHERE email = :e"
        ),
        {"e": email},
    )
    return result.one()


async def unused_links(db: AsyncSession, email: str) -> int:
    """How many links of the account could still be redeemed (not used, not cancelled)."""
    result = await db.execute(
        text(
            "SELECT count(*) FROM invitation_tokens t JOIN users u ON u.id = t.user_id "
            "WHERE u.email = :e AND t.used_at IS NULL"
        ),
        {"e": email},
    )
    return int(result.scalar_one())


async def sql(db: AsyncSession, statement: str, **params: Any) -> None:
    await db.execute(text(statement), params)
    await db.commit()


async def test_a_leftover_link_cannot_reactivate_a_deactivated_account(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
) -> None:
    """The state check alone: the link is still unused, but the account is deactivated."""
    manager = await auth(async_client, "manager@hds.test")
    assert (await invite(async_client, manager, "gone@hds.test")).status_code == 202
    token = outbox.last_token_for("gone@hds.test")
    before = await row(test_db, "gone@hds.test")
    # Simulate legacy data: the account was active once and then deactivated, link left unused.
    await sql(test_db, "UPDATE users SET activated_at = now() WHERE email = :e", e="gone@hds.test")

    response = await async_client.post(
        "/auth/invite/accept", json={"token": token, "password": NEW_PASSWORD}
    )

    assert response.status_code == 401
    assert response.json()["code"] == "auth.token_used"
    after = await row(test_db, "gone@hds.test")
    assert after.is_active is False
    assert after.password_hash == before.password_hash  # nothing was changed


async def test_a_manager_cannot_invite_a_deactivated_admin_back(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
) -> None:
    await sql(
        test_db,
        "UPDATE users SET is_active = false, activated_at = now() WHERE email = :e",
        e="admin@hds.test",
    )
    manager = await auth(async_client, "manager@hds.test")

    response = await invite(async_client, manager, "admin@hds.test")

    assert response.status_code == 409
    assert response.json()["code"] == "invitations.account_deactivated"
    assert outbox.messages == []  # no link was sent to the deactivated Admin's mailbox
    assert (await row(test_db, "admin@hds.test")).is_active is False


async def test_a_leftover_invitation_link_does_not_change_an_active_users_password_or_sessions(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
) -> None:
    manager = await auth(async_client, "manager@hds.test")
    await invite(async_client, manager, "newhire@hds.test")
    first = outbox.last_token_for("newhire@hds.test")
    assert (
        await async_client.post(
            "/auth/invite/accept", json={"token": first, "password": NEW_PASSWORD}
        )
    ).status_code == 204
    session = await login(async_client, "newhire@hds.test", NEW_PASSWORD)
    leftover = "a-leftover-invitation-token-from-older-data"
    await sql(
        test_db,
        "INSERT INTO invitation_tokens (id, user_id, token_hash, purpose, expires_at) "
        "SELECT :i, id, :h, 'invite', :x FROM users WHERE email = 'newhire@hds.test'",
        i=uuid.uuid4(),
        h=hashlib.sha256(leftover.encode()).hexdigest(),
        x=datetime.now(UTC) + timedelta(days=3),
    )

    response = await async_client.post(
        "/auth/invite/accept", json={"token": leftover, "password": "Attacker-chosen-pw-9"}
    )

    assert response.status_code == 401
    assert response.json()["code"] == "auth.token_used"
    assert (await login(async_client, "newhire@hds.test", NEW_PASSWORD))["access_token"]
    refreshed = await async_client.post(
        "/auth/refresh", json={"refresh_token": session["refresh_token"]}
    )
    assert refreshed.status_code == 200  # the existing session was not ended


async def test_re_inviting_a_pending_account_replaces_the_earlier_link_and_sets_the_role(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
) -> None:
    manager = await auth(async_client, "manager@hds.test")
    await invite(async_client, manager, "twice@hds.test", "employee")
    first = outbox.last_token_for("twice@hds.test")
    assert (await invite(async_client, manager, "twice@hds.test", "manager")).status_code == 202
    second = outbox.last_token_for("twice@hds.test")
    assert first != second

    stale = await async_client.post(
        "/auth/invite/accept", json={"token": first, "password": NEW_PASSWORD}
    )
    assert stale.status_code == 401 and stale.json()["code"] == "auth.token_used"
    fresh = await async_client.post(
        "/auth/invite/accept", json={"token": second, "password": NEW_PASSWORD}
    )
    assert fresh.status_code == 204
    account = await row(test_db, "twice@hds.test")
    assert account.role == "manager" and account.is_active is True
    assert account.activated_at is not None


async def test_a_manager_cannot_re_invite_a_pending_account_as_admin(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
) -> None:
    manager = await auth(async_client, "manager@hds.test")
    await invite(async_client, manager, "pend@hds.test", "employee")
    sent = len(outbox.messages)

    response = await invite(async_client, manager, "pend@hds.test", "admin")

    assert response.status_code == 403
    assert response.json()["code"] == "users.admin_required"
    assert (await row(test_db, "pend@hds.test")).role == "employee"
    assert len(outbox.messages) == sent


async def test_an_address_pending_in_another_organisation_is_untouched(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    other_org: OtherOrg,
    outbox: OutboxMailSender,
) -> None:
    """Email is unique across the deployment, but one organisation must not edit another's."""
    token = "other-organisation-pending-link"
    await sql(
        test_db,
        "INSERT INTO users (id, organisation_id, email, password_hash, full_name, role, "
        "is_active) VALUES (:i, :o, 'pending@other.test', 'h', 'Pending', "
        "CAST('employee' AS user_role), false)",
        i=uuid.uuid4(),
        o=other_org.org.id,
    )
    await sql(
        test_db,
        "INSERT INTO invitation_tokens (id, user_id, token_hash, purpose, expires_at) "
        "SELECT :i, id, :h, 'invite', :x FROM users WHERE email = 'pending@other.test'",
        i=uuid.uuid4(),
        h=hashlib.sha256(token.encode()).hexdigest(),
        x=datetime.now(UTC) + timedelta(days=3),
    )
    manager = await auth(async_client, "manager@hds.test")

    response = await invite(async_client, manager, "pending@other.test", "manager")

    assert response.status_code == 409
    assert response.json()["code"] == "users.duplicate_email"
    assert (await row(test_db, "pending@other.test")).role == "employee"
    assert outbox.messages == []
    accepted = await async_client.post(
        "/auth/invite/accept", json={"token": token, "password": NEW_PASSWORD}
    )
    assert accepted.status_code == 204  # the other organisation's own link still works


async def test_deactivating_through_the_api_cancels_an_outstanding_reset_link(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
) -> None:
    """Links are cancelled when an account is deactivated, and the state check backs that up."""
    manager = await auth(async_client, "manager@hds.test")
    await async_client.post("/auth/password-reset/request", json={"email": "employee@hds.test"})
    link = outbox.last_token_for("employee@hds.test")
    employee_id = (await row(test_db, "employee@hds.test")).id
    before = (await row(test_db, "employee@hds.test")).password_hash

    deactivated = await async_client.post(
        f"/api/v1/employees/{employee_id}/deactivate", headers=manager
    )
    assert deactivated.status_code == 200
    # Deactivation itself cancels the link: nothing outstanding is left to guard against.
    assert await unused_links(test_db, "employee@hds.test") == 0

    first = await async_client.post(
        "/auth/password-reset/confirm", json={"token": link, "new_password": NEW_PASSWORD}
    )
    assert first.status_code == 401 and first.json()["code"] == "auth.token_used"
    # Remove the first guard by hand: un-cancel the link. The state check must still refuse it.
    await sql(test_db, "UPDATE invitation_tokens SET used_at = NULL")
    second = await async_client.post(
        "/auth/password-reset/confirm", json={"token": link, "new_password": NEW_PASSWORD}
    )
    assert second.status_code == 401 and second.json()["code"] == "auth.token_used"
    assert (await row(test_db, "employee@hds.test")).password_hash == before


async def test_a_portal_user_of_a_deactivated_client_cannot_return_by_a_link(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
) -> None:
    manager = await auth(async_client, "manager@hds.test")
    client = await async_client.post(
        "/api/v1/clients",
        json={"name": "Acme Ltd", "contract_start": "2026-01-01"},
        headers=manager,
    )
    assert client.status_code == 201, client.text
    client_id = client.json()["id"]
    created = await async_client.post(
        f"/api/v1/clients/{client_id}/users",
        json={
            "email": "portal@acme.test",
            "full_name": "Portal",
            "initial_password": "Portal-password-1",
        },
        headers=manager,
    )
    assert created.status_code == 201, created.text
    await async_client.post("/auth/password-reset/request", json={"email": "portal@acme.test"})
    link = outbox.last_token_for("portal@acme.test")

    deactivated = await async_client.patch(
        f"/api/v1/clients/{client_id}", json={"is_active": False}, headers=manager
    )
    assert deactivated.status_code == 200
    assert await unused_links(test_db, "portal@acme.test") == 0  # cancelled with the client

    redeemed = await async_client.post(
        "/auth/password-reset/confirm", json={"token": link, "new_password": NEW_PASSWORD}
    )
    assert redeemed.status_code == 401
    assert (await row(test_db, "portal@acme.test")).is_active is False
    signin = await async_client.post(
        "/auth/login", json={"email": "portal@acme.test", "password": NEW_PASSWORD}
    )
    assert signin.status_code == 401


async def test_deactivating_a_client_keeps_activation_times_and_leaves_pending_users_alone(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
) -> None:
    """The bulk update keeps an existing activation time and never writes one (FR-226)."""
    manager = await auth(async_client, "manager@hds.test")
    client = await async_client.post(
        "/api/v1/clients",
        json={"name": "Beta Ltd", "contract_start": "2026-01-01"},
        headers=manager,
    )
    client_id = client.json()["id"]
    created = await async_client.post(
        f"/api/v1/clients/{client_id}/users",
        json={
            "email": "kept@beta.test",
            "full_name": "Kept",
            "initial_password": "Portal-password-1",
        },
        headers=manager,
    )
    assert created.status_code == 201
    before = (await row(test_db, "kept@beta.test")).activated_at
    assert before is not None
    # A pending portal user cannot be created through the API; build one the way a legacy row is.
    await sql(
        test_db,
        "INSERT INTO users (id, organisation_id, email, password_hash, full_name, role, "
        "is_active, client_id) SELECT :i, organisation_id, 'pending@beta.test', 'h', 'Pending', "
        "CAST('client' AS user_role), false, CAST(:c AS uuid) FROM users "
        "WHERE email = 'kept@beta.test'",
        i=uuid.uuid4(),
        c=client_id,
    )

    deactivated = await async_client.patch(
        f"/api/v1/clients/{client_id}", json={"is_active": False}, headers=manager
    )
    assert deactivated.status_code == 200

    kept = await row(test_db, "kept@beta.test")
    pending = await row(test_db, "pending@beta.test")
    assert kept.is_active is False and kept.activated_at == before  # an existing time is kept
    assert pending.is_active is False and pending.activated_at is None  # it stays pending
