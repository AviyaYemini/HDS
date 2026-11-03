"""Staff cannot reactivate an account that never accepted its invitation (T238, FR-220).

The database refuses an active account without an activation time, so reactivating a pending
account used to surface as the generic data-constraint error (422), which hides the cause. It must
be a clear 409 with its own code, the account must stay pending, and its invitation must still
work: only accepting the invitation activates a pending account.

Covers: FR-220, FR-015.
"""

from typing import Any

from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hds.models.user import User
from tests.conftest import OutboxMailSender


async def manager_headers(client: AsyncClient) -> dict[str, str]:
    response = await client.post(
        "/auth/login", json={"email": "manager@hds.test", "password": "Dev1234!"}
    )
    assert response.status_code == 200
    return {"Authorization": f"Bearer {response.json()['access_token']}"}


async def account(db: AsyncSession, email: str) -> Any:
    result = await db.execute(
        text("SELECT id, is_active, activated_at FROM users WHERE email = :e"), {"e": email}
    )
    return result.one()


async def test_reactivating_a_pending_account_is_a_clear_conflict_and_changes_nothing(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
) -> None:
    headers = await manager_headers(async_client)
    invited = await async_client.post(
        "/auth/invite", json={"email": "waiting@hds.test", "role": "employee"}, headers=headers
    )
    assert invited.status_code == 202
    pending = await account(test_db, "waiting@hds.test")
    assert pending.is_active is False and pending.activated_at is None

    response = await async_client.post(
        f"/api/v1/employees/{pending.id}/reactivate", headers=headers
    )

    assert response.status_code == 409, response.text
    body = response.json()
    assert body["code"] == "users.pending_cannot_reactivate"
    assert body["code"] != "validation.constraint_violation"  # never the generic constraint answer
    after = await account(test_db, "waiting@hds.test")
    assert after.is_active is False and after.activated_at is None  # still pending
    accepted = await async_client.post(
        "/auth/invite/accept",
        json={"token": outbox.last_token_for("waiting@hds.test"), "password": "A-fresh-password-1"},
    )
    assert accepted.status_code == 204  # its invitation still works


async def test_reactivating_a_deactivated_account_still_works(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
) -> None:
    headers = await manager_headers(async_client)
    employee = await account(test_db, "employee@hds.test")
    await async_client.post(f"/api/v1/employees/{employee.id}/deactivate", headers=headers)

    response = await async_client.post(
        f"/api/v1/employees/{employee.id}/reactivate", headers=headers
    )

    assert response.status_code == 200
    assert response.json()["is_active"] is True
