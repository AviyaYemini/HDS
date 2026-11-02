"""Two staff members inviting the same new address at the same moment (T237, spec Story 2.7).

The unique email index lets only one insert win. The loser must not fail: it carries on as a
re-invitation of the account the winner just created. After every round there is exactly one
pending account, exactly one usable link (the later invitation replaces the earlier one), and both
callers received a success.

Covers: FR-206, FR-208.
"""

from collections.abc import Awaitable, Callable
from typing import Any

from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hds.models.user import User
from tests.conftest import OutboxMailSender

ROUNDS = 50


async def token(client: AsyncClient, email: str) -> dict[str, str]:
    response = await client.post("/auth/login", json={"email": email, "password": "Dev1234!"})
    assert response.status_code == 200, response.text
    return {"Authorization": f"Bearer {response.json()['access_token']}"}


async def test_simultaneous_invitations_leave_one_pending_account_and_one_usable_link(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
    race: Callable[..., Awaitable[list[Any]]],
) -> None:
    manager = await token(async_client, "manager@hds.test")
    admin = await token(async_client, "admin@hds.test")

    for n in range(ROUNDS):
        email = f"race{n}@hds.test"

        def invite_as(
            headers: dict[str, str], address: str = email
        ) -> Callable[[], Awaitable[Any]]:
            async def go() -> Any:
                return await async_client.post(
                    "/auth/invite", json={"email": address, "role": "employee"}, headers=headers
                )

            return go

        results = await race(invite_as(manager), invite_as(admin))

        statuses = [getattr(r, "status_code", repr(r)) for r in results]
        assert statuses == [202, 202], f"round {n}: {statuses}"
        accounts = (
            await test_db.execute(
                text("SELECT is_active, activated_at FROM users WHERE email = :e"), {"e": email}
            )
        ).all()
        assert len(accounts) == 1, f"round {n}: {len(accounts)} accounts"
        assert accounts[0].is_active is False and accounts[0].activated_at is None
        usable = (
            await test_db.execute(
                text(
                    "SELECT count(*) FROM invitation_tokens t JOIN users u ON u.id = t.user_id "
                    "WHERE u.email = :e AND t.purpose = 'invite' AND t.used_at IS NULL"
                ),
                {"e": email},
            )
        ).scalar_one()
        assert usable == 1, f"round {n}: {usable} usable links"
    assert len(outbox.messages) == 2 * ROUNDS
