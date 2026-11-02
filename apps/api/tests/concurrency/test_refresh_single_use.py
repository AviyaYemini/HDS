"""A refresh token works exactly once, even when used twice at the same moment (T250, SC-203).

Each round presents one fresh token twice, simultaneously, through the real application on
PostgreSQL. Exactly one request must win. The loser must be refused as revoked and must NOT end any
other session: a second use within a few seconds is an honest retry, not theft (research D3).

Covers: FR-208, SC-203.
"""

import hashlib
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.security import generate_refresh_token
from hds.models.user import User

ROUNDS = 100
SIMULTANEOUS = 2


async def add_token(db: AsyncSession, user_id: uuid.UUID) -> str:
    """Store a live refresh token for the user and return its raw value."""
    raw, token_hash = generate_refresh_token()
    await db.execute(
        text(
            "INSERT INTO refresh_tokens (id, user_id, token_hash, expires_at) "
            "VALUES (:i, :u, :h, :x)"
        ),
        {
            "i": uuid.uuid4(),
            "u": user_id,
            "h": token_hash,
            "x": datetime.now(UTC) + timedelta(days=7),
        },
    )
    await db.commit()
    return raw


async def live_tokens(db: AsyncSession, user_id: uuid.UUID) -> int:
    result = await db.execute(
        text("SELECT count(*) FROM refresh_tokens WHERE user_id = :u AND revoked_at IS NULL"),
        {"u": user_id},
    )
    return int(result.scalar_one())


async def test_exactly_one_of_two_simultaneous_refreshes_wins_every_round(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    race: Callable[..., Awaitable[list[Any]]],
) -> None:
    user_id = seed_users["employee"].id
    bystander = await add_token(test_db, user_id)  # another of the user's sessions, never used

    def refresh_with(raw: str) -> Callable[[], Awaitable[Any]]:
        async def go() -> Any:
            return await async_client.post("/auth/refresh", json={"refresh_token": raw})

        return go

    for n in range(ROUNDS):
        raw = await add_token(test_db, user_id)
        before = await live_tokens(test_db, user_id)  # bystander + this round's token (+ winners)

        results = await race(*[refresh_with(raw) for _ in range(SIMULTANEOUS)])

        statuses = sorted(getattr(r, "status_code", repr(r)) for r in results)
        assert statuses == [200, 401], f"round {n}: expected one winner, got {statuses}"
        loser = next(r for r in results if r.status_code == 401)
        assert loser.json()["code"] == "auth.token_revoked"
        # The spent token is replaced by exactly one new token; nothing else changes.
        assert await live_tokens(test_db, user_id) == before, f"round {n}: sessions changed"

    # The loser of every round left the user's other session alone.
    revoked = await test_db.execute(
        text("SELECT revoked_at FROM refresh_tokens WHERE token_hash = :h"),
        {"h": hashlib.sha256(bystander.encode()).hexdigest()},
    )
    assert revoked.scalar_one() is None, "the bystander session was revoked"
