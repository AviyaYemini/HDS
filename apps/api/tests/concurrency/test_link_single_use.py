"""An invitation or reset link works exactly once, even when redeemed twice at once (T251, SC-203).

Each round redeems one link twice at the same moment, with a different password in each request.
Exactly one request must win, and the stored password must be the winner's: the harm of the race
is that the loser overwrites the winner's choice, so the test checks which password the account
ends up with, not only the status codes.

Covers: FR-208, SC-203.
"""

import hashlib
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.security import verify_password
from hds.models.user import User

ROUNDS = 100


async def make_account(db: AsyncSession, org_id: uuid.UUID, *, active: bool, tag: str) -> uuid.UUID:
    """A pending (``active=False``) or active account with an unknown password."""
    user_id = uuid.uuid4()
    await db.execute(
        text(
            "INSERT INTO users (id, organisation_id, email, password_hash, full_name, role, "
            "is_active, activated_at) VALUES (:i, :o, :e, 'x', 'Racer', CAST('employee' AS "
            "user_role), :a, :t)"
        ),
        {
            "i": user_id,
            "o": org_id,
            "e": f"{tag}@hds.test",
            "a": active,
            "t": datetime.now(UTC) if active else None,
        },
    )
    return user_id


async def add_link(db: AsyncSession, user_id: uuid.UUID, purpose: str, raw: str) -> None:
    await db.execute(
        text(
            "INSERT INTO invitation_tokens (id, user_id, token_hash, purpose, expires_at) "
            "VALUES (:i, :u, :h, :p, :x)"
        ),
        {
            "i": uuid.uuid4(),
            "u": user_id,
            "h": hashlib.sha256(raw.encode()).hexdigest(),
            "p": purpose,
            "x": datetime.now(UTC) + timedelta(days=1),
        },
    )
    await db.commit()


async def stored_hash(db: AsyncSession, user_id: uuid.UUID) -> str:
    result = await db.execute(text("SELECT password_hash FROM users WHERE id = :i"), {"i": user_id})
    return str(result.scalar_one())


@pytest.mark.parametrize(
    ("path", "purpose", "active", "password_field"),
    [
        ("/auth/invite/accept", "invite", False, "password"),
        ("/auth/password-reset/confirm", "reset", True, "new_password"),
    ],
    ids=["accept-invitation", "confirm-reset"],
)
async def test_exactly_one_of_two_simultaneous_redemptions_wins_every_round(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    race: Callable[..., Awaitable[list[Any]]],
    path: str,
    purpose: str,
    active: bool,
    password_field: str,
) -> None:
    org_id = seed_users["employee"].organisation_id
    for n in range(ROUNDS):
        user_id = await make_account(test_db, org_id, active=active, tag=f"{purpose}{n}")
        raw = f"{purpose}-link-{uuid.uuid4().hex}"
        await add_link(test_db, user_id, purpose, raw)
        passwords = [f"First-choice-{n}-aaaa", f"Second-choice-{n}-bbbb"]

        def redeem(password: str, link: str = raw) -> Callable[[], Awaitable[Any]]:
            async def go() -> Any:
                return await async_client.post(path, json={"token": link, password_field: password})

            return go

        results = await race(*[redeem(p) for p in passwords])

        statuses = [getattr(r, "status_code", repr(r)) for r in results]
        assert sorted(statuses) == [204, 401], f"round {n}: expected one winner, got {statuses}"
        assert next(r for r in results if r.status_code == 401).json()["code"] == "auth.token_used"
        winner = statuses.index(204)
        stored = await stored_hash(test_db, user_id)
        assert verify_password(passwords[winner], stored), f"round {n}: winner's password lost"
        assert not verify_password(passwords[1 - winner], stored), f"round {n}: loser overwrote it"
