"""The bulk clean-up skips rows another request holds, instead of waiting for them (003 Group D).

A refresh or a link redemption locks its row while it decides; the clean-up must not queue behind
it, and must still delete every other expired row in the batch.

Covers: FR-224, 003 FR-008.
"""

import asyncio
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hds.repositories.invitation_token import InvitationTokenRepository
from hds.repositories.refresh_token import RefreshTokenRepository

EXPIRED = 5


async def seed(db: AsyncSession, table: str) -> list[uuid.UUID]:
    org, user = uuid.uuid4(), uuid.uuid4()
    await db.execute(
        text("INSERT INTO organisations (id, name, timezone) VALUES (:o, 'Org', 'UTC')"), {"o": org}
    )
    await db.execute(
        text(
            "INSERT INTO users (id, organisation_id, email, password_hash, full_name, role, "
            "is_active, activated_at) VALUES (:u, :o, 'a@hds.test', 'h', 'A', "
            "CAST('employee' AS user_role), true, now())"
        ),
        {"u": user, "o": org},
    )
    extra, value = (", purpose", ", 'invite'") if table == "invitation_tokens" else ("", "")
    ids = [uuid.uuid4() for _ in range(EXPIRED)]
    for n, token in enumerate(ids):
        await db.execute(
            text(
                f"INSERT INTO {table} (id, user_id, token_hash, expires_at{extra}) "
                f"VALUES (:i, :u, :h, :x{value})"
            ),
            {"i": token, "u": user, "h": f"{n:064d}", "x": datetime.now(UTC) - timedelta(days=1)},
        )
    await db.commit()
    return ids


@pytest.mark.parametrize(
    ("table", "repository"),
    [("refresh_tokens", RefreshTokenRepository), ("invitation_tokens", InvitationTokenRepository)],
)
async def test_a_row_held_by_another_request_is_skipped_and_the_rest_deleted(
    table: str,
    repository: type[RefreshTokenRepository] | type[InvitationTokenRepository],
    session_pair: tuple[AsyncSession, AsyncSession],
) -> None:
    holder, cleaner = session_pair
    ids = await seed(holder, table)
    await holder.execute(text(f"SELECT 1 FROM {table} WHERE id = :i FOR UPDATE"), {"i": ids[0]})

    deleted = await asyncio.wait_for(
        repository(cleaner).delete_expired(datetime.now(UTC), 100), timeout=5
    )
    await cleaner.commit()
    await holder.rollback()

    assert deleted == EXPIRED - 1
    left = await holder.execute(text(f"SELECT id FROM {table}"))
    assert [r[0] for r in left] == [ids[0]]
