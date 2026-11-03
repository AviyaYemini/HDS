"""The bulk cleanup removes what can never be used again, and nothing else (T365, F08).

Normal requests clean up after themselves, so nothing depends on this being scheduled. It exists for
accounts that never sign in again, and for a table that has grown. Only a record's expiry decides
for credentials: a spent or cancelled link inside its lifetime stays, so its answer stays
``auth.token_used``.

Covers: FR-209, FR-221, FR-224.
"""

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from hds.core.config import get_settings
from hds.core.security import generate_refresh_token
from hds.models.user import User
from hds.repositories.invitation_token import InvitationTokenRepository
from hds.scripts import purge_expired_records as script
from hds.services.throttle import hash_key

Factory = async_sessionmaker[AsyncSession]


@pytest.fixture
def factory(migrated_test_db: str) -> Factory:
    engine = create_async_engine(migrated_test_db, poolclass=NullPool)
    return async_sessionmaker(engine, expire_on_commit=False)


def past() -> datetime:
    return datetime.now(UTC) - timedelta(hours=1)


def future() -> datetime:
    return datetime.now(UTC) + timedelta(hours=1)


async def add_token(db: AsyncSession, user_id: uuid.UUID, expires: datetime) -> str:
    _, token_hash = generate_refresh_token()
    await db.execute(
        text(
            "INSERT INTO refresh_tokens (id, user_id, token_hash, expires_at) "
            "VALUES (:i, :u, :h, :x)"
        ),
        {"i": uuid.uuid4(), "u": user_id, "h": token_hash, "x": expires},
    )
    await db.commit()
    return token_hash


async def add_link(
    db: AsyncSession, user_id: uuid.UUID, expires: datetime, used: datetime | None = None
) -> str:
    _, token_hash = generate_refresh_token()
    await db.execute(
        text(
            "INSERT INTO invitation_tokens (id, user_id, token_hash, purpose, expires_at, used_at) "
            "VALUES (:i, :u, :h, 'reset', :x, :s)"
        ),
        {"i": uuid.uuid4(), "u": user_id, "h": token_hash, "x": expires, "s": used},
    )
    await db.commit()
    return token_hash


async def add_throttle(
    db: AsyncSession, name: str, updated: timedelta, locked: timedelta | None = None
) -> str:
    key = hash_key(name, get_settings().throttle_key)
    now = datetime.now(UTC)
    await db.execute(
        text(
            "INSERT INTO login_throttle (kind, key_hash, failures, locked_until, updated_at) "
            "VALUES ('address', :h, 1, :l, :u)"
        ),
        {"h": key, "u": now + updated, "l": now + locked if locked is not None else None},
    )
    await db.commit()
    return key


async def exists(db: AsyncSession, table: str, column: str, value: str) -> bool:
    await db.rollback()
    row = await db.execute(text(f"SELECT 1 FROM {table} WHERE {column} = :v"), {"v": value})
    return row.first() is not None


async def count(db: AsyncSession, table: str) -> int:
    await db.rollback()
    return int((await db.execute(text(f"SELECT count(*) FROM {table}"))).scalar_one())


async def test_refresh_tokens_past_their_expiry_go_and_the_rest_stay(
    test_db: AsyncSession, seed_users: dict[str, User], factory: Factory
) -> None:
    user_id = seed_users["employee"].id
    expired = await add_token(test_db, user_id, past())
    live = await add_token(test_db, user_id, future())

    assert await script.run(factory) == 0

    assert not await exists(test_db, "refresh_tokens", "token_hash", expired)
    assert await exists(test_db, "refresh_tokens", "token_hash", live)


async def test_links_go_only_by_their_expiry(
    test_db: AsyncSession, seed_users: dict[str, User], factory: Factory
) -> None:
    user_id = seed_users["employee"].id
    expired = await add_link(test_db, user_id, past())
    spent_and_expired = await add_link(test_db, user_id, past(), used=past())
    live = await add_link(test_db, user_id, future())
    spent = await add_link(test_db, user_id, future(), used=past())

    await script.run(factory)

    assert not await exists(test_db, "invitation_tokens", "token_hash", expired)
    assert not await exists(test_db, "invitation_tokens", "token_hash", spent_and_expired)
    assert await exists(test_db, "invitation_tokens", "token_hash", live)
    assert await exists(test_db, "invitation_tokens", "token_hash", spent)


async def test_stale_unlocked_throttle_rows_go_and_a_live_lock_stays(
    test_db: AsyncSession, seed_users: dict[str, User], factory: Factory
) -> None:
    old = -timedelta(days=2)
    stale = await add_throttle(test_db, "stale", old)
    ran_out = await add_throttle(test_db, "ran-out", old, locked=old)
    locked = await add_throttle(test_db, "locked", old, locked=timedelta(minutes=5))
    recent = await add_throttle(test_db, "recent", -timedelta(hours=1))

    await script.run(factory)

    assert not await exists(test_db, "login_throttle", "key_hash", stale)
    assert not await exists(test_db, "login_throttle", "key_hash", ran_out)
    assert await exists(test_db, "login_throttle", "key_hash", locked)
    assert await exists(test_db, "login_throttle", "key_hash", recent)


async def test_it_works_through_more_rows_than_one_batch_and_prints_only_counts(
    test_db: AsyncSession,
    seed_users: dict[str, User],
    factory: Factory,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(script, "BATCH", 2)
    for _ in range(5):
        await add_link(test_db, seed_users["employee"].id, past())

    assert await script.run(factory) == 0

    assert await count(test_db, "invitation_tokens") == 0
    assert capsys.readouterr().out.splitlines() == [
        "refresh_tokens 0",
        "links 5",
        "throttle_rows 0",
    ]


async def test_a_failure_part_way_still_reports_what_was_already_removed(
    test_db: AsyncSession,
    seed_users: dict[str, User],
    factory: Factory,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    await add_token(test_db, seed_users["employee"].id, past())

    async def fails(self: InvitationTokenRepository, now: datetime, limit: int) -> int:
        raise RuntimeError("connection lost")

    monkeypatch.setattr(InvitationTokenRepository, "delete_expired", fails)

    with pytest.raises(RuntimeError):
        await script.run(factory)

    assert capsys.readouterr().out.splitlines() == ["refresh_tokens 1"]


async def test_a_short_batch_does_not_end_the_drain(factory: Factory) -> None:
    """Skipped rows (held by a request, or moved by an update) make a batch short (003 Group D).

    Stopping at the first short batch left the rest of a backlog for the next run.
    """
    batches = iter([3, 2, 0, 7])
    calls = 0

    async def delete(session: AsyncSession) -> int:
        nonlocal calls
        calls += 1
        return next(batches)

    assert await script._drain(factory, delete) == 5
    assert calls == 3
