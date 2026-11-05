"""A repeated assignment request with one Idempotency-Key gets the first answer (T052a).

Covers: 003 FR-115a; research R8; docs/hds-api.md (retried writes).
"""

from datetime import UTC, date, datetime
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from hds.core.security import hash_password
from hds.models.user import User, UserRole
from hds.scripts import purge_expired_records as script
from tests.roster_support import add_project, add_shift, assign, headers_for

MONDAY = date(2027, 6, 7)


@pytest.fixture
async def admin(async_client: AsyncClient, seed_users: dict[str, User]) -> dict[str, str]:
    return await headers_for(async_client, "admin")


@pytest.fixture
async def other_admin(
    async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> dict[str, str]:
    test_db.add(
        User(
            organization_id=seed_users["admin"].organization_id,
            email="second-admin@hds.test",
            password_hash=hash_password("Dev1234!"),
            full_name="Second Admin",
            role=UserRole.admin,
            activated_at=datetime.now(UTC),
        )
    )
    await test_db.commit()
    response = await async_client.post(
        "/auth/login", json={"email": "second-admin@hds.test", "password": "Dev1234!"}
    )
    return {"Authorization": f"Bearer {response.json()['access_token']}"}


@pytest.fixture
async def shift(async_client: AsyncClient, admin: dict[str, str]) -> Any:
    project = await add_project(async_client, admin)
    return (await add_shift(async_client, admin, project, MONDAY, headcount=3))["shift"]


async def assignments(db: AsyncSession) -> int:
    await db.rollback()
    return int((await db.execute(text("SELECT count(*) FROM assignments"))).scalar_one())


async def test_a_repeat_gets_the_first_answer_and_writes_nothing(
    async_client: AsyncClient,
    test_db: AsyncSession,
    admin: dict[str, str],
    shift: Any,
    seed_users: dict[str, User],
) -> None:
    idem_key = "assign-1"
    ann = seed_users["employee"].id

    first = await assign(async_client, admin, shift["id"], ann, key=idem_key)
    again = await assign(async_client, admin, shift["id"], ann, key=idem_key)

    assert first.status_code == 201, first.text
    assert (again.status_code, again.json()) == (201, first.json())
    assert await assignments(test_db) == 1


async def test_the_same_key_with_another_request_is_refused(
    async_client: AsyncClient,
    test_db: AsyncSession,
    admin: dict[str, str],
    shift: Any,
    seed_users: dict[str, User],
) -> None:
    idem_key = "assign-2"
    await assign(async_client, admin, shift["id"], seed_users["employee"].id, key=idem_key)

    other = await assign(async_client, admin, shift["id"], seed_users["manager"].id, key=idem_key)

    assert other.status_code == 422
    assert other.json()["code"] == "idempotency.key_reused"
    assert await assignments(test_db) == 1


async def test_keys_belong_to_their_caller(
    async_client: AsyncClient,
    test_db: AsyncSession,
    admin: dict[str, str],
    shift: Any,
    seed_users: dict[str, User],
    other_admin: dict[str, str],
) -> None:
    idem_key = "shared-name"

    first = await assign(async_client, admin, shift["id"], seed_users["employee"].id, key=idem_key)
    second = await assign(
        async_client,
        other_admin,
        shift["id"],
        seed_users["manager"].id,
        key=idem_key,
    )

    assert (first.status_code, second.status_code) == (201, 201), second.text
    assert await assignments(test_db) == 2


async def test_a_key_longer_than_255_characters_is_refused(
    async_client: AsyncClient, admin: dict[str, str], shift: Any, seed_users: dict[str, User]
) -> None:
    refused = await assign(
        async_client,
        admin,
        shift["id"],
        seed_users["employee"].id,
        key="k" * 256,
    )

    assert refused.status_code == 422


async def test_an_expired_key_counts_as_new(
    async_client: AsyncClient,
    test_db: AsyncSession,
    admin: dict[str, str],
    shift: Any,
    seed_users: dict[str, User],
) -> None:
    idem_key = "assign-old"
    await assign(async_client, admin, shift["id"], seed_users["employee"].id, key=idem_key)
    await test_db.execute(
        text("UPDATE idempotency_keys SET created_at = now() - interval '25 hours'")
    )
    await test_db.commit()

    reused = await assign(async_client, admin, shift["id"], seed_users["manager"].id, key=idem_key)

    assert reused.status_code == 201, reused.text
    assert await assignments(test_db) == 2


async def test_the_purge_removes_expired_keys(
    async_client: AsyncClient,
    test_db: AsyncSession,
    migrated_test_db: str,
    admin: dict[str, str],
    shift: Any,
    seed_users: dict[str, User],
) -> None:
    await assign(async_client, admin, shift["id"], seed_users["employee"].id, key="to-purge")
    await assign(async_client, admin, shift["id"], seed_users["manager"].id, key="to-keep")
    await test_db.execute(
        text(
            "UPDATE idempotency_keys SET created_at = now() - interval '25 hours' "
            "WHERE key = 'to-purge'"
        )
    )
    await test_db.commit()
    engine = create_async_engine(migrated_test_db, poolclass=NullPool)

    try:
        await script.run(async_sessionmaker(engine, expire_on_commit=False))
    finally:
        await engine.dispose()

    await test_db.rollback()
    left = (await test_db.execute(text("SELECT key FROM idempotency_keys"))).scalars().all()
    assert left == ["to-keep"]


async def test_a_repeat_with_the_shift_id_in_capitals_gets_the_first_answer(
    async_client: AsyncClient,
    test_db: AsyncSession,
    admin: dict[str, str],
    shift: Any,
    seed_users: dict[str, User],
) -> None:
    """The request is identified by the shift it names, not by how its id was spelt."""
    ann = seed_users["employee"].id
    first = await assign(async_client, admin, shift["id"], ann, key="spelling")

    again = await assign(async_client, admin, shift["id"].upper(), ann, key="spelling")

    assert (again.status_code, again.json()) == (201, first.json())
    assert await assignments(test_db) == 1
