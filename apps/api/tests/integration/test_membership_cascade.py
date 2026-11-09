"""T122: deleting an assigned group or qualification removes its memberships.

Covers: FR-018, FR-019, FR-020.
"""

import uuid

import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hds.models.group import Group
from hds.models.qualification import Qualification
from hds.models.user import User

_CASES = [
    ("groups", "user_groups", "group_id", Group),
    ("qualifications", "user_qualifications", "qualification_id", Qualification),
]


async def _assign(
    session: AsyncSession, model: type, table: str, column: str, users: list[User]
) -> uuid.UUID:
    org_id = users[0].organisation_id
    row = model(id=uuid.uuid4(), organisation_id=org_id, name="Assigned")
    session.add(row)
    await session.commit()
    for user in users:
        await session.execute(
            text(f"INSERT INTO {table} (user_id, {column}) VALUES (:u, :r)"),
            {"u": user.id, "r": row.id},
        )
    await session.commit()
    return uuid.UUID(str(row.id))


async def _memberships(session: AsyncSession, table: str) -> int:
    await session.rollback()
    return int(await session.scalar(text(f"SELECT count(*) FROM {table}")) or 0)


@pytest.mark.asyncio
@pytest.mark.parametrize(("resource", "table", "column", "model"), _CASES)
async def test_database_cascades_memberships_on_raw_delete(
    resource: str,
    table: str,
    column: str,
    model: type,
    test_db: AsyncSession,
    seed_users: dict[str, User],
) -> None:
    users = [seed_users["employee"], seed_users["manager"]]
    row_id = await _assign(test_db, model, table, column, users)
    assert await _memberships(test_db, table) == 2

    await test_db.execute(text(f"DELETE FROM {resource} WHERE id = :i"), {"i": row_id})
    await test_db.commit()

    assert await _memberships(test_db, table) == 0
    # Users themselves are untouched.
    assert await test_db.scalar(text("SELECT count(*) FROM users")) == 4


@pytest.mark.asyncio
@pytest.mark.parametrize(("resource", "table", "column", "model"), _CASES)
async def test_api_delete_removes_memberships(
    resource: str,
    table: str,
    column: str,
    model: type,
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
) -> None:
    row_id = await _assign(test_db, model, table, column, [seed_users["employee"]])
    login = await async_client.post(
        "/auth/login", json={"email": "manager@hds.test", "password": "Dev1234!"}
    )
    headers = {"Authorization": f"Bearer {login.json()['access_token']}"}

    r = await async_client.delete(f"/api/v1/{resource}/{row_id}", headers=headers)

    assert r.status_code == 204, r.text
    assert await _memberships(test_db, table) == 0
    listing = await async_client.get(
        f"/api/v1/employees/{seed_users['employee'].id}/{resource}", headers=headers
    )
    assert listing.status_code == 200 and listing.json() == []
