"""T135: email is unique across ALL organisations (sign-in resolves the account by email).

Covers: FR-012.
"""

import uuid

import pytest
from httpx import AsyncClient
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.security import hash_password
from hds.models.organisation import Organisation
from hds.models.user import User, UserRole


def _user(org_id: uuid.UUID, email: str, *, is_deleted: bool = False) -> User:
    return User(
        id=uuid.uuid4(),
        organization_id=org_id,
        email=email,
        password_hash=hash_password("Dev1234!"),
        full_name="Someone",
        role=UserRole.employee,
        is_deleted=is_deleted,
    )


async def _second_org(session: AsyncSession) -> Organisation:
    org = Organisation(id=uuid.uuid4(), name="Second Org", timezone="UTC")
    session.add(org)
    await session.commit()
    return org


@pytest.mark.asyncio
async def test_database_rejects_same_email_in_two_organisations(
    test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    other = await _second_org(test_db)
    test_db.add(_user(other.id, "employee@hds.test"))  # already used in seed_org
    with pytest.raises(IntegrityError):
        await test_db.commit()
    await test_db.rollback()


@pytest.mark.asyncio
async def test_email_may_be_reused_after_soft_delete_in_another_org(
    test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    other = await _second_org(test_db)
    test_db.add(_user(other.id, "gone@hds.test", is_deleted=True))
    await test_db.commit()
    test_db.add(_user(seed_users["admin"].organization_id, "gone@hds.test"))
    await test_db.commit()  # only non-deleted rows compete for the email


@pytest.mark.asyncio
async def test_api_rejects_email_used_in_another_organisation(
    async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    other = await _second_org(test_db)
    test_db.add(_user(other.id, "taken@other.test"))
    await test_db.commit()
    login = await async_client.post(
        "/auth/login", json={"email": "admin@hds.test", "password": "Dev1234!"}
    )
    headers = {"Authorization": f"Bearer {login.json()['access_token']}"}

    response = await async_client.post(
        "/api/v1/employees",
        json={
            "email": "taken@other.test",
            "full_name": "Dup",
            "role": "employee",
            "initial_password": "Dev1234!Dev1234!",
        },
        headers=headers,
    )
    assert response.status_code == 409, response.text
    assert response.json()["code"] == "users.duplicate_email"
