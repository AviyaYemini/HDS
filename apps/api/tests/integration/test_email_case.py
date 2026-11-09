"""One address, one account, whatever the letter case (T283, FR-215, SC-208).

Each flow that takes an account address is driven with the address in a different case from the
one stored: sign-in, creating an account, inviting, requesting a password reset and adding a
portal user. The stored address is always the lowercase form.

Covers: FR-215, FR-234, SC-208.
"""

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.security import generate_refresh_token
from hds.models.user import User
from hds.repositories.user import UserRepository
from tests.conftest import OutboxMailSender

PASSWORD = "Dev1234!"


async def admin_headers(client: AsyncClient) -> dict[str, str]:
    response = await client.post(
        "/auth/login", json={"email": "admin@hds.test", "password": PASSWORD}
    )
    assert response.status_code == 200, response.text
    return {"Authorization": f"Bearer {response.json()['access_token']}"}


def new_employee(email: str) -> dict[str, str]:
    return {"email": email, "full_name": "Somebody", "initial_password": "A-long-password-1"}


async def test_sign_in_works_with_any_letter_case(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    for spelling in ("Employee@hds.test", "EMPLOYEE@HDS.TEST", "  employee@Hds.Test "):
        response = await async_client.post(
            "/auth/login", json={"email": spelling, "password": PASSWORD}
        )
        assert response.status_code == 200, spelling


async def test_creating_an_employee_with_another_case_of_an_existing_address_is_a_duplicate(
    async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    headers = await admin_headers(async_client)

    duplicate = await async_client.post(
        "/api/v1/employees", json=new_employee("EMPLOYEE@hds.test"), headers=headers
    )

    assert duplicate.status_code == 409
    assert duplicate.json()["code"] == "users.duplicate_email"


async def test_a_new_address_is_stored_in_lowercase(
    async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    headers = await admin_headers(async_client)

    created = await async_client.post(
        "/api/v1/employees", json=new_employee("Mixed.Case@Hds.Test"), headers=headers
    )

    assert created.status_code == 201
    assert created.json()["email"] == "mixed.case@hds.test"
    stored = (
        (await test_db.execute(text("SELECT email FROM users WHERE email ILIKE 'mixed.case@%'")))
        .scalars()
        .all()
    )
    assert stored == ["mixed.case@hds.test"]


async def test_inviting_another_case_of_an_active_address_is_a_duplicate(
    async_client: AsyncClient, seed_users: dict[str, User], outbox: OutboxMailSender
) -> None:
    headers = await admin_headers(async_client)

    refused = await async_client.post(
        "/auth/invite", json={"email": "Employee@HDS.test", "role": "employee"}, headers=headers
    )

    assert refused.status_code == 409
    assert refused.json()["code"] == "users.duplicate_email"
    assert outbox.messages == []


async def test_inviting_the_same_pending_person_in_another_case_replaces_the_link(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
) -> None:
    headers = await admin_headers(async_client)
    first = await async_client.post(
        "/auth/invite", json={"email": "Pat@hds.test", "role": "employee"}, headers=headers
    )
    again = await async_client.post(
        "/auth/invite", json={"email": "PAT@HDS.TEST", "role": "employee"}, headers=headers
    )

    assert (first.status_code, again.status_code) == (202, 202)
    accounts = (
        await test_db.execute(text("SELECT count(*) FROM users WHERE email ILIKE 'pat@hds.test'"))
    ).scalar_one()
    assert accounts == 1, "two spellings made two accounts"
    assert [to for to, _, _ in outbox.messages] == ["pat@hds.test", "pat@hds.test"]


async def test_a_reset_request_in_another_case_reaches_the_account_holder(
    async_client: AsyncClient, seed_users: dict[str, User], outbox: OutboxMailSender
) -> None:
    response = await async_client.post(
        "/auth/password-reset/request", json={"email": "EMPLOYEE@Hds.test"}
    )

    assert response.status_code == 202
    assert [to for to, _, _ in outbox.messages] == ["employee@hds.test"]
    reset = await async_client.post(
        "/auth/password-reset/confirm",
        json={
            "token": outbox.last_token_for("employee@hds.test"),
            "new_password": "A-new-password-1",
        },
    )
    assert reset.status_code == 204


async def test_adding_a_portal_user_in_another_case_of_an_existing_address_is_a_duplicate(
    async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    headers = await admin_headers(async_client)
    client = await async_client.post(
        "/api/v1/clients", json={"name": "Acme", "contract_start": "2026-01-01"}, headers=headers
    )
    assert client.status_code == 201, client.text

    refused = await async_client.post(
        f"/api/v1/clients/{client.json()['id']}/users",
        json={**new_employee("MANAGER@hds.test")},
        headers=headers,
    )

    assert refused.status_code == 409
    assert refused.json()["code"] == "users.duplicate_email"


async def test_the_repository_matches_and_stores_addresses_case_insensitively(
    test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    """Second line of defence for callers that do not go through the request types."""
    repo = UserRepository(test_db)
    found = await repo.get_by_email("  ADMIN@Hds.TEST ")
    assert found is not None and found.id == seed_users["admin"].id

    created = await repo.create(
        seed_users["admin"].organisation_id,
        {
            "email": " New.Person@HDS.test",
            "password_hash": "x",
            "full_name": "N",
            "role": "employee",
        },
    )
    assert created.email == "new.person@hds.test"
    with pytest.raises(IntegrityError) as refused:
        await repo.create(
            seed_users["admin"].organisation_id,
            {
                "email": "NEW.PERSON@hds.test",
                "password_hash": "x",
                "full_name": "N",
                "role": "employee",
            },
        )
    assert "uq_users_email_ci" in str(refused.value)


async def test_an_account_stored_before_the_rule_is_found_and_its_invitation_still_works(
    async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    """A mixed-case row from before migration 016 (inserted raw) and its unused invitation."""
    org_id = seed_users["admin"].organisation_id
    user_id = uuid.uuid4()
    raw, token_hash = generate_refresh_token()
    await test_db.execute(
        text(
            "INSERT INTO users (id, organisation_id, email, password_hash, full_name, role, "
            "is_active) VALUES (:i, :o, 'Legacy.Hire@HDS.test', 'x', 'Legacy', "
            "CAST('employee' AS user_role), false)"
        ),
        {"i": user_id, "o": org_id},
    )
    await test_db.execute(
        text(
            "INSERT INTO invitation_tokens (id, user_id, token_hash, purpose, expires_at) "
            "VALUES (:i, :u, :h, 'invite', :x)"
        ),
        {
            "i": uuid.uuid4(),
            "u": user_id,
            "h": token_hash,
            "x": datetime.now(UTC) + timedelta(days=1),
        },
    )
    await test_db.commit()

    accepted = await async_client.post(
        "/auth/invite/accept", json={"token": raw, "password": "A-long-password-1"}
    )
    assert accepted.status_code == 204, accepted.text

    for spelling in ("legacy.hire@hds.test", "LEGACY.HIRE@HDS.TEST"):
        signed_in = await async_client.post(
            "/auth/login", json={"email": spelling, "password": "A-long-password-1"}
        )
        assert signed_in.status_code == 200, spelling
