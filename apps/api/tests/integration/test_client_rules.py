"""A Client account keeps its role, and a client's contract dates are in order (T396, F23, F24).

A Client-role account is tied to one client's data, so changing its role would leave a staff account
with a client link, or a portal account with staff access. A contract that ends before it starts
makes no sense, and it would be read as already over. A wrong start date can be corrected, and the
order is judged on the pair as it will be after the change (003 Group A).

Covers: FR-242, FR-243, 003 FR-001 to FR-004.
"""

from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hds.models.user import User
from tests.conftest import OutboxMailSender

PASSWORD = "Dev1234!"


async def headers_for(client: AsyncClient, email: str) -> dict[str, str]:
    response = await client.post("/auth/login", json={"email": email, "password": PASSWORD})
    assert response.status_code == 200, response.text
    return {"Authorization": f"Bearer {response.json()['access_token']}"}


@pytest.fixture
async def admin(async_client: AsyncClient, seed_users: dict[str, User]) -> dict[str, str]:
    return await headers_for(async_client, "admin@hds.test")


async def add_client(
    client: AsyncClient, headers: dict[str, str], start: str = "2026-01-01", end: str | None = None
) -> Any:
    response = await client.post(
        "/api/v1/clients",
        json={"name": "Acme", "contract_start": start, "contract_end": end},
        headers=headers,
    )
    assert response.status_code == 201, response.text
    return response.json()


@pytest.mark.parametrize("role", ["employee", "manager", "admin"])
async def test_a_client_accounts_role_cannot_change(
    role: str, async_client: AsyncClient, seed_users: dict[str, User], admin: dict[str, str]
) -> None:
    url = f"/api/v1/employees/{seed_users['client'].id}"

    refused = await async_client.patch(url, json={"role": role}, headers=admin)

    assert refused.status_code == 409
    assert refused.json()["code"] == "users.client_role_locked"
    assert (await async_client.get(url, headers=admin)).json()["role"] == "client"


async def test_a_client_accounts_other_fields_can_still_change(
    async_client: AsyncClient, seed_users: dict[str, User], admin: dict[str, str]
) -> None:
    edited = await async_client.patch(
        f"/api/v1/employees/{seed_users['client'].id}", json={"phone": "123"}, headers=admin
    )

    assert edited.status_code == 200, edited.text
    assert edited.json()["phone"] == "123"


async def test_a_staff_accounts_role_still_changes(
    async_client: AsyncClient, seed_users: dict[str, User], admin: dict[str, str]
) -> None:
    edited = await async_client.patch(
        f"/api/v1/employees/{seed_users['employee'].id}", json={"role": "manager"}, headers=admin
    )

    assert edited.status_code == 200, edited.text
    assert edited.json()["role"] == "manager"


async def test_a_new_client_cannot_end_before_it_starts(
    async_client: AsyncClient, admin: dict[str, str]
) -> None:
    refused = await async_client.post(
        "/api/v1/clients",
        json={"name": "Acme", "contract_start": "2026-02-01", "contract_end": "2026-01-31"},
        headers=admin,
    )

    assert refused.status_code == 422
    assert refused.json()["code"] == "clients.date_range_invalid"
    listed = await async_client.get("/api/v1/clients", headers=admin)
    assert listed.json()["items"] == []


async def test_a_contract_may_start_and_end_on_one_day(
    async_client: AsyncClient, admin: dict[str, str]
) -> None:
    created = await add_client(async_client, admin, "2026-02-01", "2026-02-01")

    assert created["contract_end"] == "2026-02-01"


async def test_an_edit_is_checked_against_the_stored_start(
    async_client: AsyncClient, admin: dict[str, str]
) -> None:
    created = await add_client(async_client, admin, "2026-02-01")
    url = f"/api/v1/clients/{created['id']}"

    refused = await async_client.patch(url, json={"contract_end": "2026-01-31"}, headers=admin)
    stored = await async_client.get(url, headers=admin)
    accepted = await async_client.patch(url, json={"contract_end": "2026-02-01"}, headers=admin)

    assert refused.status_code == 422
    assert refused.json()["code"] == "clients.date_range_invalid"
    assert stored.json()["contract_end"] is None
    assert accepted.status_code == 200, accepted.text


async def test_an_open_ended_contract_stays_allowed(
    async_client: AsyncClient, admin: dict[str, str]
) -> None:
    created = await add_client(async_client, admin, "2026-02-01", "2026-03-01")

    cleared = await async_client.patch(
        f"/api/v1/clients/{created['id']}", json={"contract_end": None}, headers=admin
    )

    assert cleared.status_code == 200, cleared.text
    assert cleared.json()["contract_end"] is None


async def test_a_stored_date_pair_that_breaks_the_rule_does_not_block_other_edits(
    async_client: AsyncClient,
    test_db: AsyncSession,
    admin: dict[str, str],
) -> None:
    """Rows from before the rule may break it, and a form sends every field back unchanged."""
    created = await add_client(async_client, admin, "2026-02-01")
    await test_db.execute(
        text("UPDATE clients SET contract_end = '2026-01-01' WHERE id = :i"), {"i": created["id"]}
    )
    await test_db.commit()

    edited = await async_client.patch(
        f"/api/v1/clients/{created['id']}",
        json={"name": "Renamed", "contract_end": "2026-01-01"},
        headers=admin,
    )

    assert edited.status_code == 200, edited.text
    assert edited.json()["name"] == "Renamed"


@pytest.mark.parametrize("role", ["manager", "admin"])
async def test_a_manager_or_admin_corrects_the_contract_start(
    role: str, async_client: AsyncClient, seed_users: dict[str, User], admin: dict[str, str]
) -> None:
    created = await add_client(async_client, admin, "2026-02-01")
    url = f"/api/v1/clients/{created['id']}"
    headers = await headers_for(async_client, f"{role}@hds.test")

    edited = await async_client.patch(url, json={"contract_start": "2026-01-15"}, headers=headers)

    assert edited.status_code == 200, edited.text
    assert (await async_client.get(url, headers=admin)).json()["contract_start"] == "2026-01-15"


async def test_an_employee_cannot_change_the_contract_start(
    async_client: AsyncClient, seed_users: dict[str, User], admin: dict[str, str]
) -> None:
    created = await add_client(async_client, admin, "2026-02-01")
    url = f"/api/v1/clients/{created['id']}"
    employee = await headers_for(async_client, "employee@hds.test")

    refused = await async_client.patch(url, json={"contract_start": "2026-01-15"}, headers=employee)

    assert refused.status_code == 403
    assert (await async_client.get(url, headers=admin)).json()["contract_start"] == "2026-02-01"


async def test_a_start_after_the_stored_end_is_refused_and_nothing_is_stored(
    async_client: AsyncClient, admin: dict[str, str]
) -> None:
    created = await add_client(async_client, admin, "2026-02-01", "2026-03-01")
    url = f"/api/v1/clients/{created['id']}"

    refused = await async_client.patch(
        url, json={"contract_start": "2026-03-02", "name": "Renamed"}, headers=admin
    )

    assert refused.status_code == 422
    assert refused.json()["code"] == "clients.date_range_invalid"
    stored = (await async_client.get(url, headers=admin)).json()
    assert (stored["contract_start"], stored["name"]) == ("2026-02-01", "Acme")


async def test_both_dates_sent_together_are_judged_as_the_new_pair(
    async_client: AsyncClient, admin: dict[str, str]
) -> None:
    """SC-001: the new start alone would be refused against the stored end; the pair passes."""
    created = await add_client(async_client, admin, "2026-02-01", "2026-03-01")
    url = f"/api/v1/clients/{created['id']}"

    moved = await async_client.patch(
        url, json={"contract_start": "2026-04-01", "contract_end": "2026-05-01"}, headers=admin
    )
    refused = await async_client.patch(
        url, json={"contract_start": "2026-06-01", "contract_end": "2026-05-31"}, headers=admin
    )

    assert moved.status_code == 200, moved.text
    assert refused.status_code == 422
    assert refused.json()["code"] == "clients.date_range_invalid"
    stored = (await async_client.get(url, headers=admin)).json()
    assert (stored["contract_start"], stored["contract_end"]) == ("2026-04-01", "2026-05-01")


async def test_a_new_end_is_judged_against_a_new_start_sent_with_it(
    async_client: AsyncClient, admin: dict[str, str]
) -> None:
    """The stored start would allow this end; the start sent with it does not."""
    created = await add_client(async_client, admin, "2026-02-01")
    url = f"/api/v1/clients/{created['id']}"

    refused = await async_client.patch(
        url, json={"contract_start": "2026-03-01", "contract_end": "2026-02-15"}, headers=admin
    )

    assert refused.status_code == 422
    assert refused.json()["code"] == "clients.date_range_invalid"


async def test_the_contract_start_cannot_be_cleared(
    async_client: AsyncClient, admin: dict[str, str]
) -> None:
    created = await add_client(async_client, admin, "2026-02-01")
    url = f"/api/v1/clients/{created['id']}"

    refused = await async_client.patch(url, json={"contract_start": None}, headers=admin)

    assert refused.status_code == 422
    assert refused.json()["code"] == "validation.request_invalid"
    assert (await async_client.get(url, headers=admin)).json()["contract_start"] == "2026-02-01"


async def test_a_broken_stored_pair_sent_back_unchanged_does_not_block_an_edit(
    async_client: AsyncClient,
    test_db: AsyncSession,
    admin: dict[str, str],
) -> None:
    """A form sends both dates back as they were; only the name changes."""
    created = await add_client(async_client, admin, "2026-02-01")
    await test_db.execute(
        text("UPDATE clients SET contract_end = '2026-01-01' WHERE id = :i"), {"i": created["id"]}
    )
    await test_db.commit()

    edited = await async_client.patch(
        f"/api/v1/clients/{created['id']}",
        json={"name": "Renamed", "contract_start": "2026-02-01", "contract_end": "2026-01-01"},
        headers=admin,
    )

    assert edited.status_code == 200, edited.text
    assert edited.json()["name"] == "Renamed"


async def test_a_pending_client_account_cannot_be_re_invited_with_a_staff_role(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
    admin: dict[str, str],
) -> None:
    user = seed_users["client"]
    await test_db.execute(
        text("UPDATE users SET is_active = false, activated_at = NULL WHERE id = :u"),
        {"u": user.id},
    )
    await test_db.commit()

    refused = await async_client.post(
        "/auth/invite", json={"email": user.email, "role": "manager"}, headers=admin
    )

    assert refused.status_code == 409
    assert refused.json()["code"] == "users.client_role_locked"
    await test_db.rollback()
    stored = await test_db.execute(text("SELECT role FROM users WHERE id = :u"), {"u": user.id})
    assert str(stored.scalar_one()) == "client"
    assert outbox.messages == []
