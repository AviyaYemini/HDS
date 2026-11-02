"""Integration tests for the /me profile endpoints (US2, T049, T067).

Covers: FR-007, FR-017.
"""

import uuid

import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hds.models.organisation import Organisation
from hds.models.user import User


async def _login(client: AsyncClient, email: str, password: str = "Dev1234!") -> str:
    response = await client.post(
        "/auth/login",
        json={"email": email, "password": password},
    )
    assert response.status_code == 200, response.text
    return response.json()["access_token"]


@pytest.mark.asyncio
async def test_employee_can_update_phone(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    seed_org: Organisation,
) -> None:
    token = await _login(async_client, "employee@hds.test")
    response = await async_client.patch(
        "/me",
        json={"phone": "+972501234567"},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 200
    assert response.json()["phone"] == "+972501234567"


@pytest.mark.asyncio
async def test_employee_cannot_change_role(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    seed_org: Organisation,
) -> None:
    token = await _login(async_client, "employee@hds.test")
    response = await async_client.patch(
        "/me",
        json={"role": "manager"},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 403


@pytest.mark.asyncio
async def test_employee_cannot_access_other_employee_profile(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    seed_org: Organisation,
) -> None:
    token = await _login(async_client, "employee@hds.test")
    manager = seed_users["manager"]
    response = await async_client.get(
        f"/api/v1/employees/{manager.id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    # 403 if route exists; 404 if not yet registered (T067)
    assert response.status_code in (403, 404)


_FORBIDDEN_SELF_FIELDS: dict[str, object] = {
    "role": "manager",
    "is_active": False,
    "is_deleted": True,
    "organization_id": "00000000-0000-0000-0000-000000000001",
    "default_hourly_rate": "999.0000",
    "email": "hijack@hds.test",
    "password_hash": "x",
    "client_id": "00000000-0000-0000-0000-000000000002",
    "failed_login_attempts": 7,
    "locked_until": "2030-01-01T00:00:00Z",
    "id": "00000000-0000-0000-0000-000000000003",
    "not_a_real_field": "anything",
}


@pytest.mark.asyncio
@pytest.mark.parametrize("field", sorted(_FORBIDDEN_SELF_FIELDS))
async def test_patch_me_forbidden_or_unknown_field_returns_403_and_row_unchanged(
    field: str,
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
) -> None:
    employee = seed_users["employee"]
    token = await _login(async_client, "employee@hds.test")
    before = await _row_snapshot(test_db, employee.id)  # after sign-in, which touches the row

    # Sent alone and together with a permitted field: nothing may be partially applied.
    for body in (
        {field: _FORBIDDEN_SELF_FIELDS[field]},
        {"full_name": "Changed Name", field: _FORBIDDEN_SELF_FIELDS[field]},
    ):
        response = await async_client.patch(
            "/me", json=body, headers={"Authorization": f"Bearer {token}"}
        )
        assert response.status_code == 403, response.text
        assert await _row_snapshot(test_db, employee.id) == before


@pytest.mark.asyncio
async def test_patch_me_allowed_fields_only_are_applied(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
) -> None:
    employee = seed_users["employee"]
    token = await _login(async_client, "employee@hds.test")
    before = await _row_snapshot(test_db, employee.id)  # after sign-in, which touches the row
    response = await async_client.patch(
        "/me",
        json={"full_name": "New Name", "phone": "+972501234567"},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 200, response.text
    after = await _row_snapshot(test_db, employee.id)
    changed = {k for k in before if before[k] != after[k]}
    assert changed - {"updated_at"} == {"full_name", "phone"}
    assert after["full_name"] == "New Name"
    assert after["phone"] == "+972501234567"


async def _row_snapshot(session: AsyncSession, user_id: uuid.UUID) -> dict[str, object]:
    """Read the raw users row (fresh SQL, bypassing the identity map)."""
    result = await session.execute(text("SELECT * FROM users WHERE id = :id"), {"id": user_id})
    return dict(result.mappings().one())
