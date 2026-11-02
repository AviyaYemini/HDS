"""T123: range and rate violations are 422 problem-details with stable codes.

Covers: FR-028, NFR-004.
"""

import uuid
from datetime import date

import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hds.models.client import Client
from hds.models.site import Site
from hds.models.user import User


async def _headers(client: AsyncClient) -> dict[str, str]:
    r = await client.post("/auth/login", json={"email": "admin@hds.test", "password": "Dev1234!"})
    return {"Authorization": f"Bearer {r.json()['access_token']}"}


@pytest.fixture
async def ids(test_db: AsyncSession, seed_users: dict[str, User]) -> dict[str, uuid.UUID]:
    org_id = seed_users["admin"].organization_id
    client = Client(id=uuid.uuid4(), organization_id=org_id, name="C", contract_start=date.today())
    site = Site(id=uuid.uuid4(), organization_id=org_id, name="S", address="a", timezone="UTC")
    test_db.add_all([client, site])
    await test_db.commit()
    return {"client_id": client.id, "site_id": site.id}


def _project(ids: dict[str, uuid.UUID], **over: object) -> dict[str, object]:
    body: dict[str, object] = {
        "client_id": str(ids["client_id"]),
        "site_id": str(ids["site_id"]),
        "name": "P",
        "type": "security",
        "billing_rate": "10.0000",
        "start_date": "2026-03-01",
        **over,
    }
    return body


def _assert_problem(r, code: str) -> None:  # type: ignore[no-untyped-def]
    assert r.status_code == 422, r.text
    assert r.headers["content-type"].startswith("application/problem+json")
    assert r.json()["code"] == code


@pytest.mark.asyncio
async def test_create_project_end_before_start_is_422(
    async_client: AsyncClient, ids: dict[str, uuid.UUID]
) -> None:
    r = await async_client.post(
        "/api/v1/projects",
        json=_project(ids, end_date="2026-02-28"),
        headers=await _headers(async_client),
    )
    _assert_problem(r, "projects.date_range_invalid")


@pytest.mark.asyncio
async def test_create_project_end_equal_start_is_allowed(
    async_client: AsyncClient, ids: dict[str, uuid.UUID]
) -> None:
    r = await async_client.post(
        "/api/v1/projects",
        json=_project(ids, end_date="2026-03-01"),
        headers=await _headers(async_client),
    )
    assert r.status_code == 201, r.text


@pytest.mark.asyncio
async def test_edit_project_end_before_start_is_422_and_unchanged(
    async_client: AsyncClient, test_db: AsyncSession, ids: dict[str, uuid.UUID]
) -> None:
    headers = await _headers(async_client)
    created = await async_client.post("/api/v1/projects", json=_project(ids), headers=headers)
    pid = created.json()["id"]

    r = await async_client.patch(
        f"/api/v1/projects/{pid}", json={"end_date": "2026-01-01"}, headers=headers
    )

    _assert_problem(r, "projects.date_range_invalid")
    await test_db.rollback()
    end = await test_db.scalar(text("SELECT end_date FROM projects WHERE id = :i"), {"i": pid})
    assert end is None


@pytest.mark.asyncio
@pytest.mark.parametrize("rate", ["0", "-5"])
async def test_create_employee_non_positive_rate_is_422(
    rate: str, async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    r = await async_client.post(
        "/api/v1/employees",
        json={
            "email": "rate@hds.test",
            "full_name": "Rate",
            "initial_password": "Dev1234!Dev1234!",
            "default_hourly_rate": rate,
        },
        headers=await _headers(async_client),
    )
    _assert_problem(r, "users.hourly_rate_invalid")


@pytest.mark.asyncio
@pytest.mark.parametrize("rate", ["0", "-0.01"])
async def test_edit_employee_non_positive_rate_is_422_and_unchanged(
    rate: str,
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
) -> None:
    target = seed_users["employee"]
    r = await async_client.patch(
        f"/api/v1/employees/{target.id}",
        json={"default_hourly_rate": rate},
        headers=await _headers(async_client),
    )
    _assert_problem(r, "users.hourly_rate_invalid")
    await test_db.rollback()
    stored = await test_db.scalar(
        text("SELECT default_hourly_rate FROM users WHERE id = :i"), {"i": target.id}
    )
    assert stored is None


@pytest.mark.asyncio
async def test_positive_rate_is_accepted(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    r = await async_client.patch(
        f"/api/v1/employees/{seed_users['employee'].id}",
        json={"default_hourly_rate": "42.5000"},
        headers=await _headers(async_client),
    )
    assert r.status_code == 200, r.text


@pytest.mark.asyncio
async def test_error_bodies_carry_the_request_id(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    """T141: a real problem+json (401 from bad credentials) echoes the X-Request-Id header."""
    response = await async_client.post(
        "/auth/login",
        json={"email": "admin@hds.test", "password": "wrong-password-xx"},
        headers={"X-Request-Id": "trace-me-1"},
    )
    assert response.status_code == 401
    assert response.headers["X-Request-Id"] == "trace-me-1"
    assert response.json()["request_id"] == "trace-me-1"
