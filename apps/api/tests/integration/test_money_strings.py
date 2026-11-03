"""Money travels as a plain decimal string, exactly as the schema says.

The contract declares money as a string, but numbers used to be accepted too, so a request the
schema forbids succeeded and the contract run failed at random. Only ASCII digits in the schema's
shape pass. A leading minus and zero still reach the services, which answer with their own codes.

Covers: FR-028 (feature 001), Constitution V and VII.
"""

from decimal import Decimal
from typing import Any

import pytest
from httpx import AsyncClient, Response
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hds.models.user import User

PASSWORD = "Dev1234!"
REJECTED: list[Any] = [
    75,
    75.5,
    0,
    True,
    "1e3",
    "+5",
    " 75",
    "75 ",
    "75.",
    ".5",
    "٣",
    "７５",
    "1,000",
    "75.00000",
    "123456789",
    "75\n",
    "",
    "-",
]


async def headers_for(client: AsyncClient) -> dict[str, str]:
    response = await client.post(
        "/auth/login", json={"email": "admin@hds.test", "password": PASSWORD}
    )
    assert response.status_code == 200, response.text
    return {"Authorization": f"Bearer {response.json()['access_token']}"}


async def create_employee(client: AsyncClient, rate: Any) -> Response:
    return await client.post(
        "/api/v1/employees",
        json={
            "email": "rate@hds.test",
            "full_name": "Rate",
            "initial_password": "A-long-password-1",
            "default_hourly_rate": rate,
        },
        headers=await headers_for(client),
    )


async def create_project(client: AsyncClient, rate: Any) -> Response:
    headers = await headers_for(client)
    created = await client.post(
        "/api/v1/clients", json={"name": "Acme", "contract_start": "2026-01-01"}, headers=headers
    )
    site = await client.post(
        "/api/v1/sites",
        json={"name": "Gate", "address": "1 Example St", "timezone": "UTC"},
        headers=headers,
    )
    return await client.post(
        "/api/v1/projects",
        json={
            "client_id": created.json()["id"],
            "site_id": site.json()["id"],
            "name": "Night",
            "type": "security",
            "billing_rate": rate,
            "start_date": "2026-01-01",
        },
        headers=headers,
    )


@pytest.mark.parametrize("value", REJECTED, ids=repr)
async def test_an_employee_rate_outside_the_schema_is_refused(
    value: Any, async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    answer = await create_employee(async_client, value)

    assert answer.status_code == 422, answer.text
    assert answer.json()["code"] == "validation.request_invalid"
    await test_db.rollback()
    count = await test_db.scalar(text("SELECT count(*) FROM users WHERE email = 'rate@hds.test'"))
    assert count == 0


@pytest.mark.parametrize("value", REJECTED, ids=repr)
async def test_a_billing_rate_outside_the_schema_is_refused(
    value: Any, async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    answer = await create_project(async_client, value)

    assert answer.status_code == 422, answer.text
    assert answer.json()["code"] == "validation.request_invalid"
    await test_db.rollback()
    assert await test_db.scalar(text("SELECT count(*) FROM projects")) == 0


@pytest.mark.parametrize("value", ["75", "75.5", "75.0000", "0.0001", "12345678.1234"])
async def test_a_rate_in_the_schema_shape_is_stored_exactly(
    value: str, async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    answer = await create_employee(async_client, value)

    assert answer.status_code == 201, answer.text
    await test_db.rollback()
    stored = await test_db.scalar(
        text("SELECT default_hourly_rate FROM users WHERE email = 'rate@hds.test'")
    )
    assert stored == Decimal(value)


@pytest.mark.parametrize("value", ["0", "0.00", "-5", "-0.01"])
async def test_zero_and_negative_reach_the_services_and_keep_their_own_codes(
    value: str, async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    employee = await create_employee(async_client, value)
    project = await create_project(async_client, value)

    assert employee.status_code == 422 and project.status_code == 422
    assert employee.json()["code"] == "users.hourly_rate_invalid"
    assert project.json()["code"] == "projects.billing_rate_invalid"


async def test_omitting_or_clearing_an_optional_rate_still_works(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    assert (await create_employee(async_client, None)).status_code == 201
