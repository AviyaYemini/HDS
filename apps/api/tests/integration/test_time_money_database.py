"""T125: the database itself enforces UTC-aware timestamps and positive rates.

Covers: FR-028; Constitution VII.
"""

import uuid
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from hds.models.client import Client
from hds.models.project import Project, ProjectStatus, ProjectType
from hds.models.site import Site
from hds.models.user import User


async def _login(client: AsyncClient, email: str = "employee@hds.test") -> dict[str, str]:
    r = await client.post("/auth/login", json={"email": email, "password": "Dev1234!"})
    assert r.status_code == 200, r.text
    return r.json()


@pytest.mark.asyncio
async def test_api_timestamps_are_timezone_aware_utc(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    tokens = await _login(async_client)
    r = await async_client.get("/me", headers={"Authorization": f"Bearer {tokens['access_token']}"})
    for field in ("created_at", "updated_at"):
        parsed = datetime.fromisoformat(r.json()[field])
        assert parsed.tzinfo is not None, field
        assert parsed.utcoffset() == timedelta(0), field


@pytest.mark.asyncio
async def test_every_timestamp_column_is_timestamptz_in_postgres(test_db: AsyncSession) -> None:
    rows = (
        await test_db.execute(
            text(
                "SELECT table_name, column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = 'public' AND data_type LIKE 'timestamp%' "
                "AND table_name <> 'alembic_version'"
            )
        )
    ).all()
    assert rows
    naive = [(t, c) for t, c, d in rows if d != "timestamp with time zone"]
    assert not naive, naive


@pytest.mark.asyncio
async def test_refresh_token_expiry_is_aware_and_in_the_future(
    async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    await _login(async_client)
    expires = await test_db.scalar(
        text("SELECT expires_at FROM refresh_tokens WHERE user_id = :u"),
        {"u": seed_users["employee"].id},
    )
    assert expires.tzinfo is not None
    now = datetime.now(UTC)
    assert now < expires <= now + timedelta(days=8)


@pytest.mark.asyncio
async def test_database_rejects_non_positive_hourly_rate(
    test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    with pytest.raises(IntegrityError):
        await test_db.execute(
            text("UPDATE users SET default_hourly_rate = 0 WHERE id = :i"),
            {"i": seed_users["employee"].id},
        )
    await test_db.rollback()


@pytest.mark.asyncio
@pytest.mark.parametrize("rate", [Decimal("0"), Decimal("-1.5")])
async def test_database_rejects_non_positive_billing_rate(
    rate: Decimal, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    org_id = seed_users["admin"].organization_id
    client = Client(id=uuid.uuid4(), organization_id=org_id, name="C", contract_start=date.today())
    site = Site(id=uuid.uuid4(), organization_id=org_id, name="S", address="a", timezone="UTC")
    test_db.add_all([client, site])
    await test_db.commit()
    test_db.add(
        Project(
            id=uuid.uuid4(),
            organization_id=org_id,
            client_id=client.id,
            site_id=site.id,
            name="P",
            type=ProjectType.security,
            billing_rate=rate,
            start_date=date.today(),
            status=ProjectStatus.active,
        )
    )
    with pytest.raises(IntegrityError):
        await test_db.commit()
    await test_db.rollback()


@pytest.mark.asyncio
async def test_money_round_trips_exactly_through_the_api(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    login = await _login(async_client, "admin@hds.test")
    headers = {"Authorization": f"Bearer {login['access_token']}"}
    r = await async_client.patch(
        f"/api/v1/employees/{seed_users['employee'].id}",
        json={"default_hourly_rate": "0.1000"},
        headers=headers,
    )
    assert r.status_code == 200, r.text
    assert Decimal(str(r.json()["default_hourly_rate"])) == Decimal("0.1")
