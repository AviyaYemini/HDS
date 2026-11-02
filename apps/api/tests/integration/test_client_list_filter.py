"""GET /clients?is_active= filters by active state and stays organisation-scoped (T143).

Covers: FR-021, NFR-001.
"""

import uuid
from datetime import date

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from hds.models.client import Client
from hds.models.organisation import Organisation
from hds.models.user import User
from tests.integration.conftest import OtherOrg


@pytest.mark.asyncio
async def test_clients_filter_by_is_active(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_org: Organisation,
    seed_users: dict[str, User],
    other_org: OtherOrg,
) -> None:
    for name, active in (("Active Co", True), ("Dormant Co", False)):
        test_db.add(
            Client(
                id=uuid.uuid4(),
                organization_id=seed_org.id,
                name=name,
                contract_start=date.today(),
                is_active=active,
            )
        )
    await test_db.commit()
    login = await async_client.post(
        "/auth/login", json={"email": "manager@hds.test", "password": "Dev1234!"}
    )
    headers = {"Authorization": f"Bearer {login.json()['access_token']}"}

    async def names(query: str) -> set[str]:
        r = await async_client.get(f"/api/v1/clients{query}", headers=headers)
        assert r.status_code == 200, r.text
        return {c["name"] for c in r.json()["items"]}

    assert await names("") == {"Active Co", "Dormant Co"}
    assert await names("?is_active=true") == {"Active Co"}
    assert await names("?is_active=false") == {"Dormant Co"}
    assert other_org.client.name not in await names("")
    bad = await async_client.get("/api/v1/clients?is_active=maybe", headers=headers)
    assert bad.status_code == 422
