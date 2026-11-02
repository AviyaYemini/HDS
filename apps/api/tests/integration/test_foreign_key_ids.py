"""T137: foreign-key ids in bodies, paths and queries must belong to the caller's organisation.

A foreign id is a 404 (never 403, never silently accepted) and nothing is written.

Covers: NFR-001.
"""

import uuid
from datetime import date

import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hds.models.client import Client
from hds.models.group import Group
from hds.models.qualification import Qualification
from hds.models.site import Site
from hds.models.user import User
from tests.integration.conftest import OtherOrg


async def _headers(client: AsyncClient) -> dict[str, str]:
    r = await client.post("/auth/login", json={"email": "admin@hds.test", "password": "Dev1234!"})
    assert r.status_code == 200, r.text
    return {"Authorization": f"Bearer {r.json()['access_token']}"}


async def _count(session: AsyncSession, sql: str, **params: object) -> int:
    await session.rollback()  # fresh read
    return int(await session.scalar(text(sql), params) or 0)


class Mine:
    """One resource of each kind in the caller's own organisation."""

    client: Client
    site: Site
    group: Group
    qualification: Qualification


@pytest.fixture
async def mine(test_db: AsyncSession, seed_users: dict[str, User]) -> Mine:
    org_id = seed_users["admin"].organization_id
    m = Mine()
    m.client = Client(
        id=uuid.uuid4(), organization_id=org_id, name="My Client", contract_start=date.today()
    )
    m.site = Site(
        id=uuid.uuid4(), organization_id=org_id, name="My Site", address="x", timezone="UTC"
    )
    m.group = Group(id=uuid.uuid4(), organization_id=org_id, name="My Group")
    m.qualification = Qualification(id=uuid.uuid4(), organization_id=org_id, name="My Qual")
    test_db.add_all([m.client, m.site, m.group, m.qualification])
    await test_db.commit()
    return m


def _project_body(client_id: uuid.UUID, site_id: uuid.UUID) -> dict[str, object]:
    return {
        "client_id": str(client_id),
        "site_id": str(site_id),
        "name": "P",
        "type": "security",
        "billing_rate": "10.0000",
        "start_date": "2026-01-01",
    }


# ── POST /api/v1/projects: client_id, site_id ────────────────────────────────────────


@pytest.mark.asyncio
async def test_create_project_with_own_ids_succeeds(
    async_client: AsyncClient, mine: Mine, other_org: OtherOrg
) -> None:
    r = await async_client.post(
        "/api/v1/projects",
        json=_project_body(mine.client.id, mine.site.id),
        headers=await _headers(async_client),
    )
    assert r.status_code == 201, r.text


@pytest.mark.asyncio
@pytest.mark.parametrize("which", ["client", "site", "both"])
async def test_create_project_with_foreign_ids_returns_404_and_writes_nothing(
    which: str,
    async_client: AsyncClient,
    test_db: AsyncSession,
    mine: Mine,
    other_org: OtherOrg,
) -> None:
    client_id = other_org.client.id if which in ("client", "both") else mine.client.id
    site_id = other_org.site.id if which in ("site", "both") else mine.site.id
    r = await async_client.post(
        "/api/v1/projects",
        json=_project_body(client_id, site_id),
        headers=await _headers(async_client),
    )
    assert r.status_code == 404, r.text
    assert await _count(test_db, "SELECT count(*) FROM projects WHERE name = 'P'") == 0


# ── PATCH /api/v1/projects/{id}: no FK id may be changed through the body ─────────────


@pytest.mark.asyncio
async def test_patch_project_cannot_repoint_client_or_site(
    async_client: AsyncClient,
    test_db: AsyncSession,
    mine: Mine,
    other_org: OtherOrg,
) -> None:
    headers = await _headers(async_client)
    created = await async_client.post(
        "/api/v1/projects", json=_project_body(mine.client.id, mine.site.id), headers=headers
    )
    project_id = created.json()["id"]
    r = await async_client.patch(
        f"/api/v1/projects/{project_id}",
        json={
            "name": "Renamed",
            "client_id": str(other_org.client.id),
            "site_id": str(other_org.site.id),
        },
        headers=headers,
    )
    assert r.status_code in (200, 404, 422), r.text
    await test_db.rollback()
    row = (
        await test_db.execute(
            text("SELECT client_id, site_id FROM projects WHERE id = :i"), {"i": project_id}
        )
    ).one()
    assert (row.client_id, row.site_id) == (mine.client.id, mine.site.id)


# ── GET /api/v1/projects?client_id= ───────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_list_projects_filtered_by_foreign_client_returns_404(
    async_client: AsyncClient, mine: Mine, other_org: OtherOrg
) -> None:
    headers = await _headers(async_client)
    own = await async_client.get(
        "/api/v1/projects", params={"client_id": str(mine.client.id)}, headers=headers
    )
    assert own.status_code == 200, own.text
    foreign = await async_client.get(
        "/api/v1/projects", params={"client_id": str(other_org.client.id)}, headers=headers
    )
    assert foreign.status_code == 404, foreign.text


# ── PUT /api/v1/employees/{id}/groups | qualifications: body ids ──────────────────────


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("resource", "own_attr", "foreign_attr", "table"),
    [
        ("groups", "group", "group", "user_groups"),
        ("qualifications", "qualification", "qualification", "user_qualifications"),
    ],
)
@pytest.mark.parametrize("mix_in_own", [False, True])
async def test_replace_memberships_with_foreign_id_returns_404_and_changes_nothing(
    resource: str,
    own_attr: str,
    foreign_attr: str,
    table: str,
    mix_in_own: bool,
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    mine: Mine,
    other_org: OtherOrg,
) -> None:
    headers = await _headers(async_client)
    employee_id = seed_users["employee"].id
    ids = [str(getattr(other_org, foreign_attr).id)]
    if mix_in_own:
        ids.insert(0, str(getattr(mine, own_attr).id))
    r = await async_client.put(
        f"/api/v1/employees/{employee_id}/{resource}", json={"ids": ids}, headers=headers
    )
    assert r.status_code == 404, r.text
    assert await _count(test_db, f"SELECT count(*) FROM {table}") == 0


@pytest.mark.asyncio
async def test_replace_memberships_with_own_ids_succeeds(
    async_client: AsyncClient, seed_users: dict[str, User], mine: Mine
) -> None:
    headers = await _headers(async_client)
    employee_id = seed_users["employee"].id
    r1 = await async_client.put(
        f"/api/v1/employees/{employee_id}/groups",
        json={"ids": [str(mine.group.id)]},
        headers=headers,
    )
    r2 = await async_client.put(
        f"/api/v1/employees/{employee_id}/qualifications",
        json={"ids": [str(mine.qualification.id)]},
        headers=headers,
    )
    assert (r1.status_code, r2.status_code) == (204, 204)


# ── POST/DELETE /api/v1/clients/{id}/users...: client and user ids in the path ────────


@pytest.mark.asyncio
async def test_create_client_user_for_foreign_client_returns_404_and_writes_nothing(
    async_client: AsyncClient, test_db: AsyncSession, mine: Mine, other_org: OtherOrg
) -> None:
    r = await async_client.post(
        f"/api/v1/clients/{other_org.client.id}/users",
        json={
            "email": "portal@hds.test",
            "full_name": "Portal",
            "initial_password": "Dev1234!Dev1234!",
        },
        headers=await _headers(async_client),
    )
    assert r.status_code == 404, r.text
    assert await _count(test_db, "SELECT count(*) FROM users WHERE email = 'portal@hds.test'") == 0


@pytest.mark.asyncio
async def test_remove_foreign_user_from_own_client_returns_404(
    async_client: AsyncClient, test_db: AsyncSession, mine: Mine, other_org: OtherOrg
) -> None:
    r = await async_client.delete(
        f"/api/v1/clients/{mine.client.id}/users/{other_org.user.id}",
        headers=await _headers(async_client),
    )
    assert r.status_code == 404, r.text
    assert (
        await _count(
            test_db,
            "SELECT count(*) FROM users WHERE id = :i AND is_deleted = false",
            i=other_org.user.id,
        )
        == 1
    )
