"""A Client account's projects come from a read that can only mean one client (003 Group C).

The organisation-wide listing treats an empty client as "every client", which is right for staff and
wrong for a Client account. The Client path now uses the per-client read, which has no such meaning,
and keeps the status filter.

Covers: 003 FR-006, FR-237.
"""

import uuid
from typing import Any

from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hds.models.project import ProjectStatus
from hds.models.user import User
from hds.repositories.project import ProjectRepository
from tests.integration.test_patch_null_clears import add_project, headers_for


async def two_projects(client: AsyncClient, headers: dict[str, str]) -> tuple[Any, Any]:
    """One active and one ended project of one client."""
    active = await add_project(client, headers)
    second = await client.post(
        "/api/v1/projects",
        json={
            "client_id": active["client_id"],
            "site_id": active["site_id"],
            "name": "Day Watch",
            "type": "security",
            "billing_rate": "75.0000",
            "start_date": "2026-01-01",
        },
        headers=headers,
    )
    assert second.status_code == 201, second.text
    ended = await client.patch(
        f"/api/v1/projects/{second.json()['id']}", json={"status": "ended"}, headers=headers
    )
    assert ended.status_code == 200, ended.text
    return active, ended.json()


async def test_the_per_client_read_keeps_the_status_filter(
    async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    manager = await headers_for(async_client, "manager@hds.test")
    active, ended = await two_projects(async_client, manager)
    repo = ProjectRepository(test_db)
    org_id, client_id = uuid.UUID(active["organisation_id"]), uuid.UUID(active["client_id"])

    only_ended = await repo.list_by_client(client_id, org_id, status=ProjectStatus.ended)
    every = await repo.list_by_client(client_id, org_id)

    assert [str(p.id) for p in only_ended] == [ended["id"]]
    assert {str(p.id) for p in every} == {active["id"], ended["id"]}


async def test_a_client_account_filters_its_own_projects_by_status(
    async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    manager = await headers_for(async_client, "manager@hds.test")
    active, ended = await two_projects(async_client, manager)
    await add_project(async_client, manager)  # another client's, never shown
    await test_db.execute(
        text("UPDATE users SET client_id = :c WHERE id = :u"),
        {"c": active["client_id"], "u": seed_users["client"].id},
    )
    await test_db.commit()
    portal = await headers_for(async_client, "client@hds.test")

    listed = await async_client.get("/api/v1/projects?status=ended", headers=portal)

    assert listed.status_code == 200, listed.text
    assert [p["id"] for p in listed.json()["items"]] == [ended["id"]]
