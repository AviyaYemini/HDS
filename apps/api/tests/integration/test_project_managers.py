"""Managers assigned to projects (005 T011).

Covers: 005 FR-021, FR-031, FR-032; US6 acceptance scenarios 1, 2 and 4.
"""

import uuid
from datetime import UTC, datetime

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.security import hash_password
from hds.models.user import User, UserRole
from tests.integration.conftest import OtherOrg
from tests.pay_support import project_managers
from tests.roster_support import add_project, headers_for


async def test_an_admin_replaces_a_projects_managers(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin)
    url = project_managers(project["id"])
    manager, boss = seed_users["manager"], seed_users["admin"]

    first = await async_client.put(
        url, json={"user_ids": [str(manager.id), str(boss.id), str(manager.id)]}, headers=admin
    )
    second = await async_client.put(url, json={"user_ids": [str(boss.id)]}, headers=admin)

    assert first.status_code == 200, first.text
    assert sorted(m["id"] for m in first.json()["items"]) == sorted([str(manager.id), str(boss.id)])
    assert [m["id"] for m in second.json()["items"]] == [str(boss.id)]
    read = await async_client.get(url, headers=await headers_for(async_client, "manager"))
    assert read.json() == second.json()
    assert set(read.json()["items"][0]) == {"id", "full_name"}


async def test_an_empty_list_clears_the_managers(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin)
    url = project_managers(project["id"])
    await async_client.put(url, json={"user_ids": [str(seed_users["manager"].id)]}, headers=admin)

    cleared = await async_client.put(url, json={"user_ids": []}, headers=admin)

    assert cleared.json() == {"items": []}


async def inactive_manager(db: AsyncSession, org_id: uuid.UUID) -> User:
    user = User(
        id=uuid.uuid4(),
        organisation_id=org_id,
        email="gone@hds.test",
        password_hash=hash_password("Dev1234!"),
        full_name="Gone Manager",
        role=UserRole.manager,
        is_active=False,
        activated_at=datetime.now(UTC),
    )
    db.add(user)
    await db.commit()
    return user


@pytest.mark.parametrize("who", ["employee", "client", "inactive", "foreign", "unknown"])
async def test_only_active_managers_and_admins_of_the_organisation_can_be_assigned(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    test_db: AsyncSession,
    other_org: OtherOrg,
    who: str,
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin)
    ids = {
        "employee": seed_users["employee"].id,
        "client": seed_users["client"].id,
        "foreign": other_org.user.id if other_org.user else uuid.uuid4(),
        "unknown": uuid.uuid4(),
    }
    if who == "inactive":
        ids[who] = (await inactive_manager(test_db, seed_users["admin"].organisation_id)).id
    url = project_managers(project["id"])

    answer = await async_client.put(
        url, json={"user_ids": [str(seed_users["manager"].id), str(ids[who])]}, headers=admin
    )

    assert answer.status_code == 422, answer.text
    assert answer.json()["code"] == "project_managers.not_staff"
    assert (await async_client.get(url, headers=admin)).json() == {"items": []}


@pytest.mark.parametrize("role", ["manager", "employee", "client"])
async def test_only_an_admin_changes_the_managers(
    async_client: AsyncClient, seed_users: dict[str, User], role: str
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin)

    answer = await async_client.put(
        project_managers(project["id"]),
        json={"user_ids": [str(seed_users["manager"].id)]},
        headers=await headers_for(async_client, role),
    )

    assert answer.status_code == 403


@pytest.mark.parametrize("role", ["employee", "client"])
async def test_employees_and_clients_cannot_read_the_managers(
    async_client: AsyncClient, seed_users: dict[str, User], role: str
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin)

    answer = await async_client.get(
        project_managers(project["id"]), headers=await headers_for(async_client, role)
    )

    assert answer.status_code == 403


async def test_another_organisations_project_is_not_found(
    async_client: AsyncClient, seed_users: dict[str, User], other_org: OtherOrg
) -> None:
    admin = await headers_for(async_client, "admin")
    assert other_org.project is not None
    url = project_managers(str(other_org.project.id))

    read = await async_client.get(url, headers=admin)
    changed = await async_client.put(url, json={"user_ids": []}, headers=admin)

    assert (read.status_code, changed.status_code) == (404, 404)
