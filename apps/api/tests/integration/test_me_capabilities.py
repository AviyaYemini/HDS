"""``GET /me`` serves the caller's capabilities and, for a Manager, the projects they manage.

Covers: 007 FR-001, FR-002, FR-028 (contracts/capabilities.md, task T027).
"""

from httpx import AsyncClient

from hds.core.capabilities import capabilities_for
from hds.models.user import User, UserRole
from tests.pay_support import project_managers
from tests.roster_support import add_project, headers_for


async def test_every_role_gets_its_capabilities_from_the_map(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    for role in ("employee", "manager", "admin", "client"):
        response = await async_client.get("/me", headers=await headers_for(async_client, role))
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["capabilities"] == capabilities_for(UserRole(role)), role
        assert body["email"] == f"{role}@hds.test"


async def test_a_client_holds_no_staff_capability(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    body = (await async_client.get("/me", headers=await headers_for(async_client, "client"))).json()
    assert "my_work.read" not in body["capabilities"]
    assert "roster.read" not in body["capabilities"]
    assert body["managed_project_ids"] == []


async def test_a_manager_sees_the_projects_they_manage_and_only_those(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    mine = await add_project(async_client, admin)
    await add_project(async_client, admin, name="Not managed")
    put = await async_client.put(
        project_managers(mine["id"]),
        json={"user_ids": [str(seed_users["manager"].id)]},
        headers=admin,
    )
    assert put.status_code == 200, put.text

    body = (
        await async_client.get("/me", headers=await headers_for(async_client, "manager"))
    ).json()
    assert body["managed_project_ids"] == [mine["id"]]
    admin_body = (await async_client.get("/me", headers=admin)).json()
    assert admin_body["managed_project_ids"] == []  # an Admin's scoped capabilities cover all
