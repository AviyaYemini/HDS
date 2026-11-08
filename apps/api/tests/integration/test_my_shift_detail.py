"""One of my published shifts, with the site's address and whom to contact (007 FR-023, T052).

The project's managers stand in for a shift leader. A draft, or a shift the caller is not on,
answers 404, so other shifts cannot be probed.
"""

from httpx import AsyncClient

from hds.models.user import User
from tests.pay_support import project_managers
from tests.roster_support import FIRST, add_project, add_shift, assign, headers_for


async def test_an_employee_reads_their_published_shift_with_its_contacts(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin)
    await async_client.put(
        project_managers(project["id"]),
        json={"user_ids": [str(seed_users["manager"].id)]},
        headers=admin,
    )
    shift_id = (await add_shift(async_client, admin, project, FIRST))["shift"]["id"]
    assigned = await assign(async_client, admin, shift_id, seed_users["employee"].id)
    assert assigned.status_code == 201
    employee = await headers_for(async_client, "employee")

    draft = await async_client.get(f"/me/shifts/{shift_id}", headers=employee)
    assert draft.status_code == 404

    published = await async_client.post(
        f"/api/v1/projects/{project['id']}/publish",
        json={"from": FIRST.isoformat(), "to": FIRST.isoformat()},
        headers=admin,
    )
    assert published.status_code == 200, published.text
    found = await async_client.get(f"/me/shifts/{shift_id}", headers=employee)
    assert found.status_code == 200, found.text
    detail = found.json()
    assert [c["email"] for c in detail["contacts"]] == ["manager@hds.test"]
    assert "site_address" in detail
    assert "assignments" not in detail  # nothing about who else is on it


async def test_someone_elses_shift_is_not_found(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin)
    shift_id = (await add_shift(async_client, admin, project, FIRST))["shift"]["id"]
    # On someone else's shift: the manager's, published.
    await assign(async_client, admin, shift_id, seed_users["manager"].id)
    await async_client.post(
        f"/api/v1/projects/{project['id']}/publish",
        json={"from": FIRST.isoformat(), "to": FIRST.isoformat()},
        headers=admin,
    )
    response = await async_client.get(
        f"/me/shifts/{shift_id}", headers=await headers_for(async_client, "employee")
    )
    assert response.status_code == 404
