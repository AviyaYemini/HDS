"""T134: every lookup is scoped to the caller's organisation (cross-tenant ids return 404)."""

import uuid
from dataclasses import dataclass
from datetime import date
from decimal import Decimal

import pytest
import pytest_asyncio
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.security import hash_password
from hds.models.client import Client
from hds.models.group import Group
from hds.models.organisation import Organisation
from hds.models.project import Project, ProjectStatus, ProjectType
from hds.models.qualification import Qualification
from hds.models.site import Site
from hds.models.user import User, UserRole
from hds.repositories.client import ClientRepository
from hds.repositories.group import GroupRepository
from hds.repositories.project import ProjectRepository
from hds.repositories.qualification import QualificationRepository
from hds.repositories.site import SiteRepository
from hds.repositories.user import UserRepository


@dataclass
class OtherOrg:
    org: Organisation
    user: User
    client: Client
    site: Site
    project: Project
    group: Group
    qualification: Qualification


@pytest_asyncio.fixture
async def other_org(test_db: AsyncSession, seed_users: dict[str, User]) -> OtherOrg:
    """A second organisation with one of each resource (the caller is in ``seed_org``)."""
    org = Organisation(id=uuid.uuid4(), name="Other Org", timezone="UTC")
    test_db.add(org)
    await test_db.flush()
    user = User(
        id=uuid.uuid4(),
        organization_id=org.id,
        email="employee@other.test",
        password_hash=hash_password("Dev1234!"),
        full_name="Other Employee",
        role=UserRole.employee,
    )
    client = Client(
        id=uuid.uuid4(), organization_id=org.id, name="Other Client", contract_start=date.today()
    )
    site = Site(
        id=uuid.uuid4(), organization_id=org.id, name="Other Site", address="x", timezone="UTC"
    )
    group = Group(id=uuid.uuid4(), organization_id=org.id, name="Other Group")
    qualification = Qualification(id=uuid.uuid4(), organization_id=org.id, name="Other Qual")
    test_db.add_all([user, client, site, group, qualification])
    await test_db.flush()
    project = Project(
        id=uuid.uuid4(),
        organization_id=org.id,
        client_id=client.id,
        site_id=site.id,
        name="Other Project",
        type=ProjectType.security,
        billing_rate=Decimal("10.0000"),
        start_date=date.today(),
        status=ProjectStatus.active,
    )
    test_db.add(project)
    await test_db.commit()
    return OtherOrg(org, user, client, site, project, group, qualification)


async def _admin_headers(client: AsyncClient) -> dict[str, str]:
    response = await client.post(
        "/auth/login", json={"email": "admin@hds.test", "password": "Dev1234!"}
    )
    assert response.status_code == 200, response.text
    return {"Authorization": f"Bearer {response.json()['access_token']}"}


# ── Repositories: get_by_id and list never return another organisation's rows ───────


@pytest.mark.asyncio
async def test_repositories_get_by_id_is_org_scoped(
    test_db: AsyncSession, seed_org: Organisation, other_org: OtherOrg
) -> None:
    mine, theirs = seed_org.id, other_org.org.id
    cases = [
        (UserRepository(test_db), other_org.user.id),
        (ClientRepository(test_db), other_org.client.id),
        (SiteRepository(test_db), other_org.site.id),
        (ProjectRepository(test_db), other_org.project.id),
        (GroupRepository(test_db), other_org.group.id),
        (QualificationRepository(test_db), other_org.qualification.id),
    ]
    for repo, resource_id in cases:
        assert await repo.get_by_id(resource_id, mine) is None, type(repo).__name__
        assert await repo.get_by_id(resource_id, theirs) is not None, type(repo).__name__


@pytest.mark.asyncio
async def test_repositories_list_is_org_scoped(
    test_db: AsyncSession, seed_org: Organisation, other_org: OtherOrg
) -> None:
    mine = seed_org.id
    other_ids = {
        other_org.user.id,
        other_org.client.id,
        other_org.site.id,
        other_org.project.id,
        other_org.group.id,
        other_org.qualification.id,
    }
    listings = [
        await UserRepository(test_db).list(mine),
        await ClientRepository(test_db).list_by_org(mine),
        await SiteRepository(test_db).list_by_org(mine),
        await ProjectRepository(test_db).list_by_org(mine),
        await GroupRepository(test_db).list_by_org(mine),
        await QualificationRepository(test_db).list_by_org(mine),
        await ClientRepository(test_db).list(mine),
        await ProjectRepository(test_db).list(mine),
    ]
    for rows in listings:
        assert not ({r.id for r in rows} & other_ids)
    # Child listings keyed by a foreign id are scoped too.
    assert await ClientRepository(test_db).list_users(other_org.client.id, mine) == []
    assert await ProjectRepository(test_db).list_by_client(other_org.client.id, mine) == []
    assert await GroupRepository(test_db).get_groups_for_user(other_org.user.id, mine) == []
    assert (
        await QualificationRepository(test_db).get_qualifications_for_user(other_org.user.id, mine)
        == []
    )


# ── Routes: a UUID from another organisation is a 404 ────────────────────────────────

_ROUTES: list[tuple[str, str, str, dict[str, object] | None]] = [
    # (method, path template, resource attr on OtherOrg, json body)
    ("GET", "/api/v1/clients/{id}", "client", None),
    ("PATCH", "/api/v1/clients/{id}", "client", {"name": "x"}),
    ("DELETE", "/api/v1/clients/{id}", "client", None),
    ("GET", "/api/v1/clients/{id}/users", "client", None),
    ("GET", "/api/v1/sites/{id}", "site", None),
    ("PATCH", "/api/v1/sites/{id}", "site", {"name": "x"}),
    ("GET", "/api/v1/projects/{id}", "project", None),
    ("PATCH", "/api/v1/projects/{id}", "project", {"name": "x"}),
    ("DELETE", "/api/v1/projects/{id}", "project", None),
    ("GET", "/api/v1/employees/{id}", "user", None),
    ("PATCH", "/api/v1/employees/{id}", "user", {"full_name": "x"}),
    ("DELETE", "/api/v1/employees/{id}", "user", None),
    ("POST", "/api/v1/employees/{id}/deactivate", "user", None),
    ("POST", "/api/v1/employees/{id}/reactivate", "user", None),
    ("GET", "/api/v1/employees/{id}/groups", "user", None),
    ("GET", "/api/v1/employees/{id}/qualifications", "user", None),
    ("PUT", "/api/v1/employees/{id}/groups", "user", {"ids": []}),
    ("PUT", "/api/v1/employees/{id}/qualifications", "user", {"ids": []}),
    ("PATCH", "/api/v1/groups/{id}", "group", {"name": "x"}),
    ("DELETE", "/api/v1/groups/{id}", "group", None),
    ("PATCH", "/api/v1/qualifications/{id}", "qualification", {"name": "x"}),
    ("DELETE", "/api/v1/qualifications/{id}", "qualification", None),
]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("method", "template", "attr", "body"), _ROUTES, ids=[f"{m} {t}" for m, t, _, _ in _ROUTES]
)
async def test_other_org_uuid_returns_404(
    method: str,
    template: str,
    attr: str,
    body: dict[str, object] | None,
    async_client: AsyncClient,
    seed_users: dict[str, User],
    other_org: OtherOrg,
) -> None:
    headers = await _admin_headers(async_client)
    resource_id = getattr(other_org, attr).id
    response = await async_client.request(
        method, template.format(id=resource_id), json=body, headers=headers
    )
    assert response.status_code == 404, response.text


@pytest.mark.asyncio
async def test_other_org_rows_are_untouched_by_cross_tenant_writes(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    other_org: OtherOrg,
) -> None:
    headers = await _admin_headers(async_client)
    await async_client.delete(f"/api/v1/clients/{other_org.client.id}", headers=headers)
    await async_client.patch(
        f"/api/v1/sites/{other_org.site.id}", json={"name": "hacked"}, headers=headers
    )
    await async_client.delete(f"/api/v1/groups/{other_org.group.id}", headers=headers)
    await async_client.post(f"/api/v1/employees/{other_org.user.id}/deactivate", headers=headers)
    await test_db.rollback()  # drop any cached state; re-read from the database
    org_id = other_org.org.id
    assert (await ClientRepository(test_db).get_by_id(other_org.client.id, org_id)) is not None
    site = await SiteRepository(test_db).get_by_id(other_org.site.id, org_id)
    assert site is not None
    await test_db.refresh(site)
    assert site.name == "Other Site"
    assert (await GroupRepository(test_db).get_by_id(other_org.group.id, org_id)) is not None
    user = await UserRepository(test_db).get_by_id(other_org.user.id, org_id)
    assert user is not None
    await test_db.refresh(user)
    assert user.is_active is True
