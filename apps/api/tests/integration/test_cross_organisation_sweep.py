"""Cross-organisation sweep: every operation that takes an id or an address from the request (T262).

The inventory is read from the application's OpenAPI document, so a new endpoint with an id or an
email in its path, query or body cannot be forgotten: ``test_every_operation_is_a_case_or_exempt``
fails until the operation is added to ``CASES`` or ``EXEMPT``. Each case is sent as an Admin of one
organisation with another organisation's id or address. The answer must be 404 for an id (the same
as for an id that does not exist) or 409 ``users.duplicate_email`` for an address, and nothing may
change in any organisation, shown by a digest of every table taken before and after.

Covers: NFR-001, FR-012 (feature 001); the 409 limit is recorded in the spec assumptions.
"""

import hashlib
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

import pytest
import pytest_asyncio
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.security import hash_password
from hds.main import app
from hds.models.client import Client
from hds.models.group import Group
from hds.models.project import Project, ProjectStatus, ProjectType
from hds.models.qualification import Qualification
from hds.models.site import Site
from hds.models.user import User, UserRole
from tests.integration.conftest import OtherOrg

PASSWORD = "Dev1234!"
METHODS = ("get", "post", "put", "patch", "delete")


@dataclass
class Mine:
    """The caller's own resources, for cases that mix an own id with a foreign one."""

    user: User
    client: Client
    site: Site
    project: Project
    group: Group
    qualification: Qualification


@pytest_asyncio.fixture
async def mine(test_db: AsyncSession, seed_users: dict[str, User]) -> Mine:
    org_id = seed_users["admin"].organization_id
    client = Client(
        id=uuid.uuid4(), organization_id=org_id, name="My Client", contract_start=date.today()
    )
    site = Site(
        id=uuid.uuid4(), organization_id=org_id, name="My Site", address="x", timezone="UTC"
    )
    group = Group(id=uuid.uuid4(), organization_id=org_id, name="My Group")
    qualification = Qualification(id=uuid.uuid4(), organization_id=org_id, name="My Qual")
    portal_user = User(
        id=uuid.uuid4(),
        organization_id=org_id,
        email="portal@hds.test",
        password_hash=hash_password(PASSWORD),
        full_name="My Portal",
        role=UserRole.client,
        activated_at=datetime.now(UTC),
    )
    test_db.add_all([client, site, group, qualification, portal_user])
    await test_db.flush()
    project = Project(
        id=uuid.uuid4(),
        organization_id=org_id,
        client_id=client.id,
        site_id=site.id,
        name="My Project",
        type=ProjectType.security,
        billing_rate=Decimal("10.0000"),
        start_date=date.today(),
        status=ProjectStatus.active,
    )
    test_db.add(project)
    await test_db.commit()
    return Mine(seed_users["employee"], client, site, project, group, qualification)


@dataclass
class World:
    """Everything a case may use to build its request."""

    mine: Mine
    theirs: OtherOrg

    @property
    def foreign_email(self) -> str:
        return self.theirs.user.email


@dataclass
class Case:
    """One request with a foreign id or address, and the answer it must get."""

    why: str
    build: Callable[[World], tuple[str, dict[str, Any]]]  # (path, request kwargs)
    status: int = 404
    code: str = "resource.not_found"


def req(path: str, **kwargs: Any) -> tuple[str, dict[str, Any]]:
    return path, kwargs


def new_employee(email: str) -> dict[str, Any]:
    return {
        "email": email,
        "full_name": "Somebody",
        "role": "employee",
        "initial_password": "A-long-password-1",
    }


def new_project(client_id: uuid.UUID, site_id: uuid.UUID) -> dict[str, Any]:
    return {
        "client_id": str(client_id),
        "site_id": str(site_id),
        "name": "Sneaky",
        "type": "security",
        "billing_rate": "10.0000",
        "start_date": date.today().isoformat(),
    }


DUPLICATE: dict[str, Any] = {"status": 409, "code": "users.duplicate_email"}

# Key: "METHOD /path" exactly as in the OpenAPI document; value: the requests to send.
CASES: dict[str, list[Case]] = {
    "GET /api/v1/clients/{id}": [
        Case("foreign client id", lambda w: req(f"/api/v1/clients/{w.theirs.client.id}")),
    ],
    "PATCH /api/v1/clients/{id}": [
        Case(
            "foreign client id",
            lambda w: req(f"/api/v1/clients/{w.theirs.client.id}", json={"name": "Taken over"}),
        ),
    ],
    "DELETE /api/v1/clients/{id}": [
        Case("foreign client id", lambda w: req(f"/api/v1/clients/{w.theirs.client.id}")),
    ],
    "GET /api/v1/clients/{id}/users": [
        Case("foreign client id", lambda w: req(f"/api/v1/clients/{w.theirs.client.id}/users")),
    ],
    "POST /api/v1/clients/{id}/users": [
        Case(
            "foreign client id",
            lambda w: req(
                f"/api/v1/clients/{w.theirs.client.id}/users",
                json={
                    "email": "new.portal@hds.test",
                    "full_name": "New Portal",
                    "initial_password": "A-long-password-1",
                },
            ),
        ),
        Case(
            "own client, same address in different letter case",
            lambda w: req(
                f"/api/v1/clients/{w.mine.client.id}/users",
                json={
                    "email": w.foreign_email.upper(),
                    "full_name": "New Portal",
                    "initial_password": "A-long-password-1",
                },
            ),
            **DUPLICATE,
        ),
        Case(
            "own client, address held by another organisation",
            lambda w: req(
                f"/api/v1/clients/{w.mine.client.id}/users",
                json={
                    "email": w.foreign_email,
                    "full_name": "New Portal",
                    "initial_password": "A-long-password-1",
                },
            ),
            **DUPLICATE,
        ),
    ],
    "DELETE /api/v1/clients/{id}/users/{user_id}": [
        Case(
            "foreign client id, foreign user id",
            lambda w: req(f"/api/v1/clients/{w.theirs.client.id}/users/{w.theirs.user.id}"),
        ),
        Case(
            "own client id, foreign user id",
            lambda w: req(f"/api/v1/clients/{w.mine.client.id}/users/{w.theirs.user.id}"),
        ),
    ],
    "GET /api/v1/employees": [
        Case("foreign group_id", lambda w: req(f"/api/v1/employees?group_id={w.theirs.group.id}")),
        Case(
            "foreign qualification_id",
            lambda w: req(f"/api/v1/employees?qualification_id={w.theirs.qualification.id}"),
        ),
    ],
    "POST /api/v1/employees": [
        Case(
            "address held by another organisation",
            lambda w: req("/api/v1/employees", json=new_employee(w.foreign_email)),
            **DUPLICATE,
        ),
        Case(
            "same address in different letter case",
            lambda w: req("/api/v1/employees", json=new_employee(w.foreign_email.upper())),
            **DUPLICATE,
        ),
    ],
    "GET /api/v1/employees/{id}": [
        Case("foreign user id", lambda w: req(f"/api/v1/employees/{w.theirs.user.id}")),
    ],
    "PATCH /api/v1/employees/{id}": [
        Case(
            "foreign user id",
            lambda w: req(f"/api/v1/employees/{w.theirs.user.id}", json={"full_name": "Renamed"}),
        ),
        Case(
            "foreign user id, role change",
            lambda w: req(f"/api/v1/employees/{w.theirs.user.id}", json={"role": "manager"}),
        ),
    ],
    "DELETE /api/v1/employees/{id}": [
        Case("foreign user id", lambda w: req(f"/api/v1/employees/{w.theirs.user.id}")),
    ],
    "POST /api/v1/employees/{id}/deactivate": [
        Case("foreign user id", lambda w: req(f"/api/v1/employees/{w.theirs.user.id}/deactivate")),
    ],
    "POST /api/v1/employees/{id}/reactivate": [
        Case("foreign user id", lambda w: req(f"/api/v1/employees/{w.theirs.user.id}/reactivate")),
    ],
    "GET /api/v1/employees/{id}/groups": [
        Case("foreign user id", lambda w: req(f"/api/v1/employees/{w.theirs.user.id}/groups")),
    ],
    "PUT /api/v1/employees/{id}/groups": [
        Case(
            "foreign user id, own group",
            lambda w: req(
                f"/api/v1/employees/{w.theirs.user.id}/groups", json={"ids": [str(w.mine.group.id)]}
            ),
        ),
        Case(
            "own user id, foreign group",
            lambda w: req(
                f"/api/v1/employees/{w.mine.user.id}/groups",
                json={"ids": [str(w.theirs.group.id)]},
            ),
        ),
        Case(
            "own user id, own group and foreign group",
            lambda w: req(
                f"/api/v1/employees/{w.mine.user.id}/groups",
                json={"ids": [str(w.mine.group.id), str(w.theirs.group.id)]},
            ),
        ),
    ],
    "GET /api/v1/employees/{id}/qualifications": [
        Case(
            "foreign user id",
            lambda w: req(f"/api/v1/employees/{w.theirs.user.id}/qualifications"),
        ),
    ],
    "PUT /api/v1/employees/{id}/qualifications": [
        Case(
            "foreign user id, own qualification",
            lambda w: req(
                f"/api/v1/employees/{w.theirs.user.id}/qualifications",
                json={"ids": [str(w.mine.qualification.id)]},
            ),
        ),
        Case(
            "own user id, foreign qualification",
            lambda w: req(
                f"/api/v1/employees/{w.mine.user.id}/qualifications",
                json={"ids": [str(w.theirs.qualification.id)]},
            ),
        ),
        Case(
            "own user id, own qualification and foreign qualification",
            lambda w: req(
                f"/api/v1/employees/{w.mine.user.id}/qualifications",
                json={"ids": [str(w.mine.qualification.id), str(w.theirs.qualification.id)]},
            ),
        ),
    ],
    "PATCH /api/v1/groups/{id}": [
        Case(
            "foreign group id",
            lambda w: req(f"/api/v1/groups/{w.theirs.group.id}", json={"name": "Renamed"}),
        ),
    ],
    "DELETE /api/v1/groups/{id}": [
        Case("foreign group id", lambda w: req(f"/api/v1/groups/{w.theirs.group.id}")),
    ],
    "GET /api/v1/projects": [
        Case(
            "foreign client_id", lambda w: req(f"/api/v1/projects?client_id={w.theirs.client.id}")
        ),
    ],
    "POST /api/v1/projects": [
        Case(
            "foreign client id, own site",
            lambda w: req("/api/v1/projects", json=new_project(w.theirs.client.id, w.mine.site.id)),
        ),
        Case(
            "own client, foreign site id",
            lambda w: req("/api/v1/projects", json=new_project(w.mine.client.id, w.theirs.site.id)),
        ),
    ],
    "GET /api/v1/projects/{id}": [
        Case("foreign project id", lambda w: req(f"/api/v1/projects/{w.theirs.project.id}")),
    ],
    "PATCH /api/v1/projects/{id}": [
        Case(
            "foreign project id",
            lambda w: req(f"/api/v1/projects/{w.theirs.project.id}", json={"name": "Renamed"}),
        ),
    ],
    "DELETE /api/v1/projects/{id}": [
        Case("foreign project id", lambda w: req(f"/api/v1/projects/{w.theirs.project.id}")),
    ],
    "PATCH /api/v1/qualifications/{id}": [
        Case(
            "foreign qualification id",
            lambda w: req(
                f"/api/v1/qualifications/{w.theirs.qualification.id}", json={"name": "Renamed"}
            ),
        ),
    ],
    "DELETE /api/v1/qualifications/{id}": [
        Case(
            "foreign qualification id",
            lambda w: req(f"/api/v1/qualifications/{w.theirs.qualification.id}"),
        ),
    ],
    "GET /api/v1/sites/{id}": [
        Case("foreign site id", lambda w: req(f"/api/v1/sites/{w.theirs.site.id}")),
    ],
    "PATCH /api/v1/sites/{id}": [
        Case(
            "foreign site id",
            lambda w: req(f"/api/v1/sites/{w.theirs.site.id}", json={"name": "Renamed"}),
        ),
    ],
    "POST /auth/invite": [
        Case(
            "address held by another organisation",
            lambda w: req("/auth/invite", json={"email": w.foreign_email, "role": "employee"}),
            **DUPLICATE,
        ),
        Case(
            "same address in different letter case",
            lambda w: req(
                "/auth/invite", json={"email": w.foreign_email.upper(), "role": "employee"}
            ),
            **DUPLICATE,
        ),
    ],
}

# Operations that take an id or an address but are not refused by organisation, with the reason.
EXEMPT: dict[str, str] = {
    "POST /auth/login": "public; authenticates by email and password, so another organisation's "
    "address with its password is that organisation's own sign-in",
    "POST /auth/password-reset/request": "public; answers 204 whether or not the address exists "
    "and mails only the address's own holder (tested in the password-reset tests)",
}

# Fields that carry a caller-supplied id or address.
PATH_IDS = {"id", "user_id"}


def inventory() -> set[str]:
    """Every ``METHOD /path`` in the OpenAPI document that takes an id or an email."""
    doc = app.openapi()
    schemas = doc["components"]["schemas"]
    found: set[str] = set()
    for path, item in doc["paths"].items():
        for method in METHODS:
            op = item.get(method)
            if op is None:
                continue
            takes = False
            for p in op.get("parameters", []):
                name = p["name"]
                if (p["in"] == "path" and name in PATH_IDS) or (
                    p["in"] == "query" and (name.endswith("_id") or name == "email")
                ):
                    takes = True
            for media in op.get("requestBody", {}).get("content", {}).values():
                ref = media.get("schema", {}).get("$ref", "")
                for name in schemas.get(ref.rsplit("/", 1)[-1], {}).get("properties", {}):
                    if name.endswith("_id") or name in ("ids", "email"):
                        takes = True
            if takes:
                found.add(f"{method.upper()} {path}")
    return found


def test_every_operation_is_a_case_or_exempt() -> None:
    found = inventory()
    known = set(CASES) | set(EXEMPT)
    assert not (found - known), (
        f"operations with an id or email and no sweep case: {sorted(found - known)}"
    )
    assert not (known - found), (
        f"cases for operations that no longer exist: {sorted(known - found)}"
    )
    assert not (set(CASES) & set(EXEMPT))


async def digest(db: AsyncSession) -> dict[str, str]:
    """A hash of every row of every table, so any change anywhere shows up."""
    await db.rollback()  # end any snapshot so the read sees committed data
    names = (
        await db.execute(
            text("SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY 1")
        )
    ).scalars()
    result: dict[str, str] = {}
    for name in list(names):
        rows = (
            (await db.execute(text(f'SELECT t::text FROM "{name}" t ORDER BY 1'))).scalars().all()
        )
        result[name] = hashlib.sha256("\n".join(rows).encode()).hexdigest()
    return result


async def admin_headers(client: AsyncClient) -> dict[str, str]:
    response = await client.post(
        "/auth/login", json={"email": "admin@hds.test", "password": PASSWORD}
    )
    assert response.status_code == 200, response.text
    return {"Authorization": f"Bearer {response.json()['access_token']}"}


FLAT = [(key, case) for key, cases in CASES.items() for case in cases]


@pytest.mark.parametrize("key,case", FLAT, ids=[f"{k} | {c.why}" for k, c in FLAT])
async def test_foreign_id_or_address_is_refused_and_nothing_changes(
    key: str,
    case: Case,
    async_client: AsyncClient,
    test_db: AsyncSession,
    mine: Mine,
    other_org: OtherOrg,
    outbox: Any,
) -> None:
    headers = await admin_headers(async_client)
    path, kwargs = case.build(World(mine, other_org))
    method = key.split(" ", 1)[0]
    before = await digest(test_db)

    response = await async_client.request(method, path, headers=headers, **kwargs)

    assert response.status_code == case.status, response.text
    if response.content:
        assert response.json().get("code") == case.code, response.text
    assert await digest(test_db) == before, "a refused request changed data"
    assert outbox.messages == [], "a refused request sent mail"


# ── Callers other than an Admin (T304) ───────────────────────────────────────────────────

# Cases a Manager may not perform at all: the role check comes first, so the answer is 403 and
# says nothing about whether the foreign id exists.
ADMIN_ONLY = {
    "DELETE /api/v1/employees/{id}": "users.admin_required",
    "DELETE /api/v1/clients/{id}": "users.admin_required",
    "DELETE /api/v1/projects/{id}": "users.admin_required",
}

# The two project reads that a Client user (the customer's portal account) may use for the
# client's own projects. A foreign id must still get the answer of a missing one.
PORTAL_READS = {
    "GET /api/v1/projects": "the client_id filter is ignored; only their own projects",
    "GET /api/v1/projects/{id}": "404 for another organisation's project, as for a missing one",
}

# Employee and Client users may not use any of these operations: refused by role (403) before any
# lookup by organisation, so another organisation's id never gets a different answer from a
# missing one.
NOT_STAFF = ("employee", "client")


async def headers_for(client: AsyncClient, role: str) -> dict[str, str]:
    response = await client.post(
        "/auth/login", json={"email": f"{role}@hds.test", "password": PASSWORD}
    )
    assert response.status_code == 200, response.text
    return {"Authorization": f"Bearer {response.json()['access_token']}"}


@pytest.mark.parametrize("key,case", FLAT, ids=[f"{k} | {c.why}" for k, c in FLAT])
async def test_a_manager_gets_the_same_refusals(
    key: str,
    case: Case,
    async_client: AsyncClient,
    test_db: AsyncSession,
    mine: Mine,
    other_org: OtherOrg,
    outbox: Any,
) -> None:
    headers = await headers_for(async_client, "manager")
    path, kwargs = case.build(World(mine, other_org))
    method = key.split(" ", 1)[0]
    before = await digest(test_db)

    response = await async_client.request(method, path, headers=headers, **kwargs)

    if key in ADMIN_ONLY:
        assert response.status_code == 403, response.text
        assert response.json()["code"] == ADMIN_ONLY[key]
    else:
        assert response.status_code == case.status, response.text
        assert response.json().get("code") == case.code, response.text
    assert await digest(test_db) == before, "a refused request changed data"
    assert outbox.messages == [], "a refused request sent mail"


@pytest.mark.parametrize("role", NOT_STAFF)
@pytest.mark.parametrize("key,case", FLAT, ids=[f"{k} | {c.why}" for k, c in FLAT])
async def test_employees_and_client_users_are_refused_by_role_before_any_lookup(
    role: str,
    key: str,
    case: Case,
    async_client: AsyncClient,
    test_db: AsyncSession,
    mine: Mine,
    other_org: OtherOrg,
    outbox: Any,
) -> None:
    headers = await headers_for(async_client, role)
    path, kwargs = case.build(World(mine, other_org))
    method = key.split(" ", 1)[0]
    before = await digest(test_db)

    response = await async_client.request(method, path, headers=headers, **kwargs)

    if role == "client" and key == "GET /api/v1/projects":
        assert response.status_code == 200, response.text
        assert str(other_org.project.id) not in response.text, "another organisation's project"
    elif role == "client" and key in PORTAL_READS:
        assert response.status_code == 404, response.text
        assert response.json()["code"] == "resource.not_found"
    else:
        assert response.status_code == 403, response.text
    assert await digest(test_db) == before, "a refused request changed data"
    assert outbox.messages == [], "a refused request sent mail"
