"""GET /employees search and filters, organisation-scoped and combinable (T143).

Covers name or email search and the role, is_active, group and qualification filters.

Covers: FR-013, NFR-001.
"""

import uuid
from datetime import UTC, datetime

import pytest
from httpx import AsyncClient
from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.security import hash_password
from hds.models.group import Group, user_groups
from hds.models.organisation import Organisation
from hds.models.qualification import Qualification, user_qualifications
from hds.models.user import User, UserRole
from tests.integration.conftest import OtherOrg


async def _headers(client: AsyncClient, email: str = "manager@hds.test") -> dict[str, str]:
    r = await client.post("/auth/login", json={"email": email, "password": "Dev1234!"})
    assert r.status_code == 200, r.text
    return {"Authorization": f"Bearer {r.json()['access_token']}"}


def _user(org: Organisation, email: str, name: str, role: UserRole, **kw: object) -> User:
    return User(
        id=uuid.uuid4(),
        organization_id=org.id,
        email=email,
        full_name=name,
        role=role,
        password_hash=hash_password("Dev1234!"),
        activated_at=datetime.now(UTC),
        **kw,
    )


@pytest.fixture
async def people(
    test_db: AsyncSession, seed_org: Organisation, seed_users: dict[str, User]
) -> dict[str, object]:
    """Seed four users that the filter tests tell apart.

    Alice (employee, guards, first-aid), Bob (employee, guards, inactive), Carol (manager, no
    group) and Dave (deleted employee, guards).
    """
    alice = _user(seed_org, "alice@hds.test", "Alice Cohen", UserRole.employee)
    bob = _user(seed_org, "bob@hds.test", "Bob Levi", UserRole.employee, is_active=False)
    carol = _user(seed_org, "carol@hds.test", "Carol Mizrahi", UserRole.manager)
    dave = _user(
        seed_org,
        "dave@hds.test",
        "Dave Deleted",
        UserRole.employee,
        is_deleted=True,
        deleted_at=datetime.now(UTC),
    )
    guards = Group(id=uuid.uuid4(), organization_id=seed_org.id, name="Guards")
    aid = Qualification(id=uuid.uuid4(), organization_id=seed_org.id, name="First aid")
    test_db.add_all([alice, bob, carol, dave, guards, aid])
    await test_db.flush()
    for u in (alice, bob, dave):
        await test_db.execute(insert(user_groups).values(user_id=u.id, group_id=guards.id))
    await test_db.execute(
        insert(user_qualifications).values(user_id=alice.id, qualification_id=aid.id)
    )
    await test_db.commit()
    return {"guards": guards, "aid": aid}


async def _emails(client: AsyncClient, query: str = "", **kw: dict[str, str]) -> set[str]:
    r = await client.get(f"/api/v1/employees{query}", headers=kw.get("headers"))
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["total"] == len(body["items"])
    return {item["email"] for item in body["items"]}


async def _ask(client: AsyncClient, query: str) -> set[str]:
    return await _emails(client, query, headers=await _headers(client))


BASE = {"admin@hds.test", "manager@hds.test", "employee@hds.test", "client@hds.test"}


@pytest.mark.asyncio
async def test_no_filter_lists_everyone_except_deleted(
    async_client: AsyncClient, people: dict[str, object]
) -> None:
    got = await _ask(async_client, "")
    assert got == BASE | {"alice@hds.test", "bob@hds.test", "carol@hds.test"}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "query, expected",
    [
        ("?search=alice", {"alice@hds.test"}),
        ("?search=COHEN", {"alice@hds.test"}),  # case-insensitive, full name
        ("?search=carol@hds", {"carol@hds.test"}),  # email
        ("?search=nobody-matches", set()),
        ("?search=%25", set()),  # a literal "%", not a wildcard
        ("?search=_", set()),
        ("?role=manager", {"manager@hds.test", "carol@hds.test"}),
        ("?role=admin", {"admin@hds.test"}),
        ("?is_active=false", {"bob@hds.test"}),
        ("?is_active=true", BASE | {"alice@hds.test", "carol@hds.test"}),
    ],
)
async def test_single_filters(
    async_client: AsyncClient, people: dict[str, object], query: str, expected: set[str]
) -> None:
    assert await _ask(async_client, query) == expected


@pytest.mark.asyncio
async def test_group_and_qualification_filters(
    async_client: AsyncClient, people: dict[str, object]
) -> None:
    guards, aid = people["guards"], people["aid"]
    assert await _ask(async_client, f"?group_id={guards.id}") == {  # type: ignore[attr-defined]
        "alice@hds.test",
        "bob@hds.test",
    }  # the deleted member is never listed
    assert await _ask(async_client, f"?qualification_id={aid.id}") == {  # type: ignore[attr-defined]
        "alice@hds.test"
    }
    # An id that does not exist is refused, as for every other id in a query (NFR-001).
    missing = await async_client.get(
        f"/api/v1/employees?group_id={uuid.uuid4()}", headers=await _headers(async_client)
    )
    assert missing.status_code == 404


@pytest.mark.asyncio
async def test_filters_combine_with_and(
    async_client: AsyncClient, people: dict[str, object]
) -> None:
    guards, aid = people["guards"], people["aid"]
    assert await _ask(async_client, f"?group_id={guards.id}&is_active=true") == {  # type: ignore[attr-defined]
        "alice@hds.test"
    }
    assert await _ask(async_client, f"?group_id={guards.id}&role=manager") == set()  # type: ignore[attr-defined]
    assert await _ask(
        async_client,
        f"?group_id={guards.id}&qualification_id={aid.id}&role=employee&search=ali",  # type: ignore[attr-defined]
    ) == {"alice@hds.test"}


@pytest.mark.asyncio
async def test_other_organisations_rows_never_appear(
    async_client: AsyncClient, people: dict[str, object], other_org: OtherOrg
) -> None:
    for query in (
        "",
        "?search=other",
        "?role=employee",
        "?is_active=true",
    ):
        got = await _ask(async_client, query)
        assert other_org.user.email not in got, query
    # Another organisation's group or qualification is refused like a missing one (NFR-001).
    headers = await _headers(async_client)
    for query in (
        f"?group_id={other_org.group.id}",
        f"?qualification_id={other_org.qualification.id}",
    ):
        refused = await async_client.get(f"/api/v1/employees{query}", headers=headers)
        assert refused.status_code == 404, query


@pytest.mark.asyncio
async def test_invalid_filter_values_are_422(
    async_client: AsyncClient, people: dict[str, object]
) -> None:
    headers = await _headers(async_client)
    for bad in ("?role=wizard", "?is_active=maybe", "?group_id=not-a-uuid"):
        r = await async_client.get(f"/api/v1/employees{bad}", headers=headers)
        assert r.status_code == 422, bad


@pytest.mark.asyncio
async def test_employees_cannot_list(async_client: AsyncClient, people: dict[str, object]) -> None:
    r = await async_client.get(
        "/api/v1/employees", headers=await _headers(async_client, "employee@hds.test")
    )
    assert r.status_code == 403
