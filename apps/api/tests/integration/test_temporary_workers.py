"""Temporary workers through the API (CEO demo).

A temporary worker is on one or more projects for at most three months. The employee endpoints
set and show it, the list filters by it, a Manager adds people only to projects they manage, the
roster marks them, and a hand assignment outside their projects or dates is stored with a warning.
"""

import uuid
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest
from httpx import AsyncClient
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession

from hds.models.client import Client
from hds.models.organisation import Organisation
from hds.models.project import Project, ProjectStatus, ProjectType
from hds.models.shift import Shift
from hds.models.shift_type import ShiftType, project_shift_types
from hds.models.site import Site
from hds.models.user import User

pytestmark = pytest.mark.anyio

TODAY = date.today()


async def _headers(client: AsyncClient, email: str) -> dict[str, str]:
    r = await client.post("/auth/login", json={"email": email, "password": "Dev1234!"})
    assert r.status_code == 200, r.text
    return {"Authorization": f"Bearer {r.json()['access_token']}"}


@pytest.fixture
async def projects(
    test_db: AsyncSession, seed_org: Organisation, seed_users: dict[str, User]
) -> dict[str, Project]:
    """Two projects of the organisation, each with its default shift types linked."""
    client = Client(
        id=uuid.uuid4(), organisation_id=seed_org.id, name="Acme Guarding", contract_start=TODAY
    )
    site = Site(
        id=uuid.uuid4(),
        organisation_id=seed_org.id,
        name="North Gate",
        address="1 Main St",
        timezone="Asia/Jerusalem",
    )
    test_db.add_all([client, site])
    await test_db.flush()
    made = {}
    for key in ("north", "south"):
        made[key] = Project(
            id=uuid.uuid4(),
            organisation_id=seed_org.id,
            client_id=client.id,
            site_id=site.id,
            name=f"{key.title()} Patrol",
            type=ProjectType.security,
            billing_rate=Decimal("110.0000"),
            start_date=TODAY - timedelta(days=30),
            status=ProjectStatus.active,
        )
    test_db.add_all(made.values())
    await test_db.flush()
    types = (
        (await test_db.execute(select(ShiftType).where(ShiftType.organisation_id == seed_org.id)))
        .scalars()
        .all()
    )
    for p in made.values():
        for t in types:
            await test_db.execute(
                insert(project_shift_types).values(project_id=p.id, shift_type_id=t.id)
            )
    await test_db.commit()
    return made


def _temporary(projects: list[Project], days: int = 60, **extra: object) -> dict[str, object]:
    return {
        "email": f"temp-{uuid.uuid4().hex[:8]}@hds.test",
        "full_name": "Tali Temp",
        "initial_password": "Temporary-pass-1",
        "employment_type": "temporary",
        "engaged_from": str(TODAY - timedelta(days=7)),
        "engaged_until": str(TODAY - timedelta(days=7) + timedelta(days=days)),
        "project_ids": [str(p.id) for p in projects],
        **extra,
    }


async def test_an_admin_signs_on_a_temporary_worker_and_the_list_filters_them(
    async_client: AsyncClient, projects: dict[str, Project]
) -> None:
    admin = await _headers(async_client, "admin@hds.test")
    r = await async_client.post(
        "/api/v1/employees", json=_temporary([projects["north"]]), headers=admin
    )
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["employment_type"] == "temporary"
    assert body["project_ids"] == [str(projects["north"].id)]
    assert body["engaged_until"] == str(TODAY - timedelta(days=7) + timedelta(days=60))

    temps = await async_client.get("/api/v1/employees?employment_type=temporary", headers=admin)
    assert [u["id"] for u in temps.json()["items"]] == [body["id"]]
    staff = await async_client.get("/api/v1/employees?employment_type=permanent", headers=admin)
    assert body["id"] not in {u["id"] for u in staff.json()["items"]}


async def test_longer_than_three_months_or_without_a_project_is_refused(
    async_client: AsyncClient, projects: dict[str, Project]
) -> None:
    admin = await _headers(async_client, "admin@hds.test")
    too_long = await async_client.post(
        "/api/v1/employees", json=_temporary([projects["north"]], days=100), headers=admin
    )
    assert too_long.status_code == 422
    assert too_long.json()["code"] == "employees.temporary_period_invalid"
    alone = await async_client.post("/api/v1/employees", json=_temporary([]), headers=admin)
    assert alone.status_code == 422
    assert alone.json()["code"] == "employees.temporary_projects_required"


async def test_making_them_permanent_clears_the_dates_and_projects(
    async_client: AsyncClient, projects: dict[str, Project]
) -> None:
    admin = await _headers(async_client, "admin@hds.test")
    made = await async_client.post(
        "/api/v1/employees", json=_temporary([projects["north"]]), headers=admin
    )
    r = await async_client.patch(
        f"/api/v1/employees/{made.json()['id']}",
        json={"employment_type": "permanent"},
        headers=admin,
    )
    assert r.status_code == 200, r.text
    assert r.json()["employment_type"] == "permanent"
    assert r.json()["engaged_from"] is None and r.json()["project_ids"] == []


async def test_a_manager_adds_temporary_workers_only_to_projects_they_manage(
    async_client: AsyncClient, projects: dict[str, Project]
) -> None:
    manager = await _headers(async_client, "manager@hds.test")
    r = await async_client.post(
        "/api/v1/employees", json=_temporary([projects["north"]]), headers=manager
    )
    assert r.status_code == 403
    assert r.json()["code"] == "roster.project_not_managed"


async def test_the_roster_marks_them_and_warns_outside_their_projects(
    async_client: AsyncClient,
    projects: dict[str, Project],
    test_db: AsyncSession,
    seed_org: Organisation,
) -> None:
    admin = await _headers(async_client, "admin@hds.test")
    made = await async_client.post(
        "/api/v1/employees", json=_temporary([projects["north"]]), headers=admin
    )
    temp_id = made.json()["id"]
    shift_type = (
        (await test_db.execute(select(ShiftType).where(ShiftType.organisation_id == seed_org.id)))
        .scalars()
        .first()
    )
    assert shift_type is not None
    start = datetime.combine(TODAY, datetime.min.time(), UTC) + timedelta(hours=5)
    ids = {}
    for key in ("north", "south"):
        shift = Shift(
            id=uuid.uuid4(),
            organisation_id=seed_org.id,
            project_id=projects[key].id,
            shift_type_id=shift_type.id,
            starts_at=start,
            ends_at=start + timedelta(hours=8),
            headcount=1,
            status="draft",
        )
        test_db.add(shift)
        ids[key] = shift.id
    await test_db.commit()

    inside = await async_client.post(
        f"/api/v1/shifts/{ids['north']}/assignments", json={"user_id": temp_id}, headers=admin
    )
    assert inside.status_code == 201, inside.text
    assert "warnings.temporary_outside" not in {w["code"] for w in inside.json()["warnings"]}
    week = await async_client.get(
        f"/api/v1/shifts?from={TODAY - timedelta(days=1)}&to={TODAY + timedelta(days=1)}",
        headers=admin,
    )
    on_north = next(s for s in week.json()["items"] if s["id"] == str(ids["north"]))
    assert [a["temporary"] for a in on_north["assignments"]] == [True]

    outside = await async_client.post(
        f"/api/v1/shifts/{ids['south']}/assignments", json={"user_id": temp_id}, headers=admin
    )
    assert outside.status_code == 201, outside.text
    assert "warnings.temporary_outside" in {w["code"] for w in outside.json()["warnings"]}
