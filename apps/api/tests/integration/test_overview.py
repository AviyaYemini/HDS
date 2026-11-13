"""GET /api/v1/reports/overview: the snapshot dashboard (CEO demo).

Billing is person-hours × the project's billing rate; the actual counts published shifts that have
ended, the forecast adds every shift still to come; cover is this week's places; a Manager sees
only the projects they manage; money appears only where the reader may see the project's pay. The
hours report gains the same billing figure.
"""

import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest
from httpx import AsyncClient
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession

from hds.models.client import Client
from hds.models.organisation import Organisation
from hds.models.project import Project, ProjectStatus, ProjectType
from hds.models.project_manager import ProjectManager
from hds.models.shift import Assignment, Shift
from hds.models.shift_type import ShiftType
from hds.models.site import Site
from hds.models.user import User

pytestmark = pytest.mark.anyio

ZONE = ZoneInfo("Asia/Jerusalem")
TODAY = datetime.now(ZONE).date()
SUNDAY = TODAY - timedelta(days=(TODAY.weekday() + 1) % 7)


async def _headers(client: AsyncClient, email: str) -> dict[str, str]:
    r = await client.post("/auth/login", json={"email": email, "password": "Dev1234!"})
    assert r.status_code == 200, r.text
    return {"Authorization": f"Bearer {r.json()['access_token']}"}


@pytest.fixture
async def roster(
    test_db: AsyncSession, seed_org: Organisation, seed_users: dict[str, User]
) -> dict[str, Project]:
    """Two projects at 100 an hour: the Manager manages "managed" only.

    Each has a published eight-hour shift last week (ended) with the employee on it, and a draft
    eight-hour shift this Saturday with two places and one person.
    """
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
    projects = {
        key: Project(
            id=uuid.uuid4(),
            organisation_id=seed_org.id,
            client_id=client.id,
            site_id=site.id,
            name=f"{key.title()} Patrol",
            type=ProjectType.security,
            billing_rate=Decimal("100.0000"),
            start_date=TODAY - timedelta(days=60),
            status=ProjectStatus.active,
        )
        for key in ("managed", "other")
    }
    test_db.add_all(projects.values())
    await test_db.flush()
    test_db.add(
        ProjectManager(
            organisation_id=seed_org.id,
            project_id=projects["managed"].id,
            user_id=seed_users["manager"].id,
        )
    )
    kind = (
        (await test_db.execute(select(ShiftType).where(ShiftType.organisation_id == seed_org.id)))
        .scalars()
        .first()
    )
    assert kind is not None
    employee = seed_users["employee"]
    for project in projects.values():
        for day, status, headcount in (
            (SUNDAY - timedelta(days=4), "published", 1),
            (SUNDAY + timedelta(days=6), "draft", 2),
        ):
            start = datetime.combine(day, datetime.min.time(), ZONE).replace(hour=8)
            shift = Shift(
                id=uuid.uuid4(),
                organisation_id=seed_org.id,
                project_id=project.id,
                shift_type_id=kind.id,
                starts_at=start.astimezone(UTC),
                ends_at=(start + timedelta(hours=8)).astimezone(UTC),
                headcount=headcount,
                pay_rate=Decimal("50.0000"),
                status=status,
                published_at=datetime.now(UTC) if status == "published" else None,
            )
            test_db.add(shift)
            await test_db.flush()
            await test_db.execute(
                insert(Assignment).values(
                    id=uuid.uuid4(),
                    organisation_id=seed_org.id,
                    shift_id=shift.id,
                    user_id=employee.id,
                    source="manual",
                )
            )
    await test_db.commit()
    return projects


def _range() -> str:
    first = SUNDAY - timedelta(days=7)
    return f"from={first}&to={first + timedelta(days=13)}"


async def test_an_admin_sees_actual_and_forecast_billing_cover_and_people(
    async_client: AsyncClient, roster: dict[str, Project]
) -> None:
    admin = await _headers(async_client, "admin@hds.test")
    r = await async_client.get(f"/api/v1/reports/overview?{_range()}", headers=admin)
    assert r.status_code == 200, r.text
    body = r.json()
    # Two ended shifts of eight hours at 100 an hour; the forecast adds the two drafts.
    assert body["hours_actual"] == "16.00"
    assert body["billing_actual"] == "1600.00"
    assert body["hours_forecast"] == "32.00"
    assert body["billing_forecast"] == "3200.00"
    assert Decimal(body["pay_forecast"]) > 0
    assert body["coverage"] == {
        "week_start": str(SUNDAY),
        "filled": 2,
        "needed": 4,
        "empty_places": 2,
        "projects_with_gaps": 2,
    }
    assert {p["name"] for p in body["projects"]} == {"Managed Patrol", "Other Patrol"}
    assert {p["status"] for p in body["projects"]} == {"gaps"}
    assert [w["forecast"] for w in body["weeks"]] == [False, True]


async def test_a_manager_sees_only_the_projects_they_manage(
    async_client: AsyncClient, roster: dict[str, Project]
) -> None:
    manager = await _headers(async_client, "manager@hds.test")
    r = await async_client.get(f"/api/v1/reports/overview?{_range()}", headers=manager)
    assert r.status_code == 200, r.text
    body = r.json()
    assert [p["name"] for p in body["projects"]] == ["Managed Patrol"]
    assert body["hours_forecast"] == "16.00"
    # The project's pay is not open to Managers, so no money at all.
    assert "billing_forecast" not in body or body["billing_forecast"] is None
    assert body["projects"][0]["billing"] is None


async def test_an_employee_may_not_read_it(
    async_client: AsyncClient, roster: dict[str, Project]
) -> None:
    employee = await _headers(async_client, "employee@hds.test")
    r = await async_client.get(f"/api/v1/reports/overview?{_range()}", headers=employee)
    assert r.status_code == 403


async def test_the_hours_report_bills_where_it_shows_pay(
    async_client: AsyncClient, roster: dict[str, Project]
) -> None:
    admin = await _headers(async_client, "admin@hds.test")
    r = await async_client.get(f"/api/v1/reports/hours?{_range()}&group_by=project", headers=admin)
    assert r.status_code == 200, r.text
    assert r.json()["billing"] == "1600.00"
    assert {g["billing"] for g in r.json()["groups"]} == {"800.00"}
    manager = await _headers(async_client, "manager@hds.test")
    hidden = await async_client.get(
        f"/api/v1/reports/hours?{_range()}&group_by=project", headers=manager
    )
    assert "billing" not in hidden.json()
