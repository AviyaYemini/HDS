"""Report performance on a year of work (005 T026).

100 employees on 20 projects, each employee on one 8-hour published shift every weekday from
2025-11-02 to 2026-10-30 (about 26,000 assignments). The hours report, grouped both ways, and a
project's work-log over the year must each answer in under 3 seconds, through the full ASGI stack.

Covers: 005 SC-005; research R9 (indexed read-only queries).
"""

import time
import uuid
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from hds.core.security import hash_password
from hds.main import app
from hds.models.client import Client
from hds.models.project import Project, ProjectStatus, ProjectType
from hds.models.shift import Assignment, Shift
from hds.models.shift_type import ShiftType
from hds.models.site import Site
from hds.models.user import User, UserRole
from tests.conftest import RoleEnv

LIMIT_SECONDS = 3.0
EMPLOYEES, PROJECTS = 100, 20
FIRST, LAST = date(2025, 11, 2), date(2026, 10, 30)


class Seeded:
    def __init__(self) -> None:
        self.project_id = uuid.uuid4()
        self.assignments = 0


@pytest.fixture(scope="module")
def year(role_env: RoleEnv, migrated_test_db: str) -> Seeded:
    """Bulk-load a year of published, ended shifts next to the four role users."""
    seeded = Seeded()

    async def _seed() -> None:
        engine = create_async_engine(migrated_test_db, poolclass=NullPool)
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as session:
            org_id = (await session.execute(select(User.organisation_id).limit(1))).scalar_one()
            shift_type = (
                await session.execute(
                    select(ShiftType.id).where(ShiftType.organisation_id == org_id).limit(1)
                )
            ).scalar_one()
            password_hash = hash_password("Dev1234!")
            people = [uuid.uuid4() for _ in range(EMPLOYEES)]
            session.add_all(
                User(
                    id=person,
                    organisation_id=org_id,
                    email=f"year{i}@perf.test",
                    password_hash=password_hash,
                    full_name=f"Worker {i:03d}",
                    role=UserRole.employee,
                    default_hourly_rate=Decimal("50.0000"),
                    activated_at=datetime.now(UTC),
                )
                for i, person in enumerate(people)
            )
            client = Client(organisation_id=org_id, name="Year Client", contract_start=FIRST)
            site = Site(
                organisation_id=org_id, name="Year Site", address="1 St", timezone="Asia/Jerusalem"
            )
            session.add_all([client, site])
            await session.flush()
            projects = [seeded.project_id if i == 0 else uuid.uuid4() for i in range(PROJECTS)]
            session.add_all(
                Project(
                    id=pid,
                    organisation_id=org_id,
                    client_id=client.id,
                    site_id=site.id,
                    name=f"Year Project {i:02d}",
                    type=ProjectType.security,
                    billing_rate=Decimal("120.0000"),
                    start_date=FIRST,
                    status=ProjectStatus.active,
                )
                for i, pid in enumerate(projects)
            )
            await session.flush()
            shifts: list[dict[str, Any]] = []
            assignments: list[dict[str, Any]] = []
            day = FIRST
            while day <= LAST:
                if day.weekday() not in (4, 5):  # Sunday to Thursday
                    start = datetime(day.year, day.month, day.day, 5, tzinfo=UTC)
                    for n, person in enumerate(people):
                        shift_id = uuid.uuid4()
                        shifts.append(
                            {
                                "id": shift_id,
                                "organisation_id": org_id,
                                "project_id": projects[n % PROJECTS],
                                "shift_type_id": shift_type,
                                "starts_at": start,
                                "ends_at": start + timedelta(hours=8),
                                "headcount": 1,
                                "status": "published",
                                "published_at": start,
                            }
                        )
                        assignments.append(
                            {
                                "id": uuid.uuid4(),
                                "organisation_id": org_id,
                                "shift_id": shift_id,
                                "user_id": person,
                            }
                        )
                day += timedelta(days=1)
            for chunk in range(0, len(shifts), 5000):
                await session.execute(insert(Shift), shifts[chunk : chunk + 5000])
                await session.execute(insert(Assignment), assignments[chunk : chunk + 5000])
            seeded.assignments = len(assignments)
            await session.commit()
        await engine.dispose()

    role_env.runner.run(_seed())
    return seeded


def timed(role_env: RoleEnv, path: str, params: dict[str, str]) -> tuple[float, Any]:
    """The slower of two runs after a warm-up, and the last answer."""

    async def run() -> tuple[float, Any]:
        headers = {"Authorization": f"Bearer {role_env.tokens['admin']}"}
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://perf") as client:
            answer = await client.get(path, params=params, headers=headers)
            assert answer.status_code == 200, answer.text
            worst = 0.0
            for _ in range(2):
                started = time.perf_counter()
                answer = await client.get(path, params=params, headers=headers)
                worst = max(worst, time.perf_counter() - started)
                assert answer.status_code == 200, answer.text
        return worst, answer.json()

    result: tuple[float, Any] = role_env.runner.run(run())
    return result


@pytest.mark.parametrize("group_by", ["employee", "project"])
def test_the_hours_report_over_a_year_is_under_3_seconds(
    year: Seeded, role_env: RoleEnv, group_by: str
) -> None:
    assert year.assignments > 25_000
    seconds, body = timed(
        role_env,
        "/api/v1/reports/hours",
        {"from": FIRST.isoformat(), "to": LAST.isoformat(), "group_by": group_by},
    )

    print(f"\nhours by {group_by}: {seconds * 1000:.0f} ms over {body['shifts']} lines")
    assert body["shifts"] == year.assignments
    assert seconds < LIMIT_SECONDS


def test_a_projects_work_log_over_a_year_is_under_3_seconds(
    year: Seeded, role_env: RoleEnv
) -> None:
    seconds, body = timed(
        role_env,
        "/api/v1/reports/work-log",
        {"project_id": str(year.project_id), "to": LAST.isoformat()},
    )

    print(f"\nwork-log: {seconds * 1000:.0f} ms over {len(body['rows'])} rows")
    assert len(body["rows"]) == EMPLOYEES // PROJECTS
    assert seconds < LIMIT_SECONDS
