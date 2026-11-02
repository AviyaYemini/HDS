"""T129: performance smoke test on a seeded PostgreSQL database.

* every read endpoint: p95 latency < 300 ms over repeated requests;
* sign-in: < 3 s.

Requests go through the full ASGI stack (routing, auth, services, SQL) in-process, so the
numbers measure the application and the database, not the network.

Covers: SC-001.
"""

import math
import time
import uuid
from collections.abc import Callable, Coroutine
from datetime import date
from decimal import Decimal
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from hds.core.security import hash_password
from hds.main import app
from hds.models.client import Client
from hds.models.group import Group
from hds.models.project import Project, ProjectStatus, ProjectType
from hds.models.qualification import Qualification
from hds.models.site import Site
from hds.models.user import User, UserRole
from tests.conftest import RoleEnv

P95_LIMIT_SECONDS = 0.300
SIGN_IN_LIMIT_SECONDS = 3.0
REQUESTS_PER_ENDPOINT = 40
SEED = {"employees": 300, "clients": 40, "sites": 40, "projects": 200, "groups": 20, "quals": 20}


class Seeded:
    def __init__(self) -> None:
        self.employee_id = uuid.uuid4()
        self.client_id = uuid.uuid4()
        self.site_id = uuid.uuid4()
        self.project_id = uuid.uuid4()


@pytest.fixture(scope="module")
def seeded(role_env: RoleEnv, migrated_test_db: str) -> Seeded:
    """Bulk-load a realistic data set next to the four role users from ``role_env``."""
    ids = Seeded()

    async def _seed() -> None:
        engine = create_async_engine(migrated_test_db, poolclass=NullPool)
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as session:
            org_id = (await session.execute(select(User.organization_id).limit(1))).scalar_one()
            password_hash = hash_password("Dev1234!")  # hash once; Argon2 is deliberately slow
            session.add_all(
                User(
                    id=ids.employee_id if i == 0 else uuid.uuid4(),
                    organization_id=org_id,
                    email=f"emp{i}@perf.test",
                    password_hash=password_hash,
                    full_name=f"Employee {i}",
                    role=UserRole.employee,
                    default_hourly_rate=Decimal("50.0000"),
                )
                for i in range(SEED["employees"])
            )
            clients = [
                Client(
                    id=ids.client_id if i == 0 else uuid.uuid4(),
                    organization_id=org_id,
                    name=f"Client {i}",
                    contract_start=date(2026, 1, 1),
                )
                for i in range(SEED["clients"])
            ]
            sites = [
                Site(
                    id=ids.site_id if i == 0 else uuid.uuid4(),
                    organization_id=org_id,
                    name=f"Site {i}",
                    address="1 Main St",
                    timezone="Asia/Jerusalem",
                )
                for i in range(SEED["sites"])
            ]
            session.add_all(clients + sites)
            session.add_all(
                Group(id=uuid.uuid4(), organization_id=org_id, name=f"Group {i}")
                for i in range(SEED["groups"])
            )
            session.add_all(
                Qualification(id=uuid.uuid4(), organization_id=org_id, name=f"Qual {i}")
                for i in range(SEED["quals"])
            )
            await session.flush()
            session.add_all(
                Project(
                    id=ids.project_id if i == 0 else uuid.uuid4(),
                    organization_id=org_id,
                    client_id=clients[i % len(clients)].id,
                    site_id=sites[i % len(sites)].id,
                    name=f"Project {i}",
                    type=ProjectType.security,
                    billing_rate=Decimal("120.0000"),
                    start_date=date(2026, 1, 1),
                    status=ProjectStatus.active,
                )
                for i in range(SEED["projects"])
            )
            await session.commit()
        await engine.dispose()

    role_env.runner.run(_seed())
    return ids


def _p95(samples: list[float]) -> float:
    ordered = sorted(samples)
    return ordered[max(0, math.ceil(0.95 * len(ordered)) - 1)]


async def _measure(
    send: Callable[[AsyncClient], Coroutine[Any, Any, Any]], expect: int = 200
) -> list[float]:
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://perf") as client:
        warm = await send(client)  # warm-up: first request pays imports / pool set-up
        assert warm.status_code == expect, warm.text
        samples: list[float] = []
        for _ in range(REQUESTS_PER_ENDPOINT):
            started = time.perf_counter()
            response = await send(client)
            samples.append(time.perf_counter() - started)
            assert response.status_code == expect, response.text
    return samples


def _read_endpoints(ids: Seeded) -> list[tuple[str, str]]:
    return [
        ("admin", "/me"),
        ("admin", "/api/v1/employees"),
        ("admin", f"/api/v1/employees/{ids.employee_id}"),
        ("admin", f"/api/v1/employees/{ids.employee_id}/groups"),
        ("admin", f"/api/v1/employees/{ids.employee_id}/qualifications"),
        ("admin", "/api/v1/groups"),
        ("admin", "/api/v1/qualifications"),
        ("admin", "/api/v1/clients"),
        ("admin", f"/api/v1/clients/{ids.client_id}"),
        ("admin", f"/api/v1/clients/{ids.client_id}/users"),
        ("admin", "/api/v1/sites"),
        ("admin", f"/api/v1/sites/{ids.site_id}"),
        ("admin", "/api/v1/projects"),
        ("admin", f"/api/v1/projects/{ids.project_id}"),
        ("manager", "/api/v1/projects"),
        ("employee", "/me"),
    ]


def test_seed_is_realistic(seeded: Seeded, role_env: RoleEnv) -> None:
    status, _ = role_env.call("admin", "GET", "/api/v1/employees", None, {})
    assert status == 200


def test_read_endpoints_p95_under_300_ms(seeded: Seeded, role_env: RoleEnv) -> None:
    results: dict[str, float] = {}
    for principal, path in _read_endpoints(seeded):
        headers = {"Authorization": f"Bearer {role_env.tokens[principal]}"}

        async def _send(client: AsyncClient, path: str = path, headers: dict[str, str] = headers):  # type: ignore[no-untyped-def]
            return await client.get(path, headers=headers)

        samples = role_env.runner.run(_measure(_send))
        results[f"{principal} GET {path}"] = _p95(samples)

    slow = {
        name: f"{value * 1000:.0f} ms"
        for name, value in results.items()
        if value >= P95_LIMIT_SECONDS
    }
    print("\n".join(f"{v * 1000:6.1f} ms  p95  {k}" for k, v in sorted(results.items())))
    assert not slow, f"p95 >= {P95_LIMIT_SECONDS * 1000:.0f} ms: {slow}"


def test_sign_in_under_3_seconds(seeded: Seeded, role_env: RoleEnv) -> None:
    async def _login() -> list[float]:
        timings: list[float] = []
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://perf") as client:
            for _ in range(5):
                started = time.perf_counter()
                r = await client.post(
                    "/auth/login", json={"email": "admin@matrix.test", "password": "Dev1234!"}
                )
                timings.append(time.perf_counter() - started)
                assert r.status_code == 200, r.text
        return timings

    timings = role_env.runner.run(_login())
    assert max(timings) < SIGN_IN_LIMIT_SECONDS, [f"{t:.2f}s" for t in timings]
