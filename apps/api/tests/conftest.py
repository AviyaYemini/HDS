"""Shared pytest fixtures for HDS API tests.

Provides the test database, the ASGI client, per-role sign-in helpers and the outbox mail
sender used to read invitation and reset tokens.

Covers: none directly; fixtures only.
"""

import asyncio
import contextlib
import os
import subprocess
import sys
import uuid
from collections.abc import AsyncGenerator, Generator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import email_validator
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool

# The settings refuse to load without an explicit environment and secret (specs/002 research D5),
# so the suite states both before any hds import. `test` is a development environment.
os.environ.setdefault("ENVIRONMENT", "test")
os.environ.setdefault("JWT_SECRET_KEY", "test-only-jwt-secret-0123456789-abcdef")  # >= 32 bytes
# Many tests fail sign-ins from one address on purpose. An assignment, not ``setdefault``, so a
# developer's shell cannot change it; test_login_throttle.py turns the limit on for itself.
os.environ["LOGIN_RATE_LIMIT_PER_NETWORK"] = "0"
# The same for reset requests; test_reset_request_limit.py turns the limit on for itself.
os.environ["RESET_REQUEST_LIMIT_PER_ADDRESS"] = "0"
os.environ["RESET_REQUEST_LIMIT_PER_NETWORK"] = "0"
# No metrics server in tests: an app started through its lifespan (TestClient) would bind the
# operations port, and runs at once would fight over it. test_metrics_port.py asks for one.
os.environ["METRICS_PORT"] = "0"

from hds.core.database import get_db, get_health_session_factory, get_session_factory
from hds.core.mail import get_mail_sender
from hds.core.security import hash_password
from hds.main import app
from hds.models.base import Base
from hds.models.organisation import Organisation
from hds.models.user import User, UserRole
from tests.run_database import allowed_database, database_url, process_exists, stale_databases
from tests.run_lock import RunLock

# Allow the reserved ``.test`` domain used by the seeded fixtures.
email_validator.TEST_ENVIRONMENT = True

# Integration tests run on PostgreSQL (Constitution); SQLite stays for pure unit tests only. Each
# run has its own database unless one is named, so two runs at once cannot disturb each other.
_NAMED_DATABASE = bool(os.environ.get("TEST_DATABASE_URL"))
TEST_DATABASE_URL = database_url(os.environ.get("TEST_DATABASE_URL"), os.getpid())
os.environ["TEST_DATABASE_URL"] = TEST_DATABASE_URL  # for migration_support and subprocesses
_API_DIR = Path(__file__).resolve().parent.parent


def _assert_safe_test_db(url: str) -> str:
    """Refuse to run against anything that is not a dedicated *_test database.

    Then only ``hds_p3_test`` or a run's own database outside CI, and also CI's own in CI
    (``tests.run_database.allowed_database``).
    """
    name = make_url(url).database or ""
    if not name.endswith("_test"):
        raise RuntimeError(f"TEST_DATABASE_URL must point at a '*_test' database, got {name!r}")
    allowed_database(name, in_ci=bool(os.environ.get("CI")))
    return name


def _admin_engine(url: str) -> AsyncEngine:
    return create_async_engine(
        make_url(url).set(database="postgres"), isolation_level="AUTOCOMMIT", poolclass=NullPool
    )


async def _prepare_database(url: str, named: bool) -> None:
    """Make the run's database ready.

    A named database is kept, and nothing else is touched. A run's own one starts empty, because a
    leftover with this process id would hand it old rows, and stale ones are removed on the way:
    without FORCE, so one still in use by a run this machine cannot see survives.
    """
    name = _assert_safe_test_db(url)
    admin = _admin_engine(url)
    try:
        async with admin.connect() as conn:
            names = [row[0] for row in await conn.execute(text("SELECT datname FROM pg_database"))]
            if not named:
                await conn.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
                for stale in stale_databases(names, process_exists):
                    with contextlib.suppress(DBAPIError):
                        await conn.execute(text(f'DROP DATABASE IF EXISTS "{stale}"'))
            if not named or name not in names:
                await conn.execute(text(f'CREATE DATABASE "{name}"'))
    finally:
        await admin.dispose()


async def _empty_tables(url: str) -> None:
    """Truncate every application table of ``url``, as each test does after itself."""
    engine = create_async_engine(url, poolclass=NullPool)
    tables = ", ".join(f'"{t.name}"' for t in Base.metadata.sorted_tables)
    try:
        async with engine.begin() as conn:
            await conn.execute(text(f"TRUNCATE {tables} RESTART IDENTITY CASCADE"))
    finally:
        await engine.dispose()


async def _drop_database(url: str) -> None:
    name = _assert_safe_test_db(url)
    admin = _admin_engine(url)
    try:
        async with admin.connect() as conn:
            await conn.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
    finally:
        await admin.dispose()


@pytest.fixture(scope="session")
def migrated_test_db() -> Generator[str, None, None]:
    """Create the test database, run Alembic migrations to head once and drop it at the end.

    A database that was named with ``TEST_DATABASE_URL`` is kept. Databases left by killed runs are
    dropped here, so nothing depends on a run ending cleanly. The run holds an advisory lock on its
    database for the whole session, so a second run on it stops at once instead of colliding.
    """
    _assert_safe_test_db(TEST_DATABASE_URL)
    lock = RunLock(TEST_DATABASE_URL)
    lock.acquire()
    try:
        yield from _migrated()
    finally:
        lock.release()


def _migrated() -> Generator[str, None, None]:
    """Prepare and migrate the run's database, and drop it afterwards unless it was named."""
    asyncio.run(_prepare_database(TEST_DATABASE_URL, _NAMED_DATABASE))
    env = {**os.environ, "DATABASE_URL": TEST_DATABASE_URL}
    try:
        subprocess.run(
            [sys.executable, "-m", "alembic", "upgrade", "head"],
            cwd=_API_DIR,
            env=env,
            check=True,
        )
        if _NAMED_DATABASE:
            # A named database is shared, with the ux e2e runs among others, which leave seeded
            # rows behind; tests truncate after themselves, so the run starts from empty tables.
            asyncio.run(_empty_tables(TEST_DATABASE_URL))
        yield TEST_DATABASE_URL
    finally:
        if not _NAMED_DATABASE:
            asyncio.run(_drop_database(TEST_DATABASE_URL))


@pytest.fixture(scope="session")
def anyio_backend() -> str:
    return "asyncio"


@pytest_asyncio.fixture
async def async_client() -> AsyncGenerator[AsyncClient, None]:
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        yield client


class OutboxMailSender:
    """Test mail sender that records messages instead of sending them."""

    def __init__(self) -> None:
        self.messages: list[tuple[str, str, str]] = []

    async def send(
        self,
        to: str,
        subject: str,
        body: str,
        *,
        secret: str | None = None,
        purpose: str | None = None,
        expires_at: datetime | None = None,
    ) -> None:
        """Record the message in memory; the options exist so services can pass them."""
        self.messages.append((to, subject, body))

    def last_token_for(self, to: str) -> str:
        """Return the token (last word of the body) of the latest message sent to ``to``."""
        bodies = [b for (t, _, b) in self.messages if t == to]
        assert bodies, f"no mail sent to {to}"
        return bodies[-1].split()[-1]


@pytest.fixture
def outbox() -> Generator[OutboxMailSender, None, None]:
    sender = OutboxMailSender()
    app.dependency_overrides[get_mail_sender] = lambda: sender
    yield sender
    app.dependency_overrides.pop(get_mail_sender, None)


@pytest_asyncio.fixture
async def test_db(migrated_test_db: str) -> AsyncGenerator[AsyncSession, None]:
    """PostgreSQL session shared with the app (via get_db); all tables truncated after each test."""
    engine = create_async_engine(migrated_test_db, echo=False, poolclass=NullPool)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    async def _override_get_db() -> AsyncGenerator[AsyncSession, None]:
        async with session_factory() as db:
            yield db

    app.dependency_overrides[get_db] = _override_get_db
    app.dependency_overrides[get_session_factory] = lambda: session_factory
    app.dependency_overrides[get_health_session_factory] = lambda: session_factory
    try:
        async with session_factory() as session:
            yield session
    finally:
        app.dependency_overrides.pop(get_db, None)
        app.dependency_overrides.pop(get_session_factory, None)
        app.dependency_overrides.pop(get_health_session_factory, None)
        tables = ", ".join(f'"{t.name}"' for t in Base.metadata.sorted_tables)
        async with engine.begin() as conn:
            await conn.execute(text(f"TRUNCATE {tables} RESTART IDENTITY CASCADE"))
        await engine.dispose()


@pytest_asyncio.fixture
async def seed_org(test_db: AsyncSession) -> Organisation:
    org = Organisation(id=uuid.uuid4(), name="Test Org", timezone="UTC")
    test_db.add(org)
    await test_db.commit()
    await test_db.refresh(org)
    return org


@pytest_asyncio.fixture
async def seed_users(test_db: AsyncSession, seed_org: Organisation) -> dict[str, User]:
    users = {}
    for role in UserRole:
        u = User(
            id=uuid.uuid4(),
            organization_id=seed_org.id,
            email=f"{role.value}@hds.test",
            password_hash=hash_password("Dev1234!"),
            full_name=f"Test {role.value.title()}",
            role=role,
            activated_at=datetime.now(UTC),
        )
        test_db.add(u)
        users[role.value] = u
    await test_db.commit()
    return users


@dataclass
class RoleEnv:
    runner: asyncio.Runner
    tokens: dict[str, str]

    def call(
        self, principal: str, method: str, path: str, body: Any, params: dict[str, str]
    ) -> tuple[int, str]:
        headers = {}
        if principal != "anonymous":
            headers["Authorization"] = f"Bearer {self.tokens[principal]}"

        async def _go() -> tuple[int, str]:
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://testserver"
            ) as client:
                r = await client.request(method, path.format(**params), json=body, headers=headers)
                try:
                    code = str(r.json().get("code", "")) if r.content else ""
                except (ValueError, AttributeError):
                    code = ""
                return r.status_code, code

        return self.runner.run(_go())


@pytest.fixture(scope="module")
def role_env(migrated_test_db: str) -> Generator[RoleEnv, None, None]:
    runner = asyncio.Runner()
    engine = create_async_engine(migrated_test_db, poolclass=NullPool)
    factory = async_sessionmaker(engine, expire_on_commit=False)

    async def _override() -> Any:
        async with factory() as session:
            yield session

    app.dependency_overrides[get_db] = _override
    app.dependency_overrides[get_session_factory] = lambda: factory

    async def _seed() -> dict[str, str]:
        async with factory() as session:
            org = Organisation(name="Matrix Org", timezone="UTC")
            session.add(org)
            await session.flush()
            for role in UserRole:
                session.add(
                    User(
                        organization_id=org.id,
                        email=f"{role.value}@matrix.test",
                        password_hash=hash_password("Dev1234!"),
                        full_name=role.value,
                        role=role,
                        activated_at=datetime.now(UTC),
                    )
                )
            await session.commit()
        tokens: dict[str, str] = {}
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://testserver"
        ) as client:
            for role in UserRole:
                r = await client.post(
                    "/auth/login",
                    json={"email": f"{role.value}@matrix.test", "password": "Dev1234!"},
                )
                assert r.status_code == 200, r.text
                tokens[role.value] = r.json()["access_token"]
        return tokens

    tokens = runner.run(_seed())
    try:
        yield RoleEnv(runner, tokens)
    finally:

        async def _clean() -> None:
            tables = ", ".join(f'"{t.name}"' for t in Base.metadata.sorted_tables)
            async with engine.begin() as conn:
                await conn.execute(text(f"TRUNCATE {tables} RESTART IDENTITY CASCADE"))
            await engine.dispose()

        runner.run(_clean())
        runner.close()
        app.dependency_overrides.pop(get_db, None)
        app.dependency_overrides.pop(get_session_factory, None)
