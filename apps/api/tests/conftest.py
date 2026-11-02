"""Shared pytest fixtures for HDS API tests."""

import asyncio
import os
import subprocess
import sys
import uuid
from collections.abc import AsyncGenerator, Generator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import email_validator
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from hds.core.database import get_db
from hds.core.mail import get_mail_sender
from hds.core.security import hash_password
from hds.main import app
from hds.models.base import Base
from hds.models.organisation import Organisation
from hds.models.user import User, UserRole

# Allow the reserved ``.test`` domain used by the seeded fixtures.
email_validator.TEST_ENVIRONMENT = True

# Integration tests run on PostgreSQL (Constitution); SQLite stays for pure unit tests only.
TEST_DATABASE_URL = os.environ.get(
    "TEST_DATABASE_URL", "postgresql+asyncpg://hds:hds_dev@localhost:5432/hds_test"
)
_API_DIR = Path(__file__).resolve().parent.parent


def _assert_safe_test_db(url: str) -> str:
    """Refuse to run against anything that is not a dedicated *_test database."""
    name = make_url(url).database or ""
    if not name.endswith("_test"):
        raise RuntimeError(f"TEST_DATABASE_URL must point at a '*_test' database, got {name!r}")
    return name


async def _create_database_if_missing(url: str) -> None:
    name = _assert_safe_test_db(url)
    admin = create_async_engine(
        make_url(url).set(database="postgres"), isolation_level="AUTOCOMMIT", poolclass=NullPool
    )
    try:
        async with admin.connect() as conn:
            exists = await conn.scalar(
                text("SELECT 1 FROM pg_database WHERE datname = :n"), {"n": name}
            )
            if not exists:
                await conn.execute(text(f'CREATE DATABASE "{name}"'))
    finally:
        await admin.dispose()


@pytest.fixture(scope="session")
def migrated_test_db() -> str:
    """Create the test database if missing and run Alembic migrations to head once."""
    asyncio.run(_create_database_if_missing(TEST_DATABASE_URL))
    env = {**os.environ, "DATABASE_URL": TEST_DATABASE_URL}
    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=_API_DIR,
        env=env,
        check=True,
    )
    return TEST_DATABASE_URL


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

    async def send(self, to: str, subject: str, body: str) -> None:
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
    try:
        async with session_factory() as session:
            yield session
    finally:
        app.dependency_overrides.pop(get_db, None)
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
