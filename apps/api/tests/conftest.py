"""Shared pytest fixtures for HDS API tests."""

import uuid
from collections.abc import AsyncGenerator, Generator

import email_validator
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from hds.core.mail import get_mail_sender
from hds.core.security import hash_password
from hds.main import app
from hds.models.base import Base
from hds.models.organisation import Organisation
from hds.models.user import User, UserRole

# Allow the reserved ``.test`` domain used by the seeded fixtures.
email_validator.TEST_ENVIRONMENT = True

# Use SQLite for in-process integration fixtures; real PG for integration/ tests
TEST_DATABASE_URL = "sqlite+aiosqlite:///:memory:"


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
async def test_db() -> AsyncGenerator[AsyncSession, None]:
    engine = create_async_engine(TEST_DATABASE_URL, echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as session:
        yield session
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
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
