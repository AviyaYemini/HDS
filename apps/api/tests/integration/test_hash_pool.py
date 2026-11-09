"""No database connection is held while a password is hashed, and a full queue refuses (T344).

With a small pool, requests that wait for a hash while holding a connection would use up the pool
and every other endpoint would wait for one. The hash queue is also bounded: past it, a request is
refused at once with 503 and changes nothing, on every path that hashes.

Covers: FR-222, SC-211.
"""

import asyncio
import threading
import uuid
from collections.abc import AsyncGenerator, Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
import pytest_asyncio
from httpx import AsyncClient
from sqlalchemy import delete, insert, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from hds.core import passwords, security
from hds.core.config import get_settings
from hds.core.database import get_db, unit_of_work
from hds.core.security import generate_refresh_token
from hds.main import app
from hds.models.organisation import Organisation
from hds.models.user import User
from hds.repositories.user import UserRepository
from tests.address_count import address_state, set_address_state
from tests.conftest import OutboxMailSender

PASSWORD = "Dev1234!"
NEW_PASSWORD = "A-long-password-1"


@pytest_asyncio.fixture
async def small_pool(migrated_test_db: str, test_db: AsyncSession) -> AsyncGenerator[None, None]:
    """Serve the application from a pool of 5 connections that fails fast when it is empty."""
    engine = create_async_engine(migrated_test_db, pool_size=5, max_overflow=0, pool_timeout=2)
    factory = async_sessionmaker(engine, expire_on_commit=False)

    async def override() -> AsyncGenerator[AsyncSession, None]:
        async with factory() as session:
            yield session

    before = app.dependency_overrides[get_db]
    app.dependency_overrides[get_db] = override
    try:
        yield
    finally:
        app.dependency_overrides[get_db] = before
        await engine.dispose()


async def admin_headers(client: AsyncClient) -> dict[str, str]:
    response = await client.post(
        "/auth/login", json={"email": "admin@hds.test", "password": PASSWORD}
    )
    assert response.status_code == 200, response.text
    return {"Authorization": f"Bearer {response.json()['access_token']}"}


@pytest.fixture
def gate(monkeypatch: pytest.MonkeyPatch) -> Iterator[threading.Event]:
    """Hold every hash until the test says so, so requests stay parked at the hashing step."""
    monkeypatch.setattr(get_settings(), "HASH_QUEUE_LIMIT", 100)
    opened = threading.Event()
    real_hash, real_dummy = security.hash_password, security.verify_against_dummy

    def waiting_hash(plain: str) -> str:
        opened.wait(15)
        return real_hash(plain)

    def waiting_dummy(plain: str) -> bool:
        opened.wait(15)
        return real_dummy(plain)

    monkeypatch.setattr(security, "hash_password", waiting_hash)
    monkeypatch.setattr(security, "verify_against_dummy", waiting_dummy)
    yield opened
    opened.set()


async def read_while_parked(
    client: AsyncClient, headers: dict[str, str], tasks: list[Any], opened: threading.Event
) -> list[Any]:
    """With ``tasks`` all parked at the hash, an unrelated read must still get a connection.

    A request that held its connection while waiting would use up the pool of 5, and this read
    would fail when the pool times out.
    """
    await asyncio.sleep(0.2)
    try:
        read = await asyncio.wait_for(client.get("/api/v1/sites", headers=headers), timeout=5)
        assert read.status_code == 200, read.text
    finally:
        opened.set()
    return await asyncio.gather(*tasks, return_exceptions=True)


def statuses(results: list[Any]) -> set[Any]:
    return {getattr(r, "status_code", repr(r)[:160]) for r in results}


async def test_sign_ins_do_not_hold_connections_while_hashing(
    async_client: AsyncClient,
    small_pool: None,
    seed_users: dict[str, User],
    gate: threading.Event,
) -> None:
    gate.set()
    headers = await admin_headers(async_client)
    gate.clear()

    tasks = [
        asyncio.create_task(
            async_client.post(
                "/auth/login", json={"email": f"nobody{n}@hds.test", "password": "not-it"}
            )
        )
        for n in range(50)
    ]

    assert statuses(await read_while_parked(async_client, headers, tasks, gate)) == {401}


async def test_employee_creations_do_not_hold_connections_while_hashing(
    async_client: AsyncClient,
    small_pool: None,
    seed_users: dict[str, User],
    gate: threading.Event,
) -> None:
    gate.set()
    headers = await admin_headers(async_client)
    gate.clear()

    tasks = [
        asyncio.create_task(
            async_client.post(
                "/api/v1/employees",
                json={
                    "email": f"new{n}@hds.test",
                    "full_name": "New",
                    "initial_password": NEW_PASSWORD,
                },
                headers=headers,
            )
        )
        for n in range(50)
    ]

    assert statuses(await read_while_parked(async_client, headers, tasks, gate)) == {201}


@pytest.fixture
def saturated(monkeypatch: pytest.MonkeyPatch) -> Iterator[threading.Event]:
    """One slow hash fills the only place, and nothing may queue."""
    monkeypatch.setattr(get_settings(), "HASH_CONCURRENCY", 1)
    monkeypatch.setattr(get_settings(), "HASH_QUEUE_LIMIT", 0)
    real_hash = security.hash_password
    release = threading.Event()

    def slow_hash(plain: str) -> str:
        release.wait(10)
        return real_hash(plain)

    monkeypatch.setattr(security, "hash_password", slow_hash)
    yield release
    release.set()


async def hold_the_only_place() -> asyncio.Task[Any]:
    task = asyncio.create_task(passwords.hash_password("held"))
    await asyncio.sleep(0.02)
    return task


async def add_link(db: AsyncSession, user_id: uuid.UUID, purpose: str) -> str:
    raw, token_hash = generate_refresh_token()
    await db.execute(
        text(
            "INSERT INTO invitation_tokens (id, user_id, token_hash, purpose, expires_at) "
            "VALUES (:i, :u, :h, :p, :x)"
        ),
        {
            "i": uuid.uuid4(),
            "u": user_id,
            "h": token_hash,
            "p": purpose,
            "x": datetime.now(UTC) + timedelta(hours=1),
        },
    )
    await db.commit()
    return raw


async def digest(db: AsyncSession) -> list[Any]:
    await db.rollback()
    rows = await db.execute(
        text(
            "SELECT (SELECT count(*) FROM users), "
            "(SELECT count(*) FROM invitation_tokens WHERE used_at IS NOT NULL), "
            "(SELECT count(*) FROM invitation_tokens), "
            "(SELECT count(*) FROM refresh_tokens)"
        )
    )
    return list(rows.one())


async def assert_busy(response: Any) -> None:
    assert response.status_code == 503, response.text
    assert response.json()["code"] == "service.busy"
    assert int(response.headers["Retry-After"]) >= 1


async def test_a_full_queue_refuses_on_every_hashing_path_and_changes_nothing(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
    saturated: threading.Event,
) -> None:
    headers = await admin_headers(async_client)
    employee = seed_users["employee"].id
    org_id = seed_users["admin"].organisation_id
    pending = uuid.uuid4()
    await test_db.execute(
        text(
            "INSERT INTO users (id, organisation_id, email, password_hash, full_name, role, "
            "is_active) VALUES (:i, :o, 'pending@hds.test', 'h', 'P', "
            "CAST('employee' AS user_role), false)"
        ),
        {"i": pending, "o": org_id},
    )
    await test_db.commit()
    reset_link = await add_link(test_db, employee, "reset")
    invite_link = await add_link(test_db, pending, "invite")
    client = await async_client.post(
        "/api/v1/clients", json={"name": "Acme", "contract_start": "2026-01-01"}, headers=headers
    )
    assert client.status_code == 201, client.text
    client_id = client.json()["id"]
    await test_db.commit()
    held = await hold_the_only_place()
    before = await digest(test_db)

    answers = [
        await async_client.post(
            "/auth/login", json={"email": "nobody@hds.test", "password": "not-it"}
        ),
        await async_client.post(
            "/auth/login", json={"email": "employee@hds.test", "password": PASSWORD}
        ),
        await async_client.post(
            "/auth/invite",
            json={"email": "brand.new@hds.test", "role": "employee"},
            headers=headers,
        ),
        await async_client.post(
            "/auth/invite/accept", json={"token": invite_link, "password": NEW_PASSWORD}
        ),
        await async_client.post(
            "/auth/password-reset/confirm",
            json={"token": reset_link, "new_password": NEW_PASSWORD},
        ),
        await async_client.post(
            "/api/v1/employees",
            json={"email": "e@hds.test", "full_name": "E", "initial_password": NEW_PASSWORD},
            headers=headers,
        ),
        await async_client.post(
            f"/api/v1/clients/{client_id}/users",
            json={"email": "p@hds.test", "full_name": "P", "initial_password": NEW_PASSWORD},
            headers=headers,
        ),
    ]

    for answer in answers:
        await assert_busy(answer)
    assert await digest(test_db) == before
    assert outbox.messages == []
    saturated.set()
    await held


async def test_a_sign_in_after_the_cooldown_ends_can_release_its_connection(
    async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    """An expired lock is judged from the row that was read, with no write before verification."""
    await set_address_state(
        test_db, "employee@hds.test", 5, datetime.now(UTC) - timedelta(minutes=1)
    )

    signed_in = await async_client.post(
        "/auth/login", json={"email": "employee@hds.test", "password": PASSWORD}
    )
    assert signed_in.status_code == 200, signed_in.text

    await set_address_state(
        test_db, "employee@hds.test", 5, datetime.now(UTC) - timedelta(minutes=1)
    )
    wrong = await async_client.post(
        "/auth/login", json={"email": "employee@hds.test", "password": "not-it"}
    )

    assert wrong.status_code == 401, wrong.text
    assert await address_state(test_db, "employee@hds.test") == (1, False)  # restarted, no lock


class TestReleaseConnection:
    """``release_connection`` ends a read-only transaction and refuses anything else."""

    async def org(self, db: AsyncSession) -> Organisation:
        org = Organisation(id=uuid.uuid4(), name="Acme", timezone="UTC")
        db.add(org)
        await db.commit()
        return org

    async def test_a_read_only_transaction_is_ended(self, test_db: AsyncSession) -> None:
        async with unit_of_work(test_db):
            await test_db.execute(select(Organisation.id))
            assert test_db.in_transaction()
            await UserRepository(test_db).release_connection()
            assert not test_db.in_transaction()

    @pytest.mark.parametrize("how", ["orm", "update", "insert", "delete"])
    async def test_it_refuses_after_any_write(self, test_db: AsyncSession, how: str) -> None:
        org = await self.org(test_db)
        repo = UserRepository(test_db)
        with pytest.raises(RuntimeError, match="write"):
            async with unit_of_work(test_db):
                if how == "orm":
                    test_db.add(Organisation(id=uuid.uuid4(), name="B", timezone="UTC"))
                    await test_db.flush()
                elif how == "update":
                    await test_db.execute(
                        update(Organisation).where(Organisation.id == org.id).values(name="C")
                    )
                elif how == "insert":
                    await test_db.execute(
                        insert(Organisation).values(id=uuid.uuid4(), name="D", timezone="UTC")
                    )
                else:
                    await test_db.execute(delete(Organisation).where(Organisation.id == org.id))
                await repo.release_connection()

    async def test_it_refuses_while_a_row_lock_is_held(self, test_db: AsyncSession) -> None:
        org = await self.org(test_db)
        with pytest.raises(RuntimeError, match="write"):
            async with unit_of_work(test_db):
                await test_db.execute(
                    select(Organisation.id).where(Organisation.id == org.id).with_for_update()
                )
                await UserRepository(test_db).release_connection()

    async def test_a_commit_or_rollback_clears_the_record(self, test_db: AsyncSession) -> None:
        org = await self.org(test_db)
        repo = UserRepository(test_db)
        await test_db.execute(
            update(Organisation).where(Organisation.id == org.id).values(name="E")
        )
        await test_db.commit()
        await test_db.execute(select(Organisation.id))
        await repo.release_connection()
        await test_db.execute(
            update(Organisation).where(Organisation.id == org.id).values(name="F")
        )
        await test_db.rollback()
        await repo.release_connection()


def test_the_six_hashing_operations_declare_503() -> None:
    paths = app.openapi()["paths"]
    for path, method in (
        ("/auth/login", "post"),
        ("/auth/invite", "post"),
        ("/auth/invite/accept", "post"),
        ("/auth/password-reset/confirm", "post"),
        ("/api/v1/employees", "post"),
        ("/api/v1/clients/{id}/users", "post"),
    ):
        assert "503" in paths[path][method]["responses"], path
