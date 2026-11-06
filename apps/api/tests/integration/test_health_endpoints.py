"""The health endpoints on the real app and database (T013).

``/livez``, ``/readyz`` and ``/health`` answer without authentication and are not in the OpenAPI
document. With the database unreachable, ``/readyz`` answers 503 naming ``database`` within the
2-second limit while ``/livez`` stays 200.

Covers: specs/006-deploy-operations FR-006, FR-009, FR-010; contracts/ops.md section 1.
"""

import time

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from hds.core.database import get_health_session_factory, get_session_factory
from hds.main import app

PATHS = ["/livez", "/readyz", "/health"]


@pytest.mark.parametrize("path", PATHS)
async def test_each_check_answers_ok_with_the_version_and_no_sign_in(
    async_client: AsyncClient, test_db: AsyncSession, path: str
) -> None:
    response = await async_client.get(path)
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["status"] == "ok"
    assert isinstance(body["version"], str) and body["version"]


async def test_with_the_database_unreachable_ready_fails_fast_and_live_stays_up(
    async_client: AsyncClient, test_db: AsyncSession
) -> None:
    # A closed port: the connection is refused at once, as when the database is stopped.
    engine = create_async_engine(
        "postgresql+asyncpg://hds:unused-password-0123@127.0.0.1:1/hds", poolclass=NullPool
    )
    closed = async_sessionmaker(engine, expire_on_commit=False)
    previous = app.dependency_overrides.get(get_health_session_factory)
    app.dependency_overrides[get_health_session_factory] = lambda: closed
    try:
        started = time.monotonic()
        ready = await async_client.get("/readyz")
        elapsed = time.monotonic() - started
        live = await async_client.get("/livez")
    finally:
        if previous is None:
            app.dependency_overrides.pop(get_health_session_factory, None)
        else:
            app.dependency_overrides[get_health_session_factory] = previous
        await engine.dispose()

    assert ready.status_code == 503, ready.text
    assert ready.json()["status"] == "not_ready"
    assert ready.json()["failing"] == ["database"]
    assert "127.0.0.1" not in ready.text
    assert elapsed < 2.5
    assert live.status_code == 200


async def test_none_of_the_checks_is_in_the_openapi_document(
    async_client: AsyncClient,
) -> None:
    document = (await async_client.get("/api/v1/openapi.json")).json()
    assert not set(PATHS) & set(document["paths"])


async def test_ready_answers_while_the_request_pool_is_exhausted(
    async_client: AsyncClient, test_db: AsyncSession, migrated_test_db: str
) -> None:
    # A request pool of one connection, held for the whole test: requests would wait for it.
    engine = create_async_engine(migrated_test_db, pool_size=1, max_overflow=0, pool_timeout=30)
    busy = async_sessionmaker(engine, expire_on_commit=False)
    previous = app.dependency_overrides.get(get_session_factory)
    app.dependency_overrides[get_session_factory] = lambda: busy
    try:
        async with engine.connect():
            started = time.monotonic()
            ready = await async_client.get("/readyz")
            elapsed = time.monotonic() - started
    finally:
        if previous is not None:
            app.dependency_overrides[get_session_factory] = previous
        await engine.dispose()
    assert ready.status_code == 200, ready.text
    assert elapsed < 2.0
