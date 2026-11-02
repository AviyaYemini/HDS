"""T123: IntegrityError is mapped to a 422 problem-details response, never a 500."""

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.exc import IntegrityError

from hds.core.error_handler import register_exception_handlers


def _app() -> FastAPI:
    app = FastAPI()
    register_exception_handlers(app)

    @app.get("/boom")
    async def boom() -> None:
        raise IntegrityError("INSERT ...", {"x": 1}, Exception("duplicate key value"))

    return app


@pytest.mark.asyncio
async def test_integrity_error_becomes_422_problem_details() -> None:
    async with AsyncClient(transport=ASGITransport(app=_app()), base_url="http://t") as c:
        r = await c.get("/boom")

    assert r.status_code == 422
    assert r.headers["content-type"].startswith("application/problem+json")
    body = r.json()
    assert body["code"] == "validation.constraint_violation"
    assert body["status"] == 422
    assert body["title"] == "Unprocessable Entity"
    assert set(body) >= {"type", "title", "status", "detail", "code", "instance"}


@pytest.mark.asyncio
async def test_integrity_error_does_not_leak_sql_or_driver_message() -> None:
    async with AsyncClient(transport=ASGITransport(app=_app()), base_url="http://t") as c:
        r = await c.get("/boom")

    assert "INSERT" not in r.text
    assert "duplicate key" not in r.text
