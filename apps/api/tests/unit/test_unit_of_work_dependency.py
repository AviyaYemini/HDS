"""The unit-of-work dependency inside a real FastAPI app (T206, research D1).

The design depends on three FastAPI behaviours that a plain unit test of the context manager
cannot show: a handler and its sub-dependencies share one session and one commit; a refused
request rolls back; and with ``scope="function"`` the commit runs before the response is sent, so
a failed commit is reported as an error instead of a success that was not saved.

Covers: FR-210.
"""

from collections.abc import AsyncGenerator
from typing import Any

from fastapi import Depends, FastAPI, HTTPException
from httpx import ASGITransport, AsyncClient

from hds.core.database import get_db, get_unit_of_work

SCOPED = Depends(get_unit_of_work, scope="function")


def build_app(session: Any) -> FastAPI:
    """Build a small app whose database session is the given fake."""
    app = FastAPI()

    async def fake_db() -> AsyncGenerator[Any, None]:
        yield session

    app.dependency_overrides[get_db] = fake_db

    async def current_user(db: Any = SCOPED) -> Any:
        return db

    @app.get("/ok")
    async def ok(db: Any = SCOPED, user_db: Any = Depends(current_user)) -> dict[str, bool]:
        return {"same_session": db is user_db}

    @app.get("/refused")
    async def refused(db: Any = SCOPED) -> None:
        raise HTTPException(status_code=409, detail="refused")

    return app


def client(app: FastAPI) -> AsyncClient:
    return AsyncClient(
        transport=ASGITransport(app=app, raise_app_exceptions=False), base_url="http://test"
    )


async def test_handler_and_subdependency_share_one_session_and_one_commit(
    fake_session: Any,
) -> None:
    async with client(build_app(fake_session)) as c:
        response = await c.get("/ok")
    assert response.status_code == 200
    assert response.json() == {"same_session": True}
    assert fake_session.events == ["commit"]


async def test_refused_request_rolls_back_and_does_not_commit(fake_session: Any) -> None:
    async with client(build_app(fake_session)) as c:
        response = await c.get("/refused")
    assert response.status_code == 409
    assert fake_session.events == ["rollback"]


async def test_failed_commit_is_an_error_response_not_a_success(fake_session: Any) -> None:
    fake_session.fail_commit = True
    async with client(build_app(fake_session)) as c:
        response = await c.get("/ok")
    assert response.status_code == 500
    assert fake_session.events == ["rollback"]
