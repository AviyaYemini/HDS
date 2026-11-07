"""Phone sessions, the session list and ending one session.

Covers: 007 FR-009, FR-010, N12 (research R10, ADR 016, task T027). A web sign-in without
``client_kind`` keeps 7 days; ``native`` gets 30 and keeps them across a refresh.
"""

from datetime import UTC, datetime, timedelta

from httpx import AsyncClient

from hds.models.user import User

PASSWORD = "Dev1234!"


async def _sign_in(client: AsyncClient, **extra: str) -> dict[str, str]:
    response = await client.post(
        "/auth/login", json={"email": "employee@hds.test", "password": PASSWORD, **extra}
    )
    assert response.status_code == 200, response.text
    return dict(response.json())


def _bearer(tokens: dict[str, str]) -> dict[str, str]:
    return {"Authorization": f"Bearer {tokens['access_token']}"}


async def _sessions(client: AsyncClient, tokens: dict[str, str]) -> list[dict[str, str]]:
    response = await client.get("/me/sessions", headers=_bearer(tokens))
    assert response.status_code == 200, response.text
    return list(response.json())


def _days_left(session: dict[str, str]) -> float:
    left = datetime.fromisoformat(session["expires_at"]) - datetime.now(UTC)
    return left / timedelta(days=1)


async def test_a_web_sign_in_without_client_kind_gets_seven_days(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    tokens = await _sign_in(async_client)
    [session] = await _sessions(async_client, tokens)
    assert session["client_kind"] == "web"
    assert 6.9 < _days_left(session) <= 7


async def test_a_native_sign_in_gets_thirty_days_and_keeps_them_after_a_refresh(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    tokens = await _sign_in(async_client, client_kind="native")
    refreshed = await async_client.post(
        "/auth/refresh", json={"refresh_token": tokens["refresh_token"]}
    )
    assert refreshed.status_code == 200, refreshed.text
    [session] = await _sessions(async_client, refreshed.json())
    assert session["client_kind"] == "native"
    assert 29.9 < _days_left(session) <= 30


async def test_an_unknown_client_kind_is_refused(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    response = await async_client.post(
        "/auth/login",
        json={"email": "employee@hds.test", "password": PASSWORD, "client_kind": "kiosk"},
    )
    assert response.status_code == 422


async def test_ending_one_session_leaves_the_others(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    phone = await _sign_in(async_client, client_kind="native")
    web = await _sign_in(async_client)
    listed = await _sessions(async_client, web)
    assert len(listed) == 2
    phone_session = next(s for s in listed if s["client_kind"] == "native")

    ended = await async_client.delete(f"/me/sessions/{phone_session['id']}", headers=_bearer(web))
    assert ended.status_code == 204
    refused = await async_client.post(
        "/auth/refresh", json={"refresh_token": phone["refresh_token"]}
    )
    assert refused.status_code == 401
    assert [s["client_kind"] for s in await _sessions(async_client, web)] == ["web"]


async def test_another_users_session_cannot_be_ended(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    mine = await _sign_in(async_client)
    [session] = await _sessions(async_client, mine)
    other = await async_client.post(
        "/auth/login", json={"email": "manager@hds.test", "password": PASSWORD}
    )
    response = await async_client.delete(
        f"/me/sessions/{session['id']}", headers=_bearer(other.json())
    )
    assert response.status_code == 404
    assert len(await _sessions(async_client, mine)) == 1
