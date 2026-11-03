"""Reuse of a spent refresh token, and why each token was revoked (T252, FR-209, research D3).

Presenting a token that was already *rotated* means either an honest retry (a lost response) or a
stolen copy. Inside the grace window it is refused and nothing else happens; after it, all of the
user's sessions end. Tokens revoked for any other reason (sign-out, account change, or no recorded
reason from before the column existed) are refused with no other effect, because presenting them
is not evidence of theft. The window is set to one second here so the test can step across it.

Covers: FR-209, NFR-005.
"""

import asyncio
import hashlib
from typing import Any

import pytest
from httpx import AsyncClient, Response
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.config import get_settings
from hds.models.user import User
from tests.conftest import OutboxMailSender

PASSWORD = "Dev1234!"
PAST_THE_WINDOW = 1.3  # seconds, for a one-second window


@pytest.fixture(autouse=True)
def one_second_window(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(get_settings(), "REFRESH_REUSE_GRACE_SECONDS", 1)


async def sign_in(client: AsyncClient, email: str = "employee@hds.test") -> dict[str, Any]:
    response = await client.post("/auth/login", json={"email": email, "password": PASSWORD})
    assert response.status_code == 200, response.text
    body: dict[str, Any] = response.json()
    return body


async def manager_headers(client: AsyncClient) -> dict[str, str]:
    session = await sign_in(client, "manager@hds.test")
    return {"Authorization": f"Bearer {session['access_token']}"}


async def refresh(client: AsyncClient, raw: str) -> Response:
    return await client.post("/auth/refresh", json={"refresh_token": raw})


async def state(db: AsyncSession, raw: str) -> Any:
    """``(revoked, reason)`` of a token as stored."""
    result = await db.execute(
        text(
            "SELECT revoked_at IS NOT NULL AS revoked, revoked_reason AS reason "
            "FROM refresh_tokens WHERE token_hash = :h"
        ),
        {"h": hashlib.sha256(raw.encode()).hexdigest()},
    )
    return result.one()


async def test_reuse_after_the_window_ends_every_session_of_the_user(
    async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    first, other_device = await sign_in(async_client), await sign_in(async_client)
    someone_else = await sign_in(async_client, "manager@hds.test")
    renewed = (await refresh(async_client, first["refresh_token"])).json()
    await asyncio.sleep(PAST_THE_WINDOW)

    replay = await refresh(async_client, first["refresh_token"])

    assert replay.status_code == 401 and replay.json()["code"] == "auth.token_revoked"
    assert tuple(await state(test_db, first["refresh_token"])) == (True, "rotated")
    assert tuple(await state(test_db, renewed["refresh_token"])) == (
        True,
        "all",
    )  # the thief's gain
    assert tuple(await state(test_db, other_device["refresh_token"])) == (True, "all")
    assert (await state(test_db, someone_else["refresh_token"])).revoked is False  # another user


async def test_reuse_inside_the_window_is_refused_and_keeps_every_session(
    async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    first, other_device = await sign_in(async_client), await sign_in(async_client)
    renewed = (await refresh(async_client, first["refresh_token"])).json()

    retry = await refresh(async_client, first["refresh_token"])  # an honest retry, at once

    assert retry.status_code == 401 and retry.json()["code"] == "auth.token_revoked"
    assert (await state(test_db, renewed["refresh_token"])).revoked is False
    assert (await state(test_db, other_device["refresh_token"])).revoked is False
    assert (await refresh(async_client, renewed["refresh_token"])).status_code == 200


async def test_reuse_of_a_signed_out_token_is_refused_without_ending_other_sessions(
    async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    first, other_device = await sign_in(async_client), await sign_in(async_client)
    headers = {"Authorization": f"Bearer {first['access_token']}"}
    out = await async_client.post(
        "/auth/logout", json={"refresh_token": first["refresh_token"]}, headers=headers
    )
    assert out.status_code == 204
    await asyncio.sleep(PAST_THE_WINDOW)

    replay = await refresh(async_client, first["refresh_token"])

    assert replay.status_code == 401
    assert tuple(await state(test_db, first["refresh_token"])) == (True, "logout")
    assert (await state(test_db, other_device["refresh_token"])).revoked is False


async def test_a_token_revoked_before_reasons_were_recorded_is_refused_without_other_effect(
    async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    first, other_device = await sign_in(async_client), await sign_in(async_client)
    await test_db.execute(
        text(
            "UPDATE refresh_tokens SET revoked_at = now() - interval '1 day', "
            "revoked_reason = NULL WHERE token_hash = :h"
        ),
        {"h": hashlib.sha256(first["refresh_token"].encode()).hexdigest()},
    )
    await test_db.commit()

    replay = await refresh(async_client, first["refresh_token"])

    assert replay.status_code == 401
    assert (await state(test_db, first["refresh_token"])).reason is None  # unknown stays unknown
    assert (await state(test_db, other_device["refresh_token"])).revoked is False


async def test_deactivating_an_account_revokes_its_tokens_as_account_and_reuse_does_nothing_more(
    async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    first, other_device = await sign_in(async_client), await sign_in(async_client)
    manager = await manager_headers(async_client)
    employee_id = seed_users["employee"].id
    deactivated = await async_client.post(
        f"/api/v1/employees/{employee_id}/deactivate", headers=manager
    )
    assert deactivated.status_code == 200
    await asyncio.sleep(PAST_THE_WINDOW)

    replay = await refresh(async_client, first["refresh_token"])

    assert replay.status_code == 401
    assert tuple(await state(test_db, first["refresh_token"])) == (True, "account")  # not rewritten
    assert tuple(await state(test_db, other_device["refresh_token"])) == (True, "account")


async def test_changing_a_role_revokes_the_users_tokens_as_account(
    async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    session = await sign_in(async_client)
    manager = await manager_headers(async_client)
    changed = await async_client.patch(
        f"/api/v1/employees/{seed_users['employee'].id}", json={"role": "manager"}, headers=manager
    )
    assert changed.status_code == 200
    assert tuple(await state(test_db, session["refresh_token"])) == (True, "account")


async def test_signing_out_everywhere_and_resetting_a_password_record_all(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    outbox: OutboxMailSender,
) -> None:
    everywhere = await sign_in(async_client)
    headers = {"Authorization": f"Bearer {everywhere['access_token']}"}
    assert (await async_client.post("/auth/logout-all", headers=headers)).status_code == 204
    assert tuple(await state(test_db, everywhere["refresh_token"])) == (True, "all")

    session = await sign_in(async_client)
    await async_client.post("/auth/password-reset/request", json={"email": "employee@hds.test"})
    reset = await async_client.post(
        "/auth/password-reset/confirm",
        json={
            "token": outbox.last_token_for("employee@hds.test"),
            "new_password": "A-new-password-1",
        },
    )
    assert reset.status_code == 204
    assert tuple(await state(test_db, session["refresh_token"])) == (True, "all")


async def test_after_the_theft_response_the_owner_can_sign_in_again(
    async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    first = await sign_in(async_client)
    await refresh(async_client, first["refresh_token"])
    await asyncio.sleep(PAST_THE_WINDOW)
    await refresh(async_client, first["refresh_token"])  # the replay: ends all sessions

    again = await sign_in(async_client)  # the password still works; only sessions ended

    assert (await refresh(async_client, again["refresh_token"])).status_code == 200


async def expire(db: AsyncSession, raw: str) -> None:
    await db.execute(
        text(
            "UPDATE refresh_tokens SET expires_at = now() - interval '1 minute' "
            "WHERE token_hash = :h"
        ),
        {"h": hashlib.sha256(raw.encode()).hexdigest()},
    )
    await db.commit()


async def test_replaying_an_expired_rotated_token_ends_no_session(
    async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    """F08: anyone holding an old token must not be able to sign the owner out, again and again."""
    first, other_device = await sign_in(async_client), await sign_in(async_client)
    renewed = (await refresh(async_client, first["refresh_token"])).json()
    await asyncio.sleep(PAST_THE_WINDOW)
    await expire(test_db, first["refresh_token"])

    for _ in range(2):
        replay = await refresh(async_client, first["refresh_token"])
        assert replay.status_code == 401
        assert replay.json()["code"] == "auth.token_expired"

    assert tuple(await state(test_db, first["refresh_token"])) == (True, "rotated")
    assert (await state(test_db, renewed["refresh_token"])).revoked is False
    assert (await state(test_db, other_device["refresh_token"])).revoked is False


async def test_an_expired_token_that_was_never_rotated_is_refused_as_expired(
    async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    session = await sign_in(async_client)
    await expire(test_db, session["refresh_token"])

    answer = await refresh(async_client, session["refresh_token"])

    assert answer.status_code == 401
    assert answer.json()["code"] == "auth.token_expired"
    assert (await state(test_db, session["refresh_token"])).revoked is False  # nothing changed
