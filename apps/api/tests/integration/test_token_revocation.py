"""T120: refresh tokens die with the account.

Deactivating, deleting or changing the role of a user revokes their refresh tokens, and
refresh refuses inactive or soft-deleted users even if a token somehow survived.

Covers: FR-015, FR-016, FR-238, NFR-005.
"""

import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hds.models.user import User

PASSWORD = "Dev1234!"


async def _login(client: AsyncClient, email: str) -> dict[str, str]:
    r = await client.post("/auth/login", json={"email": email, "password": PASSWORD})
    assert r.status_code == 200, r.text
    return {"access": r.json()["access_token"], "refresh": r.json()["refresh_token"]}


async def _refresh_status(client: AsyncClient, refresh_token: str) -> int:
    r = await client.post("/auth/refresh", json={"refresh_token": refresh_token})
    return r.status_code


async def _unrevoked(session: AsyncSession, user_id: object) -> int:
    await session.rollback()
    return int(
        await session.scalar(
            text("SELECT count(*) FROM refresh_tokens WHERE user_id = :u AND revoked_at IS NULL"),
            {"u": user_id},
        )
        or 0
    )


def _auth(tokens: dict[str, str]) -> dict[str, str]:
    return {"Authorization": f"Bearer {tokens['access']}"}


# ── refresh itself rejects inactive / soft-deleted users (token left untouched) ─────────


@pytest.mark.asyncio
async def test_refresh_rejects_inactive_user_even_with_unrevoked_token(
    async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    tokens = await _login(async_client, "employee@hds.test")
    await test_db.execute(
        text("UPDATE users SET is_active = false WHERE id = :i"), {"i": seed_users["employee"].id}
    )
    await test_db.commit()  # direct write: no revocation happened

    assert await _unrevoked(test_db, seed_users["employee"].id) == 1
    assert await _refresh_status(async_client, tokens["refresh"]) == 401


@pytest.mark.asyncio
async def test_refresh_rejects_soft_deleted_user_even_with_unrevoked_token(
    async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    tokens = await _login(async_client, "employee@hds.test")
    await test_db.execute(
        text("UPDATE users SET is_deleted = true, deleted_at = now() WHERE id = :i"),
        {"i": seed_users["employee"].id},
    )
    await test_db.commit()

    assert await _unrevoked(test_db, seed_users["employee"].id) == 1
    assert await _refresh_status(async_client, tokens["refresh"]) == 401


# ── revocation on deactivate, delete, role change ──────────────────────────────────────


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["deactivate", "delete", "role_change"])
async def test_action_revokes_all_refresh_tokens_of_the_user_only(
    action: str,
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
) -> None:
    target = seed_users["employee"]
    device_a = await _login(async_client, "employee@hds.test")
    device_b = await _login(async_client, "employee@hds.test")
    bystander = await _login(async_client, "manager@hds.test")
    admin = await _login(async_client, "admin@hds.test")
    assert await _unrevoked(test_db, target.id) == 2

    if action == "deactivate":
        r = await async_client.post(
            f"/api/v1/employees/{target.id}/deactivate", headers=_auth(admin)
        )
        assert r.status_code == 200, r.text
    elif action == "delete":
        r = await async_client.delete(f"/api/v1/employees/{target.id}", headers=_auth(admin))
        assert r.status_code == 204, r.text
    else:
        r = await async_client.patch(
            f"/api/v1/employees/{target.id}", json={"role": "manager"}, headers=_auth(admin)
        )
        assert r.status_code == 200, r.text

    assert await _unrevoked(test_db, target.id) == 0
    assert await _refresh_status(async_client, device_a["refresh"]) == 401
    assert await _refresh_status(async_client, device_b["refresh"]) == 401
    # Other accounts keep their sessions.
    assert await _refresh_status(async_client, bystander["refresh"]) == 200


@pytest.mark.asyncio
async def test_edit_without_role_change_keeps_refresh_tokens(
    async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    target = seed_users["employee"]
    device = await _login(async_client, "employee@hds.test")
    admin = await _login(async_client, "admin@hds.test")

    r = await async_client.patch(
        f"/api/v1/employees/{target.id}", json={"full_name": "Renamed"}, headers=_auth(admin)
    )
    assert r.status_code == 200, r.text

    assert await _unrevoked(test_db, target.id) == 1
    assert await _refresh_status(async_client, device["refresh"]) == 200


@pytest.mark.asyncio
async def test_deactivated_user_cannot_sign_in_or_use_old_access_token(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    target = seed_users["employee"]
    employee = await _login(async_client, "employee@hds.test")
    admin = await _login(async_client, "admin@hds.test")
    await async_client.post(f"/api/v1/employees/{target.id}/deactivate", headers=_auth(admin))

    me = await async_client.get("/me", headers=_auth(employee))
    assert me.status_code == 401
    login = await async_client.post(
        "/auth/login", json={"email": "employee@hds.test", "password": PASSWORD}
    )
    assert login.status_code in (401, 403)


# ── sign-out touches only the caller's own token ────────────────────────────────────────


async def test_signing_out_with_someone_elses_token_answers_204_and_revokes_nothing(
    async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    caller = await _login(async_client, "employee@hds.test")
    victim = await _login(async_client, "manager@hds.test")

    response = await async_client.post(
        "/auth/logout", json={"refresh_token": victim["refresh"]}, headers=_auth(caller)
    )

    assert response.status_code == 204
    assert await _unrevoked(test_db, seed_users["manager"].id) == 1
    assert await _refresh_status(async_client, victim["refresh"]) == 200


async def test_signing_out_with_your_own_token_still_revokes_it(
    async_client: AsyncClient, test_db: AsyncSession, seed_users: dict[str, User]
) -> None:
    tokens = await _login(async_client, "employee@hds.test")

    response = await async_client.post(
        "/auth/logout", json={"refresh_token": tokens["refresh"]}, headers=_auth(tokens)
    )

    assert response.status_code == 204
    assert await _unrevoked(test_db, seed_users["employee"].id) == 0
