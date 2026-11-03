"""Email that carries a link is sent only after the change was saved (T266, FR-210).

Without this, a failed request could mail a link to something that was never saved, and a
successful one could mail it before the saved change is visible to the person clicking it. The
sender used here asks the database, through a second connection, whether the link row exists at the
moment it is asked to send: that is true only if the commit came first.

Covers: FR-210.
"""

from collections.abc import Iterator
from datetime import datetime
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.mail import get_mail_sender
from hds.main import app
from hds.models.user import User

PASSWORD = "Dev1234!"


class WitnessSender:
    """Records, for each message, how many links were visible to another connection when sent."""

    def __init__(self, db: AsyncSession) -> None:
        self._db = db
        self.sent: list[tuple[str, int]] = []

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
        await self._db.rollback()
        visible = (
            await self._db.execute(text("SELECT count(*) FROM invitation_tokens"))
        ).scalar_one()
        self.sent.append((to, int(visible)))


@pytest.fixture
def witness(test_db: AsyncSession) -> Iterator[WitnessSender]:
    sender = WitnessSender(test_db)
    app.dependency_overrides[get_mail_sender] = lambda: sender
    yield sender
    app.dependency_overrides.pop(get_mail_sender, None)


async def admin_headers(client: AsyncClient) -> dict[str, str]:
    response = await client.post(
        "/auth/login", json={"email": "admin@hds.test", "password": PASSWORD}
    )
    assert response.status_code == 200, response.text
    return {"Authorization": f"Bearer {response.json()['access_token']}"}


async def test_an_invitation_is_mailed_exactly_once_and_only_after_it_is_saved(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    witness: WitnessSender,
) -> None:
    headers = await admin_headers(async_client)

    sent = await async_client.post(
        "/auth/invite", json={"email": "new.person@hds.test", "role": "employee"}, headers=headers
    )

    assert sent.status_code == 202
    assert witness.sent == [("new.person@hds.test", 1)], "mail must go out once, after the commit"


async def test_a_password_reset_request_is_mailed_only_after_it_is_saved(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    witness: WitnessSender,
) -> None:
    sent = await async_client.post(
        "/auth/password-reset/request", json={"email": "employee@hds.test"}
    )

    assert sent.status_code == 202
    assert witness.sent == [("employee@hds.test", 1)]


async def test_an_invitation_whose_save_fails_sends_nothing_and_leaves_no_link(
    async_client: AsyncClient,
    test_db: AsyncSession,
    seed_users: dict[str, User],
    witness: WitnessSender,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    headers = await admin_headers(async_client)
    real_commit = AsyncSession.commit
    armed = {"on": True}

    async def failing_commit(self: AsyncSession) -> None:
        if armed["on"]:  # the invitation request's own commit; sign-in committed before arming
            await self.rollback()
            raise RuntimeError("the database refused the commit")
        await real_commit(self)

    monkeypatch.setattr(AsyncSession, "commit", failing_commit)

    response: Any = None
    try:
        response = await async_client.post(
            "/auth/invite",
            json={"email": "never.saved@hds.test", "role": "employee"},
            headers=headers,
        )
    except RuntimeError:
        pass  # the transport re-raises the server error; either way nothing may have been sent

    assert response is None or response.status_code == 500
    assert witness.sent == [], "an email was sent for a change that was not saved"
    armed["on"] = False
    await test_db.rollback()
    leftovers = (
        await test_db.execute(
            text("SELECT count(*) FROM users WHERE email = 'never.saved@hds.test'")
        )
    ).scalar_one()
    links = (await test_db.execute(text("SELECT count(*) FROM invitation_tokens"))).scalar_one()
    assert (leftovers, links) == (0, 0), "a failed invitation left a user or a usable link behind"
