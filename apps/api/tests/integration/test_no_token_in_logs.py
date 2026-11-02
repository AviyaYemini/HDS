"""No one-time token or full address reaches a log or the mail outbox (T220, SC-202).

Runs the four flows that handle a token (invite, accept, reset request, reset confirm) through the
real application with log capture at DEBUG and the real development mail sender. Tokens are read
from a test-only in-memory tap, never from the outbox, because the outbox must not contain them.
The test is only meaningful if the flows really ran, so each step's status is asserted first.

Covers: FR-203, SC-202.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from httpx import AsyncClient

from hds.core.mail import get_mail_sender
from hds.main import app
from hds.models.user import User

INVITEE = "newhire@hds.test"
MANAGER = "manager@hds.test"


class TapMailSender:
    """Passes every message to the real sender and remembers the clear token in memory only."""

    def __init__(self, inner: Any) -> None:
        self.inner = inner
        self.tokens: list[str] = []

    async def send(self, to: str, subject: str, body: str, **options: Any) -> None:
        self.tokens.append(body.split()[-1])  # the token is the last word of the body
        await self.inner.send(to, subject, body, **options)


@pytest.fixture
def tap(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """The application's real mail sender, built from settings that point at a temp outbox."""
    path = tmp_path / "outbox.jsonl"
    fake = SimpleNamespace(ENVIRONMENT="test", MAIL_BACKEND="outbox", MAIL_OUTBOX_PATH=str(path))
    monkeypatch.setattr("hds.core.mail.get_settings", lambda: fake, raising=False)
    sender = TapMailSender(get_mail_sender())
    sender.path = path  # type: ignore[attr-defined]
    app.dependency_overrides[get_mail_sender] = lambda: sender
    yield sender
    app.dependency_overrides.pop(get_mail_sender, None)


async def test_no_token_or_full_address_in_any_log_record_or_the_outbox(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    tap: TapMailSender,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)

    login = await async_client.post("/auth/login", json={"email": MANAGER, "password": "Dev1234!"})
    assert login.status_code == 200
    auth = {"Authorization": f"Bearer {login.json()['access_token']}"}

    invite = await async_client.post(
        "/auth/invite", json={"email": INVITEE, "role": "employee"}, headers=auth
    )
    assert invite.status_code == 202
    accept = await async_client.post(
        "/auth/invite/accept", json={"token": tap.tokens[-1], "password": "A-long-password-1"}
    )
    assert accept.status_code == 204
    reset_request = await async_client.post("/auth/password-reset/request", json={"email": INVITEE})
    assert reset_request.status_code == 202
    reset = await async_client.post(
        "/auth/password-reset/confirm",
        json={"token": tap.tokens[-1], "new_password": "Another-long-password-2"},
    )
    assert reset.status_code == 204
    assert len(tap.tokens) == 2  # one message per flow that sends mail

    secrets = [*tap.tokens, "A-long-password-1", "Another-long-password-2"]
    addresses = [INVITEE, MANAGER]

    # 1. Logs: nothing in any record, however it was built.
    formatter = logging.Formatter()
    for record in caplog.records:
        text = record.getMessage()
        if record.exc_info:
            text += formatter.formatException(record.exc_info)
        for secret in secrets:
            assert secret not in text, f"{secret!r} found in log record from {record.name}"
        for address in addresses:
            assert address not in text, f"{address!r} found in log record from {record.name}"

    # 2. Outbox: a redacted record per message, nothing usable.
    raw = tap.path.read_text(encoding="utf-8")  # type: ignore[attr-defined]
    for secret in secrets:
        assert secret not in raw
    for address in addresses:
        assert address not in raw
    entries = [json.loads(line) for line in raw.splitlines()]
    assert [e["purpose"] for e in entries] == ["invite", "reset"]
    for entry in entries:
        assert entry["to"] == "n***@hds.test"
        assert "[REDACTED]" in entry["body"]
        datetime.fromisoformat(entry["expires_at"])  # an expiry is recorded
