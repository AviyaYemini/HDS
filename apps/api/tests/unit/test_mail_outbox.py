"""The development mail outbox stores no token and no full address (T219, research D6).

A message that carries a one-time token must never be written to disk in clear: a copy of an
account-access link is a copy of the account. The sender replaces the token with ``[REDACTED]``,
masks the recipient, refuses to exist outside a development environment, and logs only the masked
recipient and the subject.

Covers: FR-203.
"""

import json
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from hds.core.mail import RedactingOutboxMailSender, get_mail_sender

TOKEN = "3f1c9d7e-4a2b-4c55-9e0a-7b6d5c4e3f21"
ADDRESS = "alice@corp.example"
EXPIRES = datetime(2026, 11, 9, 12, 0, tzinfo=UTC)
BODY = f"Accept your invitation with this token (valid 7 days): {TOKEN}"


def lines(path: Path) -> list[dict[str, str]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


async def send_invite(sender: RedactingOutboxMailSender, body: str = BODY) -> None:
    await sender.send(
        ADDRESS, "You have been invited", body, secret=TOKEN, purpose="invite", expires_at=EXPIRES
    )


async def test_outbox_line_holds_masked_recipient_subject_purpose_expiry_and_redacted_body(
    tmp_path: Path,
) -> None:
    path = tmp_path / "outbox.jsonl"
    await send_invite(RedactingOutboxMailSender(path, environment="test"))
    (entry,) = lines(path)
    assert entry["to"] == "a***@corp.example"
    assert entry["subject"] == "You have been invited"
    assert entry["purpose"] == "invite"
    assert datetime.fromisoformat(entry["expires_at"]) == EXPIRES
    assert entry["body"] == "Accept your invitation with this token (valid 7 days): [REDACTED]"


async def test_the_file_never_contains_the_raw_token_or_the_full_address(tmp_path: Path) -> None:
    path = tmp_path / "outbox.jsonl"
    await send_invite(RedactingOutboxMailSender(path, environment="test"))
    text = path.read_text(encoding="utf-8")
    assert TOKEN not in text
    assert ADDRESS not in text


async def test_every_occurrence_of_the_token_is_replaced(tmp_path: Path) -> None:
    path = tmp_path / "outbox.jsonl"
    body = f"Use {TOKEN} now. If that fails, paste {TOKEN} again."
    await send_invite(RedactingOutboxMailSender(path, environment="test"), body)
    assert TOKEN not in path.read_text(encoding="utf-8")
    assert lines(path)[0]["body"].count("[REDACTED]") == 2


async def test_messages_are_appended_one_json_object_per_line(tmp_path: Path) -> None:
    path = tmp_path / "nested" / "dir" / "outbox.jsonl"  # parent folders are created
    sender = RedactingOutboxMailSender(path, environment="development")
    await send_invite(sender)
    await sender.send(
        "bob@corp.example",
        "Reset",
        f"Reset with {TOKEN}",
        secret=TOKEN,
        purpose="reset",
        expires_at=EXPIRES + timedelta(minutes=60),
    )
    assert [e["purpose"] for e in lines(path)] == ["invite", "reset"]


async def test_a_message_without_options_is_still_written(tmp_path: Path) -> None:
    path = tmp_path / "outbox.jsonl"
    await RedactingOutboxMailSender(path, environment="test").send(ADDRESS, "Hello", "plain body")
    (entry,) = lines(path)
    assert entry["purpose"] is None and entry["expires_at"] is None
    assert entry["body"] == "plain body"


@pytest.mark.parametrize("environment", ["development", "test", "Test"])
def test_the_sender_can_be_built_in_a_development_environment(
    tmp_path: Path, environment: str
) -> None:
    RedactingOutboxMailSender(tmp_path / "o.jsonl", environment=environment)


@pytest.mark.parametrize("environment", ["production", "staging", "qa", ""])
def test_the_sender_refuses_to_be_built_outside_development(
    tmp_path: Path, environment: str
) -> None:
    with pytest.raises(RuntimeError, match="development"):
        RedactingOutboxMailSender(tmp_path / "o.jsonl", environment=environment)


async def test_the_log_gets_only_the_masked_recipient_and_the_subject(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG)
    await send_invite(RedactingOutboxMailSender(tmp_path / "o.jsonl", environment="test"))
    text = " ".join(r.getMessage() for r in caplog.records if r.name == "hds.mail")
    assert "a***@corp.example" in text
    assert "You have been invited" in text
    assert TOKEN not in text
    assert ADDRESS not in text
    assert "Accept your invitation" not in text  # the body is not logged at all


def test_the_factory_builds_the_outbox_sender_from_the_settings(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    path = tmp_path / "factory.jsonl"
    fake = SimpleNamespace(ENVIRONMENT="test", MAIL_BACKEND="outbox", MAIL_OUTBOX_PATH=str(path))
    monkeypatch.setattr("hds.core.mail.get_settings", lambda: fake, raising=False)
    sender = get_mail_sender()
    assert isinstance(sender, RedactingOutboxMailSender)
    assert sender.path == path


def test_the_factory_refuses_an_unknown_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = SimpleNamespace(ENVIRONMENT="production", MAIL_BACKEND="outbox", MAIL_OUTBOX_PATH="x")
    monkeypatch.setattr("hds.core.mail.get_settings", lambda: fake, raising=False)
    with pytest.raises(RuntimeError, match="MAIL_BACKEND"):
        get_mail_sender()
