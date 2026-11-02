"""Unit tests for DeferredMailer (T205, research D1).

A service asks to send an email while its transaction is still open. The message must go out
only once the change is saved, exactly once, and never when the change is rolled back.

Covers: FR-210.
"""

from datetime import UTC, datetime

import pytest

from hds.core.database import unit_of_work
from hds.core.mail import DeferredMailer


class RecordingMailer:
    """A MailSender that records what it was asked to deliver."""

    def __init__(self) -> None:
        self.sent: list[tuple[str, str, str]] = []
        self.options: list[dict[str, object]] = []

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
        self.sent.append((to, subject, body))
        self.options.append({"secret": secret, "purpose": purpose, "expires_at": expires_at})


async def test_send_only_queues(fake_session) -> None:
    inner = RecordingMailer()
    mailer = DeferredMailer(fake_session, inner)
    await mailer.send("a@hds.test", "Hello", "body")
    assert inner.sent == []
    assert fake_session.events == []


async def test_message_is_delivered_once_after_commit(fake_session) -> None:
    inner = RecordingMailer()
    mailer = DeferredMailer(fake_session, inner)
    async with unit_of_work(fake_session):
        await mailer.send("a@hds.test", "Hello", "body")
        assert inner.sent == []
    assert inner.sent == [("a@hds.test", "Hello", "body")]


async def test_message_is_discarded_on_rollback(fake_session) -> None:
    inner = RecordingMailer()
    mailer = DeferredMailer(fake_session, inner)
    with pytest.raises(ValueError):
        async with unit_of_work(fake_session):
            await mailer.send("a@hds.test", "Hello", "body")
            raise ValueError("step failed")
    assert inner.sent == []


async def test_several_messages_keep_their_order(fake_session) -> None:
    inner = RecordingMailer()
    mailer = DeferredMailer(fake_session, inner)
    async with unit_of_work(fake_session):
        await mailer.send("a@hds.test", "One", "1")
        await mailer.send("b@hds.test", "Two", "2")
    assert [m[1] for m in inner.sent] == ["One", "Two"]


async def test_options_are_forwarded_to_the_real_sender(fake_session) -> None:
    """The real sender needs the secret, purpose and expiry to redact and describe the message."""
    inner = RecordingMailer()
    mailer = DeferredMailer(fake_session, inner)
    expires = datetime(2026, 11, 9, tzinfo=UTC)
    async with unit_of_work(fake_session):
        await mailer.send(
            "a@hds.test", "Hello", "body tok", secret="tok", purpose="invite", expires_at=expires
        )
    assert inner.options == [{"secret": "tok", "purpose": "invite", "expires_at": expires}]
