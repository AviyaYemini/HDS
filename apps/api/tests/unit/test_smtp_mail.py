"""The SMTP mail backend delivers over STARTTLS only and never leaks a secret (006 T025).

Each test runs a real SMTP server (``aiosmtpd``) on a free local port. The server offers STARTTLS
with a throwaway certificate made by ``openssl`` in the test's temporary folder: it is never
written into the repository, and the sender trusts it through the test's own TLS context, so
certificate checks stay on. A server without STARTTLS is refused before anything is sent.
Connection and sign-in errors raise ``MailDeliveryError``, whose text and causes carry neither the
password nor the token, and no log line holds the token or the full address.

Covers: 006 research R9 (application mail); 002 FR-203 (tokens never in clear).
"""

import asyncio
import logging
import socket
import ssl
import subprocess
from collections.abc import Iterator
from dataclasses import dataclass, field
from email import message_from_bytes
from email.message import Message
from pathlib import Path
from typing import Any

import pytest
from aiosmtpd.controller import Controller
from aiosmtpd.smtp import AuthResult, LoginPassword
from pydantic import SecretStr

from hds.core.mail import MailDeliveryError, SmtpMailSender

TOKEN = "tok-9f8e7d6c5b4a39281706f5e4d3c2b1a0"
PASSWORD = "relay-password-4b1e9c2a7d"
RECIPIENT = "dana.levi@example.com"
SENDER = "HDS <no-reply@example.org>"
BODY = f"Reset your password: https://hds.example.org/reset?token={TOKEN}\n"


@dataclass
class Received:
    """What the test server saw: each message, and whether its session was encrypted."""

    messages: list[Message] = field(default_factory=list)
    envelopes: list[tuple[str, list[str]]] = field(default_factory=list)
    encrypted: list[bool] = field(default_factory=list)

    async def handle_DATA(self, server: Any, session: Any, envelope: Any) -> str:  # noqa: N802
        self.messages.append(message_from_bytes(envelope.content))
        self.envelopes.append((envelope.mail_from, list(envelope.rcpt_tos)))
        self.encrypted.append(session.ssl is not None)
        return "250 OK"


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port: int = probe.getsockname()[1]
        return port


@pytest.fixture(scope="module")
def certificate(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, Path]:
    """A throwaway self-signed certificate and key for 127.0.0.1, made now and never stored."""
    folder = tmp_path_factory.mktemp("throwaway-tls")
    cert, key = folder / "cert.pem", folder / "key.pem"
    subprocess.run(
        [
            "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
            "-subj", "/CN=127.0.0.1", "-addext", "subjectAltName=IP:127.0.0.1",
            "-keyout", str(key), "-out", str(cert),
        ],
        check=True,
        capture_output=True,
    )  # fmt: skip
    return cert, key


@pytest.fixture
def client_tls(certificate: tuple[Path, Path]) -> ssl.SSLContext:
    """A client context that trusts the throwaway certificate, with every check still on."""
    return ssl.create_default_context(cafile=str(certificate[0]))


def server_tls(certificate: tuple[Path, Path]) -> ssl.SSLContext:
    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    context.load_cert_chain(str(certificate[0]), str(certificate[1]))
    return context


def accept_only(username: str, password: str) -> Any:
    """An authenticator that accepts exactly one LOGIN or PLAIN pair."""

    def authenticate(server: Any, session: Any, envelope: Any, mechanism: str, data: Any) -> Any:
        ok = (
            isinstance(data, LoginPassword)
            and data.login.decode() == username
            and data.password.decode() == password
        )
        return AuthResult(success=ok, handled=False)

    return authenticate


@pytest.fixture
def tls_server(certificate: tuple[Path, Path]) -> Iterator[tuple[int, Received]]:
    """A server that offers STARTTLS and asks for a sign-in only after it."""
    received = Received()
    port = free_port()
    controller = Controller(
        received,
        hostname="127.0.0.1",
        port=port,
        tls_context=server_tls(certificate),
        require_starttls=True,
        authenticator=accept_only("hds", PASSWORD),
        auth_require_tls=True,
    )
    controller.start()
    yield port, received
    controller.stop()


@pytest.fixture
def plain_server() -> Iterator[tuple[int, Received]]:
    """A server that offers no STARTTLS at all."""
    received = Received()
    port = free_port()
    controller = Controller(received, hostname="127.0.0.1", port=port)
    controller.start()
    yield port, received
    controller.stop()


def sender(port: int, tls: ssl.SSLContext | None, *, password: str = PASSWORD) -> SmtpMailSender:
    return SmtpMailSender(
        host="127.0.0.1",
        port=port,
        mail_from=SENDER,
        username="hds",
        password=SecretStr(password),
        tls_context=tls,
    )


async def send(mailer: SmtpMailSender) -> None:
    await mailer.send(RECIPIENT, "Reset your password", BODY, secret=TOKEN, purpose="reset")


def every_text(error: BaseException) -> str:
    """The error's text and repr, and its causes' and contexts', all of it."""
    parts: list[str] = []
    seen: BaseException | None = error
    while seen is not None:
        parts += [str(seen), repr(seen)]
        seen = seen.__cause__ or seen.__context__
    return " ".join(parts)


# ── delivery ─────────────────────────────────────────────────────────────────────────────────────


async def test_one_message_is_delivered_over_starttls(
    tls_server: tuple[int, Received], client_tls: ssl.SSLContext
) -> None:
    port, received = tls_server
    await send(sender(port, client_tls))
    [message] = received.messages
    assert message["From"] == SENDER
    assert message["To"] == RECIPIENT
    assert message["Subject"] == "Reset your password"
    payload = message.get_payload(decode=True)
    assert isinstance(payload, bytes)
    # SMTP carries CRLF line ends; the text, link included, arrives whole.
    assert payload.decode().replace("\r\n", "\n") == BODY
    assert received.envelopes == [("no-reply@example.org", [RECIPIENT])]
    assert received.encrypted == [True]


async def test_the_server_s_certificate_is_checked(tls_server: tuple[int, Received]) -> None:
    # The sender's own default, the system's trusted certificates, does not trust the throwaway
    # one: no mail goes to a server that cannot prove who it is.
    port, received = tls_server
    with pytest.raises(MailDeliveryError, match="TLS"):
        await send(sender(port, None))
    assert received.messages == []


def test_making_a_sender_reads_no_certificates(monkeypatch: pytest.MonkeyPatch) -> None:
    # Review R24: a sender is made per request; the CA store is read in the delivery thread.
    def forbidden(*args: object, **kwargs: object) -> ssl.SSLContext:
        raise AssertionError("the CA store was read while making a sender")

    monkeypatch.setattr(ssl, "create_default_context", forbidden)
    SmtpMailSender("127.0.0.1", 25, SENDER)


async def test_with_starttls_off_a_local_relay_is_used_in_plain(
    tls_server: tuple[int, Received],
) -> None:
    # Review R26: development's local-relay mode does not upgrade, so a relay with a self-signed
    # certificate works. The test server insists on STARTTLS before AUTH, so this one has none.
    received = Received()
    port = free_port()
    controller = Controller(received, hostname="127.0.0.1", port=port)
    controller.start()
    try:
        plain = SmtpMailSender("127.0.0.1", port, SENDER, require_starttls=False)
        await send(plain)
    finally:
        controller.stop()
    assert received.encrypted == [False]


async def test_with_starttls_off_an_offered_upgrade_is_not_taken(
    tls_server: tuple[int, Received],
) -> None:
    # The TLS server offers STARTTLS with a certificate nobody trusts; off means it is not used.
    port, received = tls_server
    plain = SmtpMailSender("127.0.0.1", port, SENDER, require_starttls=False)
    with pytest.raises(MailDeliveryError) as caught:
        await send(plain)  # this server refuses mail before STARTTLS (require_starttls=True)
    assert "TLS check" not in str(caught.value)


# ── refusals ─────────────────────────────────────────────────────────────────────────────────────


async def test_a_server_without_starttls_is_refused_and_nothing_is_sent(
    plain_server: tuple[int, Received], client_tls: ssl.SSLContext
) -> None:
    port, received = plain_server
    with pytest.raises(MailDeliveryError, match="STARTTLS"):
        await send(sender(port, client_tls))
    assert received.messages == []


# ── errors ───────────────────────────────────────────────────────────────────────────────────────


async def test_a_refused_sign_in_raises_without_the_password_or_the_token(
    tls_server: tuple[int, Received], client_tls: ssl.SSLContext
) -> None:
    port, received = tls_server
    wrong = "wrong-relay-password-77c1"
    with pytest.raises(MailDeliveryError) as caught:
        await send(sender(port, client_tls, password=wrong))
    text = every_text(caught.value)
    for secret in (wrong, PASSWORD, TOKEN, RECIPIENT):
        assert secret not in text
    assert received.messages == []


async def test_a_server_that_cannot_be_reached_raises_a_mail_delivery_error(
    client_tls: ssl.SSLContext,
) -> None:
    with pytest.raises(MailDeliveryError) as caught:
        await send(sender(free_port(), client_tls))  # nothing listens there
    text = every_text(caught.value)
    assert TOKEN not in text
    assert PASSWORD not in text


def test_the_timeout_is_ten_seconds() -> None:
    assert sender(25, None).timeout == 10.0


def test_the_password_is_not_in_the_sender_s_repr() -> None:
    assert PASSWORD not in repr(sender(25, None))


# ── logs ─────────────────────────────────────────────────────────────────────────────────────────


async def test_no_log_line_holds_the_token_or_the_full_address(
    tls_server: tuple[int, Received],
    client_tls: ssl.SSLContext,
    caplog: pytest.LogCaptureFixture,
) -> None:
    port, _ = tls_server
    caplog.set_level(logging.DEBUG)
    await send(sender(port, client_tls))
    with pytest.raises(MailDeliveryError):
        await send(sender(port, client_tls, password="wrong-relay-password-77c1"))
    text = "\n".join(r.getMessage() for r in caplog.records)
    assert caplog.records  # a delivery is logged
    assert TOKEN not in text
    assert RECIPIENT not in text
    assert PASSWORD not in text


async def test_the_send_does_not_block_the_event_loop(
    tls_server: tuple[int, Received], client_tls: ssl.SSLContext
) -> None:
    # smtplib is blocking, so it runs in a thread: another task keeps running meanwhile.
    port, _ = tls_server
    ticks = 0

    async def tick() -> None:
        nonlocal ticks
        while True:
            ticks += 1
            await asyncio.sleep(0)

    ticker = asyncio.create_task(tick())
    try:
        await send(sender(port, client_tls))
    finally:
        ticker.cancel()
    assert ticks > 1
