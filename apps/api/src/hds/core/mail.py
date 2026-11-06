"""Outbound mail abstraction. Invitation and reset tokens are delivered only through this.

Layer: core. Imports ``hds.core.config``, ``hds.core.database`` (for the after-commit queue) and
``hds.core.logging`` (for masking); services depend on the ``MailSender`` protocol so a real
provider can replace the development sender without touching business code.

A message that carries a one-time token is a copy of an account-access link, so nothing in this
module stores or logs one in clear (specs/002-security-hardening, FR-203). Two senders exist: the
development outbox, which writes a redacted record per message, and ``SmtpMailSender``, the real
backend, which delivers through a relay over STARTTLS (specs/006 research R9).
"""

import asyncio
import json
import logging
import smtplib
import ssl
from datetime import datetime
from email.message import EmailMessage
from email.utils import parseaddr
from pathlib import Path
from typing import Protocol

from pydantic import SecretStr
from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.config import DEVELOPMENT_ENVIRONMENTS, OUTBOX_BACKEND, SMTP_BACKEND, get_settings
from hds.core.database import after_commit
from hds.core.logging import mask_email, redact

logger = logging.getLogger("hds.mail")

#: What replaces the token in a stored message body.
REDACTED = "[REDACTED]"


class MailSender(Protocol):
    """Contract for anything that can deliver a message to one recipient."""

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
        """Deliver one message.

        Args:
            to: Recipient email address.
            subject: Subject line.
            body: Plain-text body; may contain a one-time link or token.
            secret: The one-time token inside ``body``, if any. A sender that stores or logs
                messages MUST replace it, because it cannot recognise a bare token on its own.
            purpose: What the message is for, such as ``invite`` or ``reset``.
            expires_at: When the token in the message stops working.
        """
        ...


class RedactingOutboxMailSender:
    """Development sender: appends a redacted record of each message to a local file.

    Each line is a JSON object with the masked recipient, subject, purpose, expiry and the body
    with the token replaced by ``[REDACTED]``. The file therefore proves a message was produced
    and to whom, but cannot be used to take over an account. To obtain a working link for a manual
    test, use ``hds.scripts.dev_issue_link`` instead.

    Attributes:
        path: The file the records are appended to.
    """

    def __init__(self, path: Path | str, environment: str) -> None:
        """Create the sender, refusing to exist outside a development environment.

        Args:
            path: File to append to; missing folders are created on the first message.
            environment: The value of ``ENVIRONMENT``.

        Raises:
            RuntimeError: If ``environment`` is not ``development`` or ``test``.
        """
        if environment.lower() not in DEVELOPMENT_ENVIRONMENTS:
            raise RuntimeError(
                "The outbox mail backend (MAIL_BACKEND=outbox) is for development only: "
                "ENVIRONMENT must be development or test"
            )
        self.path = Path(path)

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
        """Record the message with its token redacted and the recipient masked.

        Side effects:
            Appends one line to the outbox file (creating its folder) and writes one log line
            holding only the masked recipient and the subject, never the body.

        Args:
            to: Recipient email address; stored masked.
            subject: Subject line.
            body: Plain-text body; every occurrence of ``secret`` is replaced.
            secret: The one-time token inside ``body``.
            purpose: What the message is for, such as ``invite`` or ``reset``.
            expires_at: When the token stops working; stored as an ISO 8601 string.
        """
        stored_body = body.replace(secret, REDACTED) if secret else body
        entry = {
            "to": mask_email(to),
            "subject": subject,
            "purpose": purpose,
            "expires_at": expires_at.isoformat() if expires_at else None,
            "body": redact(stored_body),
        }
        line = json.dumps(entry, ensure_ascii=False) + "\n"
        await asyncio.to_thread(self._append, line)
        logger.info("mail recorded to=%s subject=%s", mask_email(to), subject)

    def _append(self, line: str) -> None:
        """Append one line to the outbox file, creating its folder first."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as outbox:
            outbox.write(line)


class MailDeliveryError(Exception):
    """A message could not be delivered. Its text names the kind of failure, never a secret.

    It is raised without its cause: a library error can quote the server's reply or the
    conversation, and neither the relay password nor a token may reach a log through it.
    """


#: How long one SMTP connection may take to answer, in seconds (specs/006 T026).
SMTP_TIMEOUT_SECONDS = 10


class SmtpMailSender:
    """The real backend: delivers each message through an SMTP relay, over STARTTLS.

    ``smtplib`` blocks, so each delivery runs in a thread and the event loop keeps serving. The
    connection is upgraded with STARTTLS before the sign-in and the message; a relay that does not
    offer it is refused before anything is sent, and the relay's certificate is checked. With
    ``require_starttls`` false (development only, which the settings enforce) the connection stays
    plain, for a local relay. The timeout applies to each step of the conversation; port 465
    (TLS from the first byte) is not supported.

    Attributes:
        timeout: Seconds one connection may take to answer.
    """

    def __init__(
        self,
        host: str,
        port: int,
        mail_from: str,
        *,
        username: str | None = None,
        password: SecretStr | None = None,
        require_starttls: bool = True,
        tls_context: ssl.SSLContext | None = None,
        timeout: int = SMTP_TIMEOUT_SECONDS,
    ) -> None:
        """Configure the relay.

        Args:
            host: The relay's host name.
            port: Its port; 587 is submission with STARTTLS.
            mail_from: The sender, as ``Name <address>`` or a bare address.
            username: The sign-in name; without one the sender does not sign in.
            password: The sign-in password, kept as a secret.
            require_starttls: Refuse a relay that does not offer STARTTLS.
            tls_context: The TLS settings; the system's trusted certificates by default.
            timeout: Seconds one connection may take to answer.
        """
        self._host = host
        self._port = port
        self._mail_from = mail_from
        self._username = username
        self._password = password
        self._require_starttls = require_starttls
        # Built in the delivery thread on first use: a sender is made per request, and reading the
        # system's CA store on the event loop would cost every request (review R24).
        self._tls = tls_context
        self.timeout = timeout

    def __repr__(self) -> str:
        """The relay and sender, never the password."""
        return f"SmtpMailSender(host={self._host!r}, port={self._port}, from={self._mail_from!r})"

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
        """Deliver one message to ``to``; the body goes as written, its link working.

        Side effects:
            Opens one connection to the relay, in a thread, and writes one log line with the
            masked recipient and the subject, never the body or the token.

        Args:
            to: Recipient email address.
            subject: Subject line.
            body: Plain-text body; may contain a one-time link or token.
            secret: The one-time token inside ``body``; it is delivered, never logged.
            purpose: What the message is for, such as ``invite`` or ``reset``.
            expires_at: When the token stops working.

        Raises:
            MailDeliveryError: The relay could not be reached, did not offer STARTTLS, failed the
                certificate check, refused the sign-in, or refused the message.
        """
        message = EmailMessage()
        message["From"] = self._mail_from
        message["To"] = to
        message["Subject"] = subject
        message.set_content(body)
        await asyncio.to_thread(self._deliver, message)
        logger.info("mail sent to=%s subject=%s purpose=%s", mask_email(to), subject, purpose)

    def _deliver(self, message: EmailMessage) -> None:
        """Connect, upgrade to TLS, sign in and send ``message``, or raise MailDeliveryError."""
        try:
            with smtplib.SMTP(self._host, self._port, timeout=self.timeout) as relay:
                relay.ehlo()
                # Off (a local relay, development only) means plain: an offered upgrade is not
                # taken, so a relay with a self-signed certificate still works (review R26).
                if self._require_starttls:
                    if not relay.has_extn("starttls"):
                        raise MailDeliveryError(
                            "the mail relay does not offer STARTTLS; nothing was sent"
                        )
                    if self._tls is None:
                        self._tls = ssl.create_default_context()
                    relay.starttls(context=self._tls)
                    relay.ehlo()
                if self._username:
                    secret = self._password.get_secret_value() if self._password else ""
                    relay.login(self._username, secret)
                relay.send_message(message, from_addr=parseaddr(self._mail_from)[1])
        except MailDeliveryError:
            raise
        except smtplib.SMTPAuthenticationError:
            raise MailDeliveryError("the mail relay refused the sign-in") from None
        except ssl.SSLError:
            raise MailDeliveryError("the mail relay failed the TLS check") from None
        except (OSError, smtplib.SMTPException) as error:
            raise MailDeliveryError(f"mail delivery failed ({type(error).__name__})") from None


class DeferredMailer:
    """A `MailSender` that sends only after the surrounding transaction has committed.

    Services use this so that an email carrying a link is never sent for a change that was then
    rolled back (a link to an account change that does not exist). ``send`` only queues the
    message on the session's unit of work; if the transaction rolls back the message is dropped.
    """

    def __init__(self, session: AsyncSession, inner: MailSender) -> None:
        """Wrap a real sender.

        Args:
            session: The request's session, whose unit of work decides when to send.
            inner: The sender that actually delivers the message after the commit.
        """
        self._session = session
        self._inner = inner

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
        """Queue one message for delivery after the commit.

        Side effects:
            Appends to the session's after-commit queue. A delivery failure after the commit is
            logged by the unit of work and does not undo the saved change.

        Args:
            to: Recipient email address.
            subject: Subject line.
            body: Plain-text body; may contain a one-time link or token.
            secret: The one-time token inside ``body``, passed on so the real sender can redact it.
            purpose: What the message is for, passed on to the real sender.
            expires_at: When the token stops working, passed on to the real sender.
        """
        after_commit(
            self._session,
            lambda: self._inner.send(
                to, subject, body, secret=secret, purpose=purpose, expires_at=expires_at
            ),
        )


def get_mail_sender() -> MailSender:
    """Return the sender the application uses (dependency-override point for tests).

    The backend comes from the ``MAIL_BACKEND`` setting. The settings already refuse to start in
    a non-development environment without a real backend, so reaching the outbox here outside
    development is a configuration error and the sender itself refuses.

    Returns:
        ``SmtpMailSender`` for ``MAIL_BACKEND=smtp``, else the development
        `RedactingOutboxMailSender`.

    Raises:
        RuntimeError: If ``MAIL_BACKEND`` names no available backend, or the outbox is requested
            outside a development environment.
    """
    settings = get_settings()
    if settings.MAIL_BACKEND == SMTP_BACKEND:
        return SmtpMailSender(
            settings.SMTP_HOST or "",
            settings.SMTP_PORT,
            settings.MAIL_FROM or "",
            username=settings.SMTP_USERNAME,
            password=settings.SMTP_PASSWORD,
            require_starttls=settings.SMTP_STARTTLS,
        )
    if settings.MAIL_BACKEND == OUTBOX_BACKEND:
        return RedactingOutboxMailSender(settings.MAIL_OUTBOX_PATH, settings.ENVIRONMENT)
    raise RuntimeError("MAIL_BACKEND does not name an available mail backend")
