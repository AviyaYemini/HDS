"""Outbound mail abstraction. Invitation and reset tokens are delivered only through this.

Layer: core. Imports only ``hds.core.database`` (for the after-commit queue); services depend on
the ``MailSender`` protocol so a real provider can replace the development sender without
touching business code.
"""

import logging
from typing import Protocol

from sqlalchemy.ext.asyncio import AsyncSession

from hds.core.database import after_commit

logger = logging.getLogger("hds.mail")


class MailSender(Protocol):
    """Contract for anything that can deliver a message to one recipient."""

    async def send(self, to: str, subject: str, body: str) -> None:
        """Deliver one message.

        Args:
            to: Recipient email address.
            subject: Subject line.
            body: Plain-text body; may contain a one-time link or token.
        """
        ...


class LoggingMailSender:
    """Development sender: writes the message to the application log instead of sending it."""

    async def send(self, to: str, subject: str, body: str) -> None:
        """Log the message instead of sending it.

        Side effects:
            Writes the body, including any token, to the application log. Acceptable only
            because this sender exists for development; production must provide a real one.

        Args:
            to: Recipient email address.
            subject: Subject line.
            body: Plain-text body.
        """
        logger.info("mail to=%s subject=%s body=%s", to, subject, body)


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

    async def send(self, to: str, subject: str, body: str) -> None:
        """Queue one message for delivery after the commit.

        Side effects:
            Appends to the session's after-commit queue. A delivery failure after the commit is
            logged by the unit of work and does not undo the saved change.

        Args:
            to: Recipient email address.
            subject: Subject line.
            body: Plain-text body; may contain a one-time link or token.
        """
        after_commit(self._session, lambda: self._inner.send(to, subject, body))


def get_mail_sender() -> MailSender:
    """Return the sender the application uses (dependency-override point for tests).

    Returns:
        The development ``LoggingMailSender``.
    """
    return LoggingMailSender()
