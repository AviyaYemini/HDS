"""Outbound mail abstraction. Invitation and reset tokens are delivered only through this.

Layer: core. Imports nothing from other ``hds`` packages; services depend on the
``MailSender`` protocol so a real provider can replace the development sender without touching
business code.
"""

import logging
from typing import Protocol

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


def get_mail_sender() -> MailSender:
    """Return the sender the application uses (dependency-override point for tests).

    Returns:
        The development ``LoggingMailSender``.
    """
    return LoggingMailSender()
