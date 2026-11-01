"""Outbound mail abstraction. Invitation and reset tokens are delivered only through this."""

import logging
from typing import Protocol

logger = logging.getLogger("hds.mail")


class MailSender(Protocol):
    async def send(self, to: str, subject: str, body: str) -> None: ...


class LoggingMailSender:
    """Development sender: writes the message to the application log instead of sending it."""

    async def send(self, to: str, subject: str, body: str) -> None:
        logger.info("mail to=%s subject=%s body=%s", to, subject, body)


def get_mail_sender() -> MailSender:
    return LoggingMailSender()
