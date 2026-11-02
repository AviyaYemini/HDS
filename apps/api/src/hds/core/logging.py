"""Structured JSON logging with redaction of secrets and personal data.

Layer: core. Depends on ``hds.core.request_context`` only.

Logs go to stdout as one JSON object per line. Redaction runs on the final text of a record, after
the format arguments have been substituted, so it does not matter whether a secret arrived in the
message template, in a ``%s`` argument, inside a dict's repr or in an exception message. Values of
keys named like ``password``, ``token`` or ``secret`` (including compound names such as
``refresh_token``), ``email`` and ``phone`` are replaced with ``[REDACTED]``, ``key=value`` pairs of
the secret kind likewise, and any other email address is masked as ``a***@domain``. Logs are
copied to systems with weaker access control than the database, and invitation or reset tokens in
them would let a reader take over an account (specs/002-security-hardening, FR-203).
"""

import json
import logging
import re

from hds.core.request_context import get_request_id

# "key": "value" and 'key': 'value' (JSON and Python repr), for sensitive key names.
_SENSITIVE_PAIR = re.compile(
    r"""(["'])(\w*password|\w*token|\w*secret|email|phone)\1\s*:\s*(["'])(.*?)\3""",
    re.IGNORECASE,
)
# key=value text, for the secret kinds only.
_SECRET_ASSIGNMENT = re.compile(
    r"""\b(\w*password|\w*token|\w*secret)=([^\s&"',;]+)""", re.IGNORECASE
)
_EMAIL = re.compile(r"([\w.%+-])[\w.%+-]*@([\w-]+(?:\.[\w-]+)+)")
_WHOLE_EMAIL = re.compile(r"^[\w.%+-]+@[\w-]+(?:\.[\w-]+)+$")
_REDACTED_PAIR = r'"\2": "[REDACTED]"'
_REDACTED_ASSIGNMENT = r"\1=[REDACTED]"
_MASKED_EMAIL = r"\1***@\2"


def mask_email(address: str) -> str:
    """Mask an email address as its first character plus ``***`` and the domain.

    The first letter keeps a masked address recognisable to the person who owns it without
    identifying them to someone reading the log.

    Args:
        address: A complete email address.

    Returns:
        ``a***@domain`` for an address; the input unchanged if it is not an address.
    """
    if not _WHOLE_EMAIL.match(address):
        return address
    return _EMAIL.sub(_MASKED_EMAIL, address)


def redact(text: str) -> str:
    """Remove secrets and mask email addresses in ``text``.

    Applying it again to its own output changes nothing.

    Args:
        text: Any text that may end up in a log.

    Returns:
        The text with sensitive values replaced by ``[REDACTED]`` and email addresses masked.
    """
    text = _SENSITIVE_PAIR.sub(_REDACTED_PAIR, text)
    text = _SECRET_ASSIGNMENT.sub(_REDACTED_ASSIGNMENT, text)
    return _EMAIL.sub(_MASKED_EMAIL, text)


class SensitiveDataFilter(logging.Filter):
    """Redact every record's final message and stamp it with the request id."""

    def filter(self, record: logging.LogRecord) -> bool:
        """Rewrite the record so its text is redacted; always keep it.

        The message is formatted first, with its arguments, and then redacted as a whole. The
        arguments are cleared afterwards so no later handler can rebuild the unredacted text. A
        message whose arguments do not fit its template is kept as its template text instead of
        raising.

        Args:
            record: The log record about to be emitted.

        Returns:
            Always ``True``: records are rewritten, never dropped.
        """
        record.request_id = get_request_id()
        try:
            text = record.getMessage()
        except (TypeError, ValueError, KeyError):
            text = str(record.msg)
        record.msg = redact(text)
        record.args = None
        return True


class JsonFormatter(logging.Formatter):
    """One valid JSON object per line, whatever the message contains."""

    def format(self, record: logging.LogRecord) -> str:
        """Render ``record`` as a single-line JSON document.

        The exception text is redacted here as well, because an exception message can carry a
        token or an address just like a log message.

        Args:
            record: The log record to render.

        Returns:
            JSON text with time, level, request id, logger, message and, when present, the
            formatted exception.
        """
        entry = {
            "time": self.formatTime(record),
            "level": record.levelname,
            "request_id": getattr(record, "request_id", "-"),
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            entry["exception"] = redact(self.formatException(record.exc_info))
        return json.dumps(entry, ensure_ascii=False)


def configure_logging() -> None:
    """Install the JSON formatter and redaction filter on the root logger at INFO level.

    Side effects:
        Calls ``logging.basicConfig``; it is a no-op if the root logger already has handlers.
    """
    handler = logging.StreamHandler()
    handler.addFilter(SensitiveDataFilter())
    handler.setFormatter(JsonFormatter())
    logging.basicConfig(level=logging.INFO, handlers=[handler])
