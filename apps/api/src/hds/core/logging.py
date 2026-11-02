"""Structured JSON logging with redaction of personal data.

Layer: core. Depends on ``hds.core.request_context`` only.

Logs go to stdout as one JSON object per line. Values of ``password``, ``token``, ``email``
and ``phone`` keys are masked before they leave the process, because logs are copied to
systems with weaker access control than the database.
"""

import json
import logging
import re

from hds.core.request_context import get_request_id

_SENSITIVE_FIELDS = re.compile(r'"(password|token|email|phone)":\s*"[^"]*"', re.IGNORECASE)
_REDACTED = r'"\1": "[REDACTED]"'


class SensitiveDataFilter(logging.Filter):
    """Mask sensitive JSON fields in log messages and stamp each record with the request id."""

    def filter(self, record: logging.LogRecord) -> bool:
        """Redact the record in place; always keep it.

        Only string messages are scanned; structured ``args`` are not, so callers must not
        pass secrets as format arguments.

        Args:
            record: The log record about to be emitted.

        Returns:
            Always ``True``: records are rewritten, never dropped.
        """
        record.request_id = get_request_id()
        if isinstance(record.msg, str):
            record.msg = _SENSITIVE_FIELDS.sub(_REDACTED, record.msg)
        return True


class JsonFormatter(logging.Formatter):
    """One valid JSON object per line, whatever the message contains."""

    def format(self, record: logging.LogRecord) -> str:
        """Render ``record`` as a single-line JSON document.

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
            entry["exception"] = self.formatException(record.exc_info)
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
