import json
import logging
import re

from hds.core.request_context import get_request_id

_SENSITIVE_FIELDS = re.compile(r'"(password|token|email|phone)":\s*"[^"]*"', re.IGNORECASE)
_REDACTED = r'"\1": "[REDACTED]"'


class SensitiveDataFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = get_request_id()
        if isinstance(record.msg, str):
            record.msg = _SENSITIVE_FIELDS.sub(_REDACTED, record.msg)
        return True


class JsonFormatter(logging.Formatter):
    """One valid JSON object per line, whatever the message contains."""

    def format(self, record: logging.LogRecord) -> str:
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
    handler = logging.StreamHandler()
    handler.addFilter(SensitiveDataFilter())
    handler.setFormatter(JsonFormatter())
    logging.basicConfig(level=logging.INFO, handlers=[handler])
