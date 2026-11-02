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


def configure_logging() -> None:
    handler = logging.StreamHandler()
    handler.addFilter(SensitiveDataFilter())
    logging.basicConfig(
        level=logging.INFO,
        format=(
            '{"time": "%(asctime)s", "level": "%(levelname)s", "request_id": "%(request_id)s", '
            '"logger": "%(name)s", "message": %(message)s}'
        ),
        handlers=[handler],
    )
