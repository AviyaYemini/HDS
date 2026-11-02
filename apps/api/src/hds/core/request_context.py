"""Per-request id, held in a context variable so logs and error bodies can read it."""

import re
import uuid
from contextvars import ContextVar

_request_id: ContextVar[str] = ContextVar("request_id", default="-")
_VALID = re.compile(r"^[A-Za-z0-9._-]{1,64}$")


def get_request_id() -> str:
    return _request_id.get()


def new_request_id(incoming: str | None) -> str:
    """Honour a well-formed incoming id; otherwise mint one (never trust arbitrary input)."""
    if incoming and _VALID.match(incoming):
        return incoming
    return str(uuid.uuid4())


def set_request_id(value: str) -> object:
    return _request_id.set(value)


def reset_request_id(token: object) -> None:
    _request_id.reset(token)  # type: ignore[arg-type]
