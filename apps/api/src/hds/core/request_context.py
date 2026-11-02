"""Per-request id, held in a context variable so logs and error bodies can read it.

Layer: core. Imports nothing from other ``hds`` packages. The request-id middleware in
``hds.main`` is the only writer; everything else reads through ``get_request_id``.
"""

import re
import uuid
from contextvars import ContextVar

_request_id: ContextVar[str] = ContextVar("request_id", default="-")
_VALID = re.compile(r"^[A-Za-z0-9._-]{1,64}$")


def get_request_id() -> str:
    """Return the id of the request being handled, or ``-`` outside a request."""
    return _request_id.get()


def new_request_id(incoming: str | None) -> str:
    """Honour a well-formed incoming id; otherwise mint one (never trust arbitrary input)."""
    if incoming and _VALID.match(incoming):
        return incoming
    return str(uuid.uuid4())


def set_request_id(value: str) -> object:
    """Bind ``value`` as the current request id.

    Args:
        value: The validated id from ``new_request_id``.

    Returns:
        An opaque token to pass to ``reset_request_id`` when the request ends.
    """
    return _request_id.set(value)


def reset_request_id(token: object) -> None:
    """Restore the request id that was current before ``set_request_id``.

    Args:
        token: The value returned by ``set_request_id``.
    """
    _request_id.reset(token)  # type: ignore[arg-type]
