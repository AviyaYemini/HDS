"""Call the application directly as an ASGI callable and record what it sends, and when.

Layer: tests. The httpx test client waits for the whole application, background tasks included,
before it returns a response. A test about what happens after the response cannot use it: a blocking
sender would hang it, and a timing would include the work that was moved out of the request.

Covers: none directly; helper for FR-228 and SC-212.
"""

import asyncio
import json
import time
from collections.abc import MutableMapping
from typing import Any

from hds.main import app

Log = list[Any]


async def post(
    path: str, payload: dict[str, str], log: Log, headers: dict[str, str] | None = None
) -> None:
    """Send one POST through the whole application, appending every message to ``log``.

    Args:
        path: The route.
        payload: The JSON body.
        log: Receives each ASGI message, with the time it was sent under ``at``.
        headers: Extra request headers.
    """
    body = json.dumps(payload).encode()
    extra = [(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()]
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "root_path": "",
        "query_string": b"",
        "headers": [
            (b"host", b"testserver"),
            (b"content-type", b"application/json"),
            (b"content-length", str(len(body)).encode()),
            *extra,
        ],
        "client": ("127.0.0.1", 50000),
        "server": ("testserver", 80),
    }
    delivered = False

    async def receive() -> dict[str, Any]:
        nonlocal delivered
        if not delivered:
            delivered = True
            return {"type": "http.request", "body": body, "more_body": False}
        await asyncio.Event().wait()  # the client stays connected
        raise AssertionError("unreachable")

    async def send(message: MutableMapping[str, Any]) -> None:
        log.append({**message, "at": time.perf_counter()})

    await app(scope, receive, send)


def status(log: Log) -> int:
    """Return the status the application answered with."""
    return int(next(m["status"] for m in log if isinstance(m, dict) and "status" in m))


def response_end(log: Log) -> int:
    """Return the position of the message that completes the response."""
    return next(
        index
        for index, message in enumerate(log)
        if isinstance(message, dict)
        and message["type"] == "http.response.body"
        and not message.get("more_body", False)
    )
