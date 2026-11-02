"""T141: the request id is in a context variable, every log line and problem+json bodies."""

import json
import logging

import pytest
from fastapi.testclient import TestClient

from hds.core.logging import SensitiveDataFilter
from hds.core.request_context import get_request_id
from hds.main import create_app


def test_default_request_id_is_dash() -> None:
    assert get_request_id() == "-"


def test_problem_body_carries_the_response_request_id() -> None:
    client = TestClient(create_app())
    response = client.get("/me")  # unauthenticated -> problem+json
    assert response.status_code in (401, 403)
    assert response.json()["request_id"] == response.headers["X-Request-Id"]


def test_incoming_request_id_is_honoured_when_valid() -> None:
    client = TestClient(create_app())
    response = client.get("/health", headers={"X-Request-Id": "abc-123"})
    assert response.headers["X-Request-Id"] == "abc-123"


def test_invalid_incoming_request_id_is_replaced() -> None:
    client = TestClient(create_app())
    response = client.get("/health", headers={"X-Request-Id": "bad id\twith junk"})
    assert response.headers["X-Request-Id"] != "bad id\twith junk"


def test_log_lines_include_the_request_id(caplog: pytest.LogCaptureFixture) -> None:
    app = create_app()

    @app.get("/_log_probe", include_in_schema=False)
    async def probe() -> dict[str, str]:
        logging.getLogger("hds.probe").info("hello")
        return {"request_id": get_request_id()}

    client = TestClient(app)
    with caplog.at_level(logging.INFO, logger="hds.probe"):
        response = client.get("/_log_probe", headers={"X-Request-Id": "rid-42"})
    assert response.json()["request_id"] == "rid-42"
    records = [r for r in caplog.records if r.name == "hds.probe"]
    assert records and all(getattr(r, "request_id", None) == "rid-42" for r in records)


def test_filter_attaches_request_id_to_every_record() -> None:
    record = logging.LogRecord("x", logging.INFO, __file__, 1, "m", None, None)
    SensitiveDataFilter().filter(record)
    assert record.request_id == "-"  # type: ignore[attr-defined]
    json.dumps({"request_id": record.request_id})  # type: ignore[attr-defined]


def test_log_lines_are_valid_json() -> None:
    from hds.core.logging import JsonFormatter

    record = logging.LogRecord(
        "hds.mail", logging.INFO, __file__, 1, 'mail to=a@b subject="Hi" body=x\nnext', None, None
    )
    SensitiveDataFilter().filter(record)
    line = JsonFormatter().format(record)
    parsed = json.loads(line)
    assert parsed["logger"] == "hds.mail"
    assert parsed["level"] == "INFO"
    assert parsed["request_id"] == "-"
    assert 'subject="Hi"' in parsed["message"] and "\nnext" in parsed["message"]
    assert "time" in parsed


def test_log_json_includes_exception_text() -> None:
    from hds.core.logging import JsonFormatter

    try:
        raise ValueError("boom")
    except ValueError:
        import sys

        record = logging.LogRecord("x", logging.ERROR, __file__, 1, "failed", None, sys.exc_info())
    SensitiveDataFilter().filter(record)
    assert "ValueError: boom" in json.loads(JsonFormatter().format(record))["exception"]
