"""The six operations that hash a password say that they can answer 503 (T349, FR-222).

Declaring the response alone is not enough: a reader of the operation's description has to learn
that a request can be refused because the queue is full, and what to do.

Covers: FR-222.
"""

from typing import Any

import pytest

from hds.main import app

OPERATIONS = [
    "/auth/login",
    "/auth/invite",
    "/auth/invite/accept",
    "/auth/password-reset/confirm",
    "/api/v1/employees",
    "/api/v1/clients/{id}/users",
]


@pytest.mark.parametrize("path", OPERATIONS)
def test_the_response_is_declared_and_the_description_explains_it(path: str) -> None:
    operation: dict[str, Any] = app.openapi()["paths"][path]["post"]

    assert "503" in operation["responses"]
    assert "503 (`service.busy`)" in operation["description"]
    assert "`Retry-After`" in operation["description"]
