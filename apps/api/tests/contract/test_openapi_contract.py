"""T128: Schemathesis contract tests as a pytest module (Constitution V).

Every operation in the app's OpenAPI document is exercised with generated inputs against the
ASGI app on the PostgreSQL test database, as an Admin. All built-in checks must pass:
no 5xx, documented status codes, response schema and content-type conformance, etc.

Covers: FR-028 (invalid input is a validation error, never a server error); Constitution IV
    and V.
"""

import json
import os
from collections.abc import Iterator
from pathlib import Path

import pytest
import schemathesis
from hypothesis import settings
from schemathesis import checks as st_checks

from hds.core.config import get_settings
from hds.main import app
from tests.conftest import RoleEnv

st_checks.load_all_checks()
ALL_CHECKS = st_checks.CHECKS.get_all()

# Operations on which a schema-valid request is correctly refused (a date order or a time zone,
# neither of which an OpenAPI 3.0 schema can state) skip only "valid data must be accepted"; every
# other check still runs on them. The list lives in one file, unprovable_operations.json, which the
# command-line script scripts/contract_cli.sh reads too, so the two cannot drift apart.
UNPROVABLE = json.loads(
    (Path(__file__).parent / "unprovable_operations.json").read_text(encoding="utf-8")
)["operations"]
CROSS_FIELD_RULE = {(op["method"], op["path"]) for op in UNPROVABLE if "pytest" in op["suites"]}

# Few examples locally for a fast loop; CI sets HDS_SCHEMATHESIS_EXAMPLES=100 for the full run.
EXAMPLES = int(os.environ.get("HDS_SCHEMATHESIS_EXAMPLES", "5"))

schema = schemathesis.openapi.from_asgi("/api/v1/openapi.json", app)


@pytest.fixture(scope="module", autouse=True)
def _database(role_env: RoleEnv) -> Iterator[RoleEnv]:
    # Generated requests can repeat an address, and 423 is not an answer the "valid data" check
    # accepts; login is exempt from that check only in the command-line suite.
    settings = get_settings()
    limit = settings.LOGIN_MAX_FAILURES
    settings.LOGIN_MAX_FAILURES = 1_000_000
    yield role_env
    settings.LOGIN_MAX_FAILURES = limit


@schema.parametrize()
@settings(max_examples=EXAMPLES, deadline=None)
def test_api_conforms_to_its_openapi_contract(case, role_env: RoleEnv) -> None:  # type: ignore[no-untyped-def]
    skip_acceptance = (case.method.upper(), case.operation.path) in CROSS_FIELD_RULE
    checks = [
        check
        for check in ALL_CHECKS
        if not (skip_acceptance and check.__name__ == "positive_data_acceptance")
    ]
    case.call_and_validate(
        headers={"Authorization": f"Bearer {role_env.tokens['admin']}"}, checks=checks
    )


def test_every_listed_operation_matches_the_openapi_document() -> None:
    """The shared list must name real operations, with the method, path and id the app has."""
    document = app.openapi()
    for op in UNPROVABLE:
        declared = document["paths"][op["path"]][op["method"].lower()]
        assert declared["operationId"] == op["operation_id"], op
        assert op["suites"] and set(op["suites"]) <= {"pytest", "cli"}, op
        assert op["reason"], op
