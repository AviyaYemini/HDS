"""T128: Schemathesis contract tests as a pytest module (Constitution V).

Every operation in the app's OpenAPI document is exercised with generated inputs against the
ASGI app on the PostgreSQL test database, as an Admin. All built-in checks must pass:
no 5xx, documented status codes, response schema and content-type conformance, etc.
"""

import pytest
import schemathesis
from schemathesis import checks as st_checks

from hds.main import app
from tests.conftest import RoleEnv

st_checks.load_all_checks()
ALL_CHECKS = st_checks.CHECKS.get_all()

# `end_date >= start_date` (FR-028) spans two fields, which an OpenAPI 3.0 schema cannot state, so
# schema-valid dates in the wrong order are correctly answered with 422
# `projects.date_range_invalid`. Only "valid data must be accepted" is skipped, only for these two
# operations; every other check still runs on them.
CROSS_FIELD_RULE = {("POST", "/api/v1/projects"), ("PATCH", "/api/v1/projects/{id}")}

schema = schemathesis.openapi.from_asgi("/api/v1/openapi.json", app)


@pytest.fixture(scope="module", autouse=True)
def _database(role_env: RoleEnv) -> RoleEnv:
    return role_env


@schema.parametrize()
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
