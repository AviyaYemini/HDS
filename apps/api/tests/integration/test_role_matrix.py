"""T127: role matrix.

For every (principal, operation) pair the outcome must match the spec, whatever ids or
bodies the caller sends:

* a forbidden role is always ``403`` with a role-gate error code (never 200/404/422);
* an allowed role is never turned away by the role gate;
* anonymous callers get ``401`` on every protected operation.

Covers: FR-004, FR-007, FR-008, FR-009, FR-009a, FR-010, FR-016, SC-002, SC-004.
"""

import uuid
from typing import Any

from hypothesis import HealthCheck, example, given, settings
from hypothesis import strategies as st

from hds.main import app
from tests.conftest import RoleEnv

ANY = frozenset({"employee", "manager", "admin", "client"})
MGR = frozenset({"manager", "admin"})
ADMIN = frozenset({"admin"})
MGR_OR_CLIENT = frozenset({"manager", "admin", "client"})
STAFF = frozenset({"employee", "manager", "admin"})
PUBLIC = None  # no authentication required

E = "/api/v1/employees/{id}"
C = "/api/v1/clients/{id}"
P = "/api/v1/projects/{id}"

MATRIX: dict[tuple[str, str], frozenset[str] | None] = {
    # auth
    ("POST", "/auth/login"): PUBLIC,
    ("POST", "/auth/refresh"): PUBLIC,
    ("POST", "/auth/logout"): ANY,
    ("POST", "/auth/logout-all"): ANY,
    ("POST", "/auth/invite"): MGR,
    ("POST", "/auth/invite/accept"): PUBLIC,
    ("POST", "/auth/password-reset/request"): PUBLIC,
    ("POST", "/auth/password-reset/confirm"): PUBLIC,
    # own profile
    ("GET", "/me"): ANY,
    ("PATCH", "/me"): ANY,
    # sessions and the contact card (007 FR-009, FR-012)
    ("GET", "/me/sessions"): ANY,
    ("DELETE", "/me/sessions/{id}"): ANY,
    ("POST", "/auth/logout-others"): ANY,
    ("GET", "/api/v1/organisation/contact"): ANY,
    ("PATCH", "/api/v1/organisation/contact"): ADMIN,
    # availability submissions and one's own shift (007 FR-013 to FR-024)
    ("GET", "/me/availability-submissions"): STAFF,
    ("POST", "/me/availability-submissions"): STAFF,
    ("POST", "/api/v1/employees/{id}/availability-submissions"): ADMIN,
    ("GET", "/me/shifts/{id}"): STAFF,
    # employees (FR-008, FR-009, FR-016)
    ("GET", "/api/v1/employees"): MGR,
    ("POST", "/api/v1/employees"): MGR,
    ("GET", E): MGR,  # or self; a random id is never the caller
    ("PATCH", E): MGR,
    ("DELETE", E): ADMIN,
    ("POST", E + "/deactivate"): MGR,
    ("POST", E + "/reactivate"): MGR,
    ("GET", E + "/groups"): MGR,
    ("PUT", E + "/groups"): MGR,
    ("GET", E + "/qualifications"): MGR,
    ("PUT", E + "/qualifications"): MGR,
    # groups and qualifications
    ("GET", "/api/v1/groups"): MGR,
    ("POST", "/api/v1/groups"): MGR,
    ("PATCH", "/api/v1/groups/{id}"): MGR,
    ("DELETE", "/api/v1/groups/{id}"): MGR,
    ("GET", "/api/v1/qualifications"): MGR,
    ("POST", "/api/v1/qualifications"): MGR,
    ("PATCH", "/api/v1/qualifications/{id}"): MGR,
    ("DELETE", "/api/v1/qualifications/{id}"): MGR,
    # clients (FR-008, FR-009a)
    ("GET", "/api/v1/clients"): MGR,
    ("POST", "/api/v1/clients"): MGR,
    ("GET", C): MGR,
    ("PATCH", C): MGR,
    ("DELETE", C): ADMIN,
    ("GET", C + "/users"): MGR,
    ("POST", C + "/users"): MGR,
    ("DELETE", C + "/users/{user_id}"): MGR,
    # sites
    ("GET", "/api/v1/sites"): MGR,
    ("POST", "/api/v1/sites"): MGR,
    ("GET", "/api/v1/sites/{id}"): MGR,
    ("PATCH", "/api/v1/sites/{id}"): MGR,
    # projects (FR-008, FR-009, FR-010; Employees are denied project screens, US4 #6)
    ("GET", "/api/v1/projects"): MGR_OR_CLIENT,
    ("POST", "/api/v1/projects"): MGR,
    ("GET", P): MGR_OR_CLIENT,
    ("PATCH", P): MGR,
    ("DELETE", P): ADMIN,
    # availability (003 FR-105 to FR-109); whose records is decided by the service
    ("GET", "/api/v1/availability-windows"): STAFF,
    ("POST", "/api/v1/availability-windows"): MGR,
    ("PATCH", "/api/v1/availability-windows/{id}"): MGR,
    ("DELETE", "/api/v1/availability-windows/{id}"): MGR,
    ("GET", E + "/constraints"): STAFF,
    ("POST", E + "/constraints"): STAFF,
    ("PATCH", "/api/v1/constraints/{id}"): STAFF,
    ("DELETE", "/api/v1/constraints/{id}"): STAFF,
    # shift types (003 FR-110, FR-111)
    ("GET", "/api/v1/shift-types"): STAFF,
    ("POST", "/api/v1/shift-types"): MGR,
    ("PATCH", "/api/v1/shift-types/{id}"): MGR,
    ("GET", P + "/shift-types"): MGR,
    ("PUT", P + "/shift-types"): MGR,
    # staffing needs and the scheduling limits (004 FR-001 to FR-003, FR-026)
    ("GET", P + "/requirements"): MGR,
    ("PUT", P + "/requirements"): MGR,
    ("GET", "/api/v1/organisation/scheduling"): MGR,
    ("PATCH", "/api/v1/organisation/scheduling"): ADMIN,
    # schedule jobs (004 FR-004 to FR-027)
    ("POST", "/api/v1/schedule-jobs"): MGR,
    ("GET", "/api/v1/schedule-jobs"): MGR,
    ("GET", "/api/v1/schedule-jobs/{id}"): MGR,
    ("POST", "/api/v1/schedule-jobs/{id}/accept"): MGR,
    ("POST", "/api/v1/schedule-jobs/{id}/discard"): MGR,
    # pay settings, special days, managers on projects and pay to date (005 FR-001 to FR-022)
    ("GET", "/api/v1/organisation/pay-settings"): MGR,
    ("PATCH", "/api/v1/organisation/pay-settings"): ADMIN,
    ("GET", P + "/pay-settings"): MGR,
    ("PATCH", P + "/pay-settings"): ADMIN,
    ("GET", "/api/v1/special-days"): MGR,
    ("POST", "/api/v1/special-days"): ADMIN,
    ("PATCH", "/api/v1/special-days/{id}"): ADMIN,
    ("DELETE", "/api/v1/special-days/{id}"): ADMIN,
    ("GET", P + "/special-days"): MGR,
    ("POST", P + "/special-days"): ADMIN,
    ("GET", P + "/managers"): MGR,
    ("PUT", P + "/managers"): ADMIN,
    ("GET", "/me/pay"): STAFF,
    ("GET", "/api/v1/reports/work-log"): MGR,
    ("GET", "/api/v1/reports/hours"): MGR,
    # the manual roster (003 FR-113 to FR-120); managers edit their own projects' since 007
    # (FR-028): another project's answers roster.project_not_managed, which is not a role gate
    ("GET", "/api/v1/shifts"): MGR,
    ("POST", "/api/v1/shifts"): MGR,
    ("PATCH", "/api/v1/shifts/{id}"): MGR,
    ("DELETE", "/api/v1/shifts/{id}"): MGR,
    ("POST", "/api/v1/shifts/{id}/assignments"): MGR,
    ("PATCH", "/api/v1/assignments/{id}"): MGR,
    ("DELETE", "/api/v1/assignments/{id}"): MGR,
    ("POST", P + "/publish"): MGR,
    ("GET", "/me/shifts"): STAFF,
    # the roster tools (007 FR-034 to FR-040)
    ("POST", "/api/v1/assignments/{id}/overrides"): MGR,
    ("GET", P + "/publish-preview"): MGR,
    ("POST", P + "/copy-week"): MGR,
    ("GET", P + "/week-templates"): MGR,
    ("POST", P + "/week-templates"): MGR,
    ("DELETE", P + "/week-templates/{template_id}"): MGR,
    ("POST", P + "/week-templates/{template_id}/apply"): MGR,
    ("POST", P + "/roster/bulk"): MGR,
    ("GET", "/api/v1/availability-windows/{id}/submissions"): MGR,
    # saved views (007 FR-043)
    ("GET", "/me/saved-views"): STAFF,
    ("POST", "/me/saved-views"): STAFF,
    ("DELETE", "/me/saved-views/{id}"): STAFF,
}

ROLE_GATE_CODES = {"users.manager_required", "users.admin_required", "users.forbidden"}
PRINCIPALS = ["anonymous", "employee", "manager", "admin", "client"]
OPERATIONS = sorted(MATRIX)


def test_matrix_covers_every_operation_in_the_openapi_document() -> None:
    documented = {
        (method.upper(), path) for path, ops in app.openapi()["paths"].items() for method in ops
    }
    assert documented == set(MATRIX), {
        "undocumented_in_matrix": sorted(documented - set(MATRIX)),
        "stale_in_matrix": sorted(set(MATRIX) - documented),
    }


def _check(env: RoleEnv, principal: str, op: tuple[str, str], ids: Any, body: Any) -> None:
    method, path = op
    allowed = MATRIX[op]
    params = {"id": str(ids[0]), "user_id": str(ids[1]), "template_id": str(ids[0])}
    status, code = env.call(principal, method, path, body, params)
    where = f"{principal} {method} {path} -> {status} {code!r}"

    if principal == "anonymous":
        if allowed is PUBLIC:
            assert code != "auth.missing_token" and status != 403, where
        else:
            assert status == 401 and code == "auth.missing_token", where
    elif allowed is PUBLIC or principal in allowed:
        assert status != 401 and code not in ROLE_GATE_CODES, where
    else:
        assert status == 403 and code in ROLE_GATE_CODES, where


_json_body = st.one_of(
    st.none(),
    st.dictionaries(
        st.text(max_size=12),
        st.one_of(st.none(), st.booleans(), st.integers(), st.text(max_size=20)),
        max_size=6,
    ),
)


def _with_every_pair(test: Any) -> Any:
    """Force each (principal, operation) pair to run at least once, then fuzz the rest."""
    for principal in PRINCIPALS:
        for op in OPERATIONS:
            test = example(
                principal=principal,
                op=op,
                ids=(
                    uuid.UUID(int=1),
                    uuid.UUID(int=2),
                ),
                body={},
            )(test)
    return test


@_with_every_pair
@settings(
    max_examples=150,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture, HealthCheck.too_slow],
)
@given(
    principal=st.sampled_from(PRINCIPALS),
    op=st.sampled_from(OPERATIONS),
    ids=st.tuples(st.uuids(), st.uuids()),
    body=_json_body,
)
def test_role_matrix_holds_for_every_principal_and_operation(
    role_env: RoleEnv,
    principal: str,
    op: tuple[str, str],
    ids: Any,
    body: Any,
) -> None:
    _check(role_env, principal, op, ids, body)
