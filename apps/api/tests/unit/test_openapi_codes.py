"""Each operation declares only the error codes it can answer (T411).

The rules in ``core/openapi.py`` used to add 409 to every write and 403 to every signed-in route, so
the document promised answers nothing could give. A client generated from it would carry branches
that never run. The conflict cases read the service's own code, so a wrong entry cannot pass by
copying the same list here.

Covers: no spec requirement; guards Constitution V (API-first).
"""

import ast
import inspect
from pathlib import Path
from typing import Any

import pytest

from hds.main import app
from hds.services.auth import AuthService
from hds.services.client import ClientService
from hds.services.employee import EmployeeService
from hds.services.group import GroupService
from hds.services.invitation import InvitationService
from hds.services.project import ProjectService
from hds.services.qualification import QualificationService
from hds.services.site import SiteService

# (path, method, the service method behind it)
NO_CONFLICT = [
    ("/auth/logout", "post", (AuthService, "logout")),
    ("/auth/logout-all", "post", (AuthService, "logout_all")),
    ("/me", "patch", (EmployeeService, "update_own_profile")),
    ("/api/v1/employees/{id}/groups", "put", (GroupService, "replace_employee_groups")),
    (
        "/api/v1/employees/{id}/qualifications",
        "put",
        (QualificationService, "replace_employee_qualifications"),
    ),
    ("/api/v1/groups/{id}", "delete", (GroupService, "delete_group")),
    ("/api/v1/qualifications/{id}", "delete", (QualificationService, "delete_qualification")),
    ("/api/v1/clients", "post", (ClientService, "create_client")),
    ("/api/v1/clients/{id}", "patch", (ClientService, "edit_client")),
    ("/api/v1/clients/{id}/users/{user_id}", "delete", (ClientService, "remove_client_user")),
    ("/api/v1/sites", "post", (SiteService, "create_site")),
    ("/api/v1/sites/{id}", "patch", (SiteService, "edit_site")),
    ("/api/v1/projects", "post", (ProjectService, "create_project")),
    ("/api/v1/projects/{id}", "patch", (ProjectService, "edit_project")),
    ("/api/v1/projects/{id}", "patch", (ProjectService, "set_status")),
    ("/api/v1/projects/{id}", "delete", (ProjectService, "soft_delete")),
]
CONFLICT = [
    ("/auth/invite", "post", (InvitationService, "create_invitation")),
    ("/api/v1/employees", "post", (EmployeeService, "create_employee")),
    ("/api/v1/employees/{id}", "patch", (EmployeeService, "edit_employee")),
    ("/api/v1/employees/{id}", "delete", (EmployeeService, "soft_delete")),
    ("/api/v1/employees/{id}/deactivate", "post", (EmployeeService, "deactivate")),
    ("/api/v1/employees/{id}/reactivate", "post", (EmployeeService, "reactivate")),
    ("/api/v1/groups", "post", (GroupService, "create_group")),
    ("/api/v1/groups/{id}", "patch", (GroupService, "rename_group")),
    ("/api/v1/qualifications", "post", (QualificationService, "create_qualification")),
    ("/api/v1/qualifications/{id}", "patch", (QualificationService, "rename_qualification")),
    ("/api/v1/clients/{id}", "delete", (ClientService, "soft_delete")),
    ("/api/v1/clients/{id}/users", "post", (ClientService, "create_client_user")),
]


def can_conflict(service: type, method: str) -> bool:
    """Read the service's code: does this method, or one it calls on itself, raise a conflict?

    Stays inside the class, because methods of other classes share names: ``soft_delete`` raises a
    conflict on employees and not on projects.
    """
    tree = ast.parse(Path(str(inspect.getsourcefile(service))).read_text())
    source = next(
        n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == service.__name__
    )
    methods = {
        n.name: n for n in source.body if isinstance(n, ast.FunctionDef | ast.AsyncFunctionDef)
    }
    pending, seen = [method], set()
    while pending:
        name = pending.pop()
        if name in seen or name not in methods:
            continue
        seen.add(name)
        for node in ast.walk(methods[name]):
            if isinstance(node, ast.Raise) and "ConflictError" in ast.dump(node):
                return True
            if isinstance(node, ast.Attribute) and ast.dump(node.value) == ast.dump(
                ast.parse("self", mode="eval").body
            ):
                pending.append(node.attr)
    return False


def responses(path: str, method: str) -> dict[str, Any]:
    return dict(app.openapi()["paths"][path][method]["responses"])


@pytest.mark.parametrize(("path", "method", "behind"), NO_CONFLICT)
def test_nothing_behind_this_operation_raises_a_conflict(
    path: str, method: str, behind: tuple[type, str]
) -> None:
    assert not can_conflict(*behind)
    assert "409" not in responses(path, method)


@pytest.mark.parametrize(("path", "method", "behind"), CONFLICT)
def test_this_operation_can_answer_a_conflict(
    path: str, method: str, behind: tuple[type, str]
) -> None:
    assert can_conflict(*behind)
    assert "409" in responses(path, method)


@pytest.mark.parametrize(
    ("path", "status"), [("/auth/login", "429"), ("/auth/login", "503"), ("/auth/invite", "503")]
)
def test_a_retry_answer_declares_its_header(path: str, status: str) -> None:
    assert "Retry-After" in responses(path, "post")[status]["headers"]


@pytest.mark.parametrize(
    ("path", "method"),
    [("/auth/logout", "post"), ("/auth/logout-all", "post"), ("/me", "get")],
)
def test_any_signed_in_user_may_call_this_so_it_never_answers_403(path: str, method: str) -> None:
    assert "403" not in responses(path, method)


def test_a_profile_edit_can_still_refuse_a_field() -> None:
    assert "403" in responses("/me", "patch")


@pytest.mark.parametrize(("path", "method"), [("/auth/logout-all", "post"), ("/me", "get")])
def test_an_operation_with_no_input_cannot_fail_validation(path: str, method: str) -> None:
    assert "422" not in responses(path, method)


def test_every_operation_with_input_can_fail_validation() -> None:
    paths = app.openapi()["paths"]
    for path, item in paths.items():
        for method, operation in item.items():
            if isinstance(operation, dict) and (
                operation.get("requestBody") or operation.get("parameters")
            ):
                # FastAPI adds its own 422 if none is declared, in another format.
                declared = operation["responses"]["422"]["content"]
                assert "application/problem+json" in declared, (path, method)
