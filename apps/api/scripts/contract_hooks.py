"""Schemathesis hooks for scripts/contract_cli.sh: keep the run's own accounts alive.

The fuzzer picks real ids out of earlier responses and happily deactivates, demotes or deletes the
very admin whose token it is using. From then on every call is answered 401 or 403, so the
operations tested after that point (the deletes of employees and portal users, and the whole second
pass) never reach their real logic, and Schemathesis warns "Authentication failed". This hook swaps
the id of any account that existed when the run started for a random one before each call, so
those accounts survive and every operation is answered by the code behind it. Accounts the run
creates itself are still fair game.

It also gives the few operations that need two or more related records something real to work
on. A random id answers 404, so without this ``POST /clients/{id}/users``,
``DELETE /clients/{id}/users/{user_id}``, ``PATCH /projects/{id}``, ``PATCH /sites/{id}`` and
``POST /projects`` (whose body names a client and a site) never reach their logic, and Schemathesis
warns "Missing test data". For those the hook
creates the records first and puts their ids into the call; on the site writes it also supplies a
real time zone on every other call, because an invented one is correctly refused (FR-217) and the
accepted path would otherwise go unexercised.

Enabled by ``SCHEMATHESIS_HOOKS=contract_hooks`` with ``HDS_CONTRACT_BASE_URL`` set (the script does
both). It signs in with the seeded admin, so it only makes sense against a seeded development
database, which is the only thing the script accepts.
"""

import json
import os
import urllib.request
import uuid
from typing import Any

import schemathesis

_protected: set[str] | None = None
_token: str | None = None
_calls = 0


def _request(path: str, token: str | None = None, body: dict[str, Any] | None = None) -> Any:
    base = os.environ["HDS_CONTRACT_BASE_URL"].rstrip("/")
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(
        base + path,
        data=json.dumps(body).encode() if body is not None else None,
        headers=headers,
    )
    with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310 (local test server)
        return json.load(response)


def admin_token() -> str:
    """The seeded admin's access token, obtained once."""
    global _token
    if _token is None:
        login = _request("/auth/login", body={"email": "admin@hds.test", "password": "Dev1234!"})
        _token = str(login["access_token"])
    return _token


def protected_ids() -> set[str]:
    """Ids of every account that exists now; read once, on first use."""
    global _protected
    if _protected is None:
        users = _request("/api/v1/employees", admin_token())["items"]
        _protected = {str(user["id"]) for user in users}
    return _protected


def make_client() -> str:
    """Create a client and return its id."""
    body = {"name": f"Fuzz client {uuid.uuid4().hex[:8]}", "contract_start": "2026-01-01"}
    return str(_request("/api/v1/clients", admin_token(), body)["id"])


def make_site() -> str:
    """Create a site and return its id."""
    body = {"name": "Fuzz site", "address": "1 Example St", "timezone": "Asia/Jerusalem"}
    return str(_request("/api/v1/sites", admin_token(), body)["id"])


def make_project() -> str:
    """Create a project (with its own client and site) and return its id."""
    body = {
        "client_id": make_client(),
        "site_id": make_site(),
        "name": "Fuzz project",
        "type": "security",
        "billing_rate": "10.0000",
        "start_date": "2026-01-01",
    }
    return str(_request("/api/v1/projects", admin_token(), body)["id"])


def make_portal_user(client_id: str) -> str:
    """Create a portal user for a client and return the user's id."""
    body = {
        "email": f"fuzz.{uuid.uuid4().hex[:10]}@hds.test",
        "full_name": "Fuzz Portal",
        "initial_password": "A-long-password-1",
    }
    return str(_request(f"/api/v1/clients/{client_id}/users", admin_token(), body)["id"])


def destroys_accounts(case: Any) -> bool:
    """Whether the call can deactivate, demote or delete the account named in its path.

    Reads and the group and qualification replacements are harmless to the run, so they keep the
    real ids and reach their logic.
    """
    path = case.operation.path
    return case.method.upper() in ("DELETE", "PATCH") or path.endswith(
        ("/deactivate", "/reactivate")
    )


def give_real_records(case: Any) -> None:
    """Point the operations that need related records at records that exist."""
    global _calls
    _calls += 1
    key = (case.method.upper(), case.operation.path)
    body = case.body if isinstance(case.body, dict) else None
    if key == ("DELETE", "/api/v1/clients/{id}/users/{user_id}"):
        client_id = make_client()
        case.path_parameters.update({"id": client_id, "user_id": make_portal_user(client_id)})
    elif key == ("POST", "/api/v1/clients/{id}/users"):
        case.path_parameters["id"] = make_client()
    elif key == ("PATCH", "/api/v1/projects/{id}"):
        case.path_parameters["id"] = make_project()
    elif key == ("PATCH", "/api/v1/sites/{id}"):
        case.path_parameters["id"] = make_site()
    elif key == ("POST", "/api/v1/projects") and body is not None:
        body.update({"client_id": make_client(), "site_id": make_site()})
    if key[1] in ("/api/v1/sites", "/api/v1/sites/{id}") and body is not None and _calls % 2 == 0:
        if key[0] == "POST" or "timezone" in body:
            body["timezone"] = "Asia/Jerusalem"


@schemathesis.hook
def before_call(context: Any, case: Any, kwargs: Any) -> None:
    """Protect the run's accounts, then give the operation real records to work on."""
    parameters = case.path_parameters
    if parameters and destroys_accounts(case):
        keep = protected_ids()
        for name, value in list(parameters.items()):
            if str(value) in keep:
                parameters[name] = str(uuid.uuid4())
    give_real_records(case)
