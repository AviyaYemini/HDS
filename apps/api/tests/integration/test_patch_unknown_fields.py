"""A change request that names a field the operation does not accept is refused (003 Group B).

An unknown field used to be dropped without a word, so a misspelt field or one the caller may not
set answered 200 as if it had worked. The staff change operations (employee, client, project,
site, group and qualification) now refuse it with the request validation error before anything
runs. Create requests are out of scope and keep their handling. Editing one's own profile keeps
its own refusal, 403 ``profile.field_not_allowed``, which names the problem more precisely.

Covers: 003 FR-005, SC-002.
"""

from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import pytest
import yaml
from httpx import AsyncClient

from hds.main import app
from hds.models.user import User
from tests.integration.test_patch_null_clears import (
    add_client,
    add_employee,
    add_project,
    add_site,
    headers_for,
)

Add = Callable[[AsyncClient, dict[str, str]], Awaitable[Any]]
CONTRACT = Path(__file__).resolve().parents[4] / (
    "specs/001-foundation-auth-org/contracts/openapi.yaml"
)


def named(path: str, name: str) -> Add:
    async def add(client: AsyncClient, headers: dict[str, str]) -> Any:
        response = await client.post(path, json={"name": name}, headers=headers)
        assert response.status_code == 201, response.text
        return response.json()

    return add


# Operation -> (how to create a record, its path, one valid field to send with the unknown one).
OPERATIONS: dict[str, tuple[Add, str, dict[str, Any]]] = {
    "employee": (add_employee, "/api/v1/employees", {"full_name": "Changed"}),
    "client": (add_client, "/api/v1/clients", {"name": "Changed"}),
    "project": (add_project, "/api/v1/projects", {"name": "Changed"}),
    "site": (add_site, "/api/v1/sites", {"name": "Changed"}),
    "group": (named("/api/v1/groups", "Night"), "/api/v1/groups", {"name": "Changed"}),
    "qualification": (
        named("/api/v1/qualifications", "First aid"),
        "/api/v1/qualifications",
        {"name": "Changed"},
    ),
}
# Lists are read back from their collection, since groups and qualifications have no single GET.
LISTED = {"group", "qualification"}
SCHEMAS = [
    "UserUpdateRequest",
    "ClientUpdateRequest",
    "ProjectUpdateRequest",
    "SiteUpdateRequest",
    "GroupRenameRequest",
    "QualificationRenameRequest",
]
CREATE_SCHEMAS = ["GroupWriteRequest", "QualificationWriteRequest"]


async def stored(client: AsyncClient, operation: str, url: str, headers: dict[str, str]) -> Any:
    if operation not in LISTED:
        return (await client.get(url, headers=headers)).json()
    path, record = url.rsplit("/", 1)
    items = (await client.get(path, headers=headers)).json()["items"]
    return next(item for item in items if item["id"] == record)


@pytest.fixture
async def manager(async_client: AsyncClient, seed_users: dict[str, User]) -> dict[str, str]:
    return await headers_for(async_client, "manager@hds.test")


@pytest.mark.parametrize("with_valid", [False, True], ids=["alone", "with-a-valid-field"])
@pytest.mark.parametrize("operation", list(OPERATIONS))
async def test_an_unknown_field_is_refused_and_nothing_changes(
    operation: str, with_valid: bool, async_client: AsyncClient, manager: dict[str, str]
) -> None:
    add, path, valid = OPERATIONS[operation]
    created = await add(async_client, manager)
    url = f"{path}/{created['id']}"
    body = {**(valid if with_valid else {}), "colour": "blue"}

    refused = await async_client.patch(url, json=body, headers=manager)

    assert refused.status_code == 422, refused.text
    assert refused.json()["code"] == "validation.request_invalid"
    # Read back as an Admin, who is given every field: since 005 FR-022a a Manager is not given
    # an employee's default rate, which ``created`` (an Admin's answer) holds.
    admin = await headers_for(async_client, "admin@hds.test")
    assert await stored(async_client, operation, url, admin) == created


@pytest.mark.parametrize("operation", ["project", "client"])
async def test_a_field_that_is_set_only_on_create_is_refused_on_a_change(
    operation: str, async_client: AsyncClient, manager: dict[str, str]
) -> None:
    """A project's client and a client's organisation are fixed at creation."""
    add, path, _ = OPERATIONS[operation]
    created = await add(async_client, manager)
    url = f"{path}/{created['id']}"
    if operation == "project":
        body = {"client_id": (await add_client(async_client, manager))["id"]}  # a real client
    else:
        body = {"organisation_id": created["organisation_id"]}  # even its own value

    refused = await async_client.patch(url, json=body, headers=manager)

    assert refused.status_code == 422, refused.text
    assert (await async_client.get(url, headers=manager)).json() == created


async def test_my_profile_keeps_its_own_refusal_of_unknown_fields(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    headers = await headers_for(async_client, "employee@hds.test")

    refused = await async_client.patch("/me", json={"colour": "blue"}, headers=headers)

    assert refused.status_code == 403
    assert refused.json()["code"] == "profile.field_not_allowed"


@pytest.mark.parametrize("schema", SCHEMAS)
def test_the_published_schema_declares_unknown_fields_refused(schema: str) -> None:
    assert app.openapi()["components"]["schemas"][schema].get("additionalProperties") is False


def test_the_profile_schema_still_accepts_unknown_fields_for_the_service_to_refuse() -> None:
    assert app.openapi()["components"]["schemas"]["ProfileUpdate"].get("additionalProperties") in (
        None,
        True,
    )


@pytest.mark.parametrize("schema", SCHEMAS)
def test_the_contract_file_declares_unknown_fields_refused(schema: str) -> None:
    """The drift check compares operations and status codes only, so the schema is checked here."""
    contract = yaml.safe_load(CONTRACT.read_text())
    assert contract["components"]["schemas"][schema].get("additionalProperties") is False


@pytest.mark.parametrize("schema", CREATE_SCHEMAS)
def test_create_requests_keep_their_handling_of_unknown_fields(schema: str) -> None:
    assert app.openapi()["components"]["schemas"][schema].get("additionalProperties") is None
