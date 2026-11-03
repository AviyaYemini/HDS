"""An explicit null in a PATCH clears an optional field, and an omitted field stays (T392, F17).

``exclude_none`` dropped every null, so a phone number or an end date could never be removed, and
the answer was 200 as if it had worked. Null for a required field is now refused, and the schema
says so: a schema that allows a null the API refuses fails the contract check.

Covers: FR-236.
"""

from pathlib import Path
from typing import Any

import pytest
import yaml
from httpx import AsyncClient

from hds.main import app
from hds.models.user import User

PASSWORD = "Dev1234!"
CONTRACT = Path(__file__).resolve().parents[4] / (
    "specs/001-foundation-auth-org/contracts/openapi.yaml"
)
# Schema name -> (fields that may be cleared, fields that may not).
FIELDS = {
    "ProfileUpdate": (["phone"], ["full_name"]),
    "UserUpdateRequest": (["phone", "default_hourly_rate"], ["full_name", "role"]),
    "ClientUpdateRequest": (
        ["contact_name", "contact_email", "contact_phone", "contract_end"],
        ["name", "is_active"],
    ),
    "ProjectUpdateRequest": (["end_date"], ["name", "billing_rate", "status"]),
    "SiteUpdateRequest": ([], ["name", "address", "timezone"]),
}


async def headers_for(client: AsyncClient, email: str) -> dict[str, str]:
    response = await client.post("/auth/login", json={"email": email, "password": PASSWORD})
    assert response.status_code == 200, response.text
    return {"Authorization": f"Bearer {response.json()['access_token']}"}


@pytest.fixture
async def manager(async_client: AsyncClient, seed_users: dict[str, User]) -> dict[str, str]:
    return await headers_for(async_client, "manager@hds.test")


async def add_client(client: AsyncClient, headers: dict[str, str]) -> Any:
    response = await client.post(
        "/api/v1/clients",
        json={
            "name": "Acme",
            "contract_start": "2026-01-01",
            "contract_end": "2027-01-01",
            "contact_name": "Ann",
            "contact_email": "ann@acme.example",
            "contact_phone": "0501234567",
        },
        headers=headers,
    )
    assert response.status_code == 201, response.text
    return response.json()


async def add_site(client: AsyncClient, headers: dict[str, str]) -> Any:
    response = await client.post(
        "/api/v1/sites",
        json={"name": "HQ", "address": "1 HQ St", "timezone": "UTC"},
        headers=headers,
    )
    assert response.status_code == 201, response.text
    return response.json()


async def add_project(client: AsyncClient, headers: dict[str, str]) -> Any:
    owner = await add_client(client, headers)
    site = await add_site(client, headers)
    response = await client.post(
        "/api/v1/projects",
        json={
            "client_id": owner["id"],
            "site_id": site["id"],
            "name": "Night Watch",
            "type": "security",
            "billing_rate": "75.0000",
            "start_date": "2026-01-01",
            "end_date": "2026-12-31",
        },
        headers=headers,
    )
    assert response.status_code == 201, response.text
    return response.json()


async def add_employee(client: AsyncClient, headers: dict[str, str]) -> Any:
    response = await client.post(
        "/api/v1/employees",
        json={
            "email": "phoned@hds.test",
            "full_name": "Phoned",
            "phone": "0501234567",
            "initial_password": "A-long-password-1",
            "default_hourly_rate": "50.0000",
        },
        headers=headers,
    )
    assert response.status_code == 201, response.text
    return response.json()


async def test_my_own_phone_can_be_cleared_and_stays_if_omitted(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    headers = await headers_for(async_client, "employee@hds.test")
    await async_client.patch("/me", json={"phone": "0501234567"}, headers=headers)

    kept = await async_client.patch("/me", json={"full_name": "New"}, headers=headers)
    cleared = await async_client.patch("/me", json={"phone": None}, headers=headers)

    assert kept.json()["phone"] == "0501234567"
    assert cleared.status_code == 200
    assert cleared.json()["phone"] is None
    assert (await async_client.get("/me", headers=headers)).json()["phone"] is None


@pytest.mark.parametrize("field", ["phone", "default_hourly_rate"])
async def test_an_employees_optional_field_can_be_cleared(
    field: str, async_client: AsyncClient, manager: dict[str, str]
) -> None:
    employee = await add_employee(async_client, manager)
    other = "default_hourly_rate" if field == "phone" else "phone"

    cleared = await async_client.patch(
        f"/api/v1/employees/{employee['id']}", json={field: None}, headers=manager
    )

    stored = await async_client.get(f"/api/v1/employees/{employee['id']}", headers=manager)
    assert cleared.status_code == 200, cleared.text
    assert stored.json()[field] is None
    assert stored.json()[other] == employee[other]


@pytest.mark.parametrize(
    "field", ["contact_name", "contact_email", "contact_phone", "contract_end"]
)
async def test_a_clients_optional_field_can_be_cleared(
    field: str, async_client: AsyncClient, manager: dict[str, str]
) -> None:
    client_row = await add_client(async_client, manager)

    cleared = await async_client.patch(
        f"/api/v1/clients/{client_row['id']}", json={field: None}, headers=manager
    )

    stored = await async_client.get(f"/api/v1/clients/{client_row['id']}", headers=manager)
    assert cleared.status_code == 200, cleared.text
    assert stored.json()[field] is None
    assert stored.json()["name"] == "Acme"


async def test_a_projects_end_date_can_be_cleared(
    async_client: AsyncClient, manager: dict[str, str]
) -> None:
    project = await add_project(async_client, manager)

    kept = await async_client.patch(
        f"/api/v1/projects/{project['id']}", json={"name": "Day Watch"}, headers=manager
    )
    cleared = await async_client.patch(
        f"/api/v1/projects/{project['id']}", json={"end_date": None}, headers=manager
    )

    stored = await async_client.get(f"/api/v1/projects/{project['id']}", headers=manager)
    assert kept.json()["end_date"] == "2026-12-31"
    assert cleared.status_code == 200, cleared.text
    assert stored.json()["end_date"] is None


@pytest.mark.parametrize("field", ["full_name"])
async def test_null_for_my_required_field_is_refused(
    field: str, async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    headers = await headers_for(async_client, "employee@hds.test")

    refused = await async_client.patch("/me", json={field: None}, headers=headers)

    assert refused.status_code == 422
    assert refused.json()["code"] == "validation.request_invalid"


@pytest.mark.parametrize("field", ["full_name", "role"])
async def test_null_for_an_employees_required_field_changes_nothing(
    field: str, async_client: AsyncClient, manager: dict[str, str]
) -> None:
    employee = await add_employee(async_client, manager)
    url = f"/api/v1/employees/{employee['id']}"

    refused = await async_client.patch(url, json={field: None, "phone": "1"}, headers=manager)

    assert refused.status_code == 422
    assert refused.json()["code"] == "validation.request_invalid"
    assert (await async_client.get(url, headers=manager)).json()["phone"] == employee["phone"]


@pytest.mark.parametrize("field", ["name", "is_active"])
async def test_null_for_a_clients_required_field_is_refused(
    field: str, async_client: AsyncClient, manager: dict[str, str]
) -> None:
    client_row = await add_client(async_client, manager)
    url = f"/api/v1/clients/{client_row['id']}"

    refused = await async_client.patch(
        url, json={field: None, "contact_phone": "1"}, headers=manager
    )

    assert refused.status_code == 422
    assert refused.json()["code"] == "validation.request_invalid"
    stored = await async_client.get(url, headers=manager)
    assert stored.json()["contact_phone"] == client_row["contact_phone"]


@pytest.mark.parametrize("field", ["name", "billing_rate", "status"])
async def test_null_for_a_projects_required_field_is_refused(
    field: str, async_client: AsyncClient, manager: dict[str, str]
) -> None:
    project = await add_project(async_client, manager)
    url = f"/api/v1/projects/{project['id']}"

    refused = await async_client.patch(url, json={field: None, "end_date": None}, headers=manager)

    assert refused.status_code == 422
    assert refused.json()["code"] == "validation.request_invalid"
    assert (await async_client.get(url, headers=manager)).json()["end_date"] == project["end_date"]


@pytest.mark.parametrize("field", ["name", "address", "timezone"])
async def test_null_for_a_sites_required_field_is_refused(
    field: str, async_client: AsyncClient, manager: dict[str, str]
) -> None:
    site = await add_site(async_client, manager)

    refused = await async_client.patch(
        f"/api/v1/sites/{site['id']}", json={field: None}, headers=manager
    )

    assert refused.status_code == 422
    assert refused.json()["code"] == "validation.request_invalid"


async def test_an_omitted_site_field_stays(
    async_client: AsyncClient, manager: dict[str, str]
) -> None:
    site = await add_site(async_client, manager)

    edited = await async_client.patch(
        f"/api/v1/sites/{site['id']}", json={"name": "Gate"}, headers=manager
    )

    assert edited.json()["address"] == site["address"]
    assert edited.json()["timezone"] == site["timezone"]


async def test_null_for_a_field_i_may_not_change_is_refused_like_any_other_value(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    headers = await headers_for(async_client, "employee@hds.test")

    refused = await async_client.patch("/me", json={"role": None}, headers=headers)

    assert refused.status_code == 403
    assert refused.json()["code"] == "profile.field_not_allowed"


def allows_null(document: dict[str, Any], schema: dict[str, Any]) -> bool:
    if schema.get("type") == "null" or schema.get("default", 0) is None:
        return True
    if "$ref" in schema:
        name = schema["$ref"].rsplit("/", 1)[-1]
        return allows_null(document, document["components"]["schemas"][name])
    if isinstance(schema.get("type"), list) and "null" in schema["type"]:
        return True
    return any(allows_null(document, part) for part in schema.get("anyOf", []))


@pytest.mark.parametrize("source", ["generated", "contract"])
@pytest.mark.parametrize("schema_name", FIELDS)
def test_the_schemas_say_what_the_api_does(source: str, schema_name: str) -> None:
    document = app.openapi() if source == "generated" else yaml.safe_load(CONTRACT.read_text())
    properties = document["components"]["schemas"][schema_name]["properties"]
    clearable, required = FIELDS[schema_name]

    for field in clearable:
        assert allows_null(document, properties[field]), f"{schema_name}.{field} may be cleared"
    for field in required:
        assert not allows_null(document, properties[field]), f"{schema_name}.{field} may not"
