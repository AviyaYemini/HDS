"""A project's staffing needs: how many people on each shift type and weekday (004 T010).

Covers: 004 FR-001 to FR-003, FR-027; US1.
"""

from typing import Any

import pytest
from httpx import AsyncClient

from hds.models.user import User
from tests.integration.conftest import OtherOrg
from tests.roster_support import add_project, headers_for


def path(project: dict[str, Any]) -> str:
    """The needs of ``project``."""
    return f"/api/v1/projects/{project['id']}/requirements"


def needs(project: dict[str, Any]) -> list[dict[str, Any]]:
    """Two on nights every day, one on mornings Sunday to Thursday."""
    types = project["types"]
    return [{"shift_type_id": types["night"], "weekday": d, "headcount": 2} for d in range(7)] + [
        {"shift_type_id": types["morning"], "weekday": d, "headcount": 1} for d in range(5)
    ]


def as_set(items: list[dict[str, Any]]) -> set[tuple[str, int, int]]:
    return {(i["shift_type_id"], i["weekday"], i["headcount"]) for i in items}


@pytest.mark.parametrize("role", ["manager", "admin"])
async def test_a_manager_or_admin_sets_and_reads_the_needs_as_a_whole(
    async_client: AsyncClient, seed_users: dict[str, User], role: str
) -> None:
    headers = await headers_for(async_client, role)
    project = await add_project(async_client, headers)

    put = await async_client.put(path(project), json={"items": needs(project)}, headers=headers)
    assert put.status_code == 200, put.text
    assert as_set(put.json()["items"]) == as_set(needs(project))

    smaller = [{"shift_type_id": project["types"]["afternoon"], "weekday": 6, "headcount": 3}]
    put = await async_client.put(path(project), json={"items": smaller}, headers=headers)
    read = await async_client.get(path(project), headers=headers)
    assert read.status_code == 200
    assert as_set(read.json()["items"]) == as_set(smaller)  # replaced, not merged


async def test_an_empty_list_removes_every_need(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    headers = await headers_for(async_client, "manager")
    project = await add_project(async_client, headers)
    await async_client.put(path(project), json={"items": needs(project)}, headers=headers)

    await async_client.put(path(project), json={"items": []}, headers=headers)

    assert (await async_client.get(path(project), headers=headers)).json()["items"] == []


async def test_a_type_the_project_does_not_use_is_refused(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    headers = await headers_for(async_client, "manager")
    project = await add_project(async_client, headers)
    await async_client.put(
        f"/api/v1/projects/{project['id']}/shift-types",
        json={"ids": [project["types"]["morning"]]},
        headers=headers,
    )
    night = {"shift_type_id": project["types"]["night"], "weekday": 0, "headcount": 1}

    answer = await async_client.put(path(project), json={"items": [night]}, headers=headers)

    assert answer.status_code == 422
    assert answer.json()["code"] == "requirements.shift_type_not_in_project"
    assert (await async_client.get(path(project), headers=headers)).json()["items"] == []


async def test_another_organisations_type_is_refused_as_not_in_the_project(
    async_client: AsyncClient, seed_users: dict[str, User], other_org: OtherOrg
) -> None:
    headers = await headers_for(async_client, "manager")
    project = await add_project(async_client, headers)
    assert other_org.shift_type is not None
    foreign = {"shift_type_id": str(other_org.shift_type.id), "weekday": 0, "headcount": 1}

    answer = await async_client.put(path(project), json={"items": [foreign]}, headers=headers)

    assert answer.status_code == 422
    assert answer.json()["code"] == "requirements.shift_type_not_in_project"


async def test_a_duplicate_type_and_weekday_is_refused(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    headers = await headers_for(async_client, "manager")
    project = await add_project(async_client, headers)
    twice = [
        {"shift_type_id": project["types"]["night"], "weekday": 0, "headcount": h} for h in (1, 2)
    ]

    answer = await async_client.put(path(project), json={"items": twice}, headers=headers)

    assert answer.status_code == 422
    assert answer.json()["code"] == "requirements.duplicate"


@pytest.mark.parametrize(
    "bad", [{"weekday": 7}, {"weekday": -1}, {"headcount": 0}, {"headcount": -2}, {"extra": 1}]
)
async def test_a_weekday_outside_the_week_or_a_headcount_below_one_is_refused(
    async_client: AsyncClient, seed_users: dict[str, User], bad: dict[str, Any]
) -> None:
    headers = await headers_for(async_client, "manager")
    project = await add_project(async_client, headers)
    item = {"shift_type_id": project["types"]["night"], "weekday": 0, "headcount": 1, **bad}

    answer = await async_client.put(path(project), json={"items": [item]}, headers=headers)

    assert answer.status_code == 422
    assert answer.json()["code"] == "validation.request_invalid"


async def test_removing_a_type_from_the_project_removes_its_needs(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    headers = await headers_for(async_client, "manager")
    project = await add_project(async_client, headers)
    await async_client.put(path(project), json={"items": needs(project)}, headers=headers)

    await async_client.put(
        f"/api/v1/projects/{project['id']}/shift-types",
        json={"ids": [project["types"]["morning"]]},
        headers=headers,
    )

    left = (await async_client.get(path(project), headers=headers)).json()["items"]
    assert {i["shift_type_id"] for i in left} == {project["types"]["morning"]}


async def test_retiring_a_type_removes_its_needs_everywhere(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    headers = await headers_for(async_client, "manager")
    first = await add_project(async_client, headers, "First")
    second = await add_project(async_client, headers, "Second")
    for project in (first, second):
        await async_client.put(path(project), json={"items": needs(project)}, headers=headers)

    retired = await async_client.patch(
        f"/api/v1/shift-types/{first['types']['night']}", json={"is_active": False}, headers=headers
    )
    assert retired.status_code == 200, retired.text

    for project in (first, second):
        left = (await async_client.get(path(project), headers=headers)).json()["items"]
        assert {i["shift_type_id"] for i in left} == {project["types"]["morning"]}


@pytest.mark.parametrize("role", ["employee", "client"])
async def test_employees_and_clients_cannot_read_or_change_needs(
    async_client: AsyncClient, seed_users: dict[str, User], role: str
) -> None:
    project = await add_project(async_client, await headers_for(async_client, "manager"))
    headers = await headers_for(async_client, role)

    assert (await async_client.get(path(project), headers=headers)).status_code == 403
    put = await async_client.put(path(project), json={"items": []}, headers=headers)
    assert put.status_code == 403


async def test_another_organisations_project_is_not_found(
    async_client: AsyncClient, seed_users: dict[str, User], other_org: OtherOrg
) -> None:
    headers = await headers_for(async_client, "manager")
    elsewhere = {"id": str(other_org.project.id)}

    assert (await async_client.get(path(elsewhere), headers=headers)).status_code == 404
    put = await async_client.put(path(elsewhere), json={"items": []}, headers=headers)
    assert put.status_code == 404


async def test_a_retired_type_is_refused_as_retired(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    headers = await headers_for(async_client, "manager")
    project = await add_project(async_client, headers)
    night = project["types"]["night"]
    retired = await async_client.patch(
        f"/api/v1/shift-types/{night}", json={"is_active": False}, headers=headers
    )
    assert retired.status_code == 200, retired.text

    answer = await async_client.put(
        path(project),
        json={"items": [{"shift_type_id": night, "weekday": 0, "headcount": 1}]},
        headers=headers,
    )

    assert answer.status_code == 422
    assert answer.json()["code"] == "requirements.shift_type_retired"


async def test_a_retired_type_is_named_before_a_foreign_one_whatever_the_order(
    async_client: AsyncClient, seed_users: dict[str, User], other_org: OtherOrg
) -> None:
    """Review finding: the code depended on set order when both problems were present."""
    headers = await headers_for(async_client, "manager")
    project = await add_project(async_client, headers)
    night = project["types"]["night"]
    await async_client.patch(
        f"/api/v1/shift-types/{night}", json={"is_active": False}, headers=headers
    )
    assert other_org.shift_type is not None
    retired = {"shift_type_id": night, "weekday": 0, "headcount": 1}
    foreign = {"shift_type_id": str(other_org.shift_type.id), "weekday": 0, "headcount": 1}

    for items in ([retired, foreign], [foreign, retired]):
        answer = await async_client.put(path(project), json={"items": items}, headers=headers)
        assert answer.json()["code"] == (
            "requirements.shift_type_retired"
            if items[0] is retired
            else "requirements.shift_type_not_in_project"
        )


async def test_more_needs_than_a_week_of_types_can_hold_are_refused_by_schema(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    """Review finding: an unbounded list reached the database as one huge ``IN`` and failed 500."""
    headers = await headers_for(async_client, "manager")
    project = await add_project(async_client, headers)
    many = [
        {"shift_type_id": f"00000000-0000-4000-8000-{n:012d}", "weekday": 0, "headcount": 1}
        for n in range(1001)
    ]

    answer = await async_client.put(path(project), json={"items": many}, headers=headers)

    assert answer.status_code == 422
    assert answer.json()["code"] == "validation.request_invalid"
