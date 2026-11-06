"""Who worked at a project, a site or a group (005 T022).

Covers: 005 FR-023 to FR-026, FR-029 to FR-032; US4 acceptance scenarios 1 to 6; SC-006.
"""

from typing import Any

import pytest
from httpx import AsyncClient

from hds.models.user import User
from tests.integration.conftest import OtherOrg
from tests.integration.test_my_pay import published_shift
from tests.report_support import WORK_LOG, employee, worked
from tests.roster_support import add_project, headers_for


def rows(answer: Any) -> list[tuple[str, str, int, str, str, str]]:
    body = answer.json()
    return [
        (
            r["full_name"],
            r["project_name"],
            r["shifts"],
            r["hours"],
            r["first_date"],
            r["last_date"],
        )
        for r in body["rows"]
    ]


async def test_a_projects_work_log_starts_on_the_project_start_by_default(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin, "Gate", start_date="2026-10-01")
    dana = await employee(async_client, admin, "Dana Levi")
    omer = await employee(async_client, admin, "Omer Cohen")
    for day, who in ((5, dana), (6, dana), (12, omer)):
        await worked(async_client, admin, project, day, who)

    answer = await async_client.get(
        WORK_LOG,
        params={"project_id": project["id"], "to": "2026-10-31"},
        headers=await headers_for(async_client, "manager"),
    )

    assert answer.status_code == 200, answer.text
    assert answer.json()["from"] == "2026-10-01"
    assert rows(answer) == [
        ("Dana Levi", "Gate", 2, "16.00", "2026-10-05", "2026-10-06"),
        ("Omer Cohen", "Gate", 1, "8.00", "2026-10-12", "2026-10-12"),
    ]
    assert "pay" not in str(answer.json())


async def test_a_range_keeps_only_its_dates(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin, "Gate", start_date="2026-10-01")
    dana = await employee(async_client, admin, "Dana Levi")
    for day in (5, 6, 12):
        await worked(async_client, admin, project, day, dana)

    answer = await async_client.get(
        WORK_LOG,
        params={"project_id": project["id"], "from": "2026-10-06", "to": "2026-10-10"},
        headers=admin,
    )

    assert rows(answer) == [("Dana Levi", "Gate", 1, "8.00", "2026-10-06", "2026-10-06")]


async def test_a_sites_work_log_covers_its_projects(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    gate = await add_project(async_client, admin, "Gate", start_date="2026-10-01")
    other = await add_project(async_client, admin, "Elsewhere", start_date="2026-10-01")
    dana = await employee(async_client, admin, "Dana Levi")
    await worked(async_client, admin, gate, 5, dana)
    await worked(async_client, admin, other, 6, dana)

    answer = await async_client.get(
        WORK_LOG,
        params={"site_id": gate["site_id"], "from": "2026-10-01", "to": "2026-10-31"},
        headers=admin,
    )

    assert rows(answer) == [("Dana Levi", "Gate", 1, "8.00", "2026-10-05", "2026-10-05")]


async def test_a_groups_work_log_lists_only_its_members_at_every_project(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    gate = await add_project(async_client, admin, "Gate", start_date="2026-10-01")
    other = await add_project(async_client, admin, "Elsewhere", start_date="2026-10-01")
    dana = await employee(async_client, admin, "Dana Levi")
    omer = await employee(async_client, admin, "Omer Cohen")
    group = await async_client.post("/api/v1/groups", json={"name": "Night team"}, headers=admin)
    joined = await async_client.put(
        f"/api/v1/employees/{dana}/groups", json={"ids": [group.json()["id"]]}, headers=admin
    )
    assert joined.status_code == 204, joined.text
    await worked(async_client, admin, gate, 5, dana)
    await worked(async_client, admin, other, 6, dana)
    await worked(async_client, admin, gate, 7, omer)

    answer = await async_client.get(
        WORK_LOG,
        params={"group_id": group.json()["id"], "from": "2026-10-01", "to": "2026-10-31"},
        headers=admin,
    )

    assert answer.status_code == 200, answer.text
    assert sorted(rows(answer)) == [
        ("Dana Levi", "Elsewhere", 1, "8.00", "2026-10-06", "2026-10-06"),
        ("Dana Levi", "Gate", 1, "8.00", "2026-10-05", "2026-10-05"),
    ]


async def test_people_and_projects_that_have_since_gone_still_appear(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin, "Gate", start_date="2026-10-01")
    dana = await employee(async_client, admin, "Dana Levi")
    omer = await employee(async_client, admin, "Omer Cohen")
    await worked(async_client, admin, project, 5, dana)
    await worked(async_client, admin, project, 6, omer)
    await async_client.post(f"/api/v1/employees/{dana}/deactivate", headers=admin)
    await async_client.delete(f"/api/v1/employees/{omer}", headers=admin)
    await async_client.patch(
        f"/api/v1/projects/{project['id']}", json={"status": "ended"}, headers=admin
    )

    answer = await async_client.get(
        WORK_LOG, params={"project_id": project["id"], "to": "2026-10-31"}, headers=admin
    )

    assert [r[0] for r in rows(answer)] == ["Dana Levi", "Omer Cohen"]


@pytest.mark.parametrize(
    "params",
    [
        {},
        {"from": "2026-10-01"},
        {"project_id": "P", "site_id": "S", "from": "2026-10-01"},
    ],
    ids=["no-scope", "no-scope-with-dates", "two-scopes"],
)
async def test_exactly_one_scope_is_needed(
    async_client: AsyncClient, seed_users: dict[str, User], params: dict[str, Any]
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin)
    sent = {
        k: project["id"] if v == "P" else project["site_id"] if v == "S" else v
        for k, v in params.items()
    }

    answer = await async_client.get(WORK_LOG, params=sent, headers=admin)

    assert answer.status_code == 422
    assert answer.json()["code"] == "reports.scope_invalid"


async def test_a_site_or_group_needs_a_start(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin)

    answer = await async_client.get(WORK_LOG, params={"site_id": project["site_id"]}, headers=admin)

    assert answer.status_code == 422
    assert answer.json()["code"] == "reports.start_required"


@pytest.mark.parametrize(
    ("first", "last"), [("2026-10-10", "2026-10-09"), ("2025-10-01", "2026-10-02")]
)
async def test_a_backwards_or_too_long_range_is_refused(
    async_client: AsyncClient, seed_users: dict[str, User], first: str, last: str
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin)

    answer = await async_client.get(
        WORK_LOG, params={"project_id": project["id"], "from": first, "to": last}, headers=admin
    )

    assert answer.status_code == 422
    assert answer.json()["code"] == "reports.range_invalid"


@pytest.mark.parametrize("role", ["employee", "client"])
async def test_employees_and_clients_are_refused(
    async_client: AsyncClient, seed_users: dict[str, User], role: str
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin)

    answer = await async_client.get(
        WORK_LOG,
        params={"project_id": project["id"]},
        headers=await headers_for(async_client, role),
    )

    assert answer.status_code == 403


@pytest.mark.parametrize("scope", ["project_id", "site_id", "group_id"])
async def test_another_organisations_scope_is_not_found(
    async_client: AsyncClient, seed_users: dict[str, User], other_org: OtherOrg, scope: str
) -> None:
    admin = await headers_for(async_client, "admin")
    theirs = {
        "project_id": other_org.project.id,
        "site_id": other_org.site.id,
        "group_id": other_org.group.id,
    }

    answer = await async_client.get(
        WORK_LOG, params={scope: str(theirs[scope]), "from": "2026-10-01"}, headers=admin
    )

    assert answer.status_code == 404


async def test_shifts_not_yet_worked_are_not_listed(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin, "Gate", start_date="2026-10-01")
    dana = await employee(async_client, admin, "Dana Levi")
    await worked(async_client, admin, project, 5, dana)
    await published_shift(
        async_client, admin, project, "2027-06-07T04:00:00Z", "2027-06-07T12:00:00Z", dana
    )

    answer = await async_client.get(
        WORK_LOG, params={"project_id": project["id"], "to": "2027-06-30"}, headers=admin
    )

    assert rows(answer) == [("Dana Levi", "Gate", 1, "8.00", "2026-10-05", "2026-10-05")]


async def test_an_old_projects_work_log_covers_it_since_it_started(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    """SC-006 for a project over a year old: no `from` means the whole project (FR-024, FR-030)."""
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin, "Old Gate", start_date="2024-01-01")
    dana = await employee(async_client, admin, "Dana Levi")
    await published_shift(
        async_client, admin, project, "2024-02-04T06:00:00Z", "2024-02-04T14:00:00Z", dana
    )
    await worked(async_client, admin, project, 5, dana)

    answer = await async_client.get(
        WORK_LOG,
        params={"project_id": project["id"], "to": "2026-10-31"},
        headers=await headers_for(async_client, "manager"),
    )

    assert answer.status_code == 200, answer.text
    assert answer.json()["from"] == "2024-01-01"
    assert rows(answer) == [("Dana Levi", "Old Gate", 2, "16.00", "2024-02-04", "2026-10-05")]


async def test_an_old_projects_explicit_range_over_366_days_is_still_refused(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin, "Old Gate", start_date="2024-01-01")

    answer = await async_client.get(
        WORK_LOG,
        params={"project_id": project["id"], "from": "2024-01-01", "to": "2026-10-31"},
        headers=admin,
    )

    assert answer.status_code == 422
    assert answer.json()["code"] == "reports.range_invalid"
