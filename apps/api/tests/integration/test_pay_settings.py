"""The organisation's premium defaults and each project's pay settings (005 T009).

Covers: 005 FR-003, FR-005 to FR-007, FR-008a (freezing the days), FR-031, FR-032; US2 acceptance
scenarios 4 to 7; the edge case of reopening an ended project.
"""

from typing import Any

import pytest
from httpx import AsyncClient

from hds.models.user import User
from tests.integration.conftest import OtherOrg
from tests.pay_support import (
    DEFAULTS,
    ORG_PAY,
    SPECIAL_DAYS,
    end_project,
    pay_settings,
    project_days,
    project_pay,
)
from tests.roster_support import add_project, headers_for


async def test_managers_read_the_defaults(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    answer = await async_client.get(ORG_PAY, headers=await headers_for(async_client, "manager"))

    assert answer.status_code == 200
    assert answer.json() == DEFAULTS


async def test_an_admin_changes_the_defaults(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    window = {
        "start_weekday": 5,
        "start_time": "16:00:00",
        "end_weekday": 6,
        "end_time": "23:30:00",
    }

    answer = await async_client.patch(
        ORG_PAY, json={"overtime1_pct": "130.00", "rest_window": window}, headers=admin
    )

    assert answer.status_code == 200, answer.text
    assert answer.json() == {**DEFAULTS, "overtime1_pct": "130.00", "rest_window": window}


@pytest.mark.parametrize("role", ["manager", "employee", "client"])
async def test_only_an_admin_changes_the_defaults(
    async_client: AsyncClient, seed_users: dict[str, User], role: str
) -> None:
    headers = await headers_for(async_client, role)

    answer = await async_client.patch(ORG_PAY, json={"rest_pct": "200.00"}, headers=headers)

    assert answer.status_code == 403
    admin = await headers_for(async_client, "admin")
    assert (await async_client.get(ORG_PAY, headers=admin)).json() == DEFAULTS


@pytest.mark.parametrize("role", ["employee", "client"])
async def test_employees_and_clients_cannot_read_the_defaults(
    async_client: AsyncClient, seed_users: dict[str, User], role: str
) -> None:
    answer = await async_client.get(ORG_PAY, headers=await headers_for(async_client, role))

    assert answer.status_code == 403


@pytest.mark.parametrize(
    "bad",
    [
        {"day_normal_minutes": 0},
        {"night_normal_minutes": 1441},
        {"tier1_minutes": 12.5},
        {"tier1_minutes": True},
        {"overtime1_pct": "99.99"},
        {"overtime2_pct": "300.01"},
        {"rest_pct": 150},
        {"overtime1_pct": "1.25e2"},
        {"increase1_points": "1E1"},
        {"increase1_points": "0"},
        {"increase2_points": "200.01"},
        {
            "rest_window": {
                "start_weekday": 7,
                "start_time": "18:00",
                "end_weekday": 6,
                "end_time": "22:00",
            }
        },
        {
            "rest_window": {
                "start_weekday": 5,
                "start_time": "25:00",
                "end_weekday": 6,
                "end_time": "22:00",
            }
        },
        {"rest_window": None},
        {"overtime1_pct": None},
        {"weekly_overtime": 2520},
    ],
)
async def test_out_of_range_or_unknown_defaults_are_refused(
    async_client: AsyncClient, seed_users: dict[str, User], bad: dict[str, Any]
) -> None:
    admin = await headers_for(async_client, "admin")

    answer = await async_client.patch(ORG_PAY, json=bad, headers=admin)

    assert answer.status_code == 422, answer.text
    assert (await async_client.get(ORG_PAY, headers=admin)).json() == DEFAULTS


async def test_a_rest_window_whose_ends_meet_is_refused(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    window = {"start_weekday": 5, "start_time": "18:00", "end_weekday": 5, "end_time": "18:00"}

    answer = await async_client.patch(ORG_PAY, json={"rest_window": window}, headers=admin)

    assert answer.status_code == 422
    assert answer.json()["code"] == "pay.rest_window_invalid"


async def test_a_new_project_follows_every_default(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    manager = await headers_for(async_client, "manager")
    project = await add_project(async_client, manager)

    settings = await pay_settings(async_client, manager, project["id"])

    assert settings == {
        "pay_enabled": True,
        "managers_see_pay": False,
        "settings": DEFAULTS,
        "own": [],
    }


async def test_an_admin_overrides_a_setting_and_returns_it_to_the_default(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin)
    url = project_pay(project["id"])

    changed = await async_client.patch(
        url,
        json={
            "pay_enabled": False,
            "managers_see_pay": True,
            "settings": {"day_normal_minutes": 540},
        },
        headers=admin,
    )
    assert changed.status_code == 200, changed.text
    assert changed.json() == {
        "pay_enabled": False,
        "managers_see_pay": True,
        "settings": {**DEFAULTS, "day_normal_minutes": 540},
        "own": ["day_normal_minutes"],
    }

    reset = await async_client.patch(
        url, json={"settings": {"day_normal_minutes": None}}, headers=admin
    )
    assert reset.json()["settings"] == DEFAULTS
    assert reset.json()["own"] == []


async def test_a_default_change_reaches_projects_that_follow_it_only(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    following = await add_project(async_client, admin, "Follows")
    own = await add_project(async_client, admin, "Own")
    await async_client.patch(
        project_pay(own["id"]), json={"settings": {"overtime1_pct": "125.00"}}, headers=admin
    )

    await async_client.patch(ORG_PAY, json={"overtime1_pct": "130.00"}, headers=admin)

    follows = await pay_settings(async_client, admin, following["id"])
    keeps = await pay_settings(async_client, admin, own["id"])
    assert follows["settings"]["overtime1_pct"] == "130.00"
    assert keeps["settings"]["overtime1_pct"] == "125.00"


async def test_ending_a_project_freezes_its_settings_and_days(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin)
    holiday = await async_client.post(
        SPECIAL_DAYS, json={"day": "2027-09-11", "name": "Yom Kippur"}, headers=admin
    )
    assert holiday.status_code == 201, holiday.text
    election = await async_client.post(
        SPECIAL_DAYS,
        json={"day": "2027-10-26", "name": "Election day", "rate_pct": "200.00"},
        headers=admin,
    )
    assert election.status_code == 201, election.text

    await end_project(async_client, admin, project["id"])
    await async_client.patch(ORG_PAY, json={"overtime1_pct": "130.00"}, headers=admin)
    await async_client.patch(
        f"{SPECIAL_DAYS}/{holiday.json()['id']}", json={"rate_pct": "200.00"}, headers=admin
    )
    later = await async_client.post(
        SPECIAL_DAYS, json={"day": "2027-12-01", "name": "Added later"}, headers=admin
    )
    assert later.status_code == 201, later.text

    frozen = await pay_settings(async_client, admin, project["id"])
    assert frozen["settings"] == DEFAULTS
    assert sorted(frozen["own"]) == sorted(DEFAULTS)
    days = (await async_client.get(project_days(project["id"]), headers=admin)).json()["items"]
    assert [(d["day"], d["rate_pct"], d["source"]) for d in days] == [
        ("2027-09-11", "150.00", "project"),
        ("2027-10-26", "200.00", "project"),
    ]


async def test_an_ended_project_cannot_be_changed(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin)
    await end_project(async_client, admin, project["id"])

    answer = await async_client.patch(
        project_pay(project["id"]), json={"pay_enabled": False}, headers=admin
    )

    assert answer.status_code == 409
    assert answer.json()["code"] == "pay.project_ended"
    assert (await pay_settings(async_client, admin, project["id"]))["pay_enabled"] is True


async def test_a_reopened_project_keeps_its_frozen_values_as_its_own(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin)
    await end_project(async_client, admin, project["id"])
    await async_client.patch(
        f"/api/v1/projects/{project['id']}", json={"status": "active"}, headers=admin
    )

    await async_client.patch(ORG_PAY, json={"rest_pct": "175.00"}, headers=admin)
    reopened = await pay_settings(async_client, admin, project["id"])
    reset = await async_client.patch(
        project_pay(project["id"]), json={"settings": {"rest_pct": None}}, headers=admin
    )

    assert reopened["settings"]["rest_pct"] == "150.00"
    assert reset.json()["settings"]["rest_pct"] == "175.00"


async def test_an_admin_creates_a_project_with_pay_settings(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin)
    body = {
        "client_id": project["client_id"],
        "site_id": project["site_id"],
        "name": "Paid",
        "type": "training",
        "billing_rate": "80.0000",
        "start_date": "2027-02-01",
        "pay_settings": {"pay_enabled": False, "settings": {"rest_pct": "200.00"}},
    }

    created = await async_client.post("/api/v1/projects", json=body, headers=admin)

    assert created.status_code == 201, created.text
    settings = await pay_settings(async_client, admin, created.json()["id"])
    assert settings["pay_enabled"] is False
    assert settings["settings"]["rest_pct"] == "200.00"
    assert settings["own"] == ["rest_pct"]


async def test_a_manager_cannot_create_a_project_with_pay_settings(
    async_client: AsyncClient, seed_users: dict[str, User]
) -> None:
    manager = await headers_for(async_client, "manager")
    project = await add_project(async_client, manager)
    body = {
        "client_id": project["client_id"],
        "site_id": project["site_id"],
        "name": "Sneaky",
        "type": "security",
        "billing_rate": "80.0000",
        "start_date": "2027-02-01",
        "pay_settings": {"managers_see_pay": True},
    }

    answer = await async_client.post("/api/v1/projects", json=body, headers=manager)

    assert answer.status_code == 403
    assert answer.json()["code"] == "projects.pay_settings_admin_only"


@pytest.mark.parametrize("role", ["manager", "employee", "client"])
async def test_only_an_admin_changes_a_projects_pay_settings(
    async_client: AsyncClient, seed_users: dict[str, User], role: str
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin)

    answer = await async_client.patch(
        project_pay(project["id"]),
        json={"managers_see_pay": True},
        headers=await headers_for(async_client, role),
    )

    assert answer.status_code == 403
    assert (await pay_settings(async_client, admin, project["id"]))["managers_see_pay"] is False


@pytest.mark.parametrize("role", ["employee", "client"])
async def test_employees_and_clients_cannot_read_a_projects_pay_settings(
    async_client: AsyncClient, seed_users: dict[str, User], role: str
) -> None:
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin)

    answer = await async_client.get(
        project_pay(project["id"]), headers=await headers_for(async_client, role)
    )

    assert answer.status_code == 403


async def test_another_organisations_project_is_not_found(
    async_client: AsyncClient, seed_users: dict[str, User], other_org: OtherOrg
) -> None:
    admin = await headers_for(async_client, "admin")
    assert other_org.project is not None
    url = project_pay(str(other_org.project.id))

    read = await async_client.get(url, headers=admin)
    changed = await async_client.patch(url, json={"pay_enabled": False}, headers=admin)

    assert (read.status_code, changed.status_code) == (404, 404)


@pytest.mark.parametrize("value", ["true", 1, 0, "yes"])
async def test_a_switch_must_be_a_json_boolean(
    async_client: AsyncClient, seed_users: dict[str, User], value: Any
) -> None:
    """Lax booleans would read "true" or 1 as true; the schema says boolean (contract finding)."""
    admin = await headers_for(async_client, "admin")
    project = await add_project(async_client, admin)

    answer = await async_client.patch(
        project_pay(project["id"]), json={"managers_see_pay": value}, headers=admin
    )
    created = await async_client.post(
        "/api/v1/projects",
        json={
            "client_id": project["client_id"],
            "site_id": project["site_id"],
            "name": "Lax",
            "type": "security",
            "billing_rate": "80.0000",
            "start_date": "2027-02-01",
            "pay_settings": {"pay_enabled": value},
        },
        headers=admin,
    )

    assert (answer.status_code, created.status_code) == (422, 422)
