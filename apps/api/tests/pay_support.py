"""Helpers shared by the pay tests of feature 005.

The paths, the organisation's default premium settings as the API shows them, and small calls the
tests repeat: ending a project, reading a project's pay settings.
"""

from typing import Any

from httpx import AsyncClient

ORG_PAY = "/api/v1/organisation/pay-settings"
SPECIAL_DAYS = "/api/v1/special-days"
DEFAULTS: dict[str, Any] = {
    "day_normal_minutes": 480,
    "night_normal_minutes": 420,
    "tier1_minutes": 120,
    "overtime1_pct": "125.00",
    "overtime2_pct": "150.00",
    "rest_window": {
        "start_weekday": 5,
        "start_time": "18:00:00",
        "end_weekday": 6,
        "end_time": "22:00:00",
    },
    "rest_pct": "150.00",
    "increase1_points": "25.00",
    "increase2_points": "50.00",
}


def project_pay(project_id: str) -> str:
    """The path of a project's pay settings."""
    return f"/api/v1/projects/{project_id}/pay-settings"


def project_days(project_id: str) -> str:
    """The path of the special days in effect for a project."""
    return f"/api/v1/projects/{project_id}/special-days"


def project_managers(project_id: str) -> str:
    """The path of a project's managers."""
    return f"/api/v1/projects/{project_id}/managers"


async def end_project(client: AsyncClient, headers: dict[str, str], project_id: str) -> None:
    """Mark a project ended."""
    answer = await client.patch(
        f"/api/v1/projects/{project_id}", json={"status": "ended"}, headers=headers
    )
    assert answer.status_code == 200, answer.text


async def pay_settings(client: AsyncClient, headers: dict[str, str], project_id: str) -> Any:
    """A project's pay settings as the API answers them."""
    answer = await client.get(project_pay(project_id), headers=headers)
    assert answer.status_code == 200, answer.text
    return answer.json()
