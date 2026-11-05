"""Helpers for the report tests of feature 005: published, ended shifts with people on them.

Shifts are made in October 2026, which has ended, at a site in Israel (UTC+3 until 2026-10-25).
"""

from typing import Any

from httpx import AsyncClient

from tests.integration.test_my_pay import published_shift

WORK_LOG = "/api/v1/reports/work-log"
HOURS = "/api/v1/reports/hours"


def day_shift(day: int) -> tuple[str, str]:
    """08:00 to 16:00 in Israel on 2026-10-``day`` (before the 25th), as UTC text."""
    return f"2026-10-{day:02d}T05:00:00Z", f"2026-10-{day:02d}T13:00:00Z"


async def worked(
    client: AsyncClient,
    admin: dict[str, str],
    project: dict[str, Any],
    day: int,
    user_id: Any,
    **extra: Any,
) -> dict[str, Any]:
    """One 8-hour published day shift on 2026-10-``day`` with ``user_id`` on it."""
    start, end = day_shift(day)
    return await published_shift(client, admin, project, start, end, user_id, **extra)


async def employee(
    client: AsyncClient,
    admin: dict[str, str],
    name: str,
    rate: str | None = "50.0000",
) -> str:
    """A new active employee called ``name``, with ``rate``; their id."""
    email = name.lower().replace(" ", ".") + "@hds.test"
    created = await client.post(
        "/api/v1/employees",
        json={
            "email": email,
            "full_name": name,
            "initial_password": "a-long-password-1",
            "default_hourly_rate": rate,
        },
        headers=admin,
    )
    assert created.status_code == 201, created.text
    return str(created.json()["id"])
