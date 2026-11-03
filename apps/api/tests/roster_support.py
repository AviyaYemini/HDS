"""Helpers shared by the availability and roster tests of feature 003 Part 2.

Signing in, opening submission windows, adding availability entries and finding the default
shift types, through the API where it exists so the tests exercise it.
"""

import uuid
from datetime import UTC, date, datetime, timedelta
from typing import Any

from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

PASSWORD = "Dev1234!"
WINDOWS = "/api/v1/availability-windows"
FIRST = date(2027, 6, 6)  # a Sunday, well in the future
LAST = date(2027, 6, 12)


async def headers_for(client: AsyncClient, role: str) -> dict[str, str]:
    """Sign in as the seeded account of ``role`` and return its bearer header."""
    response = await client.post(
        "/auth/login", json={"email": f"{role}@hds.test", "password": PASSWORD}
    )
    assert response.status_code == 200, response.text
    return {"Authorization": f"Bearer {response.json()['access_token']}"}


async def open_window(
    client: AsyncClient,
    headers: dict[str, str],
    first: date = FIRST,
    last: date = LAST,
    deadline: datetime | None = None,
) -> Any:
    """Open a window for ``first`` to ``last``, closing a day from now unless told otherwise."""
    response = await client.post(
        WINDOWS,
        json={
            "starts_on": first.isoformat(),
            "ends_on": last.isoformat(),
            "deadline": (deadline or datetime.now(UTC) + timedelta(days=1)).isoformat(),
        },
        headers=headers,
    )
    assert response.status_code == 201, response.text
    return response.json()


def constraints(user_id: uuid.UUID | str) -> str:
    """The path of an employee's availability entries."""
    return f"/api/v1/employees/{user_id}/constraints"


def body(
    kind: str = "blocked",
    first: date = FIRST,
    last: date | None = None,
    **extra: Any,
) -> dict[str, Any]:
    """An entry body; ``last`` defaults to ``first``."""
    return {
        "kind": kind,
        "starts_on": first.isoformat(),
        "ends_on": (last or first).isoformat(),
        **extra,
    }


async def shift_type_id(db: AsyncSession, org_id: uuid.UUID, name: str = "morning") -> str:
    """The id of one of the organisation's default shift types."""
    await db.rollback()
    found = await db.execute(
        text("SELECT id FROM shift_types WHERE organization_id = :o AND name = :n"),
        {"o": org_id, "n": name},
    )
    return str(found.scalar_one())


async def close_window(db: AsyncSession, window_id: str) -> None:
    """Move a window's deadline into the past, as if it had passed."""
    await db.execute(
        text("UPDATE submission_windows SET deadline = now() - interval '1 hour' WHERE id = :i"),
        {"i": window_id},
    )
    await db.commit()
