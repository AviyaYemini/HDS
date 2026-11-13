"""A date in a query string is ``YYYY-MM-DD`` text only (Schemathesis, PR 28).

FastAPI drops a ``BeforeValidator`` when the parameter's default is ``Query(...)``, so lax parsing
read ``0`` or ``+0.0`` as 1970-01-01. The routers declare dates as ``Annotated[IsoDate, Query()]``,
which keeps it: a number-like date is refused with 422 on every one of them.
"""

import uuid

import pytest
from httpx import AsyncClient

from hds.models.user import User
from tests.roster_support import headers_for

PROJECT = uuid.uuid4()

PATHS = [
    "/api/v1/shifts?from=0&to=2026-01-07",
    "/api/v1/shifts?from=2026-01-01&to=%2B0.0",
    "/me/shifts?from=0",
    "/api/v1/reports/work-log?from=%2B0.0&to=2026-01-31",
    "/api/v1/reports/hours?from=0&to=2026-01-31",
    "/api/v1/reports/overview?from=2026-01-01&to=0",
    "/api/v1/availability-windows?from=0",
    f"/api/v1/projects/{PROJECT}/publish-preview?from=%2B0.0&to=2026-01-07",
    "/api/v1/roster/warnings?from=0&to=2026-01-07",
]


@pytest.mark.parametrize("path", PATHS)
async def test_a_number_like_date_in_a_query_is_refused(
    async_client: AsyncClient, seed_users: dict[str, User], path: str
) -> None:
    admin = await headers_for(async_client, "admin")
    response = await async_client.get(path, headers=admin)
    assert response.status_code == 422, (path, response.status_code, response.text[:200])
