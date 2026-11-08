"""Helpers for the schedule generation tests of feature 004.

A project in Israel that uses the three default shift types with staffing needs, a job asked for
through the API, and one pass of the worker on the test database.
"""

import uuid
from datetime import date, timedelta
from typing import Any

from httpx import AsyncClient

from hds.core.database import get_session_factory
from hds.main import app
from hds.worker import run_once
from tests.pay_support import project_managers
from tests.roster_support import PASSWORD, add_project

JOBS = "/api/v1/schedule-jobs"
SUNDAY = date(2027, 6, 6)


async def staffed_project(
    client: AsyncClient,
    headers: dict[str, str],
    name: str = "Night Watch",
    needs: dict[str, int] | None = None,
    managed: bool = True,
) -> dict[str, Any]:
    """A project with a need on every weekday for each of ``needs`` (shift kind to headcount).

    The seeded Manager manages it unless ``managed`` is false: a Manager schedules only the
    projects they manage (007 T108).
    """
    project = await add_project(client, headers, name)
    if managed:
        await manage(client, project)
    wanted = needs if needs is not None else {"morning": 1}
    items = [
        {"shift_type_id": project["types"][kind], "weekday": d, "headcount": count}
        for kind, count in wanted.items()
        for d in range(7)
    ]
    answer = await client.put(
        f"/api/v1/projects/{project['id']}/requirements", json={"items": items}, headers=headers
    )
    assert answer.status_code == 200, answer.text
    return project


async def manage(client: AsyncClient, project: dict[str, Any]) -> None:
    """Make the seeded Manager a manager of ``project``, when both seeded accounts exist."""
    tokens = {}
    for role in ("admin", "manager"):
        login = await client.post(
            "/auth/login", json={"email": f"{role}@hds.test", "password": PASSWORD}
        )
        if login.status_code != 200:
            return
        tokens[role] = {"Authorization": f"Bearer {login.json()['access_token']}"}
    me = (await client.get("/me", headers=tokens["manager"])).json()
    answer = await client.put(
        project_managers(project["id"]), json={"user_ids": [me["id"]]}, headers=tokens["admin"]
    )
    assert answer.status_code == 200, answer.text


async def ask(
    client: AsyncClient,
    headers: dict[str, str],
    first: date = SUNDAY,
    last: date | None = None,
    **extra: Any,
) -> Any:
    """Ask for a roster for ``first`` to ``last`` (a week by default); return the answer."""
    body = {"from": first.isoformat(), "to": (last or first + timedelta(days=6)).isoformat()}
    return await client.post(JOBS, json={**body, **extra}, headers=headers)


async def work() -> uuid.UUID | None:
    """Run one pass of the worker on the test database; the id of the job it ran, if any."""
    factory = app.dependency_overrides[get_session_factory]()
    return await run_once(factory)
