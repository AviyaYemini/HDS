"""A seed waits for the weekly job's lifecycle (stabilization PR 3, review R35)."""

import asyncio
import datetime
import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

from hds.scripts import seed_wide


@pytest.mark.parametrize("terminal", ["succeeded", "failed", "cancelled"])
async def test_week_waits_past_a_lease_and_preserves_terminal_failure(
    monkeypatch: pytest.MonkeyPatch, terminal: str
) -> None:
    first = datetime.date(2026, 11, 8)
    now = datetime.datetime(2026, 11, 9, tzinfo=datetime.UTC)
    job_id = uuid.uuid4()
    project = MagicMock(id=uuid.uuid4())
    job = MagicMock(id=job_id, project_ids=[str(project.id)], status="running", proposal_state=None)
    # The existing job is usable, then remains running, is released to the queue, and finishes.
    found = MagicMock()
    found.scalars.return_value = [job]
    statuses = []
    for status in ("running", "queued", terminal):
        result = MagicMock()
        result.one.return_value = (status, "jobs.worker_lost" if status == "failed" else None)
        statuses.append(result)
    session = AsyncMock()
    session.execute.side_effect = [found, *statuses]
    factory = MagicMock()
    factory.return_value.__aenter__ = AsyncMock(return_value=session)
    factory.return_value.__aexit__ = AsyncMock(return_value=False)
    clock = MagicMock(wraps=datetime)
    # Any fixed deadline based on the initial lease has passed on the next clock read.
    clock.datetime.now.side_effect = [now, now + datetime.timedelta(hours=1)]
    monkeypatch.setattr(seed_wide, "datetime", clock)
    run_once = AsyncMock(side_effect=[None, None, job_id, None])
    monkeypatch.setattr(seed_wide, "run_once", run_once)
    sleep = AsyncMock()
    monkeypatch.setattr(asyncio, "sleep", sleep)

    if terminal == "succeeded":
        assert (
            await seed_wide._week(factory, MagicMock(), [project], first, publish=False) == job_id
        )
    else:
        error = "jobs.worker_lost" if terminal == "failed" else None
        with pytest.raises(RuntimeError) as raised:
            await seed_wide._week(factory, MagicMock(), [project], first, publish=False)
        assert str(raised.value) == f"the run for the week of {first} ended {terminal} ({error})"

    assert sleep.await_count == 2
    assert run_once.await_count == 4
    session.commit.assert_not_awaited()  # The existing job was reused.
