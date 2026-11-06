"""Reset jobs are capped per worker, so a burst cannot take every database connection (T422).

A job runs after its response, so nothing else limits how many exist. Past the cap one is dropped:
the caller already has its answer, and the alternative is an unbounded queue.

Covers: FR-228.
"""

import asyncio
import logging
import time

import pytest

from hds.core import background
from hds.core.background import run_reset_job
from hds.core.config import get_settings


@pytest.fixture(autouse=True)
def cap(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(get_settings(), "RESET_JOB_CONCURRENCY", 1)
    monkeypatch.setattr(get_settings(), "RESET_JOB_QUEUE_LIMIT", 1)


class Gate:
    """A job that records when it runs and waits to be released."""

    def __init__(self) -> None:
        self.running = 0
        self.most = 0
        self.ran = 0
        self.release = asyncio.Event()

    async def __call__(self) -> None:
        self.ran += 1
        self.running += 1
        self.most = max(self.most, self.running)
        await self.release.wait()
        self.running -= 1


async def settle() -> None:
    for _ in range(5):
        await asyncio.sleep(0)


async def test_no_more_than_the_limit_run_at_once() -> None:
    gate = Gate()
    tasks = [asyncio.create_task(run_reset_job(gate)) for _ in range(2)]
    await settle()

    assert gate.ran == 1

    gate.release.set()
    await asyncio.gather(*tasks)
    assert gate.ran == 2
    assert gate.most == 1


async def test_past_the_running_and_waiting_cap_a_job_is_dropped_and_logged_by_type(
    caplog: pytest.LogCaptureFixture,
) -> None:
    gate = Gate()
    tasks = [asyncio.create_task(run_reset_job(gate)) for _ in range(2)]
    await settle()

    with caplog.at_level(logging.WARNING, logger="hds.background"):
        await asyncio.wait_for(run_reset_job(gate), 1)  # dropped at once, never left waiting

    gate.release.set()
    await asyncio.gather(*tasks)
    assert gate.ran == 2
    assert [r.getMessage() for r in caplog.records] == [
        f"background job dropped: {background.RESET_JOB}"
    ]


async def test_a_failed_job_gives_its_place_back_and_its_error_reaches_the_caller() -> None:
    async def failing() -> None:
        raise RuntimeError("boom")

    for _ in range(3):
        with pytest.raises(RuntimeError):
            await run_reset_job(failing)

    gate = Gate()
    gate.release.set()
    await run_reset_job(gate)
    assert gate.ran == 1


@pytest.mark.wall_clock
async def test_a_job_that_hangs_is_stopped_and_gives_its_place_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(get_settings(), "RESET_JOB_TIMEOUT_SECONDS", 0.01)

    async def hung() -> None:
        await asyncio.Event().wait()

    started = time.perf_counter()
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(run_reset_job(hung), 5)
    assert time.perf_counter() - started < 2, "the job's own limit must stop it"

    gate = Gate()
    gate.release.set()
    await run_reset_job(gate)
    assert gate.ran == 1


async def test_each_event_loop_has_its_own_allowance() -> None:
    gate = Gate()
    gate.release.set()
    await run_reset_job(gate)  # would raise on a semaphore tied to another test's loop
    assert gate.ran == 1
