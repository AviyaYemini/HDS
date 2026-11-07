"""The worker's shutdown steps, with a scripted job service (006 T031, review R34 to R40).

Once the worker is told to stop it claims nothing, hands back a claimed job before building or
solving it, and logs a hand-back that found the job already reclaimed as superseded. A hand-back
that cannot be stored never marks the job failed: its lease simply runs out. An operations server
that ends on its own makes the worker drain and then exit with an error, not 0.

Covers: 006 FR-020; research R5.
"""

import asyncio
import logging
from typing import Any, cast

import pytest

import hds.worker as worker
from hds.services.schedule_job import Claim
from tests.unit.test_worker_outcomes import CLAIM, FakeJobs, fake_session, install


class StoppingJobs(FakeJobs):
    """The scripted job service, recording what the worker asked of it."""

    def __init__(self, *, release_result: bool = True, release_raises: bool = False) -> None:
        super().__init__()
        self.calls: list[str] = []
        self.release_result = release_result
        self.release_raises = release_raises
        self.stopping: asyncio.Event | None = None
        self.stop_at = ""

    def _step(self, name: str) -> None:
        self.calls.append(name)
        if self.stopping is not None and name == self.stop_at:
            self.stopping.set()

    async def claim(self) -> Claim:
        self._step("claim")
        return CLAIM

    async def build(self, claim: Claim) -> object:
        self._step("build")
        return object()

    async def release(self, claim: Claim) -> bool:
        self._step("release")
        if self.release_raises:
            raise OSError("connection reset")
        return self.release_result

    async def succeed(self, claim: Claim, built: object, result: object) -> bool:
        self._step("succeed")
        return True

    async def fail(self, claim: Claim, code: str) -> None:
        self._step(f"fail {code}")


async def run(
    monkeypatch: pytest.MonkeyPatch, jobs: StoppingJobs, stop_at: str
) -> tuple[Any, list[str]]:
    stopping = asyncio.Event()
    jobs.stopping, jobs.stop_at = stopping, stop_at
    ops = install(monkeypatch, jobs)
    ran = await worker.run_once(cast(Any, fake_session), ops, stopping)
    return ran, jobs.calls


async def test_a_job_claimed_as_the_stop_arrives_is_handed_back_unbuilt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Review R37: T031's release is for a job "claimed but not yet built".
    _, calls = await run(monkeypatch, StoppingJobs(), stop_at="claim")
    assert calls == ["claim", "release"]


async def test_a_job_built_as_the_stop_arrives_is_handed_back_unsolved(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, calls = await run(monkeypatch, StoppingJobs(), stop_at="build")
    assert calls == ["claim", "build", "release"]


async def test_a_hand_back_that_fails_never_fails_the_job(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    # Review R34: the job was never solved; its lease runs out and another worker takes it.
    _, calls = await run(monkeypatch, StoppingJobs(release_raises=True), stop_at="build")
    assert calls == ["claim", "build", "release"]
    assert not any(c.startswith("fail") for c in calls)
    assert "lease" in caplog.text


async def test_a_hand_back_of_a_job_already_reclaimed_is_logged_as_superseded(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    # Review R38: release matched no row, so nothing was handed back.
    caplog.set_level(logging.INFO, logger="hds.worker")
    await run(monkeypatch, StoppingJobs(release_result=False), stop_at="build")
    assert "claimed again" in caplog.text
    assert "handed back" not in caplog.text


async def test_nothing_is_claimed_once_the_worker_stops(monkeypatch: pytest.MonkeyPatch) -> None:
    jobs = StoppingJobs()
    ops = install(monkeypatch, jobs)
    stopping = asyncio.Event()
    stopping.set()
    assert await worker.run_once(cast(Any, fake_session), ops, stopping) is None
    assert jobs.calls == []


async def test_an_ops_server_that_ends_on_its_own_drains_and_fails_the_process(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Review R30 and R40: the worker drains, then exits with an error, not 0.
    drained: list[bool] = []

    async def loop(factory: object, ops: object, stopping: asyncio.Event) -> None:
        await stopping.wait()
        drained.append(True)

    async def ends_at_once(self: object, sockets: object = None) -> None:
        return None

    monkeypatch.setattr(worker, "migrations", lambda: None)
    monkeypatch.setattr(worker, "health_service", lambda *a, **k: object())
    monkeypatch.setattr(worker, "ops_app", lambda *a, **k: object())
    monkeypatch.setattr(worker, "run_forever", loop)
    monkeypatch.setattr(worker._OpsServer, "serve", ends_at_once)
    monkeypatch.setattr("hds.core.database.get_health_session_factory", lambda: None)
    with pytest.raises(RuntimeError, match="operations server"):
        await worker.serve(cast(Any, fake_session), 9999)
    assert drained == [True]


async def test_a_taken_ops_port_still_lets_the_current_job_finish(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # T032b, review R45: uvicorn calls sys.exit(1) when its port is taken. That SystemExit must not
    # escape the loop and cancel the job; the job finishes, then the worker exits with an error.
    events: list[str] = []

    async def loop(factory: object, ops: object, stopping: asyncio.Event) -> None:
        events.append("job started")
        await asyncio.sleep(0.2)  # the job runs on while the ops server fails
        events.append("job stored")
        await stopping.wait()

    async def port_taken(self: object, sockets: object = None) -> None:
        await asyncio.sleep(0.05)
        raise SystemExit(1)

    monkeypatch.setattr(worker, "migrations", lambda: None)
    monkeypatch.setattr(worker, "health_service", lambda *a, **k: object())
    monkeypatch.setattr(worker, "ops_app", lambda *a, **k: object())
    monkeypatch.setattr(worker, "run_forever", loop)
    monkeypatch.setattr(worker._OpsServer, "serve", port_taken)
    monkeypatch.setattr("hds.core.database.get_health_session_factory", lambda: None)
    with pytest.raises(RuntimeError, match="operations server"):
        await worker.serve(cast(Any, fake_session), 9999)
    assert events == ["job started", "job stored"]
