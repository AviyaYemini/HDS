"""The schedule worker's operations port (T017): alive, ready and its job metrics.

Alive while the loop turned in the last 30 seconds, or while the worker holds a job still inside its
lease: a solve can run for a minute with the loop awaiting it, and must not be killed for that.
Ready adds the database. The metrics count jobs by state, read when they are scraped.

Covers: specs/006-deploy-operations FR-006, FR-008, FR-010; contracts/ops.md sections 1 and 2.
"""

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta

from httpx import ASGITransport, AsyncClient

from hds.core.health import Migrations, WorkerPulse
from hds.core.metrics import JobMetrics
from hds.services.health import HealthService
from hds.worker import ops_app

NOW = datetime(2026, 11, 6, 12, 0, tzinfo=UTC)


class Clock:
    """A monotonic nanosecond clock the test moves by hand, in seconds."""

    def __init__(self) -> None:
        self.value = 1000.0

    def __call__(self) -> int:
        return int(self.value * 1_000_000_000)


class Probe:
    """A database that answers, or not."""

    def __init__(self, up: bool = True) -> None:
        self.up = up

    async def ping(self) -> None:
        if not self.up:
            raise OSError("down")

    async def revisions(self) -> list[str]:
        return ["head"]


def health(probe: Probe) -> HealthService:
    """A health service on ``probe``."""

    @asynccontextmanager
    async def open_probe() -> AsyncIterator[Probe]:
        yield probe

    return HealthService(open_probe, Migrations(frozenset({"head"}), "head"), version="w1")


async def counts() -> dict[str, int]:
    """Job counts as the table would give them."""
    return {"queued": 2, "running": 0, "succeeded": 5, "failed": 1}


async def get(app: object, path: str) -> tuple[int, str]:
    """GET ``path`` from the ops app."""
    transport = ASGITransport(app=app)  # type: ignore[arg-type]
    async with AsyncClient(transport=transport, base_url="http://ops") as client:
        response = await client.get(path)
        return response.status_code, response.text


def pulse(clock: Clock) -> WorkerPulse:
    """A pulse on ``clock`` that has just beaten."""
    return WorkerPulse(monotonic_ns=clock)


async def test_alive_while_the_loop_turns_and_not_once_it_stops() -> None:
    clock = Clock()
    beating = pulse(clock)
    app = ops_app(beating, health(Probe()), counts, JobMetrics("w1"), now=lambda: NOW)
    assert (await get(app, "/livez"))[0] == 200
    clock.value += 29
    assert (await get(app, "/livez"))[0] == 200
    clock.value += 2
    assert (await get(app, "/livez"))[0] == 503
    beating.beat()
    assert (await get(app, "/livez"))[0] == 200


async def test_a_held_job_keeps_the_worker_alive_through_a_long_solve() -> None:
    clock = Clock()
    beating = pulse(clock)
    current = [NOW]
    app = ops_app(beating, health(Probe()), counts, JobMetrics("w1"), now=lambda: current[0])
    beating.hold(NOW + timedelta(minutes=5))
    clock.value += 60  # a 60-second solve: the loop has not turned since the claim
    current[0] = NOW + timedelta(seconds=60)
    assert (await get(app, "/livez"))[0] == 200
    current[0] = NOW + timedelta(minutes=5, seconds=1)  # stuck past its lease
    assert (await get(app, "/livez"))[0] == 503
    beating.release()
    assert (await get(app, "/livez"))[0] == 503


async def test_ready_adds_the_database() -> None:
    clock = Clock()
    up = ops_app(pulse(clock), health(Probe(up=True)), counts, JobMetrics("w1"), now=lambda: NOW)
    down = ops_app(pulse(clock), health(Probe(up=False)), counts, JobMetrics("w1"), now=lambda: NOW)
    assert (await get(up, "/readyz"))[0] == 200
    status, text = await get(down, "/readyz")
    assert status == 503
    assert '"database"' in text


async def test_the_metrics_count_jobs_by_state_and_time_them() -> None:
    metrics = JobMetrics("w1")
    metrics.observe("succeeded", timedelta(seconds=12.5))
    app = ops_app(pulse(Clock()), health(Probe()), counts, metrics, now=lambda: NOW)
    status, text = await get(app, "/metrics")
    assert status == 200
    for state, value in (await counts()).items():
        assert f'hds_schedule_jobs{{state="{state}"}} {float(value)}' in text
    assert 'hds_schedule_job_duration_seconds_count{outcome="succeeded"} 1.0' in text
    assert 'hds_build_info{version="w1"} 1.0' in text


async def test_a_stuck_loop_answers_in_the_api_s_shape_naming_the_loop() -> None:
    clock = Clock()
    app = ops_app(pulse(clock), health(Probe()), counts, JobMetrics("w1"), now=lambda: NOW)
    clock.value += 31
    for path in ("/livez", "/readyz"):
        status, text = await get(app, path)
        assert status == 503
        assert json.loads(text) == {
            "status": "not_ready",
            "version": "w1",
            "failing": ["worker_loop"],
        }
