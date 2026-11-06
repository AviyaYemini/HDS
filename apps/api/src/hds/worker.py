"""The schedule worker: claims queued jobs and solves them, outside the API (004 research G7).

Run with ``uv run python -m hds.worker``, on the API's ``DATABASE_URL``. Layer: entry point, like
``hds.main``; it builds services through ``hds.wiring`` and holds no rule of its own.

Each job takes three short transactions: claim it (``FOR UPDATE SKIP LOCKED``, so several workers
can run), build the problem, then store the proposal or the failure. The solve itself runs outside
any transaction, in a thread. The worker waits on ``LISTEN schedule_jobs`` and also looks every
few seconds, so a missed notification only delays a job.

It also serves an operations app on ``METRICS_PORT`` (specs/006 research R6, contracts/ops.md):
``/livez`` (the loop turned in the last 30 s, or a held job is inside its lease), ``/readyz`` (the
database too) and ``/metrics`` (jobs by state, read from the table when scraped, and how long jobs
took).
"""

import asyncio
import logging
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

import uvicorn
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from hds.core.config import get_settings
from hds.core.health import WorkerPulse
from hds.core.metrics import JobMetrics
from hds.repositories.schedule_job import CHANNEL
from hds.services.health import HealthService
from hds.services.schedule_job import Claim, JobFailedError, solve
from hds.wiring import health_service, job_service, migrations

logger = logging.getLogger("hds.worker")
POLL_SECONDS = 5

#: The failing check the worker's health answers name when its loop is stuck.
LOOP = "worker_loop"


@dataclass
class WorkerOps:
    """What the operations app reads: the loop's pulse and the job metrics."""

    pulse: WorkerPulse = field(default_factory=WorkerPulse)
    metrics: JobMetrics = field(default_factory=lambda: JobMetrics(get_settings().HDS_VERSION))


async def run_once(
    factory: async_sessionmaker[AsyncSession], ops: WorkerOps | None = None
) -> uuid.UUID | None:
    """Claim and run one job; its id, or ``None`` when nothing was waiting.

    A failure in building or solving marks the job failed (``jobs.solver_failed``) and changes
    nothing else; it never stops the worker. With ``ops``, the held job keeps the worker alive
    until its lease ends, and the job's duration and outcome are recorded.
    """
    async with factory() as session:
        claim = await job_service(session).claim()
        await session.commit()
    if claim is None:
        return None
    started = time.monotonic_ns()
    if ops is not None and claim.lease_until is not None:
        ops.pulse.hold(claim.lease_until)
    outcome = "failed"  # also when storing the failure itself raises
    try:
        outcome = await _run(factory, claim)
    finally:
        if ops is not None:
            ops.pulse.release()
            # A result thrown away because another worker reclaimed the job is not counted:
            # that worker counts the job when it finishes.
            if outcome != SUPERSEDED:
                elapsed = timedelta(microseconds=(time.monotonic_ns() - started) / 1000)
                ops.metrics.observe(outcome, elapsed)
    return claim.id


#: The outcome of a run whose result was thrown away because another worker had reclaimed the job.
SUPERSEDED = "superseded"


async def _run(factory: async_sessionmaker[AsyncSession], claim: Claim) -> str:
    """Build, solve and store a claimed job.

    Returns:
        ``succeeded``; ``failed`` once marked failed; or ``superseded`` when another worker had
        reclaimed the job, so this result was not stored.
    """
    limit = timedelta(seconds=get_settings().SCHEDULE_TIME_LIMIT_SECONDS)
    try:
        async with factory() as session:
            built = await job_service(session).build(claim)
            await session.rollback()
        result = await solve(claim, built, limit)
    except Exception as error:
        code = error.code if isinstance(error, JobFailedError) else "jobs.solver_failed"
        logger.exception("schedule job %s failed: %s", claim.id, code)
        async with factory() as session:
            await job_service(session).fail(claim, code)
            await session.commit()
        return "failed"
    try:
        async with factory() as session:
            stored = await job_service(session).succeed(claim, built, result)
            await session.commit()
        if not stored:
            logger.warning("schedule job %s was claimed again before it finished", claim.id)
            return SUPERSEDED
    except Exception:
        # A result that cannot be stored fails the job now, with its cause, rather than being
        # solved again three times and reported as lost.
        logger.exception("schedule job %s could not store its result", claim.id)
        async with factory() as session:
            await job_service(session).fail(claim, "jobs.solver_failed")
            await session.commit()
        return "failed"
    return "succeeded"


async def run_forever(
    factory: async_sessionmaker[AsyncSession], ops: WorkerOps | None = None
) -> None:
    """Run jobs as they arrive, until cancelled; each turn of the loop beats ``ops.pulse``."""
    wake = asyncio.Event()
    engine = factory.kw["bind"]
    async with engine.connect() as listener:
        raw = await listener.get_raw_connection()
        driver = raw.driver_connection
        if driver is not None and hasattr(driver, "add_listener"):
            await driver.add_listener(CHANNEL, lambda *_: wake.set())
        while True:
            # Cleared before draining, so a job announced while the queue is being drained wakes
            # the next wait instead of being lost.
            wake.clear()
            if ops is not None:
                ops.pulse.beat()
            try:
                while await run_once(factory, ops) is not None:
                    if ops is not None:
                        ops.pulse.beat()
            except Exception:
                # A database error (a dropped connection, a failed commit) must not stop the
                # worker: the job keeps its lease and is claimed again after it.
                logger.exception("schedule worker step failed; carrying on")
            try:
                await asyncio.wait_for(wake.wait(), POLL_SECONDS)
            except TimeoutError:
                pass


def ops_app(
    pulse: WorkerPulse,
    health: HealthService,
    counts: Callable[[], Awaitable[dict[str, int]]],
    metrics: JobMetrics,
    *,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> Starlette:
    """The worker's operations app: ``/livez``, ``/readyz`` and ``/metrics`` (contracts/ops.md).

    Args:
        pulse: The loop's pulse and the held job, which ``/livez`` reads.
        health: The database check, which ``/readyz`` adds.
        counts: Reads the jobs by state when ``/metrics`` is scraped.
        metrics: The job metrics to serve.
        now: The current UTC time, for the held job's lease; tests pass a fixed one.

    Returns:
        A Starlette app to serve on ``METRICS_PORT``, never published by an ingress (FR-009).
    """

    def stuck() -> JSONResponse:
        """The API's not-ready shape, naming the loop as the failing check."""
        return JSONResponse(
            {"status": "not_ready", "version": health.live()["version"], "failing": [LOOP]},
            status_code=503,
        )

    async def livez(request: Request) -> JSONResponse:
        """200 while the loop turns or a held job is inside its lease; 503 otherwise."""
        return JSONResponse(health.live()) if pulse.alive(now()) else stuck()

    async def readyz(request: Request) -> JSONResponse:
        """Alive, and the database answers within the limit."""
        if not pulse.alive(now()):
            return stuck()
        ok, body = await health.ready()
        return JSONResponse(body, status_code=200 if ok else 503)

    async def metrics_page(request: Request) -> Response:
        """The job metrics, with the counts by state read from the table now."""
        try:
            metrics.set_counts(await counts())
        except Exception as error:
            # The other metrics still serve; the counts keep their last values.
            logger.warning("job counts unavailable for metrics (%s)", type(error).__name__)
        return Response(generate_latest(metrics.registry), media_type=CONTENT_TYPE_LATEST)

    return Starlette(
        routes=[
            Route("/livez", livez),
            Route("/readyz", readyz),
            Route("/metrics", metrics_page),
        ]
    )


async def serve(factory: async_sessionmaker[AsyncSession], port: int) -> None:
    """Run the job loop and, on ``port`` unless it is 0, the operations app, until cancelled.

    The migration history is read first, so a broken image fails here at boot.
    """
    from hds.core.database import get_health_session_factory

    migrations()
    ops = WorkerOps()

    async def counts() -> dict[str, int]:
        """The jobs by state, on a session of their own."""
        async with factory() as session:
            return await job_service(session).counts_by_state()

    tasks = [asyncio.create_task(run_forever(factory, ops))]
    if port:
        health = health_service(get_health_session_factory(), check_migrations=False)
        app = ops_app(ops.pulse, health, counts, ops.metrics)
        config = uvicorn.Config(app, host="0.0.0.0", port=port, log_level="warning")
        tasks.append(asyncio.create_task(uvicorn.Server(config).serve()))
    done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    for task in tasks:
        task.cancel()
    for task in done:
        task.result()


def main() -> None:
    """Start the worker, and its operations port, on the configured database."""
    from hds.core.database import get_session_factory

    logging.basicConfig(level=logging.INFO)
    logger.info("schedule worker started")
    asyncio.run(serve(get_session_factory(), get_settings().METRICS_PORT))


if __name__ == "__main__":
    main()
