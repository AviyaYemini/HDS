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

On SIGTERM (or SIGINT) the worker drains (specs/006 FR-020, research R5): it claims nothing more,
finishes the job it is solving and stores the result, or gives back the lease of a job it has
claimed but not started solving, so the next worker takes it at once; then it exits 0. The chart's
``terminationGracePeriodSeconds: 90`` must exceed the longest solve (``SCHEDULE_TIME_LIMIT_SECONDS``
is at most 60) plus storing, or Kubernetes kills the worker mid-store; that job is then reclaimed
once its lease ends. The operations server runs without uvicorn's own signal handling: the job loop
decides when the process stops.

Each run of a job is a span continued from the trace stored on the job when it was queued, so the
job is one trace from the request to the worker (specs/006 FR-008a, research R8). The worker's
logs are the API's JSON lines, which name that trace.
"""

import asyncio
import contextlib
import logging
import signal
import threading
import time
import uuid
from collections.abc import Awaitable, Callable, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

import uvicorn
from opentelemetry.trace import SpanKind, Status, StatusCode
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from hds.core import trace_context, tracing
from hds.core.config import get_settings
from hds.core.health import WorkerPulse
from hds.core.logging import configure_logging
from hds.core.metrics import JobMetrics
from hds.repositories.schedule_job import CHANNEL
from hds.services.health import HealthService
from hds.services.schedule_job import Claim, JobFailedError, ended_of, solve
from hds.wiring import health_service, job_service, migrations

logger = logging.getLogger("hds.worker")
POLL_SECONDS = 5
#: How often a solving job's best so far is written and its stop flag read (008 FR-027, SC-007).
FOLLOW_SECONDS = 2.0

#: The failing check the worker's health answers name when its loop is stuck.
LOOP = "worker_loop"


@dataclass
class WorkerOps:
    """What the operations app reads: the loop's pulse and the job metrics."""

    pulse: WorkerPulse = field(default_factory=WorkerPulse)
    metrics: JobMetrics = field(default_factory=lambda: JobMetrics(get_settings().HDS_VERSION))


async def run_once(
    factory: async_sessionmaker[AsyncSession],
    ops: WorkerOps | None = None,
    stopping: asyncio.Event | None = None,
) -> uuid.UUID | None:
    """Claim and run one job; its id, or ``None`` when nothing was waiting or the worker stops.

    Once ``stopping`` is set nothing more is claimed, and a job claimed but not yet solving is
    handed back (``release``) instead of solved.

    A failure in building or solving marks the job failed (``jobs.solver_failed``) and changes
    nothing else; it never stops the worker. With ``ops``, the held job keeps the worker alive
    until its lease ends, and the job's duration and outcome are recorded. The run is one span,
    continued from the trace the job was queued with, or a new trace when it has none.
    """
    if stopping is not None and stopping.is_set():
        return None  # no claim after SIGTERM
    async with factory() as session:
        claim = await job_service(session).claim()
        await session.commit()
    if claim is None:
        return None
    started = time.monotonic_ns()
    if ops is not None and claim.lease_until is not None:
        ops.pulse.hold(claim.lease_until)
    outcome = "failed"  # also when storing the failure itself raises
    span = trace_context.tracer().start_as_current_span(
        trace_context.JOB_SPAN,
        context=trace_context.continue_from(claim.trace_context),
        kind=SpanKind.CONSUMER,
        attributes={
            "hds.schedule_job.id": str(claim.id),
            "hds.schedule_job.attempt": claim.attempt,
        },
    )
    try:
        with span as current:
            outcome = await _run(factory, claim, stopping)
            # _run catches the failure itself, so the span is told how the job ended (review R6).
            current.set_attribute("hds.schedule_job.outcome", outcome)
            if outcome == "failed":
                current.set_status(Status(StatusCode.ERROR, "the schedule job failed"))
    finally:
        if ops is not None:
            ops.pulse.release()
            # A result thrown away because another worker reclaimed the job is not counted, nor
            # one handed back unsolved: the worker that finishes the job counts it.
            if outcome not in (SUPERSEDED, HANDED_BACK):
                elapsed = timedelta(microseconds=(time.monotonic_ns() - started) / 1000)
                ops.metrics.observe(outcome, elapsed)
    return claim.id


#: The outcome of a run whose result was thrown away because another worker had reclaimed the job.
SUPERSEDED = "superseded"
#: The outcome of a run stopped before its solve: the job's lease was given back (FR-020).
HANDED_BACK = "handed_back"


async def _run(
    factory: async_sessionmaker[AsyncSession],
    claim: Claim,
    stopping: asyncio.Event | None = None,
) -> str:
    """Build, solve and store a claimed job, or hand it back if the worker stops before the solve.

    Returns:
        ``succeeded``; ``failed`` once marked failed; ``superseded`` when another worker had
        reclaimed the job, so this result was not stored; or ``handed_back``.
    """
    limit = timedelta(seconds=get_settings().SCHEDULE_TIME_LIMIT_SECONDS)
    if stopping is not None and stopping.is_set():
        return await _hand_back(factory, claim)  # told to stop before the build (review R37)
    try:
        async with factory() as session:
            built = await job_service(session).build(claim)
            await session.rollback()
    except Exception as error:
        return await _fail(factory, claim, error)
    if stopping is not None and stopping.is_set():
        return await _hand_back(factory, claim)
    # Best so far and Stop (008 FR-027, FR-028, research P7): the solver's thread notes the places
    # filled; every FOLLOW_SECONDS this loop writes them and reads the stop flag, which the
    # solver polls.
    needed = sum(s.places for s in built.problem.slots)
    best: list[int | None] = [None]  # the latest only, set by the solver's thread
    stop = threading.Event()

    def heard(filled: int, _needed: int) -> None:
        best[0] = filled

    async def follow() -> None:
        written: int | None = None
        while True:
            await asyncio.sleep(FOLLOW_SECONDS)
            now = best[0]
            try:
                async with factory() as session:
                    # The best is written only when it improved, so its time says when (FR-027).
                    halt = await job_service(session).progress(
                        claim, needed, now if now != written else None
                    )
                    await session.commit()
            except Exception:
                logger.exception("schedule job %s: progress not written", claim.id)
                continue
            written = now
            if halt:
                stop.set()

    following = asyncio.create_task(follow())
    try:
        result = await solve(claim, built, limit, progress=heard, should_stop=stop.is_set)
    except Exception as error:
        return await _fail(factory, claim, error)
    finally:
        following.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await following
    if result.stopped and best[0] is None:
        # Stopped before any result: Cancel, with no proposal (FR-028).
        async with factory() as session:
            jobs = job_service(session)
            stored = await jobs.cancel(claim)
            if stored:
                await jobs.recorded(claim, "cancelled", {"before_any_result": True})
            await session.commit()
        return "cancelled" if stored else SUPERSEDED
    try:
        async with factory() as session:
            jobs = job_service(session)
            stored = await jobs.succeed(claim, built, result)
            if stored:
                # How the run ended joins its record (008 FR-030).
                await jobs.recorded(
                    claim,
                    "finished",
                    {
                        "ended": ended_of(claim, result),
                        "filled": sum(len(on) for on in result.assigned.values()),
                        "needed": needed,
                        "optimal": result.optimal,
                        "locks_kept": built.locks,
                    },
                )
            await session.commit()
        if not stored:
            logger.warning("schedule job %s was claimed again before it finished", claim.id)
            return SUPERSEDED
    except Exception:
        # A result that cannot be stored fails the job now, with its cause, rather than being
        # solved again three times and reported as lost.
        logger.exception("schedule job %s could not store its result", claim.id)
        async with factory() as session:
            jobs = job_service(session)
            if await jobs.fail(claim, "jobs.solver_failed"):
                await jobs.recorded(claim, "failed", {"code": "jobs.solver_failed"})
            await session.commit()
        return "failed"
    return "succeeded"


async def _fail(factory: async_sessionmaker[AsyncSession], claim: Claim, error: Exception) -> str:
    """Mark the job failed for an error in building or solving it; ``failed``."""
    code = error.code if isinstance(error, JobFailedError) else "jobs.solver_failed"
    logger.exception("schedule job %s failed: %s", claim.id, code)
    async with factory() as session:
        jobs = job_service(session)
        if await jobs.fail(claim, code):
            await jobs.recorded(claim, "failed", {"code": code})
        await session.commit()
    return "failed"


async def _hand_back(factory: async_sessionmaker[AsyncSession], claim: Claim) -> str:
    """Put an unsolved job back in the queue on shutdown; ``handed_back``, or ``superseded``.

    Not started, so the next worker solves it at once, rather than this one holding the pod's
    shutdown for a whole solve. Outside the solve's error handling: a hand-back that cannot be
    stored leaves the lease to run out, and never fails a job that no solve touched (review R34).
    """
    try:
        async with factory() as session:
            released = await job_service(session).release(claim)
            await session.commit()
    except Exception:
        logger.exception("schedule job %s could not be queued again; its lease runs out", claim.id)
        return HANDED_BACK
    if not released:
        logger.warning("schedule job %s was claimed again before it could be queued", claim.id)
        return SUPERSEDED
    logger.info("schedule job %s handed back on shutdown", claim.id)
    return HANDED_BACK


async def run_forever(
    factory: async_sessionmaker[AsyncSession],
    ops: WorkerOps | None = None,
    stopping: asyncio.Event | None = None,
) -> None:
    """Run jobs as they arrive until ``stopping`` is set (or cancelled), then return.

    Each turn of the loop beats ``ops.pulse``. The current job is finished or handed back first.
    """
    stopping = stopping or asyncio.Event()
    stopped = asyncio.ensure_future(stopping.wait())  # one waiter for the whole loop (R42)
    wake = asyncio.Event()
    engine = factory.kw["bind"]
    async with engine.connect() as listener:
        raw = await listener.get_raw_connection()
        driver = raw.driver_connection
        if driver is not None and hasattr(driver, "add_listener"):
            await driver.add_listener(CHANNEL, lambda *_: wake.set())
        while not stopping.is_set():
            # Cleared before draining, so a job announced while the queue is being drained wakes
            # the next wait instead of being lost.
            wake.clear()
            if ops is not None:
                ops.pulse.beat()
            try:
                while await run_once(factory, ops, stopping) is not None:
                    if ops is not None:
                        ops.pulse.beat()
            except Exception:
                # A database error (a dropped connection, a failed commit) must not stop the
                # worker: the job keeps its lease and is claimed again after it.
                logger.exception("schedule worker step failed; carrying on")
            woken = asyncio.ensure_future(wake.wait())
            await asyncio.wait(
                [woken, stopped], timeout=POLL_SECONDS, return_when=asyncio.FIRST_COMPLETED
            )
            woken.cancel()
    stopped.cancel()


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


class _OpsServer(uvicorn.Server):
    """uvicorn without its own signal handlers; the worker's handler decides when to stop.

    uvicorn's handlers re-raise SIGTERM when the server exits, which cancelled the job loop
    mid-solve (T031).
    """

    @contextlib.contextmanager
    def capture_signals(self) -> Iterator[None]:
        """Install nothing."""
        yield


async def _serve_ops(server: _OpsServer) -> None:
    """Run the operations server; its ``SystemExit`` ends it like any other stop.

    uvicorn calls ``sys.exit(1)`` when its port is taken. Raised in a task, that ``SystemExit``
    would escape the event loop and ``asyncio.run`` would cancel the job loop mid-job, which the
    drain forbids. Ended here instead, it takes the "ended on its own" path: the job finishes, then
    the worker exits non-zero (T032b, review R45).
    """
    try:
        await server.serve()
    except SystemExit as stop:
        logger.error("the operations server could not run (exit %s)", stop.code)


async def serve(factory: async_sessionmaker[AsyncSession], port: int) -> None:
    """Run the job loop and, on ``port`` unless it is 0, the operations app, until SIGTERM.

    The migration history is read first, so a broken image fails here at boot. On SIGTERM or
    SIGINT the loop drains and returns, and only then is the operations server stopped. If the
    operations server ends on its own, the loop is asked to stop too, and still finishes its job.
    """
    from hds.core.database import get_health_session_factory

    migrations()
    ops = WorkerOps()

    async def counts() -> dict[str, int]:
        """The jobs by state, on a session of their own."""
        async with factory() as session:
            return await job_service(session).counts_by_state()

    stopping = asyncio.Event()

    def drain(signum: int) -> None:
        """Stop claiming; the current job is finished or handed back. A second signal forces it.

        The second one takes the signal's default action, as uvicorn does, so an operator can
        still end a long solve; that job is then reclaimed after its lease (review R39).
        """
        if stopping.is_set():
            loop.remove_signal_handler(signum)
            signal.raise_signal(signum)
            return
        logger.info("%s: finishing the current job, then stopping", signal.Signals(signum).name)
        stopping.set()

    loop = asyncio.get_running_loop()
    for signum in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(signum, drain, signum)

    jobs = asyncio.create_task(run_forever(factory, ops, stopping))
    server: _OpsServer | None = None
    ops_task: asyncio.Task[None] | None = None
    ops_ended = False
    if port:
        health = health_service(get_health_session_factory(), check_migrations=False)
        app = ops_app(ops.pulse, health, counts, ops.metrics)
        config = uvicorn.Config(app, host="0.0.0.0", port=port, log_level="warning")
        server = _OpsServer(config)
        ops_task = asyncio.create_task(_serve_ops(server))
        await asyncio.wait([jobs, ops_task], return_when=asyncio.FIRST_COMPLETED)
        ops_ended = ops_task.done() and not stopping.is_set()
        if ops_ended:
            logger.error("the operations server stopped; finishing the current job, then stopping")
            stopping.set()
    await jobs  # the loop decides: never cancelled mid-job
    if server is not None and ops_task is not None:
        server.should_exit = True
        await ops_task
        if ops_ended:
            # Without its probes the worker would be killed anyway; exit non-zero so the
            # failure shows and the orchestrator restarts it (review R30, R40).
            raise RuntimeError("the operations server stopped on its own; the worker drained")


def main() -> None:
    """Start the worker, and its operations port, on the configured database."""
    from hds.core.database import get_session_factory

    configure_logging()
    tracing.configure("hds-worker")
    logger.info("schedule worker started")
    try:
        asyncio.run(serve(get_session_factory(), get_settings().METRICS_PORT))
    finally:
        # Batched spans are sent only on the way out: the last job's trace would lose its worker
        # half (review R7). SIGTERM reaches this once the worker drains on it (006 Group F).
        tracing.shutdown()


if __name__ == "__main__":
    main()
