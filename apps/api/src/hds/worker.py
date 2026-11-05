"""The schedule worker: claims queued jobs and solves them, outside the API (004 research G7).

Run with ``uv run python -m hds.worker``, on the API's ``DATABASE_URL``. Layer: entry point, like
``hds.main``; it builds services through ``hds.wiring`` and holds no rule of its own.

Each job takes three short transactions: claim it (``FOR UPDATE SKIP LOCKED``, so several workers
can run), build the problem, then store the proposal or the failure. The solve itself runs outside
any transaction, in a thread. The worker waits on ``LISTEN schedule_jobs`` and also looks every
few seconds, so a missed notification only delays a job.
"""

import asyncio
import logging
import uuid
from datetime import timedelta

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from hds.core.config import get_settings
from hds.repositories.schedule_job import CHANNEL
from hds.services.schedule_job import JobFailedError, solve
from hds.wiring import job_service

logger = logging.getLogger("hds.worker")
POLL_SECONDS = 5


async def run_once(factory: async_sessionmaker[AsyncSession]) -> uuid.UUID | None:
    """Claim and run one job; its id, or ``None`` when nothing was waiting.

    A failure in building or solving marks the job failed (``jobs.solver_failed``) and changes
    nothing else; it never stops the worker.
    """
    async with factory() as session:
        claim = await job_service(session).claim()
        await session.commit()
    if claim is None:
        return None
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
        return claim.id
    try:
        async with factory() as session:
            if not await job_service(session).succeed(claim, built, result):
                logger.warning("schedule job %s was claimed again before it finished", claim.id)
            await session.commit()
    except Exception:
        # A result that cannot be stored fails the job now, with its cause, rather than being
        # solved again three times and reported as lost.
        logger.exception("schedule job %s could not store its result", claim.id)
        async with factory() as session:
            await job_service(session).fail(claim, "jobs.solver_failed")
            await session.commit()
    return claim.id


async def run_forever(factory: async_sessionmaker[AsyncSession]) -> None:
    """Run jobs as they arrive, until cancelled."""
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
            try:
                while await run_once(factory) is not None:
                    pass
            except Exception:
                # A database error (a dropped connection, a failed commit) must not stop the
                # worker: the job keeps its lease and is claimed again after it.
                logger.exception("schedule worker step failed; carrying on")
            try:
                await asyncio.wait_for(wake.wait(), POLL_SECONDS)
            except TimeoutError:
                pass


def main() -> None:
    """Start the worker on the configured database."""
    from hds.core.database import get_session_factory

    logging.basicConfig(level=logging.INFO)
    logger.info("schedule worker started")
    asyncio.run(run_forever(get_session_factory()))


if __name__ == "__main__":
    main()
