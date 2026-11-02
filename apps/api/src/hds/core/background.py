"""Capped work that runs after a response.

Layer: core. Depends on ``hds.core.config`` only.

Nothing else limits work done after a response: the caller is gone, so no client waits and nothing
pushes back. Each reset job uses a database connection, and anyone can ask for one, so at most
``RESET_JOB_CONCURRENCY`` run at once in a worker and at most ``RESET_JOB_QUEUE_LIMIT`` more may
wait. A job past that is dropped. The answer has already been sent and is the same either way
(FR-228). A job that runs longer than ``RESET_JOB_TIMEOUT_SECONDS`` is stopped, so a stalled mail
provider cannot hold every place for ever.
"""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from weakref import WeakKeyDictionary

from hds.core.config import get_settings

logger = logging.getLogger("hds.background")

#: What is logged for a dropped job. Never the job's address: anyone can ask for a job.
RESET_JOB = "password_reset"


@dataclass
class _Limiter:
    """The running and waiting allowance of one event loop."""

    concurrency: int
    semaphore: asyncio.Semaphore
    outstanding: int = 0


# One per running loop: a semaphore is tied to the loop it first waits on, so a single module-level
# one raises when a second loop uses it, as tests do.
_limiters: WeakKeyDictionary[asyncio.AbstractEventLoop, _Limiter] = WeakKeyDictionary()


def _limiter(concurrency: int) -> _Limiter:
    """Return this loop's limiter, replacing an idle one if the setting changed."""
    loop = asyncio.get_running_loop()
    limiter = _limiters.get(loop)
    if limiter is None or (limiter.concurrency != concurrency and limiter.outstanding == 0):
        limiter = _Limiter(concurrency, asyncio.Semaphore(concurrency))
        _limiters[loop] = limiter
    return limiter


async def run_reset_job(job: Callable[[], Awaitable[None]]) -> None:
    """Run one reset job inside the cap, or drop it and log that by type.

    Args:
        job: The work to run. Its error reaches the caller, which logs it by type.

    Raises:
        TimeoutError: The job ran for longer than ``RESET_JOB_TIMEOUT_SECONDS``.
    """
    settings = get_settings()
    limiter = _limiter(settings.RESET_JOB_CONCURRENCY)
    if limiter.outstanding >= settings.RESET_JOB_CONCURRENCY + settings.RESET_JOB_QUEUE_LIMIT:
        logger.warning("background job dropped: %s", RESET_JOB)
        return
    limiter.outstanding += 1
    try:
        async with limiter.semaphore, asyncio.timeout(settings.RESET_JOB_TIMEOUT_SECONDS):
            await job()
    finally:
        limiter.outstanding -= 1
