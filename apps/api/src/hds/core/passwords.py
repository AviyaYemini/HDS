"""Password hashing for request handlers: off the event loop, bounded, with a bounded queue.

Layer: core. Wraps the synchronous functions of ``hds.core.security``, which scripts and the seed
keep using directly.

Argon2 is slow and memory-hungry on purpose. Run inside an async handler it stops every other
request in the worker, and the sign-in path lets anonymous callers ask for it. So each call runs in
a thread, at most ``HASH_CONCURRENCY`` run at once, at most ``HASH_QUEUE_LIMIT`` more may wait, and
anything beyond that is refused at once with ``ServiceBusyError`` (HTTP 503) (FR-222). Callers must
hold no database connection and no row lock while they wait: see
``BaseRepository.release_connection``.
"""

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from weakref import WeakKeyDictionary

from hds.core import security
from hds.core.config import get_settings
from hds.core.exceptions import ServiceBusyError


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


async def _bounded[ResultT](work: Callable[..., ResultT], *args: str) -> ResultT:
    """Run ``work`` in a thread inside the limits, or refuse at once."""
    settings = get_settings()
    limiter = _limiter(settings.HASH_CONCURRENCY)
    if limiter.outstanding >= settings.HASH_CONCURRENCY + settings.HASH_QUEUE_LIMIT:
        raise ServiceBusyError()
    limiter.outstanding += 1
    try:
        async with limiter.semaphore:
            return await asyncio.to_thread(work, *args)
    finally:
        limiter.outstanding -= 1


async def hash_password(plain: str) -> str:
    """Hash a password with Argon2id off the event loop.

    Args:
        plain: The clear-text password.

    Returns:
        The encoded hash.

    Raises:
        ServiceBusyError: ``service.busy`` (HTTP 503) when the queue is full.
    """
    return await _bounded(lambda value: security.hash_password(value), plain)


async def verify_password(plain: str, hashed: str) -> bool:
    """Check a password against a stored hash off the event loop.

    Args:
        plain: The clear-text password offered at sign-in.
        hashed: The hash stored for the user.

    Returns:
        ``True`` on a match, ``False`` on a mismatch.

    Raises:
        ServiceBusyError: ``service.busy`` (HTTP 503) when the queue is full.
    """
    return await _bounded(
        lambda value, stored: security.verify_password(value, stored), plain, hashed
    )


async def verify_against_dummy(plain: str) -> bool:
    """Spend one password check when there is no account (see ``security.verify_against_dummy``).

    Args:
        plain: The password the caller offered.

    Returns:
        Always ``False``.

    Raises:
        ServiceBusyError: ``service.busy`` (HTTP 503) when the queue is full.
    """
    return await _bounded(lambda value: security.verify_against_dummy(value), plain)
