"""Health service: the alive and ready answers of the API and the worker (specs/006 research R6).

Layer: services. Depends on ``hds.core.health``; the database is reached only through the probe it
is given, which ``hds.wiring`` builds from ``hds.repositories.health``.

Ready runs the whole check, opening the session included, under one time limit (2 seconds by
default, FR-006). Any error or timeout reports ``database`` and nothing of the error's text; a
database behind the code's migrations reports ``migrations``. Alive looks at nothing outside the
process.
"""

import asyncio
import logging
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from datetime import timedelta
from typing import Protocol

from hds.core.health import READY_LIMIT, Migrations, body

logger = logging.getLogger("hds.health")


class HealthProbe(Protocol):
    """The two reads the ready check needs; ``HealthRepository`` is the real one."""

    async def ping(self) -> None:
        """Raise when the database cannot be reached."""

    async def revisions(self) -> list[str]:
        """The rows of ``alembic_version``, empty when there are none."""


ProbeOpener = Callable[[], AbstractAsyncContextManager[HealthProbe]]


class HealthService:
    """Answers the alive and ready checks for one process."""

    def __init__(
        self,
        open_probe: ProbeOpener,
        migrations: Migrations,
        *,
        version: str,
        limit: timedelta = READY_LIMIT,
        check_migrations: bool = True,
    ) -> None:
        """Build the service.

        Args:
            open_probe: Opens a probe on a fresh database session and closes it afterwards.
            migrations: The code's own migration history.
            version: The version this process runs, reported in every answer.
            limit: How long the whole ready check may take.
            check_migrations: Whether a database behind the code is not ready. The API checks;
                the worker's readiness is the database only (contracts/ops.md §1).
        """
        self._open_probe = open_probe
        self._migrations = migrations
        self._version = version
        self._limit = limit
        self._check_migrations = check_migrations

    def live(self) -> dict[str, object]:
        """The alive answer: always ok while the process can answer at all."""
        return body(self._version)

    async def ready(self) -> tuple[bool, dict[str, object]]:
        """Check the database and its migrations within the time limit.

        The reads run in a task of their own. At the limit the answer is given and the task is
        cancelled without waiting for it: closing a session on a dead connection can hang too,
        and must not stretch the 2 seconds (FR-006).

        Returns:
            Whether the process is ready, and the body to send. The body names the failing check
            (``database`` or ``migrations``) and never an error's text.
        """
        reads = asyncio.ensure_future(self._read())
        done, _ = await asyncio.wait({reads}, timeout=self._limit.total_seconds())
        if not done:
            reads.cancel()
            reads.add_done_callback(_consume)
            logger.warning("ready check: database failed (timeout)")
            return False, body(self._version, ["database"])
        error = reads.exception()
        if error is not None:
            # The type only: the text can hold a host name or a user name (FR-009).
            logger.warning("ready check: database failed (%s)", type(error).__name__)
            return False, body(self._version, ["database"])
        if self._check_migrations and self._migrations.behind(reads.result()):
            return False, body(self._version, ["migrations"])
        return True, body(self._version)

    async def _read(self) -> list[str]:
        """Ping the database and read its revisions, on a probe of its own."""
        async with self._open_probe() as probe:
            await probe.ping()
            return await probe.revisions()


def _consume(task: "asyncio.Future[list[str]]") -> None:
    """Retrieve an abandoned check's outcome, so asyncio does not log it as never retrieved."""
    if not task.cancelled():
        task.exception()
