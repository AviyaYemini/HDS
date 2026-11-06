"""The rules behind the alive and ready checks (specs/006-deploy-operations research R6).

Layer: core. Depends on Alembic's script reader only; it imports nothing from the other ``hds``
layers. The service in ``hds.services.health`` applies these rules to a database probe.

"Behind" is the one rule about migrations. The database is behind the code when ``alembic_version``
is empty or missing, or holds a revision in the code's own history that is not its head. A revision
the code does not know must be newer, and each migration keeps the previous release working
(FR-016), so it counts as ready. Requiring an exact match would take every old pod out of service
the moment an upgrade migrates the database, and keep them out after a rollback.

The worker's liveness has one rule too (``WorkerPulse``): it is alive while its loop has turned in
the last 30 seconds, or while it holds a job still inside its lease. A solve can run for a minute
with the loop awaiting it, and must not be killed for that.
"""

import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from alembic.script import ScriptDirectory
from alembic.util import CommandError

#: How long a ready check may take in all before it reports the database as failing (FR-006).
READY_LIMIT = timedelta(seconds=2)

#: How old the worker loop's last turn may be before the worker counts as stuck.
WORKER_STALE = timedelta(seconds=30)

# apps/api/alembic in the repository, /app/alembic in the image: beside src/.
_SCRIPT_LOCATION = Path(__file__).resolve().parents[3] / "alembic"


@dataclass(frozen=True)
class Migrations:
    """The code's own migrations: every revision it knows, and its head.

    Attributes:
        known: Every revision id in the code's script history.
        head: The newest one, which a database at the code's level holds.
    """

    known: frozenset[str]
    head: str

    def behind(self, revisions: list[str]) -> bool:
        """Whether a database holding ``revisions`` in ``alembic_version`` is behind the code.

        Args:
            revisions: The rows of ``alembic_version``; empty when the table is empty or missing.

        Returns:
            True when there is none, or one the code knows that is not its head. A revision the
            code does not know is newer and never behind.
        """
        if not revisions:
            return True
        return any(revision in self.known and revision != self.head for revision in revisions)


def load_migrations(location: Path = _SCRIPT_LOCATION) -> Migrations:
    """Read the code's migration history once, at start-up.

    Args:
        location: The Alembic script directory.

    Returns:
        Every known revision and the head.

    Raises:
        RuntimeError: When the history has no single head, which would make "behind" ambiguous.
    """
    script = ScriptDirectory(str(location))
    try:
        head = script.get_current_head()
    except CommandError as error:  # Alembic's own refusal of several heads
        raise RuntimeError(f"the migration history in {location} has no single head") from error
    if head is None:
        raise RuntimeError(f"the migration history in {location} has no single head")
    return Migrations(
        known=frozenset(revision.revision for revision in script.walk_revisions()), head=head
    )


def body(version: str, failing: list[str] | None = None) -> dict[str, object]:
    """The JSON body of a health answer: ``ok``, or ``not_ready`` with the failing checks' names.

    Args:
        version: The version this process runs (``HDS_VERSION``).
        failing: The names of the checks that failed (``database``, ``migrations``); none means ok.

    Returns:
        The body. It never carries an error message, a host name or a connection string (FR-009).
    """
    if not failing:
        return {"status": "ok", "version": version}
    return {"status": "not_ready", "version": version, "failing": failing}


class WorkerPulse:
    """What the schedule worker's liveness check looks at: the loop's last turn and a held job.

    The loop calls ``beat`` on each turn, ``hold`` when it claims a job and ``release`` when the job
    is stored or failed. The clocks are injectable so tests need not wait.
    """

    def __init__(
        self,
        stale_after: timedelta = WORKER_STALE,
        monotonic_ns: Callable[[], int] | None = None,
    ) -> None:
        """Start with a fresh beat and no job held.

        Args:
            stale_after: How long after the last beat the loop counts as stuck.
            monotonic_ns: A replacement for ``time.monotonic_ns``, for tests.
        """
        self._now = monotonic_ns or time.monotonic_ns
        self._stale_after_ns = stale_after // timedelta(microseconds=1) * 1000
        self._last_beat = self._now()
        self._held_until: datetime | None = None

    def beat(self) -> None:
        """Record a turn of the worker loop."""
        self._last_beat = self._now()

    def hold(self, lease_until: datetime) -> None:
        """Record that the worker holds a job leased until ``lease_until``."""
        self._held_until = lease_until

    def release(self) -> None:
        """Record that the worker holds no job."""
        self._held_until = None

    def alive(self, now: datetime | None = None) -> bool:
        """Whether the worker counts as alive.

        Args:
            now: The current UTC time, for the lease; tests pass a fixed one.

        Returns:
            True while the loop turned within ``stale_after``, or a held job is still
            inside its lease. A solve stuck past its lease fails the check at the moment another
            worker may reclaim the job.
        """
        if self._now() - self._last_beat < self._stale_after_ns:
            return True
        current = now or datetime.now(UTC)
        return self._held_until is not None and current < self._held_until
