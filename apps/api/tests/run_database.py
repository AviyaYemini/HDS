"""Which database a test run uses, as pure functions.

Layer: tests. Each run gets its own database, ``hds_run_<process id>_test``, so two runs at once
cannot empty each other's tables. An explicit ``TEST_DATABASE_URL`` wins, as CI uses. A run that was
killed leaves its database behind; the name holds the process id, so the next run can tell it is
stale.

Covers: none directly; helpers for F26.
"""

import os
import re
from collections.abc import Callable, Iterable

from sqlalchemy.engine import make_url

DEFAULT_SERVER = "postgresql+asyncpg://hds:hds_dev@localhost:5432/postgres"
#: A database of a run. The ``run`` marker keeps a name somebody chose out of it: these get dropped.
RUN_DATABASE = re.compile(r"hds_run_(\d+)(?:_\w+)?_test")


def run_database_name(pid: int) -> str:
    """Return the database name of the run with this process id."""
    return f"hds_run_{pid}_test"


def database_url(explicit: str | None, pid: int, server: str = DEFAULT_SERVER) -> str:
    """Return the database URL a run uses.

    Args:
        explicit: ``TEST_DATABASE_URL`` if set.
        pid: The run's process id.
        server: Where to create a per-run database.

    Returns:
        ``explicit`` if given, else the URL of this run's own database.
    """
    if explicit:
        return explicit
    return make_url(server).set(database=run_database_name(pid)).render_as_string(False)


def scratch_name(name: str, pid: int) -> str:
    """Return a migration test's scratch database name for this run, so two runs do not share it.

    Args:
        name: The name the test asked for, such as ``hds_migration_014_test``.
        pid: The run's process id.

    Returns:
        A name of this run, which a later run can recognise if this one is killed.
    """
    return f"hds_run_{pid}_{name.removeprefix('hds_')}"


def stale_databases(names: Iterable[str], process_exists: Callable[[int], bool]) -> list[str]:
    """Pick out per-run databases whose process is gone.

    Args:
        names: Every database on the server.
        process_exists: Says whether a process id is running.

    Returns:
        The names to drop. Any other database is left alone, whatever it is called. The check is
        on this machine: a run elsewhere on a shared server looks stale, so the caller must not
        force the drop, and a database still in use then survives.
    """
    stale = []
    for name in names:
        match = RUN_DATABASE.fullmatch(name)
        if match is not None and not process_exists(int(match.group(1))):
            stale.append(name)
    return stale


def process_exists(pid: int) -> bool:
    """Say whether a process with this id is running, whoever owns it."""
    try:
        os.kill(pid, 0)
    except (ProcessLookupError, OverflowError):  # too large to be a process id at all
        return False
    except PermissionError:
        return True
    return True
