"""The worker finishes or hands back its job on SIGTERM, and a killed one's job is reclaimed (T030).

Each test starts the real worker as a process of its own on the test database
(``tests.worker_process``, with one stage slowed by 3 s) and signals it at that stage:

- **during a solve**: SIGTERM lets the solve finish and store, the process exits 0, and it claims
  nothing more; also with the operations server running, whose uvicorn used to re-raise the
  signal and cancel the job loop mid-solve;
- **before the solve**: SIGTERM after claiming but before solving gives the lease back
  (``lease_until`` now, ``attempts`` unchanged), and the next worker claims the job at once and
  stores it once;
- **lease expiry**: SIGKILL mid-solve leaves the job running until its lease ends; once it has
  ended, the next worker reclaims it and stores it once, as attempt 2. No lease setting is added.

Covers: 006 FR-020, US5; research R5.
"""

import asyncio
import os
import signal
import socket
import subprocess
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path

import asyncpg  # type: ignore[import-untyped]
import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession

from hds.models.user import User
from hds.repositories.schedule_job import CHANNEL
from tests.roster_support import headers_for
from tests.schedule_support import SUNDAY, ask, staffed_project, work

API_DIR = Path(__file__).resolve().parents[2]
REACH_SECONDS = 60  # the process starts (imports the solver) and claims the job in this time
EXIT_SECONDS = 30


async def job_row(test_db: AsyncSession, job_id: str) -> dict[str, object]:
    await test_db.rollback()
    found = await test_db.execute(
        text("SELECT status, attempts, lease_until FROM schedule_jobs WHERE id = :i"),
        {"i": job_id},
    )
    return dict(found.mappings().one())


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port: int = probe.getsockname()[1]
        return port


@contextmanager
def worker_process(
    url: str, mode: str, folder: Path, *, metrics_port: int = 0
) -> Iterator[tuple[subprocess.Popen[str], Path]]:
    """The real worker, slowed at ``mode``'s stage; killed at the end if it is still running."""
    marker = folder / "reached"
    env = {
        **os.environ,
        "DATABASE_URL": url,
        "ENVIRONMENT": "test",
        "METRICS_PORT": str(metrics_port),
        "SCHEDULE_TIME_LIMIT_SECONDS": "5",
    }
    env.setdefault("JWT_SECRET_KEY", "a-test-only-signing-key-for-the-drain")
    # A collector set in the caller's shell would make the exit flush wait on it (review R43).
    for name in [n for n in env if n.startswith("OTEL_")]:
        del env[name]
    log = (folder / "worker.log").open("w")
    process = subprocess.Popen(
        [sys.executable, "-m", "tests.worker_process", mode, str(marker)],
        cwd=API_DIR,
        env=env,
        stdout=log,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        yield process, marker
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
        log.close()


def wait_for(marker: Path, process: subprocess.Popen[str], folder: Path) -> None:
    """Wait until the worker reaches the slowed stage, or fail with its output."""
    deadline = time.monotonic() + REACH_SECONDS
    while not marker.exists():
        if process.poll() is not None or time.monotonic() > deadline:
            pytest.fail(
                f"the worker never reached the stage:\n{(folder / 'worker.log').read_text()}"
            )
        time.sleep(0.05)


def stop(process: subprocess.Popen[str], folder: Path) -> int:
    """Send SIGTERM and wait for the exit; its status."""
    process.send_signal(signal.SIGTERM)
    try:
        return process.wait(timeout=EXIT_SECONDS)
    except subprocess.TimeoutExpired:
        pytest.fail(f"the worker did not exit:\n{(folder / 'worker.log').read_text()}")


@pytest.mark.parametrize("ops_server", [False, True], ids=["alone", "with the ops server"])
async def test_sigterm_during_a_solve_lets_it_store_and_claims_nothing_more(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    test_db: AsyncSession,
    migrated_test_db: str,
    tmp_path: Path,
    ops_server: bool,
) -> None:
    manager = await headers_for(async_client, "manager")
    await staffed_project(async_client, manager)
    first = (await ask(async_client, manager)).json()
    second = (await ask(async_client, manager, SUNDAY + timedelta(days=7))).json()

    port = free_port() if ops_server else 0
    with worker_process(migrated_test_db, "slow-solve", tmp_path, metrics_port=port) as (
        process,
        marker,
    ):
        wait_for(marker, process, tmp_path)
        status = stop(process, tmp_path)
    assert status == 0, (tmp_path / "worker.log").read_text()

    done = await job_row(test_db, first["id"])
    assert (done["status"], done["attempts"]) == ("succeeded", 1)
    waiting = await job_row(test_db, second["id"])
    assert (waiting["status"], waiting["attempts"]) == ("queued", 0)  # no new claim


async def test_sigterm_before_the_solve_hands_the_job_back(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    test_db: AsyncSession,
    migrated_test_db: str,
    tmp_path: Path,
) -> None:
    manager = await headers_for(async_client, "manager")
    await staffed_project(async_client, manager)
    job = (await ask(async_client, manager)).json()

    # An idle worker waits on LISTEN, so "at once" means the hand-back is announced (review R41).
    listener = await asyncpg.connect(
        make_url(migrated_test_db)
        .set(drivername="postgresql")
        .render_as_string(hide_password=False)
    )
    announced: asyncio.Queue[str] = asyncio.Queue()
    await listener.add_listener(CHANNEL, lambda *args: announced.put_nowait(args[-1]))
    try:
        with worker_process(migrated_test_db, "slow-build", tmp_path) as (process, marker):
            wait_for(marker, process, tmp_path)
            status = stop(process, tmp_path)
        assert status == 0, (tmp_path / "worker.log").read_text()
        assert await asyncio.wait_for(announced.get(), 5) == job["id"]
    finally:
        await listener.close()

    # Queued again with its attempt given back: a claim that did no solve costs none (R35).
    handed_back = await job_row(test_db, job["id"])
    assert (handed_back["status"], handed_back["attempts"]) == ("queued", 0)
    assert handed_back["lease_until"] is None

    assert str(await work()) == job["id"]  # the next worker
    assert await work() is None  # stored once
    stored = await job_row(test_db, job["id"])
    assert (stored["status"], stored["attempts"]) == ("succeeded", 1)


async def test_a_killed_worker_s_job_is_reclaimed_after_its_lease(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    test_db: AsyncSession,
    migrated_test_db: str,
    tmp_path: Path,
) -> None:
    manager = await headers_for(async_client, "manager")
    await staffed_project(async_client, manager)
    job = (await ask(async_client, manager)).json()

    with worker_process(migrated_test_db, "slow-solve", tmp_path) as (process, marker):
        wait_for(marker, process, tmp_path)
        process.kill()
        process.wait(timeout=EXIT_SECONDS)

    held = await job_row(test_db, job["id"])
    assert (held["status"], held["attempts"]) == ("running", 1)
    assert await work() is None  # its lease still runs

    await test_db.execute(
        text("UPDATE schedule_jobs SET lease_until = :t WHERE id = :i"),
        {"t": datetime.now(UTC) - timedelta(seconds=1), "i": job["id"]},
    )
    await test_db.commit()
    assert str(await work()) == job["id"]
    assert await work() is None
    stored = await job_row(test_db, job["id"])
    assert (stored["status"], stored["attempts"]) == ("succeeded", 2)


async def test_a_second_sigterm_stops_the_worker_at_once(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    test_db: AsyncSession,
    migrated_test_db: str,
    tmp_path: Path,
) -> None:
    # Review R39: the first signal drains; a second one takes the default action, so an operator
    # can still end a long solve. The job is left to be reclaimed after its lease.
    manager = await headers_for(async_client, "manager")
    await staffed_project(async_client, manager)
    job = (await ask(async_client, manager)).json()

    with worker_process(migrated_test_db, "slow-solve", tmp_path) as (process, marker):
        wait_for(marker, process, tmp_path)
        process.send_signal(signal.SIGTERM)
        time.sleep(0.5)
        status = stop(process, tmp_path)
    assert status == -signal.SIGTERM
    held = await job_row(test_db, job["id"])
    assert (held["status"], held["attempts"]) == ("running", 1)


async def test_a_taken_ops_port_never_cuts_a_job_off(
    async_client: AsyncClient,
    seed_users: dict[str, User],
    test_db: AsyncSession,
    migrated_test_db: str,
    tmp_path: Path,
) -> None:
    # T032b, review R45: uvicorn exits when its port is taken. The worker then drains and exits
    # non-zero; the job is stored, or was never claimed, but is never left half-run.
    manager = await headers_for(async_client, "manager")
    await staffed_project(async_client, manager)
    job = (await ask(async_client, manager)).json()

    with socket.socket() as holder:
        holder.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 0)
        holder.bind(("0.0.0.0", 0))
        holder.listen()
        port = holder.getsockname()[1]
        with worker_process(migrated_test_db, "slow-solve", tmp_path, metrics_port=port) as (
            process,
            _,
        ):
            try:
                status = process.wait(timeout=REACH_SECONDS)
            except subprocess.TimeoutExpired:
                pytest.fail(f"the worker did not exit:\n{(tmp_path / 'worker.log').read_text()}")
    log = (tmp_path / "worker.log").read_text()
    assert status not in (0, -signal.SIGTERM, -signal.SIGKILL), log
    assert "operations server" in log
    row = await job_row(test_db, job["id"])
    assert (row["status"], row["attempts"]) in {("succeeded", 1), ("queued", 0)}, log
