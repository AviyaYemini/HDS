"""How the worker records each job's outcome (T018, the code review of Group C).

A run whose result was thrown away because another worker reclaimed the job is not counted; that
worker counts it. A job whose failure cannot even be stored is still counted as failed. The held
lease is released whatever happens.

Covers: specs/006-deploy-operations FR-008; contracts/ops.md section 2.
"""

import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, date, datetime, timedelta
from typing import Any

import pytest

import hds.worker as worker
from hds.core.health import WorkerPulse
from hds.services.schedule_job import Claim

LEASE = datetime(2026, 11, 6, 12, 5, tzinfo=UTC)
CLAIM = Claim(
    uuid.uuid4(), uuid.uuid4(), date(2027, 6, 6), date(2027, 6, 12), (), "greedy", 1, {}, LEASE
)


class FakeSession:
    """A session that only commits and rolls back."""

    async def commit(self) -> None:
        pass

    async def rollback(self) -> None:
        pass


@asynccontextmanager
async def fake_session() -> AsyncIterator[FakeSession]:
    yield FakeSession()


class FakeJobs:
    """The job service, scripted: whether storing succeeds, and whether failing raises."""

    def __init__(
        self, *, stored: bool = True, solve_fails: bool = False, fail_raises: bool = False
    ):
        self.stored = stored
        self.solve_fails = solve_fails
        self.fail_raises = fail_raises

    async def claim(self) -> Claim:
        return CLAIM

    async def build(self, claim: Claim) -> object:
        if self.solve_fails:
            raise RuntimeError("cannot build")
        return object()

    async def succeed(self, claim: Claim, built: object, result: object) -> bool:
        return self.stored

    async def fail(self, claim: Claim, code: str) -> None:
        if self.fail_raises:
            raise OSError("database down")


def install(monkeypatch: pytest.MonkeyPatch, jobs: FakeJobs) -> worker.WorkerOps:
    """Point the worker at ``jobs`` and a solver that returns at once."""

    async def solve(claim: Claim, built: object, limit: timedelta) -> object:
        return object()

    monkeypatch.setattr(worker, "job_service", lambda session: jobs)
    monkeypatch.setattr(worker, "solve", solve)
    return worker.WorkerOps()


def counted(ops: worker.WorkerOps, outcome: str) -> float | None:
    """How many jobs the duration histogram holds for ``outcome``."""
    return ops.metrics.registry.get_sample_value(
        "hds_schedule_job_duration_seconds_count", {"outcome": outcome}
    )


async def test_a_stored_job_counts_as_succeeded(monkeypatch: pytest.MonkeyPatch) -> None:
    ops = install(monkeypatch, FakeJobs())
    await worker.run_once(fake_session, ops)  # type: ignore[arg-type]
    assert counted(ops, "succeeded") == 1


async def test_a_result_another_worker_superseded_is_not_counted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ops = install(monkeypatch, FakeJobs(stored=False))
    await worker.run_once(fake_session, ops)  # type: ignore[arg-type]
    recorded = [
        sample
        for family in ops.metrics.registry.collect()
        for sample in family.samples
        if sample.name == "hds_schedule_job_duration_seconds_count"
    ]
    assert recorded == []  # under no outcome at all


async def test_a_failure_that_cannot_be_stored_still_counts_and_releases_the_lease(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ops = install(monkeypatch, FakeJobs(solve_fails=True, fail_raises=True))
    ops.pulse = WorkerPulse(stale_after=timedelta(0))  # only the held lease could keep it alive
    with pytest.raises(OSError):
        await worker.run_once(fake_session, ops)  # type: ignore[arg-type]
    assert counted(ops, "failed") == 1
    assert not ops.pulse.alive(LEASE - timedelta(minutes=1))


def test_the_claim_carries_its_lease(monkeypatch: pytest.MonkeyPatch) -> None:
    fields: dict[str, Any] = CLAIM.__dict__
    assert fields["lease_until"] == LEASE
