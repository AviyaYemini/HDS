"""Persistence of schedule jobs, which are also the worker's queue (004 research G7).

Layer: repositories. Depends on ``hds.models`` and ``hds.repositories.base`` only.
"""

import uuid
from collections.abc import Mapping
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import and_, func, or_, select, text, update
from sqlalchemy.orm.attributes import flag_modified

from hds.models.schedule_job import ScheduleJob
from hds.repositories.base import OrgScopedRepository

CHANNEL = "schedule_jobs"
"""The ``LISTEN``/``NOTIFY`` channel a new job is announced on."""


class ScheduleJobRepository(OrgScopedRepository[ScheduleJob]):
    """Jobs of an organisation, and the claim a worker makes on the next one."""

    model = ScheduleJob

    async def create(self, org_id: uuid.UUID, data: Mapping[str, Any]) -> ScheduleJob:
        """Store a queued job and announce it to workers when the transaction commits."""
        job = ScheduleJob(id=uuid.uuid4(), organization_id=org_id, **data)
        self.session.add(job)
        await self.session.flush()
        if self.session.get_bind().dialect.name == "postgresql":
            # Delivered on commit only, so a worker never wakes for a job it cannot see yet.
            await self.session.execute(
                text("SELECT pg_notify(:c, :i)"), {"c": CHANNEL, "i": str(job.id)}
            )
        return job

    async def get(self, id: uuid.UUID, org_id: uuid.UUID) -> ScheduleJob | None:
        """A job of the organisation, or ``None`` (also for another organisation's)."""
        return await self.get_by_id(id, org_id)

    async def get_locked(self, id: uuid.UUID, org_id: uuid.UUID) -> ScheduleJob | None:
        """A job of the organisation, locked until the transaction ends, read fresh.

        Two accepts (or an accept and a discard) of one proposal then run one after the other.
        """
        result = await self.session.execute(
            select(ScheduleJob)
            .where(ScheduleJob.id == id, ScheduleJob.organization_id == org_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        return result.scalar_one_or_none()

    async def page(
        self,
        org_id: uuid.UUID,
        status: str | None,
        after: tuple[datetime, uuid.UUID] | None,
        limit: int,
    ) -> list[ScheduleJob]:
        """The organisation's jobs, newest first, after the ``(created_at, id)`` of the last one."""
        query = select(ScheduleJob).where(ScheduleJob.organization_id == org_id)
        if status is not None:
            query = query.where(ScheduleJob.status == status)
        if after is not None:
            at, last = after
            query = query.where(
                or_(
                    ScheduleJob.created_at < at,
                    and_(ScheduleJob.created_at == at, ScheduleJob.id < last),
                )
            )
        result = await self.session.execute(
            query.order_by(ScheduleJob.created_at.desc(), ScheduleJob.id.desc()).limit(limit)
        )
        return list(result.scalars().all())

    async def count_by_state(self) -> dict[str, int]:
        """How many jobs are in each state, across every organisation (for the worker's metrics).

        Only counts leave this method: no job, organisation or person is named, so an operator's
        metric holds no tenant data.
        """
        found = await self.session.execute(
            select(ScheduleJob.status, func.count()).group_by(ScheduleJob.status)
        )
        return {str(status): int(count) for status, count in found.all()}

    async def claim(
        self, now: datetime, lease: timedelta, max_attempts: int
    ) -> tuple[ScheduleJob | None, list[uuid.UUID]]:
        """Claim the oldest job that is queued, or running past its lease.

        ``FOR UPDATE SKIP LOCKED``, so two workers never take the same job and neither waits for
        the other. A job past its lease after ``max_attempts`` claims is marked failed
        (``jobs.worker_lost``) instead.

        Returns:
            The claimed job (now ``running``, its attempts counted and leased), or ``None``; and
            the ids of jobs marked lost on the way.
        """
        lost: list[uuid.UUID] = []
        while True:
            result = await self.session.execute(
                select(ScheduleJob)
                .where(
                    or_(
                        ScheduleJob.status == "queued",
                        and_(ScheduleJob.status == "running", ScheduleJob.lease_until < now),
                    )
                )
                .order_by(ScheduleJob.created_at, ScheduleJob.id)
                .limit(1)
                .with_for_update(skip_locked=True)
                .execution_options(populate_existing=True)
            )
            job = result.scalar_one_or_none()
            if job is None:
                return None, lost
            if job.status == "running" and job.attempts >= max_attempts:
                job.status, job.error_code = "failed", "jobs.worker_lost"
                job.finished_at, job.lease_until = now, None
                lost.append(job.id)
                await self.session.flush()
                continue
            job.status = "running"
            job.attempts += 1
            job.lease_until = now + lease
            job.started_at = now
            await self.session.flush()
            return job, lost

    async def decide(self, job: ScheduleJob, state: str, by: uuid.UUID, at: datetime) -> None:
        """Record on a locked job that its proposal was accepted or discarded, by whom and when."""
        job.proposal_state, job.decided_by, job.decided_at = state, by, at
        if job.current is not None:
            flag_modified(job, "current")
        await self.session.flush()

    async def save_copy(self, job: ScheduleJob) -> None:
        """Write a locked job's working copy and version as the caller set them (008, ADR 020).

        ``current`` is reassigned whole by the caller, so the JSON column sees the change.
        """
        flag_modified(job, "current")
        await self.session.flush()

    async def release(self, id: uuid.UUID, attempt: int) -> bool:
        """Put a job this run claimed but has not solved back in the queue (a worker stopping).

        It is ``queued`` again with its attempt given back, as no solve ran, and no lease or start,
        so it depends on no clock; workers are told on commit, as when it was first queued.
        Nothing changes if another worker has claimed the job since (specs/006 FR-020,
        data-model.md).

        Returns:
            Whether the job was still this run's.
        """
        result = await self.session.execute(
            update(ScheduleJob)
            .where(
                ScheduleJob.id == id,
                ScheduleJob.attempts == attempt,
                ScheduleJob.status == "running",
            )
            .values(
                status="queued",
                attempts=ScheduleJob.attempts - 1,
                lease_until=None,
                started_at=None,
            )
            .returning(ScheduleJob.id)
        )
        released = result.first() is not None
        if released and self.session.get_bind().dialect.name == "postgresql":
            await self.session.execute(
                text("SELECT pg_notify(:c, :i)"), {"c": CHANNEL, "i": str(id)}
            )
        return released

    async def finish(self, id: uuid.UUID, attempt: int, values: Mapping[str, Any]) -> bool:
        """Record the end of the run that claimed ``id`` as attempt ``attempt``.

        Nothing changes if another worker has claimed the job since (its lease ran out), so a late
        run never overwrites a newer one.

        Returns:
            Whether the job was still this run's.
        """
        result = await self.session.execute(
            update(ScheduleJob)
            .where(
                ScheduleJob.id == id,
                ScheduleJob.attempts == attempt,
                ScheduleJob.status == "running",
            )
            .values(**values, lease_until=None)
            .returning(ScheduleJob.id)
        )
        return result.first() is not None
