"""The run record of schedule jobs: append and read, never change (008 FR-030, ADR 020).

Layer: repositories. Depends on ``hds.models`` and ``hds.repositories.base`` only; it MUST NOT
import services or routers. There is deliberately no update or delete here; in PostgreSQL a trigger
refuses both anyway (migration 034).
"""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import func, select

from hds.models.schedule_job_event import EVENT_KINDS, ScheduleJobEvent
from hds.repositories.base import BaseRepository


class ScheduleJobEventRepository(BaseRepository[ScheduleJobEvent]):
    """Appends steps to a run's record and reads them back in order; every query names the org."""

    model = ScheduleJobEvent

    async def append(
        self,
        org_id: uuid.UUID,
        job_id: uuid.UUID,
        kind: str,
        at: datetime,
        body: dict[str, Any],
        *,
        actor_id: uuid.UUID | None = None,
        version: int | None = None,
    ) -> ScheduleJobEvent:
        """Add one step to the run's record.

        Args:
            org_id: The run's organisation.
            job_id: The run.
            kind: One of ``EVENT_KINDS``.
            at: When it happened (UTC).
            body: Its details: the edit, the part accepted, the counts, a reason.
            actor_id: Who did it; ``None`` for the worker.
            version: The working copy's version after the step, when the step changed it.

        Raises:
            ValueError: An unknown kind (a bug; the database would refuse it too).
        """
        if kind not in EVENT_KINDS:
            raise ValueError(f"unknown run record kind {kind!r}")
        row = ScheduleJobEvent(
            id=uuid.uuid4(),
            organization_id=org_id,
            job_id=job_id,
            kind=kind,
            actor_id=actor_id,
            at=at,
            body=body,
            version=version,
        )
        self.session.add(row)
        await self.session.flush()
        return row

    async def for_job(
        self, org_id: uuid.UUID, job_id: uuid.UUID, *, offset: int = 0, limit: int | None = None
    ) -> list[ScheduleJobEvent]:
        """The run's steps in the order they were written."""
        query = (
            select(ScheduleJobEvent)
            .where(ScheduleJobEvent.organization_id == org_id, ScheduleJobEvent.job_id == job_id)
            .order_by(ScheduleJobEvent.seq)
            .offset(offset)
        )
        if limit is not None:
            query = query.limit(limit)
        result = await self.session.execute(query)
        return list(result.scalars())

    async def last_edit_not_undone(
        self, org_id: uuid.UUID, job_id: uuid.UUID
    ) -> ScheduleJobEvent | None:
        """The run's newest ``edited`` step that no ``undone`` step names, or ``None``.

        Reads the ``undone`` steps (few), then only as many of the newest edits as can hold the
        answer: one more than there are undos.
        """
        scope = (ScheduleJobEvent.organization_id == org_id, ScheduleJobEvent.job_id == job_id)
        undos = await self.session.execute(
            select(ScheduleJobEvent.body).where(*scope, ScheduleJobEvent.kind == "undone")
        )
        undone = {body.get("edit") for (body,) in undos}
        edits = await self.session.execute(
            select(ScheduleJobEvent)
            .where(*scope, ScheduleJobEvent.kind == "edited")
            .order_by(ScheduleJobEvent.seq.desc())
            .limit(len(undone) + 1)
        )
        return next((e for e in edits.scalars() if str(e.id) not in undone), None)

    async def count_for_job(self, org_id: uuid.UUID, job_id: uuid.UUID) -> int:
        """How many steps the run's record holds."""
        result = await self.session.execute(
            select(func.count())
            .select_from(ScheduleJobEvent)
            .where(ScheduleJobEvent.organization_id == org_id, ScheduleJobEvent.job_id == job_id)
        )
        return int(result.scalar_one())
