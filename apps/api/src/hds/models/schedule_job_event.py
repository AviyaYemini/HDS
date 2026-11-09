"""The run record: one row per step of a schedule job (008 FR-030, ADR 020).

Layer: models. Depends on ``hds.models.base`` only.
"""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    JSON,
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Identity,
    Index,
    Integer,
    String,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from hds.models.base import TimestampedBase

# JSONB in PostgreSQL; plain JSON where the SQLite unit-test schema is built.
_JSON = JSON().with_variant(JSONB(), "postgresql")

EVENT_KINDS = (
    "created",
    "finished",
    "stopped",
    "cancelled",
    "failed",
    "edited",
    "undone",
    "locked",
    "unlocked",
    "overridden",
    "accepted",
    "discarded",
    "expired",
)
"""Every kind of step a run's record holds."""


class ScheduleJobEvent(TimestampedBase):
    """One step of a run: what happened, who did it (null for the worker), when, and its details.

    Append-only: the repository only inserts and reads, and in PostgreSQL a trigger refuses UPDATE
    and DELETE (migration 034). ``version`` is the proposal's working copy version after the step,
    when the step changed it; undo replays the ``edited`` rows without the last one.
    """

    __tablename__ = "schedule_job_events"

    # The order the steps were written in; ``at`` alone ties within one transaction.
    seq: Mapped[int] = mapped_column(BigInteger, Identity(), nullable=False)

    organisation_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("organisations.id"), nullable=False
    )
    job_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("schedule_jobs.id"), nullable=False)
    kind: Mapped[str] = mapped_column(String(24), nullable=False)
    actor_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("users.id"))
    at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    body: Mapped[dict[str, Any]] = mapped_column(_JSON, nullable=False)
    version: Mapped[int | None] = mapped_column(Integer)

    __table_args__ = (
        CheckConstraint(
            "kind IN (" + ", ".join(f"'{k}'" for k in EVENT_KINDS) + ")",
            name="ck_schedule_job_events_kind",
        ),
        Index("idx_schedule_job_events_job_seq", "job_id", "seq"),
    )
