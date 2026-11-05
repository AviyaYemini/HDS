"""Schedule jobs: a generation run, its proposal, and the worker's queue (004 research G7).

Layer: models. Depends on ``hds.models.base`` only.
"""

import uuid
from datetime import date, datetime
from typing import Any

from sqlalchemy import (
    CHAR,
    JSON,
    Boolean,
    CheckConstraint,
    Date,
    DateTime,
    ForeignKey,
    Index,
    SmallInteger,
    String,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from hds.models.base import TimestampedBase

# JSONB in PostgreSQL; plain JSON where the SQLite unit-test schema is built.
_JSON = JSON().with_variant(JSONB(), "postgresql")


class ScheduleJob(TimestampedBase):
    """One request to generate a roster, from ``queued`` to ``succeeded`` or ``failed``.

    The range is ``first_day`` to ``last_day`` inclusive, at most 31 days; ``project_ids`` lists the
    run's projects, empty for every active one. A worker claims a queued job, or a running one past
    its ``lease_until``, at most three times (``attempts``). Once succeeded, ``result`` holds the
    proposal (data-model.md), ``fingerprint`` the SHA-256 of the problem it solved, ``optimal``
    whether it is proven, and ``proposal_state`` whether it is open, accepted or discarded; an open
    proposal finished more than 7 days ago reads as expired.
    """

    __tablename__ = "schedule_jobs"

    organization_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("organisations.id"), nullable=False
    )
    requested_by: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"), nullable=False)
    first_day: Mapped[date] = mapped_column(Date, nullable=False)
    last_day: Mapped[date] = mapped_column(Date, nullable=False)
    project_ids: Mapped[list[str]] = mapped_column(_JSON, nullable=False)
    solver: Mapped[str] = mapped_column(
        String(16), nullable=False, default="cpsat", server_default="cpsat"
    )
    status: Mapped[str] = mapped_column(
        String(16), nullable=False, default="queued", server_default="queued"
    )
    attempts: Mapped[int] = mapped_column(
        SmallInteger, nullable=False, default=0, server_default="0"
    )
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error_code: Mapped[str | None] = mapped_column(String(64))
    settings: Mapped[dict[str, Any]] = mapped_column(_JSON, nullable=False)
    fingerprint: Mapped[str | None] = mapped_column(CHAR(64))
    result: Mapped[dict[str, Any] | None] = mapped_column(_JSON)
    optimal: Mapped[bool | None] = mapped_column(Boolean)
    proposal_state: Mapped[str | None] = mapped_column(String(16))
    decided_by: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("users.id"))
    decided_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        CheckConstraint("last_day >= first_day", name="ck_schedule_jobs_order"),
        # Date subtraction is PostgreSQL's, so the SQLite unit-test schema has the order check only.
        CheckConstraint("last_day - first_day <= 30", name="ck_schedule_jobs_length").ddl_if(
            dialect="postgresql"
        ),
        CheckConstraint("solver IN ('greedy', 'cpsat')", name="ck_schedule_jobs_solver"),
        CheckConstraint(
            "status IN ('queued', 'running', 'succeeded', 'failed')",
            name="ck_schedule_jobs_status",
        ),
        CheckConstraint(
            "proposal_state IS NULL OR proposal_state IN ('open', 'accepted', 'discarded')",
            name="ck_schedule_jobs_proposal_state",
        ),
        Index("idx_schedule_jobs_status_created", "status", "created_at"),
        Index("idx_schedule_jobs_org_created", "organization_id", "created_at"),
    )
