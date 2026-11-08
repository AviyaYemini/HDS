"""One employee's submitted availability for one window (007 FR-014, data-model 026).

Layer: models. Depends on ``hds.models.base`` only.
"""

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import JSON, DateTime, ForeignKey, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from hds.models.base import TimestampedBase

# JSONB in PostgreSQL; plain JSON where the SQLite unit-test schema is built.
_JSON = JSON().with_variant(JSONB(), "postgresql")


class AvailabilitySubmission(TimestampedBase):
    """The latest submit of one employee for one window, with what they submitted.

    ``snapshot`` is the employee's blocked, open and close entries for the window's dates at
    ``submitted_at``, each clipped to the window and sorted (absences are never part of it: they
    stay editable at any time, 003 FR-105a). ``snapshot_hash`` is the SHA-256 of that list in its
    canonical form; comparing it with the hash of the live entries tells Submitted from Draft, so
    the status is derived and never stored (research R6). Submitting again replaces both. Once the
    window's deadline passes, scheduling reads the snapshot, not the live entries.

    ``submitted_by`` is the employee, or the Admin whose hand edit after the deadline refreshed the
    snapshot (003 FR-107). Removed with its window.
    """

    __tablename__ = "availability_submissions"

    organization_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("organisations.id"), nullable=False
    )
    window_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("submission_windows.id", ondelete="CASCADE"), nullable=False
    )
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"), nullable=False)
    submitted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    submitted_by: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("users.id"))
    snapshot: Mapped[list[dict[str, Any]]] = mapped_column(_JSON, nullable=False)
    snapshot_hash: Mapped[str] = mapped_column(String(64), nullable=False)

    __table_args__ = (
        UniqueConstraint("window_id", "user_id", name="uq_availability_submissions_window_user"),
    )
