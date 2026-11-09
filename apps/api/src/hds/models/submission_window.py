"""Submission windows: the period employees may submit availability for, and its deadline.

Layer: models. Depends on ``hds.models.base`` only.
"""

import uuid
from datetime import date, datetime

from sqlalchemy import Boolean, CheckConstraint, Date, DateTime, ForeignKey, text, true
from sqlalchemy.dialects.postgresql import ExcludeConstraint
from sqlalchemy.orm import Mapped, mapped_column

from hds.models.base import TimestampedBase


class SubmissionWindow(TimestampedBase):
    """The dates ``starts_on`` to ``ends_on`` (inclusive), open to employees until ``deadline``.

    Windows of one organisation never share a date: ``ex_submission_windows_overlap`` is an
    exclusion constraint, so two managers opening overlapping windows at once leave one (research
    R5). ``deadline`` is a UTC instant; the window is open while now is before it.

    ``requires_submission`` is true for every window opened since 007: an employee must submit
    their availability for it, and one who has not by the deadline counts as unavailable for all
    of it (007 FR-014). Windows that had already closed when migration 026 ran keep false, so the
    rule never reaches back into weeks that had no Submit.
    """

    __tablename__ = "submission_windows"

    organisation_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("organisations.id"), nullable=False
    )
    starts_on: Mapped[date] = mapped_column(Date, nullable=False)
    ends_on: Mapped[date] = mapped_column(Date, nullable=False)
    deadline: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_by: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("users.id"))
    requires_submission: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, server_default=true()
    )

    __table_args__ = (
        CheckConstraint("ends_on >= starts_on", name="ck_submission_windows_dates"),
        # PostgreSQL only; the SQLite unit-test schema has no ranges, so it skips this.
        ExcludeConstraint(
            ("organisation_id", "="),
            (text("daterange(starts_on, ends_on, '[]')"), "&&"),
            name="ex_submission_windows_overlap",
            using="gist",
        ).ddl_if(dialect="postgresql"),
    )
