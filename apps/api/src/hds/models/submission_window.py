"""Submission windows: the period employees may submit availability for, and its deadline.

Layer: models. Depends on ``hds.models.base`` only.
"""

import uuid
from datetime import date, datetime

from sqlalchemy import CheckConstraint, Date, DateTime, ForeignKey, text
from sqlalchemy.dialects.postgresql import ExcludeConstraint
from sqlalchemy.orm import Mapped, mapped_column

from hds.models.base import TimestampedBase


class SubmissionWindow(TimestampedBase):
    """The dates ``starts_on`` to ``ends_on`` (inclusive), open to employees until ``deadline``.

    Windows of one organisation never share a date: ``ex_submission_windows_overlap`` is an
    exclusion constraint, so two managers opening overlapping windows at once leave one (research
    R5). ``deadline`` is a UTC instant; the window is open while now is before it.
    """

    __tablename__ = "submission_windows"

    organization_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("organisations.id"), nullable=False
    )
    starts_on: Mapped[date] = mapped_column(Date, nullable=False)
    ends_on: Mapped[date] = mapped_column(Date, nullable=False)
    deadline: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_by: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("users.id"))

    __table_args__ = (
        CheckConstraint("ends_on >= starts_on", name="ck_submission_windows_dates"),
        # PostgreSQL only; the SQLite unit-test schema has no ranges, so it skips this.
        ExcludeConstraint(
            ("organization_id", "="),
            (text("daterange(starts_on, ends_on, '[]')"), "&&"),
            name="ex_submission_windows_overlap",
            using="gist",
        ).ddl_if(dialect="postgresql"),
    )
