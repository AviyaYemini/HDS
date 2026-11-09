"""Shifts and the employees assigned to them.

Layer: models. Depends on ``hds.models.base`` only.
"""

import uuid
from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Numeric,
    SmallInteger,
    String,
    UniqueConstraint,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from hds.models.base import TimestampedBase

SHIFT_STATUSES = ("draft", "published")
"""A shift starts as a draft, seen by Managers and Admins only, until it is published."""


class Shift(TimestampedBase):
    """One shift of a project, of one of the project's shift types.

    ``starts_at`` and ``ends_at`` are UTC instants (Constitution VII), the end after the start.
    ``headcount`` is how many employees it needs, at least 1; more or fewer is a warning, never a
    refusal (003 FR-116). ``pay_rate`` is the hourly pay for the shift, ``NUMERIC(12,4)`` and
    positive when set, otherwise each employee's default rate applies (FIN-1); only an Admin sets
    it. ``status`` is ``draft`` or ``published``; ``published_at`` records when it was
    published, and is set exactly when the status is published (``ck_shifts_published_at``).
    """

    __tablename__ = "shifts"

    organization_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("organisations.id"), nullable=False
    )
    project_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("projects.id"), nullable=False)
    shift_type_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("shift_types.id"), nullable=False)
    starts_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    ends_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    headcount: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    pay_rate: Mapped[Decimal | None] = mapped_column(Numeric(precision=12, scale=4, asdecimal=True))
    status: Mapped[str] = mapped_column(
        String(16), nullable=False, default="draft", server_default="draft"
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        CheckConstraint("ends_at > starts_at", name="ck_shifts_ends_after_start"),
        CheckConstraint("headcount >= 1", name="ck_shifts_headcount"),
        CheckConstraint("pay_rate IS NULL OR pay_rate > 0", name="ck_shifts_pay_rate_positive"),
        CheckConstraint("status IN ('draft', 'published')", name="ck_shifts_status"),
        CheckConstraint(
            "(status = 'published') = (published_at IS NOT NULL)", name="ck_shifts_published_at"
        ),
        Index("idx_shifts_org_start", "organization_id", "starts_at"),
        Index("idx_shifts_project_start", "project_id", "starts_at"),
        # Reports and pay to date read published shifts only (005 research R9).
        Index(
            "idx_shifts_org_published_start",
            "organization_id",
            "starts_at",
            postgresql_where=text("status = 'published'"),
            sqlite_where=text("status = 'published'"),
        ),
    )


class Assignment(TimestampedBase):
    """An employee on a shift, at most once (``uq_assignments_shift_user``).

    Removed with its shift (``ON DELETE CASCADE``). ``assigned_by`` is who made it: the Admin of a
    manual assignment, or the Manager or Admin who accepted a generated one. ``source`` is
    ``manual`` or ``generated`` (004 research G10).

    ``starts_at`` and ``ends_at`` are this person's own times in the shift, both set or both null
    (null: the shift's times), the end after the start (007 FR-027, research R5). The service keeps
    them within 4 hours of the shift's own start and end, so a half shift or two extra hours fit.
    Pay, the rule checks, the reports and My shifts read the person's times; the shift's date,
    status and headcount stay the shift's.
    """

    __tablename__ = "assignments"

    organization_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("organisations.id"), nullable=False
    )
    shift_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("shifts.id", ondelete="CASCADE"), nullable=False
    )
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"), nullable=False)
    assigned_by: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("users.id"))
    source: Mapped[str] = mapped_column(
        String(16), nullable=False, default="manual", server_default="manual"
    )
    starts_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ends_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    # A lock keeps the assignment in every later run until someone unlocks it (008 FR-018).
    # A word to the person placed, from whoever placed or moved them (008 T042g).
    note: Mapped[str | None] = mapped_column(String(500))
    locked_by: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("users.id"))
    locked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        UniqueConstraint("shift_id", "user_id", name="uq_assignments_shift_user"),
        CheckConstraint(
            "(locked_by IS NULL) = (locked_at IS NULL)", name="ck_assignments_lock_paired"
        ),
        CheckConstraint("source IN ('manual', 'generated')", name="ck_assignments_source"),
        CheckConstraint(
            "(starts_at IS NULL) = (ends_at IS NULL)", name="ck_assignments_times_paired"
        ),
        CheckConstraint(
            "starts_at IS NULL OR ends_at > starts_at", name="ck_assignments_times_order"
        ),
        Index("idx_assignments_user", "user_id"),
    )
