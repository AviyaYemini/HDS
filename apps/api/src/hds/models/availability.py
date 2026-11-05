"""Availability entries: when an employee cannot work, would like to, or would rather not.

Layer: models. Depends on ``hds.models.base`` only.
"""

import uuid
from datetime import date

from sqlalchemy import CheckConstraint, Date, ForeignKey, Index, SmallInteger, String
from sqlalchemy.orm import Mapped, mapped_column

from hds.models.base import TimestampedBase

ENTRY_KINDS = ("blocked", "open", "close", "absence")
"""Entry kinds: cannot work, would like to, would rather not, and away (leave and the like)."""

ABSENCE_KINDS = ("leave", "reserve_duty", "sick")
"""The kinds of absence (AVL-4)."""


class AvailabilityEntry(TimestampedBase):
    """One employee's entry over the local dates ``starts_on`` to ``ends_on`` (inclusive).

    Blocked, open and close entries may be limited to one shift type and one weekday (0 Sunday to
    6 Saturday). An absence has an ``absence_kind`` and covers whole days, so it takes neither
    (``ck_availability_entries_absence_has_kind``, ``ck_availability_entries_absence_whole_days``).
    Open and close are preferences only (003 FR-103). That one employee holds no two entries for
    the same shift on one date is a service rule under the employee's row lock, because entries are
    ranges (research R6).
    """

    __tablename__ = "availability_entries"

    organization_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("organisations.id"), nullable=False
    )
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"), nullable=False)
    kind: Mapped[str] = mapped_column(String(16), nullable=False)
    absence_kind: Mapped[str | None] = mapped_column(String(16))
    starts_on: Mapped[date] = mapped_column(Date, nullable=False)
    ends_on: Mapped[date] = mapped_column(Date, nullable=False)
    shift_type_id: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("shift_types.id"))
    weekday: Mapped[int | None] = mapped_column(SmallInteger)

    __table_args__ = (
        CheckConstraint(
            "kind IN ('blocked', 'open', 'close', 'absence')", name="ck_availability_entries_kind"
        ),
        CheckConstraint(
            "absence_kind IS NULL OR absence_kind IN ('leave', 'reserve_duty', 'sick')",
            name="ck_availability_entries_absence_kind",
        ),
        CheckConstraint(
            "(kind = 'absence') = (absence_kind IS NOT NULL)",
            name="ck_availability_entries_absence_has_kind",
        ),
        CheckConstraint(
            "kind <> 'absence' OR (shift_type_id IS NULL AND weekday IS NULL)",
            name="ck_availability_entries_absence_whole_days",
        ),
        CheckConstraint("ends_on >= starts_on", name="ck_availability_entries_dates"),
        CheckConstraint(
            "weekday IS NULL OR weekday BETWEEN 0 AND 6", name="ck_availability_entries_weekday"
        ),
        Index("idx_availability_entries_user", "user_id", "starts_on"),
        Index("idx_availability_entries_shift_type", "shift_type_id"),
    )
