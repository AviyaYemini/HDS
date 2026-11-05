"""Staffing needs: how many people a project needs on a shift type on a weekday (004 FR-001).

Layer: models. Depends on ``hds.models.base`` only.
"""

import uuid

from sqlalchemy import CheckConstraint, ForeignKey, Index, SmallInteger, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from hds.models.base import TimestampedBase


class StaffingNeed(TimestampedBase):
    """A project's need for ``headcount`` people on one shift type on one weekday.

    The weekday counts 0 Sunday to 6 Saturday; at most one need per project, shift type and weekday.
    The generator makes a slot from each need on each date of the range (004 FR-006). The shift type
    must be one the project uses and not retired; the service checks that, and removing a type from
    a project or retiring it removes its needs (FR-002). Removed with the project.
    """

    __tablename__ = "staffing_needs"

    organization_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("organisations.id"), nullable=False
    )
    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("projects.id", ondelete="CASCADE"), nullable=False
    )
    shift_type_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("shift_types.id"), nullable=False)
    weekday: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    headcount: Mapped[int] = mapped_column(SmallInteger, nullable=False)

    __table_args__ = (
        CheckConstraint("weekday BETWEEN 0 AND 6", name="ck_staffing_needs_weekday"),
        CheckConstraint("headcount >= 1", name="ck_staffing_needs_headcount_positive"),
        UniqueConstraint(
            "project_id", "shift_type_id", "weekday", name="uq_staffing_needs_project_type_weekday"
        ),
        Index("idx_staffing_needs_shift_type", "shift_type_id"),
    )
