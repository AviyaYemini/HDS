"""Shift types: an organisation's kinds of shift, and which of them each project uses.

Layer: models. Depends on ``hds.models.base`` only. The prompt's "shift templates" are these.
"""

import uuid
from datetime import time

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Column,
    ForeignKey,
    Index,
    String,
    Table,
    Time,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from hds.models.base import Base, TimestampedBase

project_shift_types = Table(
    "project_shift_types",
    Base.metadata,
    Column("project_id", ForeignKey("projects.id"), primary_key=True),
    Column("shift_type_id", ForeignKey("shift_types.id"), primary_key=True),
    Index("idx_project_shift_types_type", "shift_type_id"),
)
"""Which shift types a project uses; a shift must be of one of them (003 FR-111)."""


class ShiftType(TimestampedBase):
    """A kind of shift with local start and end times, for example night 23:00 to 07:00.

    The times are local to the site a shift is at; a shift's real instants are computed from them
    when it is created (research R4), and later changes to the type do not move existing shifts
    (003 FR-112). ``crosses_midnight`` is true exactly when the end is not after the start
    (``ck_shift_types_crossing``). Active names are unique per organisation in any letter case
    (``uq_shift_types_org_name``); a retired type (``is_active`` false) frees its name and keeps its
    shifts. Every organisation starts with morning, afternoon and night, inserted by the database
    trigger ``trg_organisations_default_shift_types`` (migration 020), so no application code
    inserts them, and they are removed with their organisation.
    """

    __tablename__ = "shift_types"

    organization_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("organisations.id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    starts_at_local: Mapped[time] = mapped_column(Time, nullable=False)
    ends_at_local: Mapped[time] = mapped_column(Time, nullable=False)
    crosses_midnight: Mapped[bool] = mapped_column(Boolean, nullable=False)
    is_active: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=True, server_default=text("true")
    )

    __table_args__ = (
        CheckConstraint(
            "crosses_midnight = (ends_at_local <= starts_at_local)", name="ck_shift_types_crossing"
        ),
        Index("idx_shift_types_org", "organization_id"),
        Index(
            "uq_shift_types_org_name",
            "organization_id",
            text("lower(name)"),
            unique=True,
            postgresql_where=text("is_active"),
        ),
    )
