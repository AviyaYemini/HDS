"""Overrides of roster warnings, saved week templates, and publish events (007 data-model 032).

Layer: models. Depends on ``hds.models.base`` only.
"""

import uuid
from datetime import date, datetime
from typing import Any

from sqlalchemy import (
    JSON,
    Boolean,
    CheckConstraint,
    Date,
    DateTime,
    ForeignKey,
    Index,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from hds.models.base import TimestampedBase

# JSONB in PostgreSQL; plain JSON where the SQLite unit-test schema is built.
_JSON = JSON().with_variant(JSONB(), "postgresql")


class RuleOverride(TimestampedBase):
    """A Manager's or Admin's decision to keep an assignment despite a warning (007 FR-034, S10).

    One per assignment and warning code, with who decided, when (``created_at``) and why. A warning
    with an override is no longer open in the publish review. Warnings never refuse a change (ADR
    009); hard stops have no override. Removed with its assignment.
    """

    __tablename__ = "rule_overrides"

    organization_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("organisations.id"), nullable=False
    )
    assignment_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("assignments.id", ondelete="CASCADE"), nullable=False
    )
    warning_code: Mapped[str] = mapped_column(String(64), nullable=False)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    created_by: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"), nullable=False)

    __table_args__ = (
        UniqueConstraint("assignment_id", "warning_code", name="uq_rule_overrides_assignment_code"),
        CheckConstraint("length(reason) BETWEEN 1 AND 500", name="ck_rule_overrides_reason"),
    )


class WeekTemplate(TimestampedBase):
    """A saved week of a project's shifts, applied to other weeks (007 FR-038, S14).

    ``body`` is a list of planned shifts, each ``{"day": 0-6 (0 Sunday), "start": "HH:MM",
    "minutes": int, "shift_type_id": str, "headcount": int, "people": [{"user_id": str,
    "start_minutes": int | null, "end_minutes": int | null}]}``, local to the project's site;
    ``start_minutes`` and ``end_minutes`` are a person's own times as offsets from the shift's
    start. Names are unique per project. Removed with its project.
    """

    __tablename__ = "week_templates"

    organization_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("organisations.id"), nullable=False
    )
    project_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("projects.id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(80), nullable=False)
    body: Mapped[list[dict[str, Any]]] = mapped_column(_JSON, nullable=False)
    created_by: Mapped[uuid.UUID | None] = mapped_column(ForeignKey("users.id"))

    __table_args__ = (
        UniqueConstraint("project_id", "name", name="uq_week_templates_project_name"),
        CheckConstraint("length(name) BETWEEN 1 AND 80", name="ck_week_templates_name"),
    )


class PublishEvent(TimestampedBase):
    """One person's share of one publish, recorded for delivery (007 FR-036, research R12).

    One row per affected person per publish, however many shifts it covers. ``notify`` is the
    publisher's choice (Notify everyone or Publish silently). Feature 010 delivers the rows and
    sets ``delivered_at``; 007 only records them.
    """

    __tablename__ = "publish_events"

    organization_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("organisations.id"), nullable=False
    )
    project_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("projects.id"), nullable=False)
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"), nullable=False)
    published_by: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id"), nullable=False)
    first_day: Mapped[date] = mapped_column(Date, nullable=False)
    last_day: Mapped[date] = mapped_column(Date, nullable=False)
    shift_count: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    notify: Mapped[bool] = mapped_column(Boolean, nullable=False)
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        CheckConstraint("shift_count >= 1", name="ck_publish_events_shift_count"),
        Index("idx_publish_events_undelivered", "organization_id", "delivered_at"),
    )
