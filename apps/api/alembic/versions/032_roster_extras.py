"""032 rule overrides, week templates and publish events.

What changes: three new tables.

- ``rule_overrides``: a Manager's or Admin's recorded reason for keeping an assignment despite a
  warning, unique per assignment and warning code, removed with the assignment.
- ``week_templates``: a project's saved week of shifts (JSONB), its name unique per project,
  removed with the project.
- ``publish_events``: one row per person affected by a publish, with the publisher's notify choice
  and a ``delivered_at`` feature 010 sets.

Why: layered warnings with an override reason (specs/007-ux-redesign FR-034, S10), copy week and
week templates (FR-038, S14), and the publish review's one notification per person, recorded for
delivery by feature 010 (FR-036, research R12).

Existing rows: none; the tables start empty.

Rolling update: an expand step. The previous release never reads or writes these tables (006
FR-016).

Downgrade drops the three tables; overrides, templates and undelivered events are lost, never a
shift or an assignment.

Renumbered from 028 for the same reason as 031; it revises 031.

Revision ID: 032_roster_extras
Revises: 031_assignment_times
Create Date: 2026-11-07
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "032_roster_extras"
down_revision: str | None = "031_assignment_times"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _base() -> list[sa.Column[object]]:
    return [
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("organization_id", sa.Uuid(), sa.ForeignKey("organisations.id"), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
    ]


def upgrade() -> None:
    """Create the three tables."""
    op.create_table(
        "rule_overrides",
        *_base(),
        sa.Column(
            "assignment_id",
            sa.Uuid(),
            sa.ForeignKey("assignments.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("warning_code", sa.String(64), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("created_by", sa.Uuid(), sa.ForeignKey("users.id"), nullable=False),
        sa.UniqueConstraint(
            "assignment_id", "warning_code", name="uq_rule_overrides_assignment_code"
        ),
        sa.CheckConstraint("length(reason) BETWEEN 1 AND 500", name="ck_rule_overrides_reason"),
    )
    op.create_table(
        "week_templates",
        *_base(),
        sa.Column(
            "project_id",
            sa.Uuid(),
            sa.ForeignKey("projects.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("name", sa.String(80), nullable=False),
        sa.Column("body", postgresql.JSONB(), nullable=False),
        sa.Column("created_by", sa.Uuid(), sa.ForeignKey("users.id"), nullable=True),
        sa.UniqueConstraint("project_id", "name", name="uq_week_templates_project_name"),
        sa.CheckConstraint("length(name) BETWEEN 1 AND 80", name="ck_week_templates_name"),
    )
    op.create_table(
        "publish_events",
        *_base(),
        sa.Column("project_id", sa.Uuid(), sa.ForeignKey("projects.id"), nullable=False),
        sa.Column("user_id", sa.Uuid(), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("published_by", sa.Uuid(), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("first_day", sa.Date(), nullable=False),
        sa.Column("last_day", sa.Date(), nullable=False),
        sa.Column("shift_count", sa.SmallInteger(), nullable=False),
        sa.Column("notify", sa.Boolean(), nullable=False),
        sa.Column("delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("shift_count >= 1", name="ck_publish_events_shift_count"),
    )
    op.create_index(
        "idx_publish_events_undelivered", "publish_events", ["organization_id", "delivered_at"]
    )


def downgrade() -> None:
    """Drop the three tables."""
    op.drop_index("idx_publish_events_undelivered", table_name="publish_events")
    op.drop_table("publish_events")
    op.drop_table("week_templates")
    op.drop_table("rule_overrides")
