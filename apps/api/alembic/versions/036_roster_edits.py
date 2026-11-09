"""036 roster edits.

What changes:

- ``assignments`` gains ``note``: a word to the person placed, from whoever placed or moved them,
  shown on that person's own shift page.
- A new table ``roster_edit_events``: one row per manual roster edit (an assignment made, removed,
  moved or swapped), with the people it affects, the note, and who should be alerted: everyone on
  the edited roster, only the people affected, or no one.

Why: specs/008-auto-scheduler-review T042f, T042g (the owner's roster requests of 2026-11-09). As
with ``publish_events``, 008 only records the choice; feature 010 delivers the alerts.

Existing rows: assignments get no note; the table starts empty.

Rolling update: an expand step. ``note`` is nullable and the previous release never reads the new
table (006 FR-016).

Downgrade drops the table and the column; notes and recorded edits are lost, nothing else.

Revision ID: 036_roster_edits
Revises: 035_proposal_downgrade_states
Create Date: 2026-11-09
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "036_roster_edits"
down_revision: str | None = "035_proposal_downgrade_states"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add the assignment's note and the roster edit events."""
    with op.batch_alter_table("assignments") as batch:
        batch.add_column(sa.Column("note", sa.String(500), nullable=True))
    op.create_table(
        "roster_edit_events",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("organization_id", sa.Uuid(), sa.ForeignKey("organisations.id"), nullable=False),
        sa.Column("project_id", sa.Uuid(), sa.ForeignKey("projects.id"), nullable=False),
        sa.Column(
            "shift_id", sa.Uuid(), sa.ForeignKey("shifts.id", ondelete="SET NULL"), nullable=True
        ),
        sa.Column("kind", sa.String(16), nullable=False),
        sa.Column("actor_id", sa.Uuid(), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("affected", postgresql.JSONB(), nullable=False),
        sa.Column("note", sa.String(500), nullable=True),
        sa.Column("alert", sa.String(16), nullable=False),
        sa.Column("delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.CheckConstraint(
            "kind IN ('assigned', 'unassigned', 'moved', 'swapped')",
            name="ck_roster_edit_events_kind",
        ),
        sa.CheckConstraint(
            "alert IN ('everyone', 'affected', 'none')", name="ck_roster_edit_events_alert"
        ),
    )
    op.create_index(
        "idx_roster_edit_events_undelivered",
        "roster_edit_events",
        ["organization_id", "delivered_at"],
    )


def downgrade() -> None:
    """Drop the roster edit events and the assignment's note."""
    op.drop_index("idx_roster_edit_events_undelivered", table_name="roster_edit_events")
    op.drop_table("roster_edit_events")
    with op.batch_alter_table("assignments") as batch:
        batch.drop_column("note")
