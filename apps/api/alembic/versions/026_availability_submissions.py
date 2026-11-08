"""026 availability submissions.

What changes: a new table ``availability_submissions`` (one row per employee and window: when and
by whom it was last submitted, a JSONB snapshot of the submitted entries and its SHA-256), unique
on ``(window_id, user_id)`` and removed with its window. ``submission_windows`` gains
``requires_submission`` (boolean, not null, default true).

Why: availability gets an explicit Submit (specs/007-ux-redesign FR-014, decision O4). Once a
window's deadline passes, scheduling reads what each employee submitted, and an employee who did not
submit counts as unavailable for the whole window (clarification Q4, research R6).

Existing rows: windows whose deadline had already passed get ``requires_submission = false``, so the
new rule never reaches back into weeks that had no Submit; their entries keep working as before.
Windows still open get true: their employees see the Submit banner from now on. No submission rows
are created.

Rolling update: an expand step. The previous release neither reads nor writes the table or the
column, and its inserts into ``submission_windows`` get the default (006 FR-016).

Downgrade drops the table and the column; submissions are lost, never an availability entry.

Revision ID: 026_availability_submissions
Revises: 025_org_contact_sessions
Create Date: 2026-11-07
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "026_availability_submissions"
down_revision: str | None = "025_org_contact_sessions"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the submissions table and mark the windows that closed before 007."""
    op.add_column(
        "submission_windows",
        sa.Column("requires_submission", sa.Boolean(), nullable=False, server_default=sa.true()),
    )
    op.execute("UPDATE submission_windows SET requires_submission = false WHERE deadline <= now()")
    op.create_table(
        "availability_submissions",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("organization_id", sa.Uuid(), sa.ForeignKey("organisations.id"), nullable=False),
        sa.Column(
            "window_id",
            sa.Uuid(),
            sa.ForeignKey("submission_windows.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("user_id", sa.Uuid(), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("submitted_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("submitted_by", sa.Uuid(), sa.ForeignKey("users.id"), nullable=True),
        sa.Column("snapshot", postgresql.JSONB(), nullable=False),
        sa.Column("snapshot_hash", sa.String(64), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.UniqueConstraint("window_id", "user_id", name="uq_availability_submissions_window_user"),
    )


def downgrade() -> None:
    """Drop the submissions table and the window column."""
    op.drop_table("availability_submissions")
    op.drop_column("submission_windows", "requires_submission")
