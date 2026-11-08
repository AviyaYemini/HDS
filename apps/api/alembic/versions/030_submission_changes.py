"""030 submission changes.

What changes: ``availability_submissions`` gains ``changed_by`` (an Admin, nullable) and
``changed_at`` (nullable): who last changed a submitted snapshot by hand, and when.

Why: an Admin's hand edit of an employee's availability counts at once in that employee's
submitted snapshot (003 FR-107; 007 review R51, R52). It must not overwrite ``submitted_at`` and
``submitted_by``: an employee who submitted on time stays shown as on time, and the Admin's change
is recorded beside it. A fresh submission by the employee clears both columns.

Numbered 030 because 027 to 029 are taken by group D and F migrations, which revise 026 and are
not merged yet; when they are, a merge revision joins the two heads (no applied migration changes).

Existing rows: none of the columns' values are set; every existing submission stays the
employee's own.

Rolling update: an expand step. The previous release neither reads nor writes the columns.

Downgrade drops both columns; the record of who changed a snapshot is lost, never the snapshot.

Revision ID: 030_submission_changes
Revises: 026_availability_submissions
Create Date: 2026-11-08
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "030_submission_changes"
down_revision: str | None = "026_availability_submissions"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add the two nullable columns."""
    op.add_column(
        "availability_submissions",
        sa.Column("changed_by", sa.Uuid(), sa.ForeignKey("users.id"), nullable=True),
    )
    op.add_column(
        "availability_submissions",
        sa.Column("changed_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    """Drop the two columns."""
    op.drop_column("availability_submissions", "changed_at")
    op.drop_column("availability_submissions", "changed_by")
