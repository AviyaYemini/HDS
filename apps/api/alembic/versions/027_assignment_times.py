"""027 assignment times.

What changes: ``assignments`` gains ``starts_at`` and ``ends_at`` (``TIMESTAMPTZ``, nullable) with
the checks ``ck_assignments_times_paired`` (both set or both null) and ``ck_assignments_times_order``
(the end after the start).

Why: each person's work in a shift gets its own start and end, defaulting to the shift's, so the
roster can show a half shift or two extra hours, and pay and the rule checks use the real times
(specs/007-ux-redesign FR-026, FR-027, decision O7, research R5). How far they may reach past the
shift (4 hours either side) is a service rule, because it compares with another table.

Existing rows: both null, which means the shift's times, exactly what every assignment meant before.

Rolling update: an expand step. The previous release neither reads nor writes the columns, and its
inserts leave them null (006 FR-016).

Downgrade drops the checks and the columns; own times are lost and everyone works the shift's
times again, never an assignment.

Revision ID: 027_assignment_times
Revises: 026_availability_submissions
Create Date: 2026-11-07
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "027_assignment_times"
down_revision: str | None = "026_availability_submissions"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add the nullable times and their two checks."""
    op.add_column("assignments", sa.Column("starts_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("assignments", sa.Column("ends_at", sa.DateTime(timezone=True), nullable=True))
    op.create_check_constraint(
        "ck_assignments_times_paired", "assignments", "(starts_at IS NULL) = (ends_at IS NULL)"
    )
    op.create_check_constraint(
        "ck_assignments_times_order", "assignments", "starts_at IS NULL OR ends_at > starts_at"
    )


def downgrade() -> None:
    """Drop the checks and the columns."""
    op.drop_constraint("ck_assignments_times_order", "assignments", type_="check")
    op.drop_constraint("ck_assignments_times_paired", "assignments", type_="check")
    op.drop_column("assignments", "ends_at")
    op.drop_column("assignments", "starts_at")
