"""009 user_qualifications.

Creates the ``user_qualifications`` link table: an employee can hold zero or more
qualifications (FR-020). The composite primary key stops the same assignment from being stored
twice.

The foreign key to ``qualifications`` has no cascade here; migration 013 adds it so deleting a
qualification removes its assignments.

Revision ID: 009_user_qualifications
Revises: 008_user_groups
Create Date: 2026-11-01
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "009_user_qualifications"
down_revision: str | None = "008_user_groups"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the user_qualifications link table."""
    op.create_table(
        "user_qualifications",
        sa.Column("user_id", sa.UUID(), nullable=False),
        sa.Column("qualification_id", sa.UUID(), nullable=False),
        sa.ForeignKeyConstraint(["qualification_id"], ["qualifications.id"]),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"]),
        sa.PrimaryKeyConstraint("user_id", "qualification_id"),
    )


def downgrade() -> None:
    """Drop the user_qualifications link table."""
    op.drop_table("user_qualifications")
