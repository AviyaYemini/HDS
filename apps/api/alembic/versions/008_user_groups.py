"""008 user_groups.

Creates the ``user_groups`` link table: an employee can belong to zero or more groups (FR-020).
The composite primary key stops the same membership from being stored twice.

The foreign key to ``groups`` has no cascade here; migration 013 adds it so deleting a group
removes its memberships.

Revision ID: 008_user_groups
Revises: 007_qualifications
Create Date: 2026-11-01
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "008_user_groups"
down_revision: str | None = "007_qualifications"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the user_groups link table."""
    op.create_table(
        "user_groups",
        sa.Column("user_id", sa.UUID(), nullable=False),
        sa.Column("group_id", sa.UUID(), nullable=False),
        sa.ForeignKeyConstraint(["group_id"], ["groups.id"]),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"]),
        sa.PrimaryKeyConstraint("user_id", "group_id"),
    )


def downgrade() -> None:
    """Drop the user_groups link table."""
    op.drop_table("user_groups")
