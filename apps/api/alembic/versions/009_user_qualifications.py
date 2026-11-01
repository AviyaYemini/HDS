"""009 user_qualifications

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
    op.create_table(
        "user_qualifications",
        sa.Column("user_id", sa.UUID(), nullable=False),
        sa.Column("qualification_id", sa.UUID(), nullable=False),
        sa.ForeignKeyConstraint(["qualification_id"], ["qualifications.id"]),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"]),
        sa.PrimaryKeyConstraint("user_id", "qualification_id"),
    )


def downgrade() -> None:
    op.drop_table("user_qualifications")
