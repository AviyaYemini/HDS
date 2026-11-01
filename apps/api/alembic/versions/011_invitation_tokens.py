"""011 invitation_tokens

Revision ID: 011_invitation_tokens
Revises: 010_projects
Create Date: 2026-11-01
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "011_invitation_tokens"
down_revision: str | None = "010_projects"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "invitation_tokens",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("user_id", sa.UUID(), nullable=False),
        sa.Column("token_hash", sa.String(64), nullable=False, unique=True),
        sa.Column(
            "purpose",
            sa.String(16),
            nullable=False,
            comment="invite | reset",
        ),
        sa.Column("expires_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("used_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint("purpose IN ('invite','reset')", name="ck_invitation_tokens_purpose"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_invitation_tokens_user", "invitation_tokens", ["user_id"])


def downgrade() -> None:
    op.drop_index("idx_invitation_tokens_user", table_name="invitation_tokens")
    op.drop_table("invitation_tokens")
