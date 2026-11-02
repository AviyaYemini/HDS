"""005 refresh tokens.

Creates ``refresh_tokens``: a server-side record of every issued refresh token.

Why only a hash is stored (``token_hash``, unique): a database leak must not yield usable
tokens. Why ``revoked_at`` instead of deleting: rotation, sign-out and sign-out-everywhere
(FR-002, FR-003) revoke tokens, and a replayed old token is then refused as revoked.

Revision ID: 005_refresh_tokens
Revises: 004_users
Create Date: 2026-11-01
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "005_refresh_tokens"
down_revision: str | None = "004_users"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the refresh_tokens table and its user index."""
    op.create_table(
        "refresh_tokens",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("user_id", sa.UUID(), nullable=False),
        sa.Column("token_hash", sa.String(64), nullable=False, unique=True),
        sa.Column("expires_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("revoked_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_refresh_tokens_user", "refresh_tokens", ["user_id"])


def downgrade() -> None:
    """Drop the refresh_tokens index and table."""
    op.drop_index("idx_refresh_tokens_user", table_name="refresh_tokens")
    op.drop_table("refresh_tokens")
