"""004 users

Revision ID: 004_users
Revises: 003_sites
Create Date: 2026-11-01
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "004_users"
down_revision: str | None = "003_sites"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "users",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("organization_id", sa.UUID(), nullable=False),
        sa.Column("email", sa.String(320), nullable=False),
        sa.Column("password_hash", sa.String(255), nullable=False),
        sa.Column("full_name", sa.String(255), nullable=False),
        sa.Column("phone", sa.String(32), nullable=True),
        sa.Column("role", sa.Enum("employee", "manager", "admin", "client", name="user_role"), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("is_deleted", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("deleted_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("default_hourly_rate", sa.Numeric(12, 4), nullable=True),
        sa.Column("client_id", sa.UUID(), nullable=True),
        sa.Column("failed_login_attempts", sa.SmallInteger(), nullable=False, server_default=sa.text("0")),
        sa.Column("locked_until", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint("default_hourly_rate > 0", name="ck_users_hourly_rate_positive"),
        sa.ForeignKeyConstraint(["organization_id"], ["organisations.id"]),
        sa.ForeignKeyConstraint(["client_id"], ["clients.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "uq_users_org_email",
        "users",
        ["organization_id", "email"],
        unique=True,
        postgresql_where=sa.text("is_deleted = false"),
    )
    op.create_index("idx_users_org", "users", ["organization_id"])
    op.create_index(
        "idx_users_client",
        "users",
        ["client_id"],
        postgresql_where=sa.text("client_id IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index("idx_users_client", table_name="users")
    op.drop_index("idx_users_org", table_name="users")
    op.drop_index("uq_users_org_email", table_name="users")
    op.drop_table("users")
