"""001 init organisations and enums.

Creates the ``organisations`` table, the tenant that owns every other record, and the three
PostgreSQL enum types (``user_role``, ``project_type``, ``project_status``).

Why the enums are here: later tables reference them with ``create_type=False``, so they must
exist first. Adding a value to an enum needs its own migration.

Downgrade drops the table and then the enums.

Revision ID: 001_init_organisations
Revises:
Create Date: 2026-11-01
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "001_init_organisations"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the three enum types, then the organisations table."""
    op.execute("CREATE TYPE user_role AS ENUM ('employee', 'manager', 'admin', 'client')")
    op.execute("CREATE TYPE project_type AS ENUM ('security', 'training')")
    op.execute("CREATE TYPE project_status AS ENUM ('active', 'ended')")

    op.create_table(
        "organisations",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("timezone", sa.String(64), nullable=False),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade() -> None:
    """Drop the organisations table, then the enum types."""
    op.drop_table("organisations")
    op.execute("DROP TYPE IF EXISTS project_status")
    op.execute("DROP TYPE IF EXISTS project_type")
    op.execute("DROP TYPE IF EXISTS user_role")
