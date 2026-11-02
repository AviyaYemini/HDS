"""006 groups.

Creates ``groups``: named sets of employees (FR-018).

Why the unique index ``uq_groups_org_name``: two groups of one organisation cannot share a
name, and the service reports the clash as ``groups.duplicate_name``; the index is the backstop
when two requests race.

Revision ID: 006_groups
Revises: 005_refresh_tokens
Create Date: 2026-11-01
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "006_groups"
down_revision: str | None = "005_refresh_tokens"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the groups table and its unique name index."""
    op.create_table(
        "groups",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("organization_id", sa.UUID(), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
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
        sa.ForeignKeyConstraint(["organization_id"], ["organisations.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "uq_groups_org_name",
        "groups",
        ["organization_id", "name"],
        unique=True,
    )


def downgrade() -> None:
    """Drop the groups index and table."""
    op.drop_index("uq_groups_org_name", table_name="groups")
    op.drop_table("groups")
