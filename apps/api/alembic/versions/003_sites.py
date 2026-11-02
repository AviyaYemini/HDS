"""003 sites.

Creates ``sites``: the physical locations where projects run, each with an address and an IANA
time zone.

Why a time zone per site: timestamps are stored in UTC and shown in the zone of the place the
work happens (Constitution VII). Sites have no soft-delete flag because the API cannot delete
them.

Revision ID: 003_sites
Revises: 002_clients
Create Date: 2026-11-01
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "003_sites"
down_revision: str | None = "002_clients"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the sites table and its organisation index."""
    op.create_table(
        "sites",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("organization_id", sa.UUID(), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("address", sa.Text(), nullable=False),
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
        sa.ForeignKeyConstraint(["organization_id"], ["organisations.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_sites_org", "sites", ["organization_id"])


def downgrade() -> None:
    """Drop the sites index and table."""
    op.drop_index("idx_sites_org", table_name="sites")
    op.drop_table("sites")
