"""007 qualifications.

Creates ``qualifications``: named skills or certificates an employee can hold (FR-019).

Why the unique index ``uq_qualifications_org_name``: names are unique within an organisation,
reported by the service as ``qualifications.duplicate_name``; the index covers concurrent
requests.

Revision ID: 007_qualifications
Revises: 006_groups
Create Date: 2026-11-01
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "007_qualifications"
down_revision: str | None = "006_groups"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the qualifications table and its unique name index."""
    op.create_table(
        "qualifications",
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
        "uq_qualifications_org_name",
        "qualifications",
        ["organization_id", "name"],
        unique=True,
    )


def downgrade() -> None:
    """Drop the qualifications index and table."""
    op.drop_index("uq_qualifications_org_name", table_name="qualifications")
    op.drop_table("qualifications")
