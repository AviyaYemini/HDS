"""010 projects

Revision ID: 010_projects
Revises: 009_user_qualifications
Create Date: 2026-11-01
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "010_projects"
down_revision: str | None = "009_user_qualifications"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "projects",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("organization_id", sa.UUID(), nullable=False),
        sa.Column("client_id", sa.UUID(), nullable=False),
        sa.Column("site_id", sa.UUID(), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("type", postgresql.ENUM("security", "training", name="project_type", create_type=False), nullable=False),
        sa.Column(
            "billing_rate",
            sa.Numeric(12, 4),
            nullable=False,
            comment="Must be > 0. Stored as NUMERIC(12,4), never Float.",
        ),
        sa.Column("start_date", sa.Date(), nullable=False),
        sa.Column("end_date", sa.Date(), nullable=True),
        sa.Column(
            "status",
            postgresql.ENUM("active", "ended", name="project_status", create_type=False),
            nullable=False,
            server_default="active",
        ),
        sa.Column("is_deleted", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("deleted_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint("billing_rate > 0", name="ck_projects_billing_rate_positive"),
        sa.ForeignKeyConstraint(["client_id"], ["clients.id"]),
        sa.ForeignKeyConstraint(["organization_id"], ["organisations.id"]),
        sa.ForeignKeyConstraint(["site_id"], ["sites.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("idx_projects_org", "projects", ["organization_id"])
    op.create_index("idx_projects_client", "projects", ["client_id"])


def downgrade() -> None:
    op.drop_index("idx_projects_client", table_name="projects")
    op.drop_index("idx_projects_org", table_name="projects")
    op.drop_table("projects")
