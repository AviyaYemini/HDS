"""002 clients.

Creates ``clients``: the customers that projects are delivered for, with contact details, a
contract start and optional end date, and ``is_active`` / ``is_deleted`` flags.

Why soft delete: users and projects reference a client, and history must survive its removal
(ADR 002). Why now: ``users.client_id`` in the next-but-one migration points here.

Revision ID: 002_clients
Revises: 001_init_organisations
Create Date: 2026-11-01
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "002_clients"
down_revision: str | None = "001_init_organisations"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the clients table and its organisation index."""
    op.create_table(
        "clients",
        sa.Column("id", sa.UUID(), server_default=sa.text("gen_random_uuid()"), nullable=False),
        sa.Column("organization_id", sa.UUID(), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("contact_name", sa.String(255), nullable=True),
        sa.Column("contact_email", sa.String(320), nullable=True),
        sa.Column("contact_phone", sa.String(32), nullable=True),
        sa.Column("contract_start", sa.Date(), nullable=False),
        sa.Column("contract_end", sa.Date(), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("is_deleted", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("deleted_at", sa.TIMESTAMP(timezone=True), nullable=True),
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
    op.create_index("idx_clients_org", "clients", ["organization_id"])


def downgrade() -> None:
    """Drop the clients index and table."""
    op.drop_index("idx_clients_org", table_name="clients")
    op.drop_table("clients")
