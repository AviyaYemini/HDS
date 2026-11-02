"""012 global unique email.

Sign-in resolves the account by email alone (FR-001), so an email must be unique across
ALL organisations, not just within one. Replaces ``uq_users_org_email``.

Fails if two non-deleted users in different organisations already share an email; resolve
those rows first.

Revision ID: 012_global_unique_email
Revises: 011_invitation_tokens
Create Date: 2026-11-01
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "012_global_unique_email"
down_revision: str | None = "011_invitation_tokens"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Replace the per-organisation unique email index with a global one.

    Raises:
        sqlalchemy.exc.IntegrityError: If two non-deleted users in different organisations
            already share an email; resolve those rows first.
    """
    op.drop_index("uq_users_org_email", table_name="users")
    op.create_index(
        "uq_users_email",
        "users",
        ["email"],
        unique=True,
        postgresql_where=sa.text("is_deleted = false"),
    )


def downgrade() -> None:
    """Restore the per-organisation unique email index.

    Always succeeds: global uniqueness already implies uniqueness within each organisation.
    """
    op.drop_index("uq_users_email", table_name="users")
    op.create_index(
        "uq_users_org_email",
        "users",
        ["organization_id", "email"],
        unique=True,
        postgresql_where=sa.text("is_deleted = false"),
    )
