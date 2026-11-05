"""019 expiry index, retire lock columns.

What changes: adds the indexes ``idx_refresh_tokens_expires_at`` and
``idx_invitation_tokens_expires_at`` on ``expires_at``, and drops
``users.failed_login_attempts`` and ``users.locked_until``.

Why: the bulk clean-up finds expired sign-in tokens and one-time links by ``expires_at``. Without an
index it read every row, so its work grew with all records instead of the expired ones
(specs/003-availability, FR-008). Since 002, every address's sign-in failures are counted in
``login_throttle``, so the two account columns hold nothing anyone reads (FR-009). 002 stopped using
them and nothing is deployed, which meets the two-step rule for dropping columns (Constitution X).

Existing rows: kept. Any values left in the two columns from before 002 are discarded.

Downgrade drops the indexes and adds the columns back as migration 004 defined them,
``failed_login_attempts SMALLINT NOT NULL DEFAULT 0`` and ``locked_until TIMESTAMPTZ NULL``, so
every account starts with no count and no lock. The dropped values are not restored.

Revision ID: 019_expiry_index_retire_locks
Revises: 018_login_throttle
Create Date: 2026-11-03
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "019_expiry_index_retire_locks"
down_revision: str | None = "018_login_throttle"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Index token expiry and drop the two retired account columns."""
    op.create_index("idx_refresh_tokens_expires_at", "refresh_tokens", ["expires_at"])
    op.create_index("idx_invitation_tokens_expires_at", "invitation_tokens", ["expires_at"])
    op.drop_column("users", "failed_login_attempts")
    op.drop_column("users", "locked_until")


def downgrade() -> None:
    """Add the columns back empty, as 004 defined them, and drop the indexes."""
    op.add_column(
        "users",
        sa.Column(
            "failed_login_attempts", sa.SmallInteger(), nullable=False, server_default=sa.text("0")
        ),
    )
    op.add_column("users", sa.Column("locked_until", sa.TIMESTAMP(timezone=True), nullable=True))
    op.drop_index("idx_invitation_tokens_expires_at", table_name="invitation_tokens")
    op.drop_index("idx_refresh_tokens_expires_at", table_name="refresh_tokens")
