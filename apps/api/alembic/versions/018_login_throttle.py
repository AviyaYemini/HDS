"""018 login throttle.

What changes: adds the table ``login_throttle``: ``kind`` and ``key_hash`` (the primary key),
``failures``, ``window_started_at``, ``locked_until`` and ``updated_at``, with the check constraints
``ck_login_throttle_kind`` and ``ck_login_throttle_failures`` and the index
``ix_login_throttle_updated_at``.

Why: the failure count and lock live on the user row, so an address with no account had nothing to
count against, and a locked registered address could be told apart from an unregistered one
(specs/002-security-hardening, FR-221). The same table holds the failure count per network address
and the request count per address for password resets (FR-247). Only hashes are stored.

Existing rows: none; the table starts empty.

Downgrade drops the table. The counts are disposable: losing them only restarts each count.

Revision ID: 018_login_throttle
Revises: 017_pending_backfill_correction
Create Date: 2026-11-03
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "018_login_throttle"
down_revision: str | None = "017_pending_backfill_correction"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

TABLE = "login_throttle"
INDEX = "ix_login_throttle_updated_at"


def upgrade() -> None:
    """Create the table, its two check constraints and the index used by the batch clean."""
    op.create_table(
        TABLE,
        sa.Column("kind", sa.String(16), nullable=False),
        sa.Column("key_hash", sa.String(64), nullable=False),
        sa.Column("failures", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("window_started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("locked_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.PrimaryKeyConstraint("kind", "key_hash"),
        sa.CheckConstraint(
            "kind IN ('address', 'network', 'reset')", name="ck_login_throttle_kind"
        ),
        sa.CheckConstraint("failures >= 0", name="ck_login_throttle_failures"),
    )
    op.create_index(INDEX, TABLE, ["updated_at"])


def downgrade() -> None:
    """Drop the index and the table; the counts are disposable."""
    op.drop_index(INDEX, table_name=TABLE)
    op.drop_table(TABLE)
