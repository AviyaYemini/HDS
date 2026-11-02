"""015 refresh revoked reason.

What changes: adds ``refresh_tokens.revoked_reason`` (short text, null) and the check constraint
``ck_refresh_tokens_revoked_reason``: ``revoked_reason IS NULL OR revoked_reason IN ('rotated',
'logout', 'all', 'account')``.

Why: a refresh token is single use. If a token that was already replaced is presented again, that
may mean it was stolen, and the right response is to end all of the user's sessions
(specs/002-security-hardening, FR-209). But a token can also be revoked because the user signed out,
because every session ended, or because the account was deactivated, and presenting one of those
is not evidence of theft. Only ``rotated`` can be. The reason therefore has to be stored.

Existing rows: tokens revoked before this migration keep an empty reason. The application treats an
empty reason as "unknown": it refuses the token and does nothing else, so the migration can never
cause a user to be signed out everywhere because of data it could not classify.

Downgrade drops the constraint and then the column; the reasons are lost, the tokens are kept.

Revision ID: 015_refresh_revoked_reason
Revises: 014_user_activation
Create Date: 2026-11-02
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "015_refresh_revoked_reason"
down_revision: str | None = "014_user_activation"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

CONSTRAINT = "ck_refresh_tokens_revoked_reason"
CONDITION = "revoked_reason IS NULL OR revoked_reason IN ('rotated', 'logout', 'all', 'account')"


def upgrade() -> None:
    """Add the nullable reason column and restrict it to the four known values."""
    op.add_column("refresh_tokens", sa.Column("revoked_reason", sa.String(16), nullable=True))
    op.create_check_constraint(CONSTRAINT, "refresh_tokens", CONDITION)


def downgrade() -> None:
    """Drop the check constraint and then the column; the tokens themselves are kept."""
    op.drop_constraint(CONSTRAINT, "refresh_tokens", type_="check")
    op.drop_column("refresh_tokens", "revoked_reason")
