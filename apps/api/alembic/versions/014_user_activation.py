"""014 user activation.

What changes: adds ``users.activated_at`` (timestamp with time zone, null), fills it for every
existing account that was ever active, checks that no active account is left without it, and then
adds the constraint ``ck_users_active_has_activated_at``:
``CHECK (is_active = false OR activated_at IS NOT NULL)``.

Why: an invitation must complete only a *pending* account (invited, never activated). Before this,
a pending account and an account that staff had deactivated looked the same (both inactive), so a
leftover invitation link could bring a deactivated person back (review finding 1,
specs/002-security-hardening, FR-205). ``activated_at`` tells them apart, and the constraint stops
any writer that forgets to set it from creating an active account that would look pending once it
is deactivated.

Backfill: every account gets ``activated_at = created_at`` EXCEPT those that are inactive, hold an
invitation link and have never used one. Those stay pending (null). That rule is exact for the
cases that can be told apart from the data: an inactive account with a used invitation was
activated and later deactivated; one that was re-invited after being activated still has the
used link; one with only a reset link was active (resets are sent to active accounts only); one
created directly by staff and deactivated has no invitation at all. The earlier time is
``created_at``, which is when the account first existed, so it is not the exact moment of first
sign-in; nothing reads it for anything but the pending-or-not question.

Order and safety: the backfill runs BEFORE the constraint is added, and ``ensure_no_rejected_rows``
runs between them. If any active account still has no ``activated_at`` (for example one written
by an old application instance while the migration ran), the migration stops with a message that
lists the account ids, and because PostgreSQL DDL is transactional nothing is left half-applied.
Fix those rows (set ``activated_at``) and run the migration again.

Downgrade drops the constraint, then the column. The activation times are lost; upgrading again
rebuilds them with the same pending/not-pending result from the same data.

Revision ID: 014_user_activation
Revises: 013_membership_cascade
Create Date: 2026-11-02
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "014_user_activation"
down_revision: str | None = "013_membership_cascade"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

CONSTRAINT = "ck_users_active_has_activated_at"
CONDITION = "is_active = false OR activated_at IS NOT NULL"

# Pending = inactive, holds an invitation link, never used one. Everyone else was active once.
BACKFILL = """
UPDATE users SET activated_at = created_at
WHERE NOT (
    is_active = false
    AND EXISTS (
        SELECT 1 FROM invitation_tokens t
        WHERE t.user_id = users.id AND t.purpose = 'invite'
    )
    AND NOT EXISTS (
        SELECT 1 FROM invitation_tokens t
        WHERE t.user_id = users.id AND t.purpose = 'invite' AND t.used_at IS NOT NULL
    )
)
"""

# How many account ids the error message lists; the count is always exact.
LIST_LIMIT = 20


def ensure_no_rejected_rows(connection: sa.Connection) -> None:
    """Raise, naming the accounts, if the constraint would reject any existing row.

    Args:
        connection: The connection the migration is running on.

    Raises:
        RuntimeError: If an active account has no ``activated_at``. The message gives the exact
            count and the first ``LIST_LIMIT`` account ids (identifiers only, no addresses).
    """
    ids = (
        connection.execute(
            sa.text(
                "SELECT id FROM users WHERE is_active AND activated_at IS NULL "
                "ORDER BY created_at, id"
            )
        )
        .scalars()
        .all()
    )
    if not ids:
        return
    listed = ", ".join(str(i) for i in ids[:LIST_LIMIT])
    more = f" (first {LIST_LIMIT} shown)" if len(ids) > LIST_LIMIT else ""
    raise RuntimeError(
        f"Cannot add {CONSTRAINT}: {len(ids)} active user(s) have no activated_at{more}: "
        f"{listed}. Set activated_at on these accounts, then run the migration again."
    )


def upgrade() -> None:
    """Add the column, backfill it, verify, then add the check constraint, in that order.

    Raises:
        RuntimeError: From `ensure_no_rejected_rows` when the constraint would reject a row.
    """
    op.add_column("users", sa.Column("activated_at", sa.TIMESTAMP(timezone=True), nullable=True))
    op.execute(BACKFILL)
    ensure_no_rejected_rows(op.get_bind())
    op.create_check_constraint(CONSTRAINT, "users", CONDITION)


def downgrade() -> None:
    """Drop the check constraint and then the column; the activation times are lost."""
    op.drop_constraint(CONSTRAINT, "users", type_="check")
    op.drop_column("users", "activated_at")
