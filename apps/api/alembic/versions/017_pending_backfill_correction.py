"""017 pending backfill correction.

What changes: accounts that migration 014 left pending although they were active once get
``activated_at = created_at``, so they count as deactivated. No schema changes.

Why: migration 014 called an account pending if it was inactive, held an invitation link and had
never used one. A staff-created employee who was deactivated and then re-invited under the old code
matches that rule, so a leftover link could reactivate them (specs/002-security-hardening, F07,
FR-229). Migration 014 is applied and cannot be edited, so the correction is a new migration.

Existing rows: an inactive, non-deleted account with an empty ``activated_at`` is corrected when
there is evidence of past activity, which is either of:

* any refresh token, because only a sign-in creates one;
* any password-reset link, used or not, because reset links are only ever sent to active accounts.

Invitation links are NOT evidence. Cancelling a link also sets ``used_at``, and re-inviting a
pending account cancels its earlier link, so a genuine invitee can hold a "used" invitation link.
Since migration 014 an accepted invitation always sets ``activated_at``, so an empty
``activated_at`` with a used invitation link can only mean a cancelled link.

Accounts with no evidence stay pending. Those whose latest invitation link has already expired
cannot be told apart from a deactivated account, so they are never guessed: the migration prints
their ids (ids only, no addresses) for staff to review, and leaves them pending.

Downgrade is a no-op: the correction is not reversible, because nothing records which accounts it
changed, and undoing it would bring the mistake back.

Revision ID: 017_pending_backfill_correction
Revises: 016_case_insensitive_email
Create Date: 2026-11-03
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "017_pending_backfill_correction"
down_revision: str | None = "016_case_insensitive_email"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

PENDING = "is_active = false AND is_deleted = false AND activated_at IS NULL"

RECLASSIFY = f"""
UPDATE users SET activated_at = created_at
WHERE {PENDING}
  AND (
      EXISTS (SELECT 1 FROM refresh_tokens r WHERE r.user_id = users.id)
      OR EXISTS (
          SELECT 1 FROM invitation_tokens t WHERE t.user_id = users.id AND t.purpose = 'reset'
      )
  )
"""

UNDECIDABLE = f"""
SELECT users.id
FROM users
JOIN LATERAL (
    SELECT t.expires_at FROM invitation_tokens t
    WHERE t.user_id = users.id AND t.purpose = 'invite'
    ORDER BY t.created_at DESC, t.expires_at DESC
    LIMIT 1
) latest ON true
WHERE {PENDING} AND latest.expires_at <= now()
ORDER BY users.id
"""


def reclassify_pending(connection: sa.Connection) -> int:
    """Mark accounts with evidence of past activity as once active.

    Args:
        connection: The connection the migration is running on.

    Returns:
        The number of accounts corrected.
    """
    return connection.execute(sa.text(RECLASSIFY)).rowcount


def undecidable_pending(connection: sa.Connection) -> list[str]:
    """List the pending accounts whose latest invitation link has expired.

    Args:
        connection: The connection the migration is running on.

    Returns:
        Their ids. They have no evidence either way, so they stay pending.
    """
    return [str(user_id) for user_id in connection.execute(sa.text(UNDECIDABLE)).scalars()]


def upgrade() -> None:
    """Correct the accounts and report the undecidable ones by id."""
    connection = op.get_bind()
    corrected = reclassify_pending(connection)
    undecidable = undecidable_pending(connection)
    print(f"017: {corrected} account(s) corrected from pending to deactivated.")
    print(f"017: {len(undecidable)} pending account(s) could not be decided and were left as is.")
    for user_id in undecidable:
        print(f"017: review pending account {user_id}")


def downgrade() -> None:
    """Change nothing: the correction is not reversible (see the module docstring)."""
