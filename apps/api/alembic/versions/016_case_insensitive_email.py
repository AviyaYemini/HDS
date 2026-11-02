"""016 case-insensitive email.

What changes: every user address is lowercased, and the unique index ``uq_users_email`` (on
``email``, non-deleted users) is replaced by ``uq_users_email_ci`` (on ``lower(email)``, same
condition).

Why: ``Bob@corp.com`` and ``bob@corp.com`` are one address (specs/002-security-hardening, FR-215).
Until now they could be two accounts, typing a different case at sign-in failed, and a password
reset for another spelling silently did nothing.

Existing rows, and why this can stop: if two non-deleted accounts already hold addresses that
differ only by case (in the same organisation or in different ones), the new index cannot be
built, and nothing may be merged or deactivated automatically. ``ensure_no_case_clashes`` runs
first and stops the migration with the account ids and organisation ids of every clash (ids only,
no addresses). Staff resolve them (the read-only report ``python -m
hds.scripts.find_email_case_duplicates`` prints the same list), then run the migration again.
Because PostgreSQL DDL is transactional, a stopped migration leaves nothing half-applied. Deleted
accounts are not compared, as the old index did not compare them either.

Downgrade recreates ``uq_users_email`` and drops ``uq_users_email_ci``. The lowercasing of the
stored addresses is NOT reversed: the original spellings are not kept, and the lowercase form
identifies the same accounts.

Revision ID: 016_case_insensitive_email
Revises: 015_refresh_revoked_reason
Create Date: 2026-11-02
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "016_case_insensitive_email"
down_revision: str | None = "015_refresh_revoked_reason"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

OLD, NEW = "uq_users_email", "uq_users_email_ci"
LIVE = sa.text("is_deleted = false")

CLASHES = """
SELECT lower(email) AS address, id, organization_id
FROM users
WHERE is_deleted = false
  AND lower(email) IN (
      SELECT lower(email) FROM users WHERE is_deleted = false
      GROUP BY lower(email) HAVING count(*) > 1
  )
ORDER BY lower(email), created_at, id
"""


def ensure_no_case_clashes(connection: sa.Connection) -> None:
    """Raise, naming the accounts, if two live accounts differ only by address case.

    Args:
        connection: The connection the migration is running on.

    Raises:
        RuntimeError: If any clash exists. The message lists each clash as a group of
            ``account id (organisation id)`` pairs, with no addresses, and says what to do.
    """
    groups: dict[str, list[str]] = {}
    for address, user_id, org_id in connection.execute(sa.text(CLASHES)):
        groups.setdefault(address, []).append(f"{user_id} (organisation {org_id})")
    if not groups:
        return
    lines = "; ".join(f"[{', '.join(members)}]" for members in groups.values())
    raise RuntimeError(
        f"Cannot make email addresses case-insensitive: {len(groups)} address(es) are held by "
        f"more than one account that differs only by letter case: {lines}. Resolve each group "
        "(deactivate, delete or change the address of all but one account), then run the "
        "migration again. Nothing was changed."
    )


def upgrade() -> None:
    """Check for clashes, lowercase every address, then swap the unique index.

    Raises:
        RuntimeError: From `ensure_no_case_clashes` when two live accounts would collide.
    """
    ensure_no_case_clashes(op.get_bind())
    op.execute("UPDATE users SET email = lower(email) WHERE email <> lower(email)")
    op.drop_index(OLD, table_name="users")
    op.create_index(NEW, "users", [sa.text("lower(email)")], unique=True, postgresql_where=LIVE)


def downgrade() -> None:
    """Recreate the case-sensitive index; the lowercased addresses stay lowercase."""
    op.drop_index(NEW, table_name="users")
    op.create_index(OLD, "users", ["email"], unique=True, postgresql_where=LIVE)
