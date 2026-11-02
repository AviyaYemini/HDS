"""013 membership cascade.

Deleting a group or qualification removes its memberships (FR-018, FR-019, FR-020) at the
database level, not only through the ORM.

What changes: the foreign keys from ``user_groups`` and ``user_qualifications`` to their parent
table are recreated with ``ON DELETE CASCADE``. Only the membership rows go; the users stay.
Downgrade restores the keys without the cascade.

Revision ID: 013_membership_cascade
Revises: 012_global_unique_email
Create Date: 2026-11-02
"""

from collections.abc import Sequence

from alembic import op

revision: str = "013_membership_cascade"
down_revision: str | None = "012_global_unique_email"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# (table, column, referenced table, constraint name)
_FKS = [
    ("user_groups", "group_id", "groups", "user_groups_group_id_fkey"),
    (
        "user_qualifications",
        "qualification_id",
        "qualifications",
        "user_qualifications_qualification_id_fkey",
    ),
]


def upgrade() -> None:
    """Recreate both membership foreign keys with ON DELETE CASCADE."""
    for table, column, ref, name in _FKS:
        op.drop_constraint(name, table, type_="foreignkey")
        op.create_foreign_key(name, table, ref, [column], ["id"], ondelete="CASCADE")


def downgrade() -> None:
    """Recreate both membership foreign keys without the cascade."""
    for table, column, ref, name in _FKS:
        op.drop_constraint(name, table, type_="foreignkey")
        op.create_foreign_key(name, table, ref, [column], ["id"])
