"""033 saved views.

What changes: a new table ``saved_views`` (one user's named filters on one screen's table: the
screen, a name unique per user and screen, and the web address's query string).

Why: admin and report tables keep saved views as tabs (specs/007-ux-redesign FR-043, A2), and the
web address holds filters and views (FR-049, A19); saving them on the server lets a user find them
on any device.

Existing rows: none; the table starts empty.

Rolling update: an expand step. The previous release never reads or writes it (006 FR-016).

Downgrade drops the table; saved views are lost, nothing else.

Revision ID: 033_saved_views
Revises: 032_roster_extras
Create Date: 2026-11-07
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "033_saved_views"
down_revision: str | None = "032_roster_extras"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the saved views table."""
    op.create_table(
        "saved_views",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("organization_id", sa.Uuid(), sa.ForeignKey("organisations.id"), nullable=False),
        sa.Column("user_id", sa.Uuid(), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("screen", sa.String(48), nullable=False),
        sa.Column("name", sa.String(60), nullable=False),
        sa.Column("query", sa.String(2000), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.UniqueConstraint("user_id", "screen", "name", name="uq_saved_views_user_screen_name"),
        sa.CheckConstraint("length(name) BETWEEN 1 AND 60", name="ck_saved_views_name"),
    )
    op.create_index("idx_saved_views_user_screen", "saved_views", ["user_id", "screen"])


def downgrade() -> None:
    """Drop the saved views table."""
    op.drop_index("idx_saved_views_user_screen", table_name="saved_views")
    op.drop_table("saved_views")
