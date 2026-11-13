"""038 temporary workers (CEO demo).

What changes:

- ``users`` gains ``employment_type`` (``permanent`` by default, or ``temporary``) and the
  engagement's ``engaged_from`` and ``engaged_until``. A temporary worker has both dates, the last
  no more than three months after the first; a permanent one has neither.
- ``project_members`` is new: the projects a person works on, the start of project teams (O15).
  A temporary worker works only on theirs, and only between their dates.
- ``projects`` gains ``terms_estimated``: the billing and pay terms are estimates, so screens say so.

Why: the CEO wants temporary workers who belong to a project, or a few, for up to three months.

Existing rows: every user is permanent and no one is on a project's team; no rate is estimated.

Rolling update: additive only (new columns with defaults and a new table), so the previous release
runs against it unchanged.

Downgrade drops the table and the columns.

Revision ID: 038_temporary_workers
Revises: 037_organisation_spelling
Create Date: 2026-11-13
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "038_temporary_workers"
down_revision: str | None = "037_organisation_spelling"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add the employment type and engagement, project members and estimated rates."""
    with op.batch_alter_table("users") as batch:
        batch.add_column(
            sa.Column("employment_type", sa.String(16), nullable=False, server_default="permanent")
        )
        batch.add_column(sa.Column("engaged_from", sa.Date(), nullable=True))
        batch.add_column(sa.Column("engaged_until", sa.Date(), nullable=True))
        batch.create_check_constraint(
            "ck_users_employment_type", "employment_type IN ('permanent', 'temporary')"
        )
        batch.create_check_constraint(
            "ck_users_engagement",
            "(employment_type = 'permanent' AND engaged_from IS NULL AND engaged_until IS NULL)"
            " OR (employment_type = 'temporary' AND engaged_from IS NOT NULL"
            " AND engaged_until IS NOT NULL AND engaged_until >= engaged_from"
            " AND engaged_until <= engaged_from + INTERVAL '3 months')",
        )
    op.create_table(
        "project_members",
        sa.Column("organisation_id", sa.Uuid(), sa.ForeignKey("organisations.id"), nullable=False),
        sa.Column(
            "project_id",
            sa.Uuid(),
            sa.ForeignKey("projects.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("user_id", sa.Uuid(), sa.ForeignKey("users.id"), primary_key=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
    )
    op.create_index("idx_project_members_user", "project_members", ["user_id"])
    with op.batch_alter_table("projects") as batch:
        batch.add_column(
            sa.Column("terms_estimated", sa.Boolean(), nullable=False, server_default=sa.false())
        )


def downgrade() -> None:
    """Drop estimated rates, project members and the engagement."""
    with op.batch_alter_table("projects") as batch:
        batch.drop_column("terms_estimated")
    op.drop_index("idx_project_members_user", table_name="project_members")
    op.drop_table("project_members")
    with op.batch_alter_table("users") as batch:
        batch.drop_constraint("ck_users_engagement", type_="check")
        batch.drop_constraint("ck_users_employment_type", type_="check")
        batch.drop_column("engaged_until")
        batch.drop_column("engaged_from")
        batch.drop_column("employment_type")
