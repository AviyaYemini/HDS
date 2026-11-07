"""025 organisation contact and session kinds.

What changes: ``organisations`` gains ``contact_phone`` (``VARCHAR(32)``), ``contact_email``
(``VARCHAR(254)``) and ``opening_hours`` (``JSONB``), all nullable. ``refresh_tokens`` gains
``client_kind`` (``VARCHAR(8)``, not null, default ``'web'``) with the check
``ck_refresh_tokens_client_kind`` (``web`` or ``native``).

Why: the contact card a client sees and an Admin edits (specs/007-ux-redesign FR-012, C6), and
phone sessions that last 30 days while the web keeps 7 (FR-010, research R10, ADR 016). The kind is
stored on the token so a rotation keeps its lifetime class.

Existing rows: organisations have no contact details until an Admin adds them; every existing
refresh token becomes ``web``, the lifetime it was issued with.

Rolling update: an expand step. The previous release neither reads nor writes the new columns, and
its inserts into ``refresh_tokens`` get the ``'web'`` default, so the two run side by side while the
update rolls out (006 FR-016).

Downgrade drops the four columns and the check; contact details and session kinds are lost, never a
session or an organisation.

Revision ID: 025_org_contact_sessions
Revises: 024_schedule_job_trace_context
Create Date: 2026-11-07
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "025_org_contact_sessions"
down_revision: str | None = "024_schedule_job_trace_context"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add the contact columns and the session kind with its check."""
    op.add_column("organisations", sa.Column("contact_phone", sa.String(32), nullable=True))
    op.add_column("organisations", sa.Column("contact_email", sa.String(254), nullable=True))
    op.add_column("organisations", sa.Column("opening_hours", postgresql.JSONB(), nullable=True))
    op.add_column(
        "refresh_tokens",
        sa.Column("client_kind", sa.String(8), nullable=False, server_default="web"),
    )
    op.create_check_constraint(
        "ck_refresh_tokens_client_kind", "refresh_tokens", "client_kind IN ('web', 'native')"
    )


def downgrade() -> None:
    """Drop the check and the four columns."""
    op.drop_constraint("ck_refresh_tokens_client_kind", "refresh_tokens", type_="check")
    op.drop_column("refresh_tokens", "client_kind")
    op.drop_column("organisations", "opening_hours")
    op.drop_column("organisations", "contact_email")
    op.drop_column("organisations", "contact_phone")
