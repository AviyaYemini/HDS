"""034 proposal review.

What changes:

- ``schedule_jobs`` gains the proposal's working copy (``current``) and its ``version``; the run's
  ``controls``, ``chips`` and ``based_on_id``; its progress (``needed``, ``best_filled``,
  ``progress_at``); ``stop_requested_at``; and how it ``ended``. ``status`` may be ``cancelled`` and
  ``proposal_state`` may be ``partly_accepted``.
- ``assignments`` gains a lock: ``locked_by`` and ``locked_at``, both set or both null.
- A new table ``schedule_job_events``: the run record, one row per step, in the order written
  (``seq``). A trigger refuses UPDATE and DELETE on it, so the record only grows.

Why: specs/008-auto-scheduler-review: proposals are edited and accepted in parts (FR-003 to
FR-012), assignments are locked for later runs (FR-018), runs are steered (FR-017, FR-020) and
stopped early (FR-027, FR-028), and every run keeps a record no one can change (FR-030; ADR 020).

Existing rows: jobs get empty controls and chips, version 0 and no working copy (the service reads
the result when ``current`` is null); assignments are unlocked; the record starts empty.

Rolling update: an expand step. Every new column is nullable or has a server default, and the two
widened checks accept every value the previous release writes (006 FR-016).

Downgrade drops the record, the lock and the new job columns, and restores the narrower checks;
jobs cancelled or partly accepted must be gone first (the checks would fail otherwise).

Revision ID: 034_proposal_review
Revises: 033_saved_views
Create Date: 2026-11-08
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "034_proposal_review"
down_revision: str | None = "033_saved_views"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_JSON = sa.JSON().with_variant(postgresql.JSONB(), "postgresql")

_APPEND_ONLY = """
CREATE FUNCTION schedule_job_events_append_only() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'schedule_job_events is append-only (ADR 020)';
END;
$$ LANGUAGE plpgsql;
"""


def upgrade() -> None:
    """Add the working copy, steering, progress and lock columns, and the run record."""
    with op.batch_alter_table("schedule_jobs") as batch:
        batch.add_column(sa.Column("controls", _JSON, nullable=False, server_default="{}"))
        batch.add_column(sa.Column("chips", _JSON, nullable=False, server_default="[]"))
        batch.add_column(
            sa.Column("based_on_id", sa.Uuid(), sa.ForeignKey("schedule_jobs.id"), nullable=True)
        )
        batch.add_column(sa.Column("current", _JSON, nullable=True))
        batch.add_column(sa.Column("version", sa.Integer(), nullable=False, server_default="0"))
        batch.add_column(sa.Column("needed", sa.Integer(), nullable=True))
        batch.add_column(sa.Column("best_filled", sa.Integer(), nullable=True))
        batch.add_column(sa.Column("progress_at", sa.DateTime(timezone=True), nullable=True))
        batch.add_column(sa.Column("stop_requested_at", sa.DateTime(timezone=True), nullable=True))
        batch.add_column(sa.Column("ended", sa.String(16), nullable=True))
        batch.drop_constraint("ck_schedule_jobs_status", type_="check")
        batch.create_check_constraint(
            "ck_schedule_jobs_status",
            "status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')",
        )
        batch.drop_constraint("ck_schedule_jobs_proposal_state", type_="check")
        batch.create_check_constraint(
            "ck_schedule_jobs_proposal_state",
            "proposal_state IS NULL OR proposal_state IN "
            "('open', 'accepted', 'discarded', 'partly_accepted')",
        )
        batch.create_check_constraint(
            "ck_schedule_jobs_ended",
            "ended IS NULL OR ended IN ('finished', 'stopped', 'timed_out', 'cancelled', 'failed')",
        )

    with op.batch_alter_table("assignments") as batch:
        batch.add_column(
            sa.Column("locked_by", sa.Uuid(), sa.ForeignKey("users.id"), nullable=True)
        )
        batch.add_column(sa.Column("locked_at", sa.DateTime(timezone=True), nullable=True))
        batch.create_check_constraint(
            "ck_assignments_lock_paired", "(locked_by IS NULL) = (locked_at IS NULL)"
        )

    op.create_table(
        "schedule_job_events",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("seq", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("organization_id", sa.Uuid(), sa.ForeignKey("organisations.id"), nullable=False),
        sa.Column("job_id", sa.Uuid(), sa.ForeignKey("schedule_jobs.id"), nullable=False),
        sa.Column("kind", sa.String(24), nullable=False),
        sa.Column("actor_id", sa.Uuid(), sa.ForeignKey("users.id"), nullable=True),
        sa.Column("at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("body", _JSON, nullable=False),
        sa.Column("version", sa.Integer(), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.CheckConstraint(
            "kind IN ('created', 'finished', 'stopped', 'cancelled', 'failed', 'edited', 'undone', "
            "'locked', 'unlocked', 'overridden', 'accepted', 'discarded', 'expired')",
            name="ck_schedule_job_events_kind",
        ),
    )
    op.create_index("idx_schedule_job_events_job_at", "schedule_job_events", ["job_id", "at"])
    if op.get_bind().dialect.name == "postgresql":
        op.execute(_APPEND_ONLY)
        op.execute(
            "CREATE TRIGGER schedule_job_events_append_only BEFORE UPDATE OR DELETE "
            "ON schedule_job_events FOR EACH ROW EXECUTE FUNCTION "
            "schedule_job_events_append_only()"
        )


def downgrade() -> None:
    """Drop the run record, the lock and the new job columns; restore the narrower checks."""
    if op.get_bind().dialect.name == "postgresql":
        op.execute("DROP TRIGGER schedule_job_events_append_only ON schedule_job_events")
        op.execute("DROP FUNCTION schedule_job_events_append_only()")
    op.drop_index("idx_schedule_job_events_job_at", table_name="schedule_job_events")
    op.drop_table("schedule_job_events")

    with op.batch_alter_table("assignments") as batch:
        batch.drop_constraint("ck_assignments_lock_paired", type_="check")
        batch.drop_column("locked_at")
        batch.drop_column("locked_by")

    with op.batch_alter_table("schedule_jobs") as batch:
        batch.drop_constraint("ck_schedule_jobs_ended", type_="check")
        batch.drop_constraint("ck_schedule_jobs_proposal_state", type_="check")
        batch.create_check_constraint(
            "ck_schedule_jobs_proposal_state",
            "proposal_state IS NULL OR proposal_state IN ('open', 'accepted', 'discarded')",
        )
        batch.drop_constraint("ck_schedule_jobs_status", type_="check")
        batch.create_check_constraint(
            "ck_schedule_jobs_status", "status IN ('queued', 'running', 'succeeded', 'failed')"
        )
        for column in (
            "ended",
            "stop_requested_at",
            "progress_at",
            "best_filled",
            "needed",
            "version",
            "current",
            "based_on_id",
            "chips",
            "controls",
        ):
            batch.drop_column(column)
