"""035 proposal downgrade states.

What changes: nothing on upgrade. On downgrade, before 034's narrower checks come back, a proposal
``partly_accepted`` becomes ``accepted`` (part of it reached the roster) and a ``cancelled`` run
becomes ``failed``, so the jobs and the drafts they wrote are kept and 034's downgrade can run.

Why: specs/008-auto-scheduler-review review-ledger R18 (CodeRabbit on PR 21): 034's downgrade
failed on any job in a state only 008 writes.

Rolling update: no schema change.

Revision ID: 035_proposal_downgrade_states
Revises: 034_proposal_review
Create Date: 2026-11-09
"""

from collections.abc import Sequence

from alembic import op

revision: str = "035_proposal_downgrade_states"
down_revision: str | None = "034_proposal_review"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Nothing: this revision only prepares 034's downgrade."""


def downgrade() -> None:
    """Map the states only 008 writes to ones the previous release knows."""
    op.execute(
        "UPDATE schedule_jobs SET proposal_state = 'accepted' "
        "WHERE proposal_state = 'partly_accepted'"
    )
    op.execute("UPDATE schedule_jobs SET status = 'failed' WHERE status = 'cancelled'")
