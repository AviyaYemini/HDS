"""024 schedule job trace context.

What changes: ``schedule_jobs`` gains ``trace_context``, a nullable ``VARCHAR(55)``: the W3C
``traceparent`` of the request that queued the job (specs/006 data-model.md, research R8).

Why: one trace across the API and the worker for every schedule job (original §9 Observability,
006 FR-008a). There is no broker message to carry a trace header, so the job row carries it, and
the worker continues that trace when it runs the job.

Existing rows: null, as are jobs queued by a request that carries no trace; the worker starts a new
trace for them. The column is written once when the job is queued and never changed.

Rolling update: an expand step. The previous release neither reads nor writes the column, so the two
run side by side while the update rolls out (006 FR-016).

Downgrade drops the column; only trace links are lost, never a job.

Revision ID: 024_schedule_job_trace_context
Revises: 023_freeze_deleted_projects
Create Date: 2026-11-06
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "024_schedule_job_trace_context"
down_revision: str | None = "023_freeze_deleted_projects"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add the nullable trace column; existing jobs keep null."""
    op.add_column("schedule_jobs", sa.Column("trace_context", sa.String(55), nullable=True))


def downgrade() -> None:
    """Drop the trace column."""
    op.drop_column("schedule_jobs", "trace_context")
