"""023 freeze deleted projects.

What changes: no schema. Every project that was deleted without being ended gets the organisation
defaults it follows copied into its own columns, and the organisation's special days it does not
override copied into its own list, as the service now does when it deletes a project (005 FR-007,
as amended after the review of PR 7).

Why: before this, deleting a project did not fix its pay as ending does. Its shifts still count in
pay to date and the reports, so a later change to the defaults or to the organisation's special
days rewrote the pay of work already done, and an Admin could no longer end the project to stop it.

Existing rows: only deleted projects whose status is not ``ended`` change; an ended one was frozen
when it ended (or by 022). A setting a project already has its own value for is kept.

Downgrade does nothing: the copied values are the ones those projects were paid with, and there is
no way to tell them from values an Admin set.

Revision ID: 023_freeze_deleted_projects
Revises: 022_pay_reports
Create Date: 2026-11-05
"""

from collections.abc import Sequence

from alembic import op

revision: str = "023_freeze_deleted_projects"
down_revision: str | None = "022_pay_reports"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SETTINGS = (
    "pay_day_normal_minutes",
    "pay_night_normal_minutes",
    "pay_tier1_minutes",
    "pay_overtime1_pct",
    "pay_overtime2_pct",
    "pay_rest_start_weekday",
    "pay_rest_start_time",
    "pay_rest_end_weekday",
    "pay_rest_end_time",
    "pay_rest_pct",
    "pay_increase1_points",
    "pay_increase2_points",
)
DELETED = "p.is_deleted AND p.status <> 'ended'"


def upgrade() -> None:
    """Fix the pay of projects deleted without being ended."""
    # The four rest-window columns are all set or all NULL, so copying each one that is NULL keeps
    # the window whole.
    copies = ", ".join(f"{c} = COALESCE(p.{c}, o.{c})" for c in SETTINGS)
    op.execute(
        f"UPDATE projects AS p SET {copies} FROM organisations AS o "
        f"WHERE o.id = p.organization_id AND {DELETED}"
    )
    op.execute(
        "INSERT INTO special_days (id, organization_id, project_id, day, name, rate_pct) "
        "SELECT gen_random_uuid(), d.organization_id, p.id, d.day, d.name, d.rate_pct "
        "FROM special_days AS d JOIN projects AS p ON p.organization_id = d.organization_id "
        f"WHERE d.project_id IS NULL AND {DELETED} "
        "AND NOT EXISTS (SELECT 1 FROM special_days AS own "
        "WHERE own.project_id = p.id AND own.day = d.day)"
    )


def downgrade() -> None:
    """Nothing to undo: see the module docstring."""
