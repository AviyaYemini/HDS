"""022 pay reports.

What changes:

- ``organisations``: the premium defaults (005 FR-006). Normal day 480 minutes, normal night 420,
  first overtime tier 120; overtime at 125.00 and 150.00 percent; the weekly rest from weekday 5
  (Friday) 18:00 to weekday 6 (Saturday) 22:00 at 150.00 percent; overtime increases of 25.00 and
  50.00 points on a rest or special day. Bounds (``ck_organisations_pay_*``): lengths 1 to 1440,
  rates 100 to 300, increases above 0 and at most 200, weekdays 0 to 6, and a rest window whose
  ends differ.
- ``projects``: ``pay_enabled`` (default true) and ``managers_see_pay`` (default false), and the
  same twelve settings as nullable overrides, ``NULL`` following the organisation (FR-005 to
  FR-007), with the same bounds (``ck_projects_pay_*``); the four rest-window columns are all set or
  all ``NULL``.
- ``special_days``: dates paid at their own rate (100 to 300, default 150.00), for the organisation
  (``project_id`` NULL) or one project (FR-008, FR-008a); one organisation day per date and one day
  per project and date (partial unique indexes); removed with their project.
- ``project_managers``: Managers and Admins assigned to a project (FR-021), once each; removed with
  their project.
- ``idx_shifts_org_published_start``: published shifts by organisation and start, for the reports
  and pay to date (research R9).

Why: specs/005-pay-reports (FR-003, FR-005 to FR-008a, FR-021, FR-022; data-model.md).

Existing rows: every organisation gets the defaults and every active project follows them, with
pay on and closed to Managers. A project that has already ended gets the defaults copied into its
own columns, as the service does when a project ends (FR-007), so later changes to the defaults do
not reach it; there are no special days yet to copy. No money amount is stored: pay is computed
when read (research R1).

Downgrade drops the two tables, the index and the columns.

Revision ID: 022_pay_reports
Revises: 021_schedule_generation
Create Date: 2026-11-05
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "022_pay_reports"
down_revision: str | None = "021_schedule_generation"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

MINUTES = ("pay_day_normal_minutes", "pay_night_normal_minutes", "pay_tier1_minutes")
PERCENTS = ("pay_overtime1_pct", "pay_overtime2_pct", "pay_rest_pct")
POINTS = ("pay_increase1_points", "pay_increase2_points")
WEEKDAYS = ("pay_rest_start_weekday", "pay_rest_end_weekday")
REST = (
    "pay_rest_start_weekday",
    "pay_rest_start_time",
    "pay_rest_end_weekday",
    "pay_rest_end_time",
)
DEFAULTS: dict[str, tuple[sa.types.TypeEngine[object], str]] = {
    "pay_day_normal_minutes": (sa.SmallInteger(), "480"),
    "pay_night_normal_minutes": (sa.SmallInteger(), "420"),
    "pay_tier1_minutes": (sa.SmallInteger(), "120"),
    "pay_overtime1_pct": (sa.Numeric(5, 2), "125.00"),
    "pay_overtime2_pct": (sa.Numeric(5, 2), "150.00"),
    "pay_rest_start_weekday": (sa.SmallInteger(), "5"),
    "pay_rest_start_time": (sa.Time(), "'18:00'"),
    "pay_rest_end_weekday": (sa.SmallInteger(), "6"),
    "pay_rest_end_time": (sa.Time(), "'22:00'"),
    "pay_rest_pct": (sa.Numeric(5, 2), "150.00"),
    "pay_increase1_points": (sa.Numeric(5, 2), "25.00"),
    "pay_increase2_points": (sa.Numeric(5, 2), "50.00"),
}
DIFFER = (
    "NOT (pay_rest_start_weekday = pay_rest_end_weekday "
    "AND pay_rest_start_time = pay_rest_end_time)"
)


def _checks(table: str, optional: bool) -> list[tuple[str, str]]:
    """Return the name and condition of each bound on the settings of ``table``."""

    def rule(column: str, condition: str) -> tuple[str, str]:
        sql = f"{column} IS NULL OR ({condition})" if optional else condition
        return f"ck_{table}_{column}", sql

    checks = [rule(c, f"{c} BETWEEN 1 AND 1440") for c in MINUTES]
    checks += [rule(c, f"{c} BETWEEN 100 AND 300") for c in PERCENTS]
    checks += [rule(c, f"{c} > 0 AND {c} <= 200") for c in POINTS]
    checks += [rule(c, f"{c} BETWEEN 0 AND 6") for c in WEEKDAYS]
    if optional:
        none = " AND ".join(f"{c} IS NULL" for c in REST)
        every = " AND ".join(f"{c} IS NOT NULL" for c in REST)
        checks.append((f"ck_{table}_pay_rest", f"({none}) OR ({every} AND {DIFFER})"))
    else:
        checks.append((f"ck_{table}_pay_rest", DIFFER))
    return checks


def upgrade() -> None:
    """Add the premium settings, the project switches, special days and managers on projects."""
    for column, (type_, default) in DEFAULTS.items():
        op.add_column(
            "organisations",
            sa.Column(column, type_, nullable=False, server_default=sa.text(default)),
        )
        op.add_column("projects", sa.Column(column, type_, nullable=True))
    for table, optional in (("organisations", False), ("projects", True)):
        for name, condition in _checks(table, optional):
            op.create_check_constraint(name, table, condition)
    op.add_column(
        "projects",
        sa.Column("pay_enabled", sa.Boolean(), nullable=False, server_default=sa.text("true")),
    )
    op.add_column(
        "projects",
        sa.Column(
            "managers_see_pay", sa.Boolean(), nullable=False, server_default=sa.text("false")
        ),
    )

    op.create_table(
        "special_days",
        sa.Column(
            "id",
            sa.UUID(),
            nullable=False,
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("organization_id", sa.UUID(), sa.ForeignKey("organisations.id"), nullable=False),
        sa.Column(
            "project_id",
            sa.UUID(),
            sa.ForeignKey("projects.id", ondelete="CASCADE"),
            nullable=True,
        ),
        sa.Column("day", sa.Date(), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("rate_pct", sa.Numeric(5, 2), nullable=False, server_default=sa.text("150.00")),
        sa.Column(
            "created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.CheckConstraint("rate_pct BETWEEN 100 AND 300", name="ck_special_days_rate"),
        sa.CheckConstraint("trim(name) <> ''", name="ck_special_days_name"),
    )
    op.create_index(
        "uq_special_days_org_day",
        "special_days",
        ["organization_id", "day"],
        unique=True,
        postgresql_where=sa.text("project_id IS NULL"),
    )
    op.create_index(
        "uq_special_days_project_day",
        "special_days",
        ["project_id", "day"],
        unique=True,
        postgresql_where=sa.text("project_id IS NOT NULL"),
    )

    op.create_table(
        "project_managers",
        sa.Column("organization_id", sa.UUID(), sa.ForeignKey("organisations.id"), nullable=False),
        sa.Column(
            "project_id",
            sa.UUID(),
            sa.ForeignKey("projects.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("user_id", sa.UUID(), sa.ForeignKey("users.id"), primary_key=True),
        sa.Column(
            "created_at", sa.TIMESTAMP(timezone=True), server_default=sa.func.now(), nullable=False
        ),
    )
    op.create_index("idx_project_managers_user", "project_managers", ["user_id"])

    op.create_index(
        "idx_shifts_org_published_start",
        "shifts",
        ["organization_id", "starts_at"],
        postgresql_where=sa.text("status = 'published'"),
    )

    # Freeze the projects that have already ended (FR-007).
    copies = ", ".join(f"{c} = o.{c}" for c in DEFAULTS)
    op.execute(
        f"UPDATE projects AS p SET {copies} FROM organisations AS o "
        "WHERE o.id = p.organization_id AND p.status = 'ended'"
    )


def downgrade() -> None:
    """Drop everything ``upgrade`` added."""
    op.drop_index("idx_shifts_org_published_start", table_name="shifts")
    op.drop_table("project_managers")
    op.drop_table("special_days")
    op.drop_column("projects", "managers_see_pay")
    op.drop_column("projects", "pay_enabled")
    for table, optional in (("organisations", False), ("projects", True)):
        for name, _ in _checks(table, optional):
            op.drop_constraint(name, table, type_="check")
    for column in DEFAULTS:
        op.drop_column("projects", column)
        op.drop_column("organisations", column)
