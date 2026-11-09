"""037 organisation spelling.

What changes:

- ``organization_id`` becomes ``organisation_id`` in every table that has it (22), and each one's
  foreign key to ``organisations`` takes the matching name (``<table>_organisation_id_fkey``).
  Indexes and other constraints keep their names, which never held the word; PostgreSQL carries
  the renamed column into them.
- The trigger function ``hds_default_shift_types``, which inserts a new organisation's default
  shift types, names the column in its body, which a rename does not reach: it is replaced with
  the new name.
- Idempotent replays stored before the rename (``idempotency_keys.response_body``) answer with the
  field's new name, as the live endpoint now does.

Why: one spelling for organisation, as the tables, models and documentation already use
(docs/adr/021-organisation-spelling-in-one-step.md).

Existing rows: kept as they are; only names change, in stored replays too.

Rolling update: not compatible with the previous release, whose code names the old column. An
exemption recorded in ADR 021 while nothing is deployed (``infra/tests/compat-exemptions.txt``):
for the step from 036 to this revision, ``infra/tests/compat.sh`` passes with a note and
``infra/tests/rolling_update.sh`` starts from the head's own release.

Downgrade renames everything back and restores the function's old body.

Revision ID: 037_organisation_spelling
Revises: 036_roster_edits
Create Date: 2026-11-09
"""

from collections.abc import Sequence

from alembic import op

revision: str = "037_organisation_spelling"
down_revision: str | None = "036_roster_edits"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

TABLES = (
    "assignments",
    "availability_entries",
    "availability_submissions",
    "clients",
    "groups",
    "project_managers",
    "projects",
    "publish_events",
    "qualifications",
    "roster_edit_events",
    "rule_overrides",
    "saved_views",
    "schedule_job_events",
    "schedule_jobs",
    "shift_types",
    "shifts",
    "sites",
    "special_days",
    "staffing_needs",
    "submission_windows",
    "users",
    "week_templates",
)

OLD = "organization_id"
NEW = "organisation_id"

# The body migration 020 wrote, with the column named as given.
FUNCTION = (
    "CREATE OR REPLACE FUNCTION hds_default_shift_types() RETURNS trigger LANGUAGE plpgsql "
    "SET search_path = pg_catalog, public AS $$ BEGIN "
    "INSERT INTO public.shift_types (id, {column}, name, starts_at_local, ends_at_local, "
    "crosses_midnight) "
    "SELECT gen_random_uuid(), NEW.id, d.name, d.starts, d.ends, d.crosses "
    "FROM (VALUES ('morning', TIME '07:00', TIME '15:00', false), "
    "('afternoon', TIME '15:00', TIME '23:00', false), "
    "('night', TIME '23:00', TIME '07:00', true)) AS d(name, starts, ends, crosses)"
    "; RETURN NEW; END $$"
)


def _rename(old: str, new: str) -> None:
    """Rename the column and its foreign key in every table, the function's body, the replays."""
    for table in TABLES:
        op.alter_column(table, old, new_column_name=new)
        op.execute(
            f'ALTER TABLE "{table}" RENAME CONSTRAINT "{table}_{old}_fkey" TO "{table}_{new}_fkey"'
        )
    op.execute(FUNCTION.format(column=new))
    # A stored replay answers as the endpoint does now: its field takes the new name.
    op.execute(
        "UPDATE idempotency_keys SET response_body = "
        f"replace(response_body::text, '\"{old}\"', '\"{new}\"')::jsonb "
        f"WHERE response_body::text LIKE '%\"{old}\"%'"
    )


def upgrade() -> None:
    """Spell every column, foreign key and the trigger function's body ``organisation``."""
    _rename(OLD, NEW)


def downgrade() -> None:
    """Restore the ``organization`` names and the function's old body."""
    _rename(NEW, OLD)
