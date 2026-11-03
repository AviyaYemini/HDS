# 003 Part 2: decision log

Decisions, review findings, mutation results and deviations for Part 2 (availability and manual
shifts), phases P8 to P13. Tests run on the private database `hds_p3_test`, never `hds_test`, and never
while another run uses it.

## Setup (T030)

Owner decisions of 2026-11-03, before implementation:

- Managers publish drafts; only Admins create, change or delete shifts and assignments.
- Absences (leave, reserve duty, sick) can be reported at any time; blocked, open and close follow the
  submission window.
- The Expo app lives in `apps/client` (the prompt's "apps/mobile" is read as this app).
- `POST /shifts/{id}/assignments` accepts an `Idempotency-Key` (FR-115a).
- An availability entry without a shift type covers every shift that date (FR-104).

Spec quality checklist: 32 of 32 items pass. `hds_p3_test` had no connections at the start.

## Group E: the pure rules (T031 to T036a)

- Failing first: every new test failed at import, because the package did not exist. The 31 first
  tests then passed on the first implementation.
- Decision: the warning type is `RuleWarning`, after review, not `Warning`. `validate` takes `focus`
  (the shifts being changed) and reports only warnings that involve one of them.
- Mutations on the first version, all caught (13): rest `<=`; touching ends overlap; Monday-start
  weeks; close not reported; night shift dated by its end; weekly limit inclusive; inactive not
  reported; weekday counted from Monday; shift type ignored; focus ignored; an impure import; DST with
  `fold=1`; under-headcount missed.
- ADR 009 written (T036a) and listed in `docs/README.md`.
- /code-review high, ten findings, all fixed, tests first. 7 new tests failed for the right reason:
  1. **A shift id given twice was counted twice (fixed):** for example a changed shift with its stored
     copy. `validate` refuses a duplicate id; the service must pass only the new version.
  2. **`local_span` could end before it starts (fixed):** an end inside the spring gap, or an end
     before the start without crossing midnight. It now raises `ValueError`; the service will turn it
     into a refusal.
  3. **Rest was judged between non-neighbours (fixed):** a shift two places later was reported again.
     Rest is now judged only between a shift and the next one, with no other shift starting in
     between. The property test's reference changed the same way; that is the rule changing, by
     review, not a test weakened.
  4. **A missing employee was treated as active (fixed):** it now raises `KeyError`.
  5. **An unknown entry kind was ignored (fixed):** the kind is a `Literal`, and other values raise
     `ValueError`.
  6. **`Warning` shadowed the built-in (fixed):** renamed `RuleWarning`.
  7. and 8. **Week counting repeated per shift, and the weekday convention duplicated (fixed):** each
     shift's week is computed once, and one `_sunday_index` helper serves both.
  9. **The data model's description of the inputs was stale (fixed).**
  10. **The purity test let `..` imports through (fixed):** only imports from the package itself pass.
- Mutations on the final version, all caught (19). E6 (weekly limit inclusive) was caught only by the
  property test's random examples, so `test_exactly_the_weekly_maximum_is_allowed` was added; E6 is
  now caught by it. New mutations E14 to E19 cover findings 1 to 4 and 10.
- /hds-review: no violations. The package imports only the standard library (checked by two tests and
  the hook). No endpoint or API change yet.
- Final gates: all green; full suite 1564 passed; concurrency 36 passed three times.

## Group F: migration 020 and the models (T037 to T040)

- Failing first: `test_migration_020.py` failed because the revision did not exist. After the first
  draft, 25 tests passed once the test bound dates as `date` values (asyncpg refuses strings).
- Decisions:
  - Kinds and statuses are text with check constraints, as `login_throttle` does, not PostgreSQL
    enums.
  - The three default shift types come from a database trigger on `organisations`, so every path
    that creates an organisation gets them (FR-110). No application code creates organisations
    today; only the seed and the test fixtures do.
  - `btree_gist` is created only if absent and marked with a comment; the downgrade removes it only
    when marked.
- Mutations on the first draft, all caught (12): only identical windows excluded; absence without its
  kind; zero pay rate; crossing rule missing; names unique in exact case only; no trigger for new
  organisations; downgrade leaving a table; downgrade always or never removing the extension;
  assignments kept with their shift; existing organisations not seeded; weekday 7.
- /code-review high, ten findings:
  1. **An organisation created after 020 could not be deleted (fixed).** The trigger's shift types
     blocked it, and `test_hash_pool.py::TestReleaseConnection::test_it_refuses_after_any_write[delete]`
     failed. `shift_types.organization_id` now cascades. Test first:
     `test_an_organisation_with_only_its_default_shift_types_can_be_deleted`.
  2. **`alembic upgrade --sql` crashed (fixed).** The migration queried the database for the
     extension. In offline mode the upgrade uses `CREATE EXTENSION IF NOT EXISTS` without the mark,
     and the downgrade keeps it. Test first: `test_the_migration_can_be_written_out_as_sql`.
  3. **T039's wording asked for application-side defaults (fixed in tasks.md):** they would clash
     with the trigger.
  4. and 5. **The new tables' `id` had no database default, and the seed toggled one (fixed):** `id`
     defaults to `gen_random_uuid()` as in 001 to 011, and the toggle is gone.
  6. **Cross-organisation references are not prevented by the schema (not changed).** Every table
     since 001 leaves organisation scoping to the services and repositories (Constitution VI). The
     Part 2 services check each referenced row's organisation, and Group J's sweep tests it.
  7. **`status` and `published_at` could disagree (fixed):** `ck_shifts_published_at`.
  8. **The trigger depended on the session's `search_path` (fixed):** the function has a fixed
     `search_path` and names `public.shift_types`. Test first: the trigger seeds with
     `search_path` set to `pg_catalog`.
  9. **Foreign keys used in lookups were not indexed (fixed):** `idx_shift_types_org`,
     `idx_project_shift_types_type`, `idx_availability_entries_shift_type`.
  10. **T039's file names (fixed in tasks.md).**
- **Incident: the dev database `hds` was migrated by mistake (owner informed, restored).**
  - Several Alembic commands were written as `E="... DATABASE_URL=...hds_p3_test"; env $E uv run
    alembic ...`. The shell is zsh, which does not split an unquoted `$E`, so `DATABASE_URL` was never
    set and Alembic used `alembic.ini`'s default, the dev database `hds`.
  - Those commands upgraded `hds` from 019 to drafts of 020, ran a downgrade and an upgrade, and ran
    `alembic check` against it. The running API container does not migrate and was unaffected.
    Existing data in `hds` was not changed; only the new tables, the two columns and the default
    shift types were added.
  - The owner chose to restore it. `hds` was downgraded to 019 through Alembic with the URL written
    out: none of the seven tables, no new columns, and the extension removed (020 had created it).
  - It also explained a stale `hds_p3_test`: it still held the first 020 draft, whose downgrade no
    longer matched. It was repaired through Alembic with a temporary `if_exists` on three index
    removals, reverted at once (the file was copied before and restored after).
  - From now on every Alembic command names its database inline. A memory note records the rule.
- Mutations on the fixes, all caught: F13 shift types blocking an organisation's removal; F14 and F15
  offline upgrade and downgrade asking the database; F16 status and time disagreeing; F18 no database
  id default; F19 the organisation index missing. F17, the trigger's fixed `search_path` removed
  alone, was not caught: the fully qualified `public.shift_types` keeps it working. Both defences are
  kept; removing both (F17b) is caught by the `search_path` test.
- Gate finding: the SQLite unit tests (`tests/unit/test_dev_issue_link.py`, 14 errors) build the
  schema with `create_all`, which cannot compile the PostgreSQL exclusion constraint. The constraint
  is now `ddl_if(dialect="postgresql")`, and `idempotency_keys.response_body` is JSON with a JSONB
  variant on PostgreSQL. `alembic check` is still clean, and the unit suite passes (821).
- Final gates: all green; full suite 1612 passed; concurrency 36 passed three times; `alembic check` clean.
- /hds-review: no violations; migration 020 is new and reversible, and no applied migration was edited.
