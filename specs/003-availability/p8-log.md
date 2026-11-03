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

## Group G: availability entries and submission windows (T041 to T048)

- Failing first: the new integration and concurrency tests (45) failed, because the routes did not
  exist (11 failures, 33 setup errors). One test passed before, as it should: an unknown window id
  answers 404 either way.
- Decisions:
  - **Role gate as a dependency.** `require_staff` (new) and `require_manager` run before the body is
    validated, so a forbidden role gets 403 whatever it sends, as the role matrix requires. Whose
    records the caller may read or change is decided in the service.
  - **Window rule.** It applies to blocked, open and close entries of every non-Admin caller,
    Managers editing their own included. Every date must lie in a window (else
    `availability.no_window`), and every such window must be open (else `availability.window_closed`).
    A change is judged on the dates the entry has and the dates it gets.
  - **Who sees what.**
    - An Employee addressing another employee's entries gets 404, so existence does not leak.
    - A Manager changing another employee's gets 403 `availability.admin_required`.
    - A Client account gets 403 from the gate, and no entries can be kept for one (404).
  - **Clashes (FR-104).** They are judged day by day over the shared dates: an entry without a shift
    type or weekday covers every one. The employee's row is locked first (R6).
  - **Strict date and time inputs.** `IsoDate` (query parameters) and `IsoDateTime` (the deadline)
    are new types in `core/types.py` that accept only ISO text. Schemathesis showed lax parsing
    taking `to=0` and `deadline: 0` as 1970. Existing fields keep `StrictDate`, so nothing outside
    this feature changes.
- **Deadlock found by the race test.** Two overlapping windows created at once deadlocked inside the
  exclusion constraint (round 0). Window creation now locks the organisation's row first, so the
  second waits, meets the constraint and answers 409 `windows.overlap`.
- Coverage tests extended for the new routes:
  - The role matrix: 8 operations; `STAFF` is a new allowed set.
  - The cross-organisation sweep: 7 cases. The second organisation's fixture gained a window, an
    entry and a shift type.
  - `unprovable_operations.json`: three operations, for their cross-field rules and the window
    rule.
  - The contract: 8 operations and 8 schemas, in the 001 file.
  - The locale texts: 9 codes in `en` and `he`.
  - `docs/hds-api.md`: the Availability row and the codes.
- Mutations, all caught by a named test (12): deadline counted as open; absences bound to the window;
  Manager editing anyone's; clash ignoring weekdays; window rule on the first date only; Employee
  seeing others' entries; old dates of a change not checked; no row lock; window creation not
  serialised; Client account accepted as a target; shift types not scoped; clash ignoring shift
  types.
  - G7 (old dates not checked) was not caught at first: no test moved an entry from a closed window
    into an open one. `test_an_entry_in_a_closed_window_cannot_be_moved_into_an_open_one` was added,
    and G7 is now caught.
- /hds-review: no violations. Each new operation has allowed and forbidden tests; codes are in `en` and
  `he`; the contract and `docs/hds-api.md` are updated.
- /code-review high, ten findings:
  1. **A change judged a stale copy of the entry (fixed).** It read the entry before taking the
     employee's lock. It now locks, then reads again with `populate_existing`; remove does the same.
     Test first: a unit test fails unless the fresh read follows the lock and the clash check uses
     the fresh dates.
  2. **An absence could not be reported over an entry in a closed window (owner decision, fixed).**
     The employee could not remove the old entry, so they needed an Admin. The owner chose: an
     absence may overlap blocked, open and close entries, and only two absences may not overlap.
     - FR-104, the Clarifications and research R6 say so.
     - Changed test, with the reason: the conflict case "absence then open night" became "absence
       then absence", because the owner's decision makes the first allowed. It moved to the
       no-conflict cases, with "close over an absence" added beside it.
     - New test: an absence reported over an open entry after the deadline.
  3. **Window creation took a plain `FOR UPDATE` on the organisation (fixed):** it now takes
     `FOR NO KEY UPDATE`. The plain lock conflicts with the `FOR KEY SHARE` that every foreign key to
     the organisation takes, so a window and an entry made at once deadlocked. The new race test
     reproduced it in round 0 before the fix.
  4. **The clash and window checks walked every day of a range (fixed).**
     - A clash returns at once when the weekdays differ, and otherwise looks at most a week ahead.
     - The window rule walks the windows, not the days.
     - Test: a clash check over centuries takes under 50 ms.
  5. **Body dates accepted numeric strings (fixed):** `StrictDate` now accepts only `YYYY-MM-DD` with
     ASCII digits, and `IsoDate` is the same type. This tightens the existing date fields too: a
     numeric string or a date without leading zeros is now 422. That is a deliberate change for any
     client sending them, and the existing suite still passes.
  6. **A retired shift type was accepted (fixed):** 422 `availability.shift_type_retired`.
  7. **The window rule judged days a weekday entry does not cover, and an entry covering no day was
     stored (fixed).** Only the matching days need a window, and only the windows holding one must be
     open. An entry whose weekday never occurs in its range is refused with 422
     `availability.no_matching_day`.
  8. **Research R5 said "one window" while adjacent windows together may cover a range (fixed in
     research):** the behaviour is kept and R5 now says so.
  9. **`unprovable_operations.json` was reformatted (fixed):** the file is restored with only the new
     entries added.
  10. **Sentinel dates for an open-ended window range (fixed):** `covering` takes optional bounds.
- Mutations on the fixes, all caught except one equivalent: G13 an absence clashing with other kinds;
  G14 a change judged on the stale read; G15 a retired shift type accepted; G16 a weekday entry
  needing a window on every date; G17 an entry covering no date; G19 a plain `FOR UPDATE`; G20 a
  numeric date string.
  - G16 was not caught at first. `test_a_closed_window_on_days_the_entry_does_not_cover_does_not_block_it`
    was added, and G16 is now caught.
  - G18 (the week cap removed) cannot be caught. The early return for differing weekdays, and `any`
    stopping at the first match within a week, give the same result without it. It is an
    equivalent mutant, and the cap stays as a bound.
- Gate findings, nine failures, all fixed:
  - **Two tests and one sweep case failed on `MissingGreenlet`.** The sweep fixture read the default
    shift type after its commit, which opened a transaction. The tests' digest then rolled it back,
    which expired the fixture's objects. The read now comes before the commit.
  - **The sweep expected Employees to be refused by role on every case.** Availability is the
    employee's own, so an Employee passes the gate, and another organisation's id gets 404 like a
    missing one.
    - `EMPLOYEE_OPEN` lists the four operations, with that expectation.
    - A case can now state what a Manager gets: on "own user id, foreign shift type" a Manager gets
      403 `availability.admin_required`, because the entry is another employee's.
    - No existing expectation was weakened; the new cases are stated, not exempted.
- Final gates: all green; full suite 1707 passed; concurrency 39 passed three times; drift check agrees.
