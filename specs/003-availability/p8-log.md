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

## Group H: shift types per project (T049, T050)

- Failing first: the 12 tests in `test_shift_types.py` failed, because the routes did not exist.
- Decisions:
  - **Crossing midnight is computed**, never sent: true exactly when the end is not after the start.
    So T049's "a non-crossing type ending before it starts is refused" cannot arise; the database
    check `ck_shift_types_crossing` still guards it.
  - **Names are unique only through the database index** (`uq_shift_types_org_name`, active names,
    any letter case), mapped to 409 `shift_types.duplicate_name`. The service's own name check was
    removed: mutations H3 and H8 showed it changed nothing over the index, which also covers two
    requests at once.
  - **Retiring** is `is_active` false. A retired type frees its name and keeps its shifts; it cannot
    be added to a project (422 `shift_types.retired`) or used by an availability entry.
  - **A project's set is replaced whole** (`PUT /projects/{id}/shift-types`).
    `GET /projects/{id}/shift-types` was added to read it back (not in the plan's contract table, but
    needed to show the set).
  - **Local times are text only.** `IsoTime` (requests) and `LocalTimeOut` (responses) are published
    as a string pattern with valid hours and minutes, not `format: time`: that format requires a UTC
    offset, and these times are local to a site. Schemathesis showed both problems.
- Coverage tests extended for the new routes: the role matrix (5), the sweep (4 cases), `_CONFLICTS`
  (2), the locale texts (2 codes), the contract (5 operations, 6 schemas), and `docs/hds-api.md`.
- Mutations, all caught (H1, H2, H4 to H7, H9, H10): crossing inverted; crossing not recomputed on a
  change; a retired or a foreign type allowed in a project; retired names still taken; a project's
  set appended rather than replaced; the unique-name violation not mapped to 409, on add or on
  rename.
- /hds-review: no violations. Every new operation has allowed and forbidden tests; codes are in `en`
  and `he`; the contract and `docs/hds-api.md` are updated.
- /code-review high, nine findings:
  1. **A retired type stayed in a project's set, so sending the set back failed (fixed).** A
     project's set now holds active types only. Test first.
  2. **Crossing midnight was computed from a row read without a lock (fixed).** The type is read
     `FOR UPDATE` with `populate_existing`. The new race test, two changes each moving one time,
     failed in round 1 before the fix.
  3. **Replacing a project's set had no lock (fixed).** The project row is locked (`FOR NO KEY
     UPDATE`) and the listed types `FOR SHARE`, so a concurrent retirement waits. The race test,
     two identical replacements, failed in round 1 before the fix.
  4. **Equal start and end times were accepted as a 24-hour shift (fixed):** 422
     `shift_types.times_equal`.
  5. **`project_uses` ignored retirement (fixed):** it requires an active type. It is for Group I,
     whose tests cover it.
  6. **One query per id when replacing a set (fixed):** one locking query for all of them.
  7. **The savepoints around the unique-name writes (not changed):** the user repository uses the
     same pattern for the same reason.
  8. **`STAFF_ROLES` imported from another service (fixed):** it moved to `core/authz.py`.
  9. **The id-list request model duplicates `IDListRequest` (not changed, cosmetic).**
- Mutations on the fixes, all caught: H11 retired types left in a set; H12 equal times accepted;
  H13 the type read without a lock; H14 the project not locked for a set.
- Gate: 1743 passed, one contract failure: equal times are a cross-field rule, so `POST` and `PATCH /shift-types` joined `unprovable_operations.json`; the contract tests then pass. Concurrency 41 passed three times.

## Group I: shifts and assignments with warnings (T051 to T057)

- Failing first: the 29 integration and concurrency tests failed, and 6 errored, because the routes
  did not exist.
  - One test was itself wrong: the weekly-hours test seeded six 8-hour shifts, which is exactly the
    48-hour limit and so allowed. It now seeds seven (Sunday to Saturday, 56 hours). The rule was
    right; the test's arithmetic was not.
- Decisions:
  - **Times.** A shift's times are its type's local times on the date at the project's site
    (`local_span`), unless `starts_at` and `ends_at` are both given. Times that do not end after
    they start, or only one of the two given, are 422 `shifts.times_invalid`.
  - **Admin only.** Every write is gated by `require_admin`; Managers read the roster (FR-120).
  - **Warnings** come from `hds.scheduling.validate`. The service loads the employees' shifts in
    every project, drafts included, from the start of the earliest week touched (or the longest
    shift plus the minimum rest before) to the end of the latest (R3), with their availability
    entries and whether each is still active. A warning never refuses (FR-116); removing an
    assignment answers the warnings the shift has left.
  - **Locks.** Shift writes and assignments lock the shift (`FOR NO KEY UPDATE`, read fresh).
  - **Duplicates.** A second assignment of one employee is caught by `uq_assignments_shift_user`,
    including at the same moment, and answered 409 `assignments.duplicate`.
  - **Idempotency-Key (FR-115a).**
    - The key is claimed with an insert on the (user, key) primary key, with `ON CONFLICT DO
      NOTHING`. A second request with the key waits for the first, then reads its stored answer.
    - The answer is stored in the same transaction as the assignment, so a refused request leaves
      no key behind.
    - The same key with another request is 422 `idempotency.key_reused`.
    - A key older than 24 hours counts as new, and the purge script removes old ones (a fourth
      line, `idempotency_keys N`).
  - **Paging.** `GET /shifts` takes `from` and `to` as dates in the organisation's zone and pages
    by an opaque cursor of (start, id). Another organisation's `project_id` is 404, as for the
    projects filter.
- Changed test, with the reason:
  `test_purge_expired_records.py::test_it_works_through_more_rows_than_one_batch_and_prints_only_counts`
  asserts the script's exact output. The new `idempotency_keys 0` line was added to it.
- Coverage tests extended for the new routes:
  - The role matrix: 6 operations.
  - The sweep: 8 cases and 5 Admin-only entries. Both organisations' fixtures gained a shift, and
    the other one an assignment.
  - `_CONFLICTS`: 1. `unprovable_operations.json`: 3, for the cursor, the times and the project's
    types.
  - The locale texts: 15 codes, including the nine warning texts used for each warning's `detail`.
  - The contract: 6 operations and 11 schemas. `docs/hds-api.md`.
- Mutations, all caught by a named test except one equivalent:
  - I2 a warning turned into a refusal; I3 an inactive employee assigned; I4 a type the project does
    not use accepted.
  - I5 the stored answer not returned on a repeat; I6 the body hash ignored.
  - I9 the site zone ignored; I10 availability not weighed; I11 the duplicate not mapped to 409;
    I12 a headcount change ignored.
  - I1b warnings from the same project only; I8b expired keys still counting; I13b a foreign project
    filter answered empty.
  - Three first versions did not count: I1 removed every nearby shift; I8 failed on a `NameError`;
    I13 did not apply, because the formatter had changed the text. Each was redone as the "b"
    version above.
  - I7 (the stored key looked up by key alone) cannot be caught. The claim is an insert on the
    (user, key) primary key, so another user's key never conflicts and the lookup is never reached.
    Equivalent mutant.
- /hds-review: no violations once finding 7 below was fixed. Admin-only writes have allowed and
  forbidden tests; codes are in `en` and `he`; the contract and `docs/hds-api.md` are updated.
- /code-review high, ten findings:
  1. **A deleted employee left on a shift made the rules raise `KeyError`, a 500 (fixed).** Deleting
     an employee keeps their assignments. The judged set now includes deleted users, as no longer
     active, so the shift answers `warnings.employee_inactive`. Test first.
  2. **A zero or negative pay rate reached the database constraint (fixed):** 422
     `shifts.pay_rate_invalid`. Test first: it answered 422 `validation.constraint_violation`
     before.
  3. and 4. **Assigning did not lock the employee (fixed).** The employee's row is now locked before
     `is_active` is read.
     - A deactivation in progress makes the assignment wait, then refuses it.
     - Two assignments of one employee run one after the other, so the second's warnings see the
       first.
     - Tests first: two clashing assignments at once, where neither answer saw the clash (round 0);
       and a deterministic test where a second connection holds an uncommitted deactivation and the
       assignment must wait, then get 409.
     - Changed test, with the reason: my first version of the deactivation race asserted that an
       inactive employee is never left assigned. That is not the rule: deactivating someone already
       assigned is allowed, and they stay with a warning. It was replaced by the deterministic test,
       which pins the real rule (no assignment on a stale "active" read).
  5. **Two removals of one assignment both answered 200 (fixed).** The assignment is read again
     under the shift lock, so the second gets 404. The race test failed in round 0 before the fix.
  6. **The key's hash used the raw URL path (fixed):** it uses the parsed shift id. Test first: a
     repeat with the id in capitals got 422.
  7. **The idempotency logic was in the router (fixed, Constitution II).** It is now
     `IdempotencyService.run_once` in `services/idempotency.py`; the router only passes the request
     and the write.
  8. **Availability was loaded one employee at a time (fixed):** one `for_users` query.
     - Not changed: views are read again after `_judge`. That is one extra read per write, and
       cosmetic.
  9. **The service copied the week arithmetic (fixed):** `hds.scheduling.week_start` is public and
     used by both.
  10. **The key lifetime was written twice (fixed):** the purge uses `KEY_LIFETIME` from the service.
- Mutations on the fixes, all caught: I14 the employee row not locked; I15 a deleted employee counted
  active; I16 a deleted employee missing from the judged set; I17 the pay rate not checked; I18 the
  assignment not re-read under the lock.
  - I18 was not caught by the sequential test, because after one committed removal the second read
    finds nothing anyway. The race test catches it.
- Final gates: interrogate first found one missing docstring (`assign.run`, 99.8%), now added (100%); full suite 1825 passed; concurrency 46 passed three times; drift check agrees.

## Group J: publishing and what employees see (T058 to T062)

- Failing first: 13 of the 14 publish and my-shifts tests failed, because the routes did not exist.
  The one that passed was the Client refusal: the missing route answered 404.
- Decisions:
  - **Publishing** (`POST /api/v1/shifts/publish`, Manager or Admin): a project's drafts that start
    on `from` to `to`, dates in the organisation's zone. It answers how many were published.
    Published shifts are left alone, so publishing again counts only new drafts. A range that ends
    before it starts is 422 `shifts.date_range_invalid`.
  - **`GET /me/shifts`** sits beside `/me`, outside `/api/v1`, as `/me` already does. It has its own
    schema (`MyShift`): id, start, end, the site's zone, the shift type's name, the project and the
    site. It has no assignments, headcount or pay field, so it cannot name a co-worker (R7). Only
    published shifts the caller is on.
  - **The leak sweep** (`test_roster_visibility_sweep.py`) lists every Part 2 read from the
    document; a new one fails until it is added. Each read is called as an Employee, a Client and
    another organisation's employee, and checked for the other employee's id, their shift, the
    draft and their entry. An id the caller sent in the path is excluded, because a 404 repeats it,
    which is not a leak.
- Coverage tests extended: the role matrix (2 operations), the sweep (1 case), the locale texts (1
  code), `unprovable_operations.json` (1, the publish range), the contract (2 operations, 5
  schemas), and `docs/hds-api.md`.
- Mutations, all caught except one equivalent:
  - J1 drafts in my shifts; J2 co-workers' shifts in mine.
  - J4 another organisation's project publishable; J5 published shifts published again; J6 publish
    ignoring the project.
  - J7 `published_at` not set, caught by `ck_shifts_published_at`; J8 the range order not checked.
  - J3, the publish route's gate loosened to all staff, cannot be caught: the service's own
    `ensure_role` still refuses Employees. Defence in depth, and an equivalent mutant.
- /hds-review: no violations. Publishing has allowed (Manager, Admin) and forbidden (Employee, Client)
  tests, and `/me/shifts` allowed (staff) and forbidden (Client) tests. Codes are in `en` and `he`.
- /code-review high, ten findings:
  1. **An employee still saw a deleted project's shifts (fixed):** `own_published` filters out
     deleted projects. Test first.
  2. **The first or last calendar day crashed with `OverflowError`, a 500 (fixed):** they count as
     open ends. Test first, on both reads.
  3. **Publishing used the organisation's dates, while shifts are made on site dates (fixed).**
     Publishing now uses the project's site zone. Test first: a site 14 hours ahead
     (`Pacific/Kiritimati`) published nothing before the fix. The roster list (`GET /shifts`) spans
     projects and keeps the organisation's zone.
  4. **`/me/shifts` has no paging (not changed; logged as a follow-up).** It is bounded by the
     caller's own shifts, so it is a performance item, not a correctness one.
  5. **A backwards range was accepted on the reads (fixed):** `/me/shifts` and `/shifts` answer 422
     `shifts.date_range_invalid`, like publishing. `/me/shifts` joined
     `unprovable_operations.json`.
  6. and 10. **The bounds code was copied, and publishing narrowed it with an `assert` (fixed):** one
     pure `_bounds` helper, with no `assert`.
  7. **`/me/shifts` imported the factory from another router (fixed):** both use
     `routers/roster_wiring.py`, which only builds the service.
  8. **Routers import the session type (not changed):** every router does, for the unit-of-work
     dependency. The hook allows it and the import graph is clean.
  9. **The sweep matched `/me/shifts` by path, so a new `/me` read would escape it (fixed):** the
     router is tagged `my roster`, and the sweep finds reads by tag only. Test first: the inventory
     check failed until the tag was added.
- Mutations on the fixes, all caught: J9 deleted projects' shifts shown; J10 publishing by the
  organisation's zone; J11 read ranges not checked for order; J12 the last day not an open end
  (caught by the `OverflowError` itself, which is the crash the finding described).

## Group K: the employee app (T063 to T071)

- **Tooling (T063, R10).** Node.js 22.23.4, installed from nodejs.org into `~/.local/share/hds-tools/node`
  with its SHA-256 checked, and pnpm 12.15.0, pinned in the root `package.json` (`packageManager`).
  Playwright's Chromium (headless shell, build 1248) went to the user's Playwright cache. Every
  package version is exact and was published on or before 2026-10-21. Two were added beyond the plan's
  list: `test-renderer` 1.3.1, a peer of Testing Library 14, and `@types/node` 22.20.5 for the tests'
  file checks. CI gains the `client` job (ESLint, `tsc --noEmit`, Jest with coverage, the
  generated-client drift check) and the `client-e2e` job (Playwright on the web build, with the RTL
  smoke test), replacing the T147 placeholder.
- **The generated client (T065).** `packages/api-client/scripts/generate.mjs` exports the document
  from the API (`hds.scripts.export_openapi`), runs `openapi-typescript` and writes `src/schema.d.ts`;
  `--check` compares instead and fails on a difference. `createApiClient` takes `{baseUrl, fetch,
  middleware}`, so the session supplies the transport and tests a fake API.
- **/hds-review finding: `.gitignore` hid `packages/api-client/src/` (fixed).** A line from the 001
  scaffold ignored the generated client, so it could never be committed or checked. Removed.
- **Decisions:**
  - **Read-only after the deadline, but absences any time.** T068 says the screen stays read-only
    after the deadline, and the owner decided (2026-11-03) that absences may be reported at any time.
    So blocked, open and close entries lose their edit and remove buttons once a date falls outside
    an open window, absences keep them, and with no open window only "Absence" can be added. The
    screen's window check only decides which buttons show; the API decides, and its refusal is shown
    translated.
  - **Tokens (R9).** `expo-secure-store` on phones, memory only on the web. A 401 refreshes once,
    shared by requests refused together, and sends the request again; a refused refresh ends the
    session. An unreachable server during refresh does not end it.
  - **The e2e API runs on port 8100.** Port 8000 belongs to the Docker dev API, which these tests must
    not touch, so `pnpm e2e` exports the web build against 8100 and Playwright starts its own API there
    (`reuseExistingServer: false`). Locally it ran on `hds_p3_test`, emptied afterwards; in CI it runs
    on `hds_e2e_test`. The config refuses a database whose name does not end in `_test`.
    `LOGIN_RATE_LIMIT_PER_NETWORK=0` there, as the contract job does, because the lockout specs fail
    sign-ins on purpose.
  - **Single-page web export.** Expo's static export failed inside expo-router
    (`getRouteInfoForMatchedRoute is not a function`, a mismatch in the pinned packages), so the web
    build is a single page, served by `tests/e2e/serve.mjs` with a fallback to `index.html`.
  - **The 001 sign-in specs.** Three became runnable: signing out, and the locked-account message in
    English and Hebrew. Their assertions are unchanged. The lockout specs gained a `beforeAll` that
    locks `locked@hds.test` with five failed sign-ins through the API (the lock is the same for an
    unknown address, FR-221). The four role-dashboard specs stay skipped, now with the reason in
    `test.skip(true, ...)`, because no dashboard exists yet.
  - **Staff journey specs (T070a)** for windows (US5), the roster (US6) and shift types (US8) are
    written against the planned screens and skipped "pending the staff screens".
- **Tests:**
  - Jest: 52 tests in 7 files; coverage 95.6% of lines (the gate is 70%).
  - Playwright: 8 passed and 23 skipped (all with reasons), twice in a row.
  - The literal-string rule refused a planted text and a planted label.
- **Mutations, all 11 caught:**
  - K1 no retry after the refresh; K2 the refresh not shared; K3 web tokens in the secure store;
  - K4 closed entries editable; K5 absences not exempt; K6 the site's zone ignored;
  - K7 the direction fixed left to right; K8 an unknown code shown raw; K9 an absence keeping its
    shift type;
  - K10 a span over two windows judged by the first only; K11 a refused refresh keeping the session.
- /code-review high: ten findings and two extra, all real and all fixed (each test first, except the
  last three, which are clean-ups):
  1. **Starting a phone offline signed the user out for good:** `restore` now keeps the stored tokens
     when the API cannot be reached.
  2. **Signing out with an expired access token revoked the old refresh token, not the one it had
     been rotated to:** the logout is sent again after the refresh with the new tokens.
  3. **The sign-out e2e spec could not fail:** it now takes the refresh token the page sent to
     `/auth/logout` and checks that the API refuses it. A sign-out that skips the call fails the spec.
     The assertions it had are kept.
  4. **A phone kept the previous run's right-to-left direction for English:** the direction follows
     the language only.
  5. **Weekday entries read as every day:** the entry shows "Sundays only" and the like.
  6. **With no window open, an absence could be changed into a blocked entry that the API was sure
     to refuse:** it can now only become another absence.
  7. **A failed reload after a change was hidden behind the old list:** the error and a retry are
     shown above it.
  8. **The screens read the whole history:** windows, entries and `/me/shifts` are read from today.
  9. **A date formatter was built per call:** formatters are cached.
  10. **`.gitignore` repeated the Playwright folders:** the duplicates were removed.
  11. **The skipped staff specs named "Dana Employee":** the seeded name is "Employee User".
  12. **A failed start left a blank app:** the root layout shows the routes either way.
- Mutations on the fixes, all 7 caught (K12 to K18): offline restore clearing the tokens, the logout
  not retried, the direction taken from `I18nManager`, absences turning into any kind, the reload
  error hidden, the weekday not shown, and `/me/shifts` reading the whole history. Jest is at 60
  tests (95.5% of lines); Playwright is unchanged at 8 passed and 23 skipped.
- Follow-up: Hebrew strings for the new client keys (Constitution VIII asks for externalising, which
  is done); an httpOnly refresh cookie for the web, if a reload sign-in proves a burden (ADR 010).

## Checkpoint P13: the merge gate (T072)

Run on the committed tree after Group K (`41b2d9a`):

| Gate | Result |
| --- | --- |
| ruff, format, `mypy --strict` (252 files), interrogate 100%, import graph, pip-audit | All pass |
| Full suite | 1870 passed, 2 deselected (the timing tests, run below) |
| Concurrency, three runs | 46 passed each time |
| Timing (SC-207 and the reset request) | Ratios 1.002 and 1.012; both pass |
| Schemathesis against a seeded server | 3345 cases passed on the first attempt, no 5xx. The only warnings are "missing test data" (random ids answer 404) |
| Migrations, scratch database | 013 → head → 013 → head; `alembic check` clean |
| Migrations, seeded | `seed_dev` (with the default shift types from 020's trigger), then head → 019 → head; `alembic check` clean |
| OpenAPI drift | The contract and the implementation agree |
| Client | ESLint clean; `tsc --noEmit` clean; Jest 60 passed (95.5% of lines); the generated client is up to date; Playwright 8 passed and 23 skipped with reasons, including the RTL smoke test |

Quickstart Part 2, row by row:

- **Rules (R1):** `tests/property/test_rules_properties.py`, which checks overlap and rest, weekly
  hours, and blocked or absence coverage.
- **US4:** `test_availability_entries.py` covers storing each kind,
  `test_after_the_deadline_changes_are_refused_and_nothing_changes` and
  `test_an_absence_is_accepted_at_any_time`. In the app, the Jest availability tests and the
  Playwright availability spec.
- **US5:** `test_windows.py`, and in `test_availability_roles.py`
  `test_a_manager_or_admin_sees_an_employees_entries`,
  `test_a_manager_cannot_edit_another_employees_entries` and
  `test_an_admin_edits_anyones_entries_after_the_deadline`.
- **US6:** `test_assignments.py` covers rest, overlap across projects and drafts, weekly hours, entries
  and headcount, each stored with its warning.
- **US7:** `test_my_shifts.py` covers own published shifts only, no co-worker and the Client refused;
  `test_publish.py` covers before and after publishing. In the app, the My shifts spec.
- **SC-104:** `test_roster_visibility_sweep.py`.
- **SC-106:** `tests/unit/scheduling/test_local_span.py` gives 8, 7 and 9 hours; the client's
  `time.test.ts` shows the same nights at the site's times.
- **Client, and the checkpoint:** the table above.

## Part 2 closed (T073)

All of Part 2 (T030 to T073) is done and committed on `003-availability`; nothing is pushed.

Open items, none of them blocking:

- Page `GET /me/shifts`. The app now asks from today on, which bounds it in practice.
- `CONCURRENTLY` for later index builds on large tables.
- Refusing unknown fields in create requests, as the PATCH requests do.
- The query shape of the `login_throttle` purge.
- Hebrew strings for the new client keys.
- An httpOnly refresh cookie for the web (ADR 010).
- The staff screens, which make the skipped US5, US6 and US8 specs and the 001 dashboard specs
  runnable.
- Expo's static web export, which fails with the pinned packages.

## Proposed follow-ups (Part 2)

- Page `GET /me/shifts`, as `GET /shifts` is paged, if employees' histories grow large.
- **Gate finding: a route collision (fixed by moving the operation).** Schemathesis sent
  `PATCH /api/v1/shifts/publish`. It matched `PATCH /shifts/{id}` with the id "publish" and answered
  422 instead of 405.
  - A `uuid` path converter was tried first and reverted: an id that is not a UUID then reached the
    framework's plain 404, which is not problem+json, so the contract test failed another way.
  - Publishing is now `POST /api/v1/projects/{id}/publish` with `{from, to}`. It acts on one
    project's drafts, so the path names the project.
  - Updated to match: the tests, the role matrix, the sweep, `unprovable_operations.json`, the
    contract, `docs/hds-api.md` and `contracts/part2-api.md`.
- Final gates: all green; full suite 1870 passed; concurrency 46 passed three times; drift check agrees.
