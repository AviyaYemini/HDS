# 003 Part 1, phase P7: decision log

Decisions, review findings, mutation results and deviations for Part 1 (the leftovers from 002).
Tests run on the private database `hds_p3_test`; `hds_test` is never used.

## Setup (T001)

- `hds_p3_test` exists and had no connections when the phase started (`pg_stat_activity`).
- Analysis before implementation, two findings acted on:
  - T022 originally forced `enable_seqscan off`, which proves the index can be used, not that the
    clean-up uses it. It now seeds SC-003's volumes and reads the real plan.
  - SC-004 says sign-in, lockout and reset tests pass unchanged, but
    `test_a_registered_address_is_counted_in_the_throttle_table_only` reads the two columns 019 drops.
    Its intent (the count lives only in the throttle table) stays; only the column check changes (T025).

## Group A: contract start editing (T002 to T008)

- Decision (E1): `contract_start` joins `ClientUpdateRequest` as `StrictDate = omittable()`, so it may be
  left out but not sent as null. `edit_client` builds the resulting pair (sent value, else stored) and
  judges it only when it differs from the stored pair. That one condition covers FR-002 and FR-003,
  including a form that sends a broken stored pair back unchanged.
- Failing first: 8 tests (6 integration, 2 unit) failed because the start was ignored (200, value
  unchanged) or never judged (`DID NOT RAISE`). Unit tests for the unchanged pair passed before and
  after, as they should: they pin FR-003, which 002 already met for the end alone.
- Mutations, all caught by a named test, files restored:
  - A1 end judged against the stored start only: `test_a_new_start_is_judged_against_the_stored_end`
  - A2 stored pair judged on every edit: `test_an_unchanged_pair_is_not_judged[no-dates]` and
    `test_a_stored_date_pair_that_breaks_the_rule_does_not_block_other_edits`. The first run was
    caught only by a mock comparison `TypeError` in an unrelated test, which does not count, so it
    was re-run against the intended tests.
  - A3 judged only when the end changes: `test_a_new_start_is_judged_against_the_stored_end`
  - A4 start clearable: `test_the_contract_start_cannot_be_cleared`
  - A5 start not accepted: `test_a_manager_or_admin_corrects_the_contract_start[manager]`
- Gates: ruff, format, mypy strict (203 files), interrogate 100%, import graph, `alembic check`,
  full suite 1470 passed. Drift check: contract and implementation agree.
- /hds-review: no violations.
- /code-review high, seven findings:
  1. **Race on the date check (fixed).** With both dates editable, one request moving the start
     later and another moving the end earlier each passed against the old row and together stored
     a broken pair. The failing test reproduced it in round 0. Fix: `ClientRepository.get_by_id`
     gained `for_update` (with `populate_existing`, so the locked read sees the committed row), and
     `edit_client` locks when either date is sent.
     - The lock created a new deadlock, also reproduced first (round 2): deactivation and delete took
       the portal users and then the client row, while a date edit that deactivates took the client
       row first. Fix: one order everywhere, client row first. `edit_client` also locks on
       deactivation, and `soft_delete` always locks.
     - Tests: `tests/concurrency/test_client_dates_race.py` (the pair race, deadlock races against a
       deactivation and against a delete at 100 rounds each, and the fresh read under the lock), plus
       deterministic unit tests of the lock and its order in `tests/unit/test_client_service.py`.
     - Mutations, all caught: A6 no lock on a date edit; A7 no lock on deactivation; A8 no lock on
       delete; A9 lock without `populate_existing`. The delete race alone caught A8 in only 1 of 3
       runs, so the unit test `test_the_client_row_is_locked_before_any_portal_user[delete]` is the
       reliable catch. A9's first "catch" was the new test failing on its own (it read attributes
       after a rollback), which does not count; it was fixed and re-run.
  2. **Stale route description and docstring (fixed):** they now say "contract dates" and state the
     pair rule.
  3. **Backwards test docstring (fixed).**
  4. **Spec wording against the code (deviation, spec wording clarified).** FR-002 said "sets". The
     code judges only a pair that differs from the stored one. That follows the owner's wording ("an
     edit that does not touch either date") and 002's existing test, which sends the stored end back
     with a rename and expects 200. FR-002 now says "to a value other than the stored one", and
     FR-003 adds "or sends both back as stored". The behaviour is unchanged, so this is a wording
     clarification, not a new requirement.
  5. **`_client()` mock had no date defaults (fixed):** real defaults, `_dated` removed.
  6. **Contract description incomplete (fixed):** it states the unchanged-pair case.
  7. **Commit hygiene (fixed):** the T022 change went in its own commit (`docs(spec): make T022 ...`),
     and T002 to T008 are marked done with this commit.
- Final gates after the fixes: ruff, format, mypy strict (204 files), interrogate 100%, import graph,
  `alembic check`, full suite 1479 passed, concurrency suite 34 passed three times, drift check
  agrees.

## Group B: unknown fields refused (T009 to T014)

- Decision (E2): a `ChangeRequest` base in `core/types.py` with `extra="forbid"`, used by the employee,
  client, project and site PATCH models. `ProfileUpdate` keeps `extra="allow"`, because its service
  answers 403 `profile.field_not_allowed`, which names the problem more precisely. Create requests are
  out of scope (spec Assumptions).
- Failing first: 14 of 16 new tests failed, with 200 instead of 422 and `additionalProperties`
  missing from the schemas. The "with a valid field" cases also showed the valid field being applied
  while the unknown one was dropped, which is the hazard FR-005 names. The two `/me` tests passed
  before and after, as they should.
- T011: the unit and integration suites (1411 tests) passed unchanged with the stricter models, so no
  existing test relied on an unknown field being ignored. No test was changed.
- Mutations, all caught by a named test: B1 to B4, each model back to `BaseModel`; B5, the base set to
  `extra="ignore"`; B6, `ProfileUpdate` set to `forbid` (caught by
  `test_my_profile_keeps_its_own_refusal_of_unknown_fields`, 422 instead of 403).
- Contract: `additionalProperties: false` on the request schemas. The drift check compares operations,
  status codes and query parameters only, not component schemas, so
  `test_the_contract_file_declares_unknown_fields_refused` reads the contract file itself.
- /hds-review: no violations.
- /code-review high, six findings:
  1. **Group and qualification renames still dropped unknown fields (fixed; spec assumption
     corrected).** FR-005 and SC-002 cover every change operation. The spec's list of five missed
     these two, which share one model with their create request. Each rename now has its own
     `GroupRenameRequest` and `QualificationRenameRequest` on `ChangeRequest`, so create requests keep
     their handling as the spec says. The spec's Assumptions now list the renames. Tests failed first
     (200, and the schemas did not exist).
  2. **The API doc stated a rule for every PATCH (fixed):** it names the six operations and says create
     requests still ignore unknown fields.
  3. **The create-only test's docstring and values (fixed):** the project case sends a real other
     client's id.
  4. **The log claimed the drift check covered schemas (fixed):** the contract-file test above was
     added and the log corrected.
  5. **The module docstring (fixed).**
  6. **Create requests also ignore unknown fields (not changed; out of scope by the spec):** recorded
     as a Proposed follow-up below.
  - The reviewer ran the suite itself without `TEST_DATABASE_URL`, so on its own per-run database. It
    never touched `hds_test`, which was checked to be empty with no connections. My first test run of
    the fixes collided with my own gate run on `hds_p3_test` (13 errors on user inserts). The gate run
    was stopped and re-run after the fixes, so no result from the collision is used.
- More mutations, all caught: B7 and B8, each rename back to `BaseModel`; B9, the group create made
  strict (caught by `test_create_requests_keep_their_handling_of_unknown_fields`); B10, one
  declaration removed from the contract file.
- Final gates: ruff, format, mypy strict (205 files), interrogate 100%, import graph, `alembic check`,
  full suite 1509 passed, concurrency suite 34 passed three times, drift check agrees.

## Group C: the Client project path and the unreachable promotes (T015 to T020)

- Decision (E3): a Client account's listing returns from `ProjectRepository.list_by_client`, which
  gained the status filter. The no-client guard in the service stays. (Revised after review: see
  finding 6 below.)
- Failing first: 3 unit tests (the Client branch did not call `list_by_client`) and 1 repository test
  (`list_by_client` took no `status`).
- Changed tests, with the reason: `test_list_projects_client_scoped_to_own_client` and
  `test_list_projects_client_ignores_external_client_id_filter` asserted the `list_by_org` call that
  FR-006 replaces. They now assert `list_by_client` with the caller's own client, and that
  `list_by_org` is never awaited. Their rule, own client whatever the filter, is unchanged.
- Decision (E4), the promote methods. Before removal, each rule their tests pinned was matched to a
  test on `edit_employee`, the only path a request reaches:
  - `test_promote_to_manager_by_manager_revokes_sessions` is now covered by the new
    `test_a_manager_promoting_to_manager_by_edit_ends_the_sessions`.
  - `test_promote_to_admin_by_manager_forbidden` is covered by
    `test_manager_cannot_set_role_admin_via_edit`, which now also asserts the code and that nothing
    was written, and by integration `test_non_admin_promoting_to_admin_returns_403`.
  - `test_promote_to_admin_by_admin_succeeds` is now covered by the new
    `test_an_admin_making_an_admin_by_edit_ends_the_sessions`.
  - `test_manager_cannot_demote_admin_via_promote_to_manager` is covered by
    `test_manager_cannot_edit_admin`, now parametrised over a name and a role change, with the code
    asserted.
  - `test_a_promotion_of_a_client_account_is_refused[manager, admin]` is now covered by
    `test_an_edit_of_a_client_accounts_role_is_refused`, parametrised over employee, manager and
    admin, and integration `test_a_client_accounts_role_cannot_change`.
  - The new tests passed on the existing code, as coverage tests should. Mutations of `edit_employee`
    showed they bite: C1 no session revocation on a role change; C2 no Admin-target guard (caught
    then by a separate test that review finding 8 folded into `test_manager_cannot_edit_admin[role]`);
    C3 Client role unlocked. All caught.
  - Then `promote_to_manager` and `promote_to_admin` and the 5 test functions (6 collected cases)
    were removed. Nothing else named them, apart from 001's historical tasks.md.
- Mutations on the Client path, all caught: C4 back to `list_by_org`; C5 status not passed in the
  service; C6 status filter dropped in the repository; C7 no-client guard removed. C7 is caught by
  the unit test only. The integration test still passes because `projects.client_id` is NOT NULL, so
  `client_id IS NULL` matches nothing. That safety comes from the column, not from the query.
- /hds-review: no violations.
- /code-review high: no correctness bug found; nine findings:
  1. **The Manager-assigns-Admin unit test asserted only the exception type (fixed):** it now asserts
     `users.admin_required` and that nothing was written.
  2. **Tasks not ticked (done at commit, as in A and B).**
  3. **The new test file instead of an existing one (skipped, cosmetic).**
  4. **The log said `list_by_client` was safe "by construction, because its filter is equality"
     (fixed):** SQLAlchemy writes `== None` as `IS NULL`, so the safety comes from the NOT NULL
     column. Corrected above.
  5. **`list_by_client` repeats `list_by_org`'s query (skipped, cosmetic).**
  6. **The root cause remained (fixed).** `list_by_org(client_id=None)` still meant "every client", so
     any future Client-facing caller could repeat 002's hazard, which FR-006's "through every
     internal path" forbids.
     - `list_by_org` no longer takes a client, so it always means the whole organisation (staff only).
       A staff listing filtered by client goes through `list_by_client`, whose client is required.
     - Tests failed first: the staff filter did not use `list_by_client`; `list_by_org` was called
       with `client_id=None`; and `test_no_project_read_takes_an_optional_client` pins both
       signatures.
     - Mutations: C8, staff filter dropped (caught by
       `test_a_staff_listing_filtered_by_client_uses_the_per_client_read`); C9, unknown-client 404
       skipped (caught by the existing `test_list_projects_filtered_by_foreign_client_returns_404`).
       C9's first run did not include that test file and showed "not caught"; re-run with it.
  7. **Duplicate no-client test (fixed):** the new copy was dropped. The existing parametrised
     `test_a_client_account_with_no_client_lists_nothing` now also asserts `list_by_client` is never
     awaited.
  8. **Redundant role-edit refusal test (fixed):** folded into `test_manager_cannot_edit_admin`.
  9. **JSON strings passed where `uuid.UUID` is typed (fixed).**
  - The first Group C gate run was stopped when these fixes started, because it was testing older
    code. Only the re-run counts.
- Final gates: ruff, format, mypy strict (206 files), interrogate 100%, import graph, `alembic check`,
  full suite 1514 passed, concurrency suite 34 passed three times.

## Group D: migration 019 (T021 to T027)

- Failing first:
  - `test_migration_019.py`: 3 tests failed because the revision did not exist.
  - `test_the_clean_up_reads_only_the_expired_rows`, at SC-003's volumes (100,000 unexpired, 1,000
    expired): both tables were read by sequential scan.
- Deviation, the revision id. `019_expiry_indexes_retire_lock_columns` is 38 characters, and
  `alembic_version.version_num` is VARCHAR(32). The upgrade failed at its last step; PostgreSQL
  rolled the whole migration back, and `hds_p3_test` stayed at 018 with its columns (checked).
  Renamed `019_expiry_index_retire_locks`. A name containing the SQL keyword for removing a column
  was avoided, because the project hook reads it next to the database client as a manual schema
  change. The new `test_every_revision_id_fits_the_version_table` keeps any future id within 32
  characters.
- Deviation, the clean-up query (plan said indexes only).
  - With the index, the plan still read 101,000 rows. The inner locking select used the new index,
    but the outer delete by `id IN (subquery)` hash-joined it against a sequential scan of the whole
    table.
  - Tried first: two statements (lock the ids, then delete by primary key). At this size the planner
    still chose a sequential scan for 1,000 key lookups, since it is cheaper there. Reverted.
  - Kept: the delete repeats `expires_at <= now` next to `id IN (subquery)`, so the planner drives
    the delete from the expiry index too. It now reads 3,000 rows (index and join) instead of
    101,000, with no sequential scan. The repeated condition also re-checks expiry on the deleted
    rows. Same in both repositories.
  - The test replays every statement the clean-up runs (captured with a cursor event) under
    `EXPLAIN (ANALYZE, FORMAT JSON)` in a rolled-back transaction. It asserts no sequential scan and
    at most 5,000 rows read (SC-003, "a few times" the 1,000 expired).
- Changed test, with the reason: `test_a_registered_address_is_counted_in_the_throttle_table_only`
  read the two retired columns and asserted (0, None). Its intent, that the count lives only in the
  throttle table, now compares the committed account row before and after the failed sign-ins (after
  review; see the first review, finding 7). The other sign-in, lockout
  and reset tests are unchanged (SC-004). `test_me.py:82` still sends `failed_login_attempts` to
  PATCH /me and still gets 403. It never depended on the column.
- Docstrings: the `User` and `LoginThrottle` models no longer describe the columns as kept.
- Mutations, all caught by a named test:
  - D1, the downgrade does not restore `failed_login_attempts`.
  - D2, `locked_until` restored without a time zone.
  - D3, one index missing.
  - D4, one retired column kept.
  - D5 and D6, the delete without the repeated expiry condition, for each table. D6's first run did
    not apply, because the formatter had wrapped the line; it was re-run on the wrapped text.
  - These two were superseded by the review fix below.
- /hds-review: no violations. The migration is new, reversible and documented, and no applied
  migration was edited.
- /code-review high, eight findings:
  1. and 2. **The repeated expiry condition made each batch read the whole expired backlog
     (fixed).** The planner drove the delete from the expiry index over every expired row and then
     joined the batch's ids, so draining a large backlog was quadratic. My test had seeded exactly
     one batch, so it could not see this.
     - Fix: the batch's physical row addresses (`ctid`) are selected through the expiry index and
       locked with `SKIP LOCKED`, and the delete removes exactly those addresses
       (`ctid = ANY(ARRAY(subquery))`, a TID scan). The subquery runs once, as an InitPlan. A row
       locked `FOR UPDATE` cannot move until the transaction ends, so its address is stable.
     - An `ORDER BY expires_at` was added at first, because without it a plan showed a bitmap scan
       reading every expired index entry. The second review showed that plan was an artefact (see
       the second review, finding 1), and no test could show the order is needed, so it was removed.
  3. **The test could not tell a batch from the backlog (fixed):** it now also seeds five batches'
     worth (5,000) and asserts at most 3x the deleted count read per call. Before the fix, the
     backlog cases failed.
  4. **The row counter ignored loops and index rechecks (fixed):** each node's rows are multiplied by
     its loops, and rechecked rows count. With the stricter counter the old query failed even the
     one-batch case.
  5. **Index build not `CONCURRENTLY` (not changed):** a plain build blocks writes to the two tables
     while it runs. Nothing is deployed, so there is no table worth waiting for. Recorded as a
     follow-up below.
  6. **`test_me.py` names and 002's data model (not changed).** The `test_me` cases still guard that
     those names are never writable. 002's spec is a record of that feature, so it is left as it was.
  7. **The lockout test lost its behaviour check (fixed):** it now compares the whole account row,
     committed, before and after two failed sign-ins, so it again ties failures to the throttle
     table only.
  8. **The revision-length guard parsed text (fixed):** it reads every revision through Alembic's
     `ScriptDirectory`.
- More mutations, all caught by a named test:
  - D5 (both tables), the old `id IN (subquery)` delete restored: caught by the volume test. A first
    version of this mutation only swapped the selected column and failed on a type error, which
    does not count; it was re-run as a full revert.
  - D7 (both tables), `SKIP LOCKED` removed: not caught by any existing test. This was a gap from
    002. The new `tests/concurrency/test_expired_cleanup_skips_locked.py` holds one expired row in
    another transaction and requires the clean-up to finish within 5 s, delete the rest and leave
    that row. With the mutation, the clean-up waits and the timeout fails the test.
- Second /code-review high, of the rewritten delete, ten findings:
  1. **`ANALYZE` was rolled back with the test (fixed).** Statistics are transactional, and the
     seed's `ANALYZE` ran in the transaction the test rolled back, so the replayed plans had no
     column statistics. It is committed now.
     - With real statistics, the no-`ORDER BY` mutation (D6) was not caught for either table, even
       on a backlog that was not yet analyzed (a third case added for this). So the earlier
       "bitmap scan reads the backlog" plan was an artefact, and the order was removed.
     - The old delete (D5) is still caught with real statistics.
  2. **A row moved by a concurrent update is skipped, and a short batch ended the drain (fixed).**
     A delete by `ctid` skips a row that an update moved while the batch ran. The purge's `_drain`
     stopped at the first batch shorter than `BATCH`. That also happened before 003, when
     `SKIP LOCKED` skipped a row, so part of a backlog could wait for the next run.
     - `_drain` now stops only when a batch removes nothing. A skipped row is taken by a later
       batch.
     - Test first: `test_a_short_batch_does_not_end_the_drain` (3 == 5 before the fix). D8, the old
       stop condition restored, is caught.
  3. **`login_throttle.purge_stale` has the same `IN (subquery)` shape (not changed).** It is outside
     FR-008, which names sign-in tokens and one-time links. Recorded as a follow-up.
  4. **The ctid block is duplicated in two repositories (skipped, cosmetic).**
  5. **Each batch steps over the dead index entries of earlier batches until vacuum (not
     changed).** This is a cost of any batched delete in index order before vacuum. It is bounded by
     one drain, and recorded with 3.
  6. **The T025 wording and the log said the lockout test asserts the columns are gone (fixed in the
     log).** The test compares the whole account row before and after (first review, finding 7),
     and `test_migration_019` asserts the columns are gone. Deviation from T025's wording, by review.
  7. **`test_me.py` names and a stale e2e comment (comment fixed).** The skipped Playwright test in
     `apps/client/tests/e2e/test_us1_signin.spec.ts` now says to seed a `login_throttle` row.
     `test_me.py` is unchanged, as before.
  8. **The downgrade default check was `"0" in ...` (fixed):** it is exact now. D9, default `10`, is
     caught.
  9. and 10. **Test helper duplication and the volume test's cost (skipped).**
- `hds_p3_test` was left with rows by a killed run (my test started while a gate run was using
  the database, a second time). It was emptied with the fixtures' own truncate of every table, and
  from then on no test ran while a gate run was going.
- Final gates: ruff, format, mypy strict (208 files), interrogate 100%, import graph, `alembic check`,
  full suite 1526 passed, concurrency suite 36 passed three times.

## Proposed follow-ups (not in Part 1)

- Build the expiry indexes `CONCURRENTLY` in any later migration that indexes a large live table.
  019 builds them plainly, which is fine before the first deployment.
- `LoginThrottleRepository.purge_stale` deletes with `IN (subquery)`, which the 019 work showed can
  read the whole table per batch. A shared batched delete by `ctid` for all three clean-ups would fix
  it in one place.

- Create requests (`POST` of employees, clients, projects, sites, groups, qualifications) still drop
  unknown fields, so a misspelt optional field answers 201 without it. `ChangeRequest` could become a
  general strict request base. This needs the owner's decision, because the spec put create requests
  out of scope.

## Checkpoint P7 (T028, T029)

The 002 merge gate, run without `TEST_DATABASE_URL` (per-run databases), on b0e5691:

| Check | Result |
| --- | --- |
| ruff, format, mypy strict (208 files), interrogate, import graph, pip-audit | all pass |
| Full suite | 1526 passed |
| Concurrency suite, three runs | 36 passed each time |
| Timing | sign-in ratio 1.003 (unknown 63.4 ms, known 63.6 ms); reset request ratio 1.016 |
| Contract (`scripts/contract_cli.sh`, throwaway database) | passed on the first attempt, no 5xx |
| Migrations, throwaway database | 013 to head, down to 013, head again; `alembic check` clean |
| Seeded round trip | seed, down to 018, head again; `alembic check` clean |
| Drift check | contract and implementation agree |
| Per-run databases left | 0 |

quickstart.md rows: FR-001 to FR-004 (Group A tests), FR-005 (Group B), FR-006 and FR-007 (Group C and the
unchanged suite), FR-008 (the volume test's replayed plans), FR-009 (migration 019 tests and the
seeded round trip) are each covered by a named test above.

Part 1 is complete on `003-availability`. Not pushed. Left open for the owner:

- The follow-ups above: strict create requests, `CONCURRENTLY` for later index builds on live tables,
  and the `login_throttle` purge's query shape.
- Spec wording clarified by review: FR-002 and FR-003 (Group A), and the Assumptions list of change
  operations, which now includes the group and qualification renames (Group B). Behaviour matches
  the owner's stated intent in both. Please confirm.
