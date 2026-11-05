# 004 decision log

Owner instruction (2026-11-05): "Go with recommended to all - your suggestions are logical. Run until
youre stuck or ready to merge the feature - based on the existing harness." The standing rules of 003
apply: tests on a private `_test` database (`hds_p3_test`), nothing else using it; no push; no edit to
committed migrations; no test deleted or weakened, and every test change explained here; mail after the
response; each group failing tests first, then code, mutation checks, the gates, /hds-review and
/code-review high, the log and a commit; stop on an unexplained failure, a change to a spec.md
requirement, a security regression, an unplanned migration, a push or anything destructive.

## Stages

- **Specify**: two owner answers (slots from staffing needs; drafts only change), recorded in
  spec.md Clarifications.
- **Clarify**: four questions, the recommended option each time (Managers and Admins as candidates
  only with an open entry; no entries means free, listed; project dates bound the slots; the run's
  surroundings).
- **Plan**: research G1 to G14. G1: OR-Tools has no wheel for Python 3.15, and the local environment
  had drifted to 3.15 while the image, mypy and docs/hds-spec.md say 3.12, so the API is pinned to
  3.12. G2: `ortools` is imported by `cpsat.py` only; the purity test is widened for it.
- **Tasks**: T001 to T027 in groups A to G.
- **Analyze** (inline):
  - Every FR and SC has a task.
  - The only drift is in names: the spec's "waiting" and "finished" job states are the prompt's
    `queued` and `succeeded`, now noted in contracts/api.md.
  - No constitution conflict beyond the noted ortools import (plan, Complexity Tracking).

## Group A: Python 3.12 and OR-Tools (T001, T002)

- `apps/api/.python-version` pins 3.12. uv installed CPython 3.12.16 and recreated `.venv`, which had
  drifted to 3.15.2. `ortools>=9.15` resolved to 9.15.6755, published 2026-01-14.
- **Test change: `tests/unit/scheduling/test_purity.py`.** It allowed the standard library only in
  every module of the package. Now `cpsat.py` may also import `ortools`; every other module stays
  standard library only, and the bare-interpreter test also asserts that `import scheduling` loads no
  `ortools`. The reason: the plan prompt and docs/hds-spec.md section 5 put CP-SAT in the pure
  package. Constitution III forbids app, database and network imports, not a solver library
  (research G2, Complexity Tracking). The test is no weaker for every other module.

## Group B: the pure engine (T003 to T007)

- **Built:**
  - `problem.py`: types and reason codes.
  - `eligibility.py`: one `Roster.refusal` for both solvers and the reasons.
  - `greedy.py`: research G5.
  - `cpsat.py`: research G4.
  - `generate.py`: solve, repair pass, a final `validate` check, reasons, summary and changes.
  - `rules.entry_covers`: the one reading of "does an entry cover this shift", now shared by
    `validate` and the generator. A refactor; the 003 rule tests are unchanged and green.
- **Design change during the build: CP-SAT solves the goals in stages, not as one weighted sum.**
  Keeping FR-017's order strict with tier weights needs coefficients near 10^15, which hurt the
  solver's relaxation. Each stage now keeps the earlier best values as constraints and starts from
  their solution. `optimal` means every stage was proven. Research G4 is updated.
- **Reason order:** unavailable, then overlap, minimum rest and weekly maximum against fixed shifts,
  then "needed elsewhere" for clashes with slots the proposal filled (research G6, updated to match
  FR-020's list).
- **Tests:**
  - 81 unit tests: eligibility, and generation for both solvers.
  - 3 Hypothesis properties: both solvers break no hard rule; CP-SAT, when optimal, fills at least
    as many places as greedy. At 1,000 examples each they take about 14 seconds.
- **CI gap found and fixed:** `tests/property` was never run in CI. The unit job ran `tests/unit`
  only, so 003's rule properties never ran there either. The job now runs `tests/unit` and
  `tests/property`, with `HDS_GENERATE_EXAMPLES=1000`.
- **Mutations:**
  - All 19 caught (B1 to B19).
  - The first run left 8 uncaught. 6 were gaps in my tests and are now covered:
    - the repair pass (B7) and the `optimal` flag (B18) only matter when the search runs out of time:
      a zero-budget test, and a stage forced to report "feasible";
    - the runs (B10), hours (B11) and preference (B13) tests passed by the solver's own
      tie-breaking: they now use bigger cases, and the runs test runs both ways round;
    - greedy's preference order (B14): the preference test now runs both solvers;
    - fixed nights in the summary (B19): a summary test.
  - B17 (skipping the final `validate` check) is defence in depth: no correct solver can trigger it,
    so it has a direct test of `_check`.
  - B3 (the fixed-shift check skipped) changes only the reason given, never the roster, so the
    property tests cannot see it; the unit tests catch it.
- /hds-review: one finding, fixed. The package docstring still said "standard library only"; it now
  names the `cpsat.py` exception.
- /code-review high: ten findings. The tenth (Groups A and B uncommitted together) was already
  resolved: Group A was committed alone (`6f4357d`) before the review finished. Fixed, each with a
  failing test first except the clean-ups:
  1. **The repair pass ignored the minutes the solver had already given.** It balanced on fixed
     shifts only, so it could pile empty places on one person. It now counts every chosen slot
     (`worked_minutes`).
  2. **A draft that already holds non-candidates could be over-filled.** Their places were kept
     outside the slot. `Slot.kept` now holds them: they take places (`Slot.places`), count as fixed
     for their own rules, and are judged with the slot in the final check.
  3. **The `generate` function hid its own module**, so a patch of `hds.scheduling.generate._repair`
     silently did nothing. The module is now `engine.py`.
  4. **The time limit started after the model was built.** The clock now starts when `generate` is
     called, and CP-SAT takes a deadline.
  5. **Run days were read in the organisation's zone** for fixed shifts but in the site's zone for
     slots. Every booking now carries its own site-local day. No test: it needs two zones on one
     roster, which the services never build.
  6. **Minutes were rounded down for the weekly limit.** They are now rounded up (`problem.minutes`),
     so the limit is never crossed by seconds.
  7. **`Slot.day` was stored and could disagree with `starts_at`.** It is now derived.
  8. **The place-filling loop was copied.** Greedy and repair now share `eligibility.fill`.
  9. **Eligibility ran its checks twice.** It is now one pass that tells fixed clashes from chosen
     ones.
- Mutations after the fixes: 23 of 23 caught. New ones: B20 repair ignoring chosen minutes; B21 kept
  people taking no places; B22 minutes rounded down; B23 the final check ignoring kept people,
  defence in depth like B17, with a direct test.
- **The Group B commit (`3067ba1`) went in with one failing test (my mistake).** The commit ran in
  parallel with the unit and property run, before its result was read. The failing test was
  `test_time_money_invariants.py::test_source_has_no_float_annotations_or_calls_on_money`
  (Constitution VII bans the name `float` in `src`): the CP-SAT deadline was annotated `float`. The
  test is right, so the code changed: the deadline is now integer nanoseconds
  (`time.monotonic_ns()`). Fixed in the next commit; from here on, a commit waits for its tests.
- **Second /code-review high** (engine and Group C), ten findings:
  1. **A draft an Admin overfilled by hand, or a kept person who already broke a rule, failed every
     run with `SolverError`.** 003 never refuses a manual change, so this can happen. The final
     check now judges only the warnings the proposal causes: rules broken by someone it chose, and
     headcounts it filled past. `Slot.places` never goes below zero.
  2. **A slot's own draft could be given both in `fixed` and as the slot's `kept` people**, counting
     them twice. The `Problem` contract now says it never goes in `fixed`, and `generate` refuses it
     (`ValueError`).
  3. **Kept people listed in `current` came out as "removed".** They are ignored in the changes.
  4. **An empty place could have no reason** when every active candidate was already on the slot. It
     now reads `no_active_employee` (0).
  5. **On SQLite, `ddl_if` dropped the whole range check**, not only its length part. It is now two
     checks: `ck_schedule_jobs_order` everywhere and `ck_schedule_jobs_length` on PostgreSQL only.
     Checked by inserting a backwards range into a SQLite schema built from the models: refused.
  6. **`project_ids` is JSONB, while data-model.md said `uuid[]`.** Kept as JSONB, so the SQLite
     unit schema builds; data-model.md now says so, and the service validates the ids (Group E).
  7. **A candidate who could take no slot pinned the smallest total** in the hours and nights
     spreads. Both spreads now count only people who can take at least one slot.
  8. **CP-SAT rounded fixed hours up to whole minutes**, so it could refuse what the rule allows. The
     weekly constraint now counts whole seconds, as the rule does.
  9. **Hot-loop cost:** `Slot.day` and `Slot.minutes` are cached, and the week of each slot is
     worked out once per roster.
  10. **Duplicated helpers:** there is now one `local_day`, in `rules.py`. `validate`'s own overlap
      loop is left as it is: tested 003 code, and only cosmetic.
- Mutations on these fixes: R1 to R6 caught. R7 (`places` allowed below zero) is equivalent, because
  every use of `places` skips anything at or below zero.

## Group C: migration 021 and the models (T008, T009)

- Migration 021: `staffing_needs`, `schedule_jobs`, `assignments.source`, and the settings bounds
  (rest 1 to 24 hours, weekly 1 to 168, replacing 020's positive-only checks). Models
  `StaffingNeed` and `ScheduleJob`; `Assignment.source`; the organisation's new check names.
  `alembic check` is clean.
- **Test fix during the build:** `test_migration_021.py` first passed dates as strings, which asyncpg
  refuses. Because the "bad row" cases accepted any `DBAPIError`, they would have passed on that
  type error instead of on the constraints. Dates are now `date` objects, and the bad rows expect
  `IntegrityError` only, so each one fails on its constraint alone.
- Mutations: C1 to C11 all caught (weekday and headcount checks, range order and length, the unique
  need, the source values and default, the settings bounds, the status and proposal-state values).
- /hds-review: no findings. /code-review: covered with the engine above (finding 5, the SQLite
  range check, is fixed here).
- Full suite, then on Python 3.12: 1,944 passed.

## Group D: staffing needs and the scheduling limits (T010 to T014)

- **Built:**
  - `GET`/`PUT /api/v1/projects/{id}/requirements`: the needs, replaced as a whole under the
    project's row lock, with the named types share-locked.
  - `GET`/`PATCH /api/v1/organisation/scheduling`: the limits; Admin changes them.
  - Removing a type from a project, or retiring it, removes its needs (FR-002).
  - Three error codes in `en` and `he`; the role matrix, the cross-organisation sweep (a foreign
    shift type in an own project answers 422 `requirements.shift_type_not_in_project`, revealing
    nothing) and `unprovable_operations.json` extended.
  - The contract (`openapi.yaml`) and `docs/hds-api.md` updated; the drift check agrees.
- **Harness slip:** the Group D tests were written before the code, but I did not run them before
  writing it, so I never saw them fail. The mutation checks stand in for that: D1 to D15, with each
  surviving mutant then given a test.
- **Mutations:**
  - D2 (a retired type accepted) survived: a retired type also leaves the project's set, so it was
    refused as "not in the project" instead. A direct test now expects `shift_type_retired`.
  - D8 and D11 (the services' own role checks removed) are equivalent: the routers' role gates
    refuse first. They stay as defence in depth.
  - All others caught, including D4 and D5 (the project and type locks), by the race tests.
- **/hds-review:** no findings. All four operations have allowed and forbidden tests.
- **/code-review high**, nine findings:
  1. **Retiring a type could deadlock against replacing a project's needs.** The two deletes locked
     shared rows in opposite orders. Fixed: replacing the needs, and replacing a project's types,
     now share-lock the types of the project's current needs before touching any need row, as a
     retirement locks its type first. A seven-row race test was added, but it never reproduced the
     deadlock, so the fix rests on the lock order, not on a failing test.
  2. **An unbounded list of needs became one huge `IN` and a 500.** The list is capped at 1,000
     items (422). Test first.
  3. **The error code depended on set order** when a request held both a retired and a foreign
     type. Validation now follows the request order. Test first.
  4. **A composite foreign key from needs to project shift types**, suggested so the schema enforces
     FR-002: declined. Migration 021 is committed, so this would need another migration, and the
     service rule is tested, races included. Recorded as a follow-up.
  5. **`update_limits` took the organisation row lock that window operations also take.** It is now
     a single `UPDATE … RETURNING`.
  6. **`Weekday` was defined twice.** The required, strict form now lives in `hds.core.types`.
     Availability's optional `weekday` field is a different shape and keeps its own.
  7. and 8. **A duplicated tenant check and an extra read after the replace:** cosmetic, left.
  9. **`for_projects` did not exclude deleted projects or retired types.** It does now.

## Group E: jobs and the worker (T015 to T019)

- **Built:**
  - `POST`/`GET /api/v1/schedule-jobs` and `GET /api/v1/schedule-jobs/{id}`, with `Idempotency-Key`
    through the existing service.
  - The problem builder, `ScheduleInputService`: slots from the needs within each project's own
    dates, drafts standing in for their need, every other draft a slot too, published shifts
    fixed and short ones reported. The surroundings run from the Sunday of the first week to the
    Saturday of the last, plus the minimum rest either side. Candidates follow FR-007; people on a
    draft who are not candidates are kept on it. The fingerprint covers the problem, the run's
    projects and their shift types.
  - `python -m hds.worker`: claim with `FOR UPDATE SKIP LOCKED`, a 2-minute lease, three attempts,
    `LISTEN schedule_jobs` with a 5-second poll, the solve in a thread, and three short
    transactions per job.
  - `SCHEDULE_TIME_LIMIT_SECONDS` (default 20) in the settings and `.env.example`.
- **The hook caught a layering slip:**
  - The first `schedule_jobs.py` imported SQLAlchemy and repositories into a router, as 003's
    routers do.
  - `requirements.py` and `scheduling_settings.py` (Group D) had the same imports. I had written
    them through the shell, so the hook never saw them; that was a slip, not a deliberate way
    round the hook.
  - Fixed at the cause: a composition root, `hds/wiring.py`, builds the services and offers them
    as FastAPI dependencies, so the three new routers import neither the database nor a
    repository.
  - The 003 routers still import both. Recorded as a follow-up; they predate this feature.
- **Test fixes while building, all in tests written for this group, before they ever passed:**
  - The worker test asked for 7 mornings with one employee: 56 hours, over the weekly 48. It now
    asks for Sunday to Thursday.
  - A project's start date cannot be changed through the API; the test sets it in the database.
- **Code fix found by the tests:** the fingerprint missed a project's shift types when no need used
  the changed one. FR-024 names those changes, so the fingerprint now includes the (project, type)
  pairs in use.
- **The cross-organisation sweep's inventory missed body fields named `*_ids`**, so a list of ids
  could have escaped it. It now includes them; no older operation surfaced.
- **The visibility sweep** covers the four new reads too: a proposal names employees, and an
  Employee, a Client or another organisation sees none of it.
- **Mutations:** E1 to E18 run.
  - E9 (no rest margin before the range) and E10 (the fingerprint ignoring entries) survived at
    first; their tests are now sharper and both are caught.
  - E15 (a late run overwriting a newer claim) survived; a new test holds a second claim open while
    the first run ends, and it is caught.
  - E12 (no `SKIP LOCKED`) is equivalent for correctness: PostgreSQL re-checks the row after the
    lock wait, so the second worker finds nothing; `SKIP LOCKED` only saves the wait.
- An earlier full suite started before these changes hung at 97% for 11 minutes with no database
  activity, while its files changed under it. I stopped it; a fresh full run covers D and E.
- **/code-review high on D and E**, ten findings:
  1. **The worker solved with the organisation's limits at claim time, not the job's own.** A job
     records the limits it was asked with and now solves with them (`build(..., limits=)`).
     Accepting still compares with the current limits, so a change makes it out of date. Test
     first.
  2. **The 2-minute lease could run out mid-solve**, and the time limit had no upper bound. The time
     limit is now 1 to 60 seconds and the lease 5 minutes.
  3. **A database error in the claim or store steps stopped the worker.** Each step of the loop is
     now caught and logged, and the job keeps its lease to be claimed again.
  4. **My comment on `update_limits` was wrong:** a plain `UPDATE` takes the row lock too. The
     comment now says so. The contention with window operations stays; both are rare and short.
  5. **A chosen project that ended before the run was silently left out.** The job now fails with
     `jobs.project_not_active` (`en`, `he`). Test first.
  6. **Groups D and E were not committed separately.** True: D's routers moved onto the wiring E
     added, so the two cannot be split cleanly now. They are committed together, with this note.
  7. **The new repositories extended `BaseRepository`, not `OrgScopedRepository`.** The job and need
     repositories now use the tenant-scoped base. The inputs repository reads across tables, each
     query scoped by hand.
  8. **Duplicated helpers:** `rules.sunday_index` is now the one weekday helper, used by the problem
     builder and the availability service. The cursor helper still has two copies (cosmetic, left).
  9. **The builder re-sorted every cell per project and every need per day.** Both are now grouped
     once.
  10. **A notification arriving during the drain was lost.** The wake event is cleared before the
      drain.
- **Contract test findings (Schemathesis):**
  - `GET /schedule-jobs` with a random cursor answers 422, as `GET /shifts` does; it joins
    `unprovable_operations.json`.
  - `PATCH /organisation/scheduling` refused `5.0`, which JSON Schema counts as an integer. The
    limits are no longer `strict`; `5.5` and `"eleven"` are still refused.

## Group F: accept and discard (T020 to T023)

- `POST /schedule-jobs/{id}/accept` and `/discard`. Accepting locks the job, then the run's
  projects in id order, then every staff account of the organisation in id order. It rebuilds the
  problem and compares fingerprints (409 `schedule.proposal_out_of_date`), then writes. Existing
  drafts are updated in place; new slots become new drafts; assignments get `source = 'generated'`
  and `assigned_by` the accepter. Nothing is published. Codes `schedule.not_ready`,
  `schedule.proposal_closed` and `schedule.proposal_out_of_date` in `en` and `he`; 409 added to both
  operations in `core/openapi.py`.
- Tests seen failing first this time (16 of 17, as 404s), then passing. One test fix while building:
  shift answers have no `date` field, so the Monday check reads the start time.
- **A fix found by the tests:** discard answered with a stale copy of the job, because the
  repository updated the row behind the session's back. `decide` now sets the fields on the locked
  job itself.
- Mutations F1 to F10, all caught after one gap: F10 (people the proposal takes off a draft were
  kept) needed a test where the person on a draft becomes absent before the run.
- Race tests: two accepts at once store the proposal once; an accept racing an absence is either
  first (200) or refused as out of date.
- **/code-review high on F**, nine findings:
  1. **Accept did not block the shift-level writes.** Publishing, changing, deleting, assigning and
     unassigning could commit between the fingerprint check and accept's writes.
  2. **Accept did not lock all the fingerprint's inputs:** projects with no slots, shift types, the
     organisation's limits.
     Fixed together. Accept now locks, in an order consistent with the other paths: the job; the
     organisation row (`FOR NO KEY UPDATE`; a change of limits waits); every project in scope,
     `FOR UPDATE` (needs, shift types and new shifts there wait, a new shift's key-share lock
     included); the shift types (shared; a retirement waits); every shift in the surroundings
     (`FOR NO KEY UPDATE`, as `get_locked`); then the staff. Shifts come before users because an
     assignment locks its shift and then its user. A race test with publishing was added (8
     rounds). **The race tests cannot prove the fix:** with the shift or staff locks removed (F11,
     F12), they still pass, because the window is too narrow to hit on demand. The fix rests on
     the lock order, and is recorded as such.
  3. **While accept runs, staff sign-ins and refreshes wait on their user rows.** Accepted, and
     documented in the service and ADR 011: accept is rare and short, and this is what makes the
     out-of-date check exact.
  4. **A failure storing the result was retried three times and reported as lost.** The worker now
     fails the job with `jobs.solver_failed` at once.
  5. **A third attempt finishing just past its lease is marked lost.** Left as is: the lease is 5
     minutes and a solve at most 60 seconds.
  6. **One savepoint per assignment while holding the locks.** New shifts and assignments are now
     inserted in bulk, one statement each.
  7. **A redundant projects query.** The job's project check now reads the built problem's own list.
  8. **The cursor helper is duplicated.** Cosmetic; left.
  9. **F was built on uncommitted D and E.** True: D to F are committed together, with this note.
- **Full suite after D to F:** 2,081 passed, 1 failed. `test_deployment_files` wants every setting in
  the API README's configuration table; `SCHEDULE_TIME_LIMIT_SECONDS` was missing and is added. The
  gates found one missing docstring (`draft_slot`); fixed. Everything else is green: ruff, format,
  `mypy --strict` (290 files), the import graph, `alembic check`.

## Group G: benchmark, docs, worker service, generated client (T024 to T026)

- Benchmark `benchmarks/compare_solvers.py`, plus the `benchmark` marker (deselected by default):

  | Employees | Greedy fills | CP-SAT fills | CP-SAT optimal | CP-SAT time |
  |---|---|---|---|---|
  | 10 | 59 of 63 | 60 of 63 | yes | 1 s |
  | 30 | 126 of 126 | 126 of 126 | yes | 1 s |
  | 100 | 504 of 504 | 504 of 504 | no (time limit) | 13 s |

  CP-SAT also spreads nights more evenly, and at 10 employees its hours spread is 0 against
  greedy's 8. SC-001 (30 employees, a week, under 30 seconds) passes; `-m benchmark`: 3 passed.
- ADR 011, listed in `docs/README.md`. The root README covers the worker and the benchmark.
- A `worker` service in `infra/docker-compose.yml`, from the API's image.
- Skipped staff journey specs `test_us_generate.spec.ts` and `test_us_requirements.spec.ts`, "pending
  the staff screens".
- `packages/api-client` regenerated; the drift check is green; client lint, type check and Jest (60)
  pass.

## Merge gate (T027)

Run on the committed tree after Group G (`e39118e`):

| Gate | Result |
| --- | --- |
| ruff, format, `mypy --strict` (288 files), interrogate 100%, import graph, pip-audit | All pass |
| Full suite | 2,082 passed, 5 deselected (timing and benchmark, run below) |
| Concurrency, three runs | 54 passed each time |
| Timing (SC-207 and the reset request) | Ratios 1.004 and 1.005; both pass |
| Property tests at the CI profile (1,000 examples) | 6 passed |
| Benchmark (`-m benchmark`) | 3 passed; SC-001 holds |
| Schemathesis against a seeded server | Passed on the first attempt, no 5xx |
| Migrations | 013 → head → 013 → head on a scratch database; `seed_dev`, then head → 020 → head on the seeded one; `alembic check` clean both times |
| OpenAPI drift | The contract and the implementation agree |
| Client | The generated client is up to date; ESLint and `tsc` clean; Jest 60 passed |

Quickstart, row by row:

- **Purity:** `test_purity.py`.
- **Hard rules (SC-002):** `test_generate_properties.py`, at 1,000 examples.
- **Legacy flaws (SC-005):** in `test_generate.py`, night then morning, a close preference yielding,
  hours not counts, nights balanced, two projects at once.
- **Coverage and reasons (SC-003, SC-004):** in `test_generate.py`, each reason built on purpose.
- **Fairness (SC-006):** the hours-spread and nights tests, and the absent-candidate balance test.
- **US1:** `test_requirements.py` and its races.
- **US7:** `test_scheduling_settings.py`.
- **US2:** `test_schedule_jobs.py` and `test_schedule_inputs.py`.
- **Queue:** `test_worker.py` and `test_worker_claims.py`.
- **US3:** `test_schedule_accept.py` and `test_accept_races.py`.
- **US6, SC-007:** regenerating after accepting proposes no change; one absence changes only its own
  day.
- **Access:** the role matrix, the cross-organisation sweep and the visibility sweep.
- **Benchmark (SC-001):** the table above, in Group G.

## 004 closed

All tasks T001 to T027 are done and committed on `004-schedule-generation`; nothing is pushed.

**Deviations, each recorded above:**
- Groups D to F were committed together; their routers share the composition root.
- One commit (Group B) went in with a failing test and was fixed in the next commit.
- The Group D tests were not seen failing before the code.
- The lock-order fixes in accept rest on reasoning, not a failing test.

**Open items, none blocking:**
- The 003 routers still import SQLAlchemy and repositories; moving them onto `hds.wiring` is a
  follow-up.
- A composite foreign key from staffing needs to project shift types (FR-002 in the schema).
- One shared cursor helper for the jobs and shifts lists.
- Paging `GET /me/shifts` (from 003).
- The staff screens: generating, reviewing and accepting a roster, and staffing needs; their
  Playwright specs are written and skipped.
- The goal weights and the time limit per organisation are fixed in this feature (spec assumptions).
- Qualifications on slots (the rest of H5).
- Locking chosen assignments (SCH-5).
