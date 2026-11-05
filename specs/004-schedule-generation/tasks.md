# Tasks: Schedule generation

**Input**: `specs/004-schedule-generation/`: spec.md (US1 to US7, FR-001 to FR-027, SC-001 to SC-009),
plan.md (P1 to P7), research.md (G1 to G14), data-model.md, contracts/api.md, quickstart.md.

**Tests**: required (Constitution IV). Every rule gets a test that is run and seen failing for the right
reason before the code exists. Each group ends with mutation checks, the gates (ruff, format,
`mypy --strict src/ tests/`, interrogate, the import graph, `alembic check`), /hds-review and
/code-review high, the log (`p1-log.md`) and a commit. Tests run on a private `_test` database.

**Organization**: one group per plan phase; each group ends green and is committed.

## Format: `[ID] [P?] [Story] Description`

## Phase 1: Setup, P1, Group A

- [X] T001 Pin the API to Python 3.12 in `apps/api/.python-version`, recreate the environment, add `ortools` to `apps/api/pyproject.toml` (research G1); the full suite stays green on 3.12; record in the log
- [X] T002 Widen `tests/unit/scheduling/test_purity.py`: `cpsat.py` may import `ortools` and nothing else outside the standard library and the package; every other module stays standard library only; `import scheduling` in a bare interpreter still loads no `ortools` (research G2). Log the test change and its reason; commit `build(api): pin Python 3.12 and add OR-Tools (004 Group A)`

## Phase 2: Foundational, P2, Group B: the pure engine (blocks every story)

- [X] T003 [P] Failing unit tests `tests/unit/scheduling/test_eligibility.py`: a candidate is refused for a slot when inactive, blocked (date, shift type, weekday), absent, overlapping a fixed or chosen shift, closer than the minimum rest to one (night 22:00 to 06:00 then a 06:00 or 07:00 morning), or over the weekly maximum counting fixed hours in the same Sunday-to-Saturday week; DST nights count real hours
- [X] T004 [P] Failing property tests `tests/property/test_generate_properties.py`: for generated problems (random slots across projects and days, fixed shifts, candidates, entries, settings), each solver's result has no `validate` warning for overlap, rest, weekly hours, blocked, absence or inactive (FR-011 to FR-016, SC-002); assigned counts never exceed headcount; every unfilled place has reasons whose counts add up to the candidates (FR-020); at least 1,000 examples under the CI profile
- [X] T005 [P] Failing example tests `tests/unit/scheduling/test_generate.py`: a feasible week is filled completely by both solvers (SC-003); each unfilled reason built on purpose (`unavailable`, `weekly_maximum`, `minimum_rest`, `overlap`, `needed_elsewhere`, `no_active_employee`) is reported with its count (SC-004); the legacy flaws (SC-005): night then morning, a close preference yielding to coverage, hours rather than shift counts balanced, nights balanced, two projects in one run; with CP-SAT, the hours spread is at most one shift's length and the nights spread at most one (SC-006); a run longer than 6 days is avoided when the same coverage allows it; regenerating with the current drafts unchanged proposes zero changes (SC-007), and one new absence changes only that day's slots; `optimal` is true for a small proven case and false for greedy
- [X] T006 Types in `src/hds/scheduling/problem.py`; the shared eligibility test in `eligibility.py`; `greedy.py` (research G5); `cpsat.py` (research G4); `generate.py` with the repair pass, the reasons (G6), the summary, the changes and a final `validate` check; exports in `__init__.py` without loading `cpsat`
- [X] T007 Mutation checks (for example: rest checked by date only; close treated as a block; the weekly maximum ignoring fixed hours; reasons counted twice; the repair pass removed; the change cost dropped). Gates, both reviews, log; commit `feat(scheduling): greedy and CP-SAT roster generation (004 Group B)`

## Phase 3: Foundational, P3, Group C: migration 021 and the models

- [X] T008 Failing tests `tests/integration/test_migration_021.py`: upgrade from 020 and downgrade back on a seeded database; the staffing-need checks (weekday 0 to 6, headcount at least 1, unique per project, shift type and weekday); the job checks (range order and at most 31 days, the status and proposal-state values); `assignments.source` defaults to `manual` for existing rows; the organisation settings bounds (rest 1 to 24, weekly 1 to 168)
- [X] T009 Migration `alembic/versions/021_schedule_generation.py` and the models `src/hds/models/{staffing_need,schedule_job}.py`, `Assignment.source` (data-model.md); `alembic check` clean. Gates, log; commit `feat(db): staffing needs, schedule jobs and assignment source (004 Group C)`

## Phase 4: US1 and US7, staffing needs and settings (P1, P3), Group D, P4

**Goal**: FR-001 to FR-003, FR-026. **Independent test**: US1 and US7 independent tests in spec.md.

- [X] T010 [P] [US1] Failing integration tests `tests/integration/test_requirements.py`: Manager and Admin replace and read a project's needs; a type the project does not use 422 `requirements.shift_type_not_in_project`; a retired type 422 `requirements.shift_type_retired`; a duplicate (type, weekday) 422 `requirements.duplicate`; headcount 0 or weekday 7 refused 422; removing a type from the project or retiring it removes its needs (FR-002); Employee and Client 403; another organisation's project 404
- [X] T011 [P] [US7] Failing integration tests `tests/integration/test_scheduling_settings.py`: Admin changes both limits and 003's assignment warnings use them; Manager reads and is refused the change (403); 0, 25 and 169 refused 422; an unknown field 422; Employee and Client 403
- [X] T012 [P] [US1] Failing concurrency test `tests/concurrency/test_requirements_races.py`: two replacements of one project's needs at once leave exactly one complete set; a replacement racing the removal of a type from the project never leaves a need on a type the project does not use
- [X] T013 [US1] Repository, service and router for needs (`requirements`) and settings (`scheduling_settings`), following contracts/api.md; the needs clean-up in the shift-type service; error codes with `en` and `he` text; the role matrix, the cross-organisation sweep and `unprovable_operations.json` extended
- [X] T014 [US1] Contract (`openapi.yaml`), `docs/hds-api.md`, drift check; mutation checks; gates, both reviews, log; commit `feat(api): staffing needs and scheduling settings (004 Group D)`

## Phase 5: US2 and US4, jobs and the worker (P1, P2), Group E, P5

**Goal**: FR-004 to FR-022, FR-027. **Independent test**: US2 independent test in spec.md.

- [X] T015 [P] [US2] Failing integration tests `tests/integration/test_schedule_jobs.py`: Manager and Admin create a job (202, `queued`) and read it and the list; a backwards or 32-day range 422 `schedule.range_invalid`; an ended or another organisation's project 422 `schedule.project_not_active`; the same `Idempotency-Key` answers the first job, another body with it 422 `idempotency.key_reused`; Employee and Client 403; another organisation's job 404; the stored roster is unchanged by a run (FR-022)
- [X] T016 [P] [US2] Failing integration tests `tests/integration/test_schedule_inputs.py` on building the problem: slots from needs on each project date within the project's own dates (FR-006), an existing draft used as its slot with its headcount, a draft without a need still a slot, a published shift never a slot and reported when short; the surroundings (weeks the range touches plus the minimum rest, FR-016) as fixed shifts in every project; candidates: every active Employee, a Manager or Admin only with an open entry in the range (FR-007), a non-candidate's draft assignments kept as fixed; the candidates with no entries listed (FR-019); the fingerprint stable for the same data and changed by each input of FR-024
- [X] T017 [P] [US2] Failing worker tests `tests/integration/test_worker.py` and `tests/concurrency/test_worker_claims.py`: a queued job runs to `succeeded` with a proposal and `optimal`; two workers claim one job once; a job past its lease is claimed again, and failed `jobs.worker_lost` after 3 attempts; a solver error marks it failed `jobs.solver_failed` and changes nothing; a result `validate` rejects is never stored
- [X] T018 [US2] Repositories `schedule_job.py` and `scheduling_inputs.py`, services `schedule_job.py` and `schedule_inputs.py`, router `schedule_jobs.py` (create, list, read), `src/hds/worker.py` (claim with `SKIP LOCKED`, lease, `LISTEN`, retries, solve in a thread); reason and error codes with `en` and `he` text; the role matrix, the sweeps and `unprovable_operations.json` extended
- [X] T019 [US2] Contract, `docs/hds-api.md`, drift check; mutation checks; gates, both reviews, log; commit `feat(api): schedule jobs and the worker (004 Group E)`

## Phase 6: US3 and US6, accept and discard (P1, P3), Group F, P6

**Goal**: FR-023 to FR-025, SC-007, SC-009. **Independent test**: US3 independent test in spec.md.

- [X] T020 [P] [US3] Failing integration tests `tests/integration/test_schedule_accept.py`: accept stores every proposed shift and assignment as drafts (`source` generated), updates existing drafts in place, publishes nothing, and employees' `/me/shifts` is unchanged (SC-009); accepting twice 409 `schedule.proposal_closed`; a queued or failed job 409 `schedule.not_ready`; each change of FR-024 (an entry, a shift in the surroundings, an employee deactivated, a need, a project's shift types, the settings) makes accept 409 `schedule.proposal_out_of_date` with nothing changed; discard closes the proposal; a proposal older than 7 days reads `expired` and is refused; Employee and Client 403; regenerating after accepting proposes zero changes (SC-007)
- [X] T021 [P] [US3] Failing concurrency tests `tests/concurrency/test_accept_races.py`: two accepts of one proposal at once store it once; an accept racing an availability change or an assignment of a candidate either accepts first, or is refused as out of date, never both applied on stale data
- [X] T022 [US3] Accept and discard in the schedule-job service and router (locks: projects, then candidates in id order; rebuild and compare the fingerprint; write the drafts), error codes with `en` and `he` text; matrix and sweeps extended
- [X] T023 [US3] Contract, `docs/hds-api.md`, drift check; mutation checks; gates, both reviews, log; commit `feat(api): accept and discard schedule proposals (004 Group F)`

## Phase 7: Polish and checkpoint, P7, Group G

- [ ] T024 [P] Benchmark `apps/api/benchmarks/compare_solvers.py` and `tests/benchmark/test_solver_benchmark.py` (marker `benchmark`, deselected by default): coverage, hours and nights spread and runtime at 10, 30 and 100 employees; SC-001 asserted for 30 employees and one week
- [ ] T025 [P] ADR `docs/adr/011-solvers-and-job-queue.md`, listed in `docs/README.md`; the README (the worker, the benchmark); `infra/docker-compose.yml` worker service; skipped staff journey specs `apps/client/tests/e2e/test_us_generate.spec.ts` and `test_us_requirements.spec.ts` "pending the staff screens"
- [ ] T026 Regenerate `packages/api-client`; client gates green; commit `docs(api): document schedule generation and the worker (004 Group G)`
- [ ] T027 The merge gate (all API gates, full suite, concurrency three times, timing, contract, drift, migration round trips 020 ↔ 021 on a seeded database, the property tests at the CI profile, the benchmark) plus the client gates; every quickstart row checked; close `p1-log.md`; commit `docs(spec): close 004`. Do not push.

## Dependencies

- Group A before everything. Group B after A. Group C after A. Group D after C. Group E after B, C and D.
  Group F after E. Group G after F.

## Parallel examples

- Group B: T003, T004 and T005 (different test files).
- Group D: T010, T011 and T012. Group E: T015, T016 and T017. Group F: T020 and T021.
- Group G: T024 and T025.

## Implementation strategy

The pure engine (B) is shippable as a library on its own and is where most of the risk is, so it comes
first, with property tests. The API groups then make it reachable: needs and settings (D), jobs (E), and
accepting (F), which is the MVP end to end.
