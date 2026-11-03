---

description: "Task list for 003-availability, Part 1 (leftovers from 002)"
---

# Tasks: Availability, Part 1 (leftovers from 002)

**Input**: `specs/003-availability/` (spec.md Part 1, plan.md, research.md E1 to E6, data-model.md,
contracts/part1-api-changes.md, quickstart.md)

**Tests**: required (Constitution IV). Every rule gets a test that is run and seen failing for the
right reason before the change, then a mutation check that reverts the rule and names the test that
catches it. Results go in `specs/003-availability/p7-log.md`.

**Organization**: plan Phase P7, one group per story slice; each group ends green and is committed.
Part 2 (availability) tasks are appended later.

All source paths are under `apps/api/`. Tests use the private database `hds_p3_test`, never `hds_test`.

## Format: `[ID] [P?] [Story] Description`

## Phase 1: Setup

- [X] T001 Start the decision log `specs/003-availability/p7-log.md` (decisions, findings, mutation results, deviations) and confirm `hds_p3_test` exists and no other session uses it (`pg_stat_activity`)

## Phase 2: Foundational

None. Each group stands alone.

## Phase 3: User Story 1 - Correct a client's contract start date (P1), Group A

**Goal**: FR-001 to FR-004.

**Independent test**: PATCH `/api/v1/clients/{id}` with `contract_start` alone, with both dates, with a pair out of order, with `null`, and a rename on a stored pair that is out of order.

- [X] T002 [US1] Failing integration tests in `tests/integration/test_client_rules.py`: a Manager and an Admin change `contract_start` (stored; an Employee gets 403); a start after the stored end gives 422 `clients.date_range_invalid` and stores nothing; start and end sent together are judged as a pair (a valid pair that the stored end alone would refuse is stored; an invalid pair is refused); `contract_start: null` gives 422 `validation.request_invalid`; a rename on a client whose stored pair is out of order (set by SQL) is stored
- [X] T003 [P] [US1] Failing unit tests in `tests/unit/test_client_service.py` for `edit_client`: the rule runs on the resulting pair when either date is sent and differs; not when neither is sent; sending the stored values unchanged is not judged
- [X] T004 [US1] Add `contract_start: date = omittable()` ("required date, now editable; never empty") to `ClientUpdateRequest` in `src/hds/routers/clients.py`
- [X] T005 [US1] In `ClientService.edit_client` (`src/hds/services/client.py`) judge the resulting pair (sent value, else stored) with `_validate_contract_dates` when `contract_start` or `contract_end` is sent and the pair differs from the stored pair (research E1)
- [X] T006 [US1] Contract: `ClientUpdateRequest.contract_start` (format date, not nullable) in `specs/001-foundation-auth-org/contracts/openapi.yaml`; `docs/hds-api.md` clients section; regenerate and pass the drift check (`uv run python -m hds.scripts.export_openapi --check`)
- [X] T007 [US1] Mutation checks: judge only the end against the stored start; judge the stored pair on unrelated edits; allow null start. Each must be caught by a named test from T002 or T003
- [X] T008 [US1] Gates (ruff, format, `mypy --strict src/ tests/`, interrogate, `uv run python -m hds.tools.check_imports`, `alembic check`, full suite on `hds_p3_test`), /hds-review and /code-review high, fixes with failing tests first, log; commit `feat(api): let managers correct a client's contract start (003 Group A)`

**Checkpoint**: US1 is complete and shippable alone.

## Phase 4: User Story 2 - Edits never silently ignore a field (P2), Group B

**Goal**: FR-005.

**Independent test**: PATCH each of employees, clients, projects and sites with one unknown field; PATCH `/me` with `role`.

- [X] T009 [US2] Failing tests in new `tests/integration/test_patch_unknown_fields.py`, parametrised over the four PATCH operations: an unknown field alone and with a valid field gives 422 `validation.request_invalid` and the stored row is unchanged; `/me` with an unknown field still gives 403 `profile.field_not_allowed` (`tests/integration/test_me.py:82` stays as is)
- [X] T010 [US2] Add a shared request base with `extra="forbid"` in `src/hds/core/types.py` and use it for `UserUpdateRequest` (`src/hds/routers/employees.py`), `ClientUpdateRequest` (`src/hds/routers/clients.py`), `ProjectUpdateRequest` (`src/hds/routers/projects.py`) and `SiteUpdateRequest` (`src/hds/routers/sites.py`); `ProfileUpdate` in `src/hds/routers/me.py` keeps `extra="allow"` (research E2)
- [X] T011 [US2] Fix any existing test that sent an unknown field to these four operations and relied on it being ignored; record each in the log with the reason
- [X] T012 [US2] Contract: `additionalProperties: false` on the four request schemas in `specs/001-foundation-auth-org/contracts/openapi.yaml`; regenerate; drift check; `docs/hds-api.md` notes the refusal
- [X] T013 [US2] Mutation checks: each of the four models back to the default `extra`; `ProfileUpdate` to `forbid` (must be caught by the `/me` 403 test)
- [X] T014 [US2] Gates, both reviews, fixes, log; commit `fix(api): refuse unknown fields on change requests (003 Group B)`

## Phase 5: User Story 3 - Housekeeping that changes nothing users see (P3)

**Goal**: FR-006 to FR-010.

**Independent test**: the full suite and the drift check unchanged; the Client project path empty without a client; the migration round trip; the clean-up's plan uses the new index.

### Group C: the Client project path and the unreachable methods

- [X] T015 [US3] Failing unit test in `tests/unit/test_project_client_scope.py`: the Client branch of `ProjectService.list_projects` reads through `ProjectRepository.list_by_client` with the status filter passed through, and never calls `list_by_org`; a repository test in `tests/integration/test_client_portal.py` (or the existing Client project test file) shows `list_by_client` honours the status filter
- [X] T016 [US3] Add the status filter to `ProjectRepository.list_by_client` (`src/hds/repositories/project.py`) and use it in the Client branch of `ProjectService.list_projects` (`src/hds/services/project.py`); the early refusal of an account with no client stays (research E3)
- [X] T017 [US3] For each rule pinned by the `promote_to_manager` / `promote_to_admin` unit tests in `tests/unit/test_employee_manager.py` (lines near 166, 179, 189, 225, 498), confirm a test of `edit_employee` pins the same rule; add any missing one first and see it fail under a mutation
- [X] T018 [US3] Remove `EmployeeService.promote_to_manager` and `promote_to_admin` from `src/hds/services/employee.py` and their unit tests; log each removed test with the test that now covers its rule (research E4)
- [X] T019 [US3] Mutation checks: the Client branch back to `list_by_org(client_id=...)` without the guard; the status filter dropped from `list_by_client`
- [X] T020 [US3] Gates, both reviews, fixes, log; commit `refactor(api): narrow the client project read and drop unreachable promotes (003 Group C)`

### Group D: migration 019

- [X] T021 [US3] Failing migration test `tests/integration/test_migration_019.py` on a scratch database, as in `test_migration_018.py`: after 019 both indexes exist on `expires_at` and the `users` columns are gone, users kept; after downgrade to 018 the indexes are gone and both columns are back as in 004 ("`failed_login_attempts SMALLINT NOT NULL DEFAULT 0`, `locked_until TIMESTAMPTZ NULL`") with every user at 0 and no lock; upgrade again succeeds
- [X] T022 [P] [US3] Failing test in `tests/integration/test_expired_cleanup.py`: seed SC-003's volumes with `generate_series` (100,000 unexpired and 1,000 expired rows), `ANALYZE`, then `EXPLAIN (ANALYZE, FORMAT JSON)` the select each `delete_expired` runs (`src/hds/repositories/refresh_token.py`, `src/hds/repositories/invitation_token.py`): no sequential scan on the token table, and no more than 5,000 rows examined (SC-003)
- [X] T023 [US3] Write `alembic/versions/019_expiry_index_retire_locks.py`: create `idx_refresh_tokens_expires_at` and `idx_invitation_tokens_expires_at`; drop `users.failed_login_attempts` and `users.locked_until`; downgrade reverses all four (research E5, E6)
- [X] T024 [US3] Declare the indexes in `src/hds/models/refresh_token.py` and `src/hds/models/invitation_token.py`; remove the two attributes from `src/hds/models/user.py`; `alembic check` clean
- [X] T025 [US3] Update every test and document that names the dropped columns, including `tests/integration/test_one_address_count.py::test_a_registered_address_is_counted_in_the_throttle_table_only` (now asserts the columns do not exist) and the `LoginThrottle` docstring in `src/hds/models/login_throttle.py`; log each changed test with the reason
- [X] T026 [US3] Mutation checks: downgrade without restoring a column; one index missing in the upgrade
- [X] T027 [US3] Gates, migration round trip on a scratch database, both reviews, fixes, log; commit `feat(db): index token expiry and drop the retired lock columns (003 Group D)`

## Phase 6: Polish and checkpoint P7

- [X] T028 Merge gate as at the end of 002: all gates, full suite, concurrency suite three times, timing tests, `scripts/contract_cli.sh` on a throwaway database, drift check, migration round trip 018 ↔ 019 on a seeded database; quickstart.md rows checked
- [X] T029 Close the log with the results and anything left open; commit `docs(spec): close 003 Part 1`. Do not push.

## Dependencies

- Setup T001 first. Groups A, B, C and D are independent in code; they run in order A, B, C, D because A and B both edit `src/hds/routers/clients.py` and the contract.
- Within a group: tests, then code, then contract, then mutations, then gates and commit.
- T028 after all four groups.

## Parallel examples

- Group A: T002 and T003 (different test files).
- Group D: T021 and T022 (different test files).

## Implementation strategy

US1 first (the only new capability), then US2, then US3. Each group is committed on its own, so the branch is in a good state after any of them.

---

# Tasks: Part 2, availability and manual shifts

**Input**: spec.md Part 2 (US4 to US8, FR-101 to FR-125, SC-101 to SC-106), plan.md Part 2 (P8 to P13),
research.md R1 to R10, data-model.md Part 2, contracts/part2-api.md, quickstart.md Part 2.

**Tests**: required (Constitution IV), as in Part 1. Each group: failing tests seen failing for the right
reason, then code, the contract, mutation checks naming the catching test, all gates on `hds_p3_test`
(never `hds_test`; never while another run uses it), /hds-review and /code-review high, the log
`specs/003-availability/p8-log.md`, one commit. API paths are under `apps/api/`; client paths under
`apps/client/` unless stated.

**Story order**: US8 (shift types) is P2 in the spec but runs before US6, because a shift is created from
its project's shift types (FR-111). US4 and US5 share the availability modules and run as one group.

## Phase 7: Setup (Part 2)

- [X] T030 Start `specs/003-availability/p8-log.md` (decisions, findings, mutation results, deviations); record the owner decisions of 2026-11-03 (Managers publish, absences any time, app in `apps/client`)

## Phase 8: Foundational, P8 (blocks every Part 2 story)

### Group E: the pure rules

- [X] T031 [P] Failing unit tests in `tests/unit/scheduling/test_rules.py` for `hds.scheduling.validate`: overlap (`warnings.overlap`); rest below `min_rest` (`warnings.rest_too_short`, 22:00–06:00 then 07:00 gives 1 hour); weekly hours over `max_weekly` in the Sunday-to-Saturday week of `week_zone` (`warnings.weekly_hours_exceeded`); assignment on a blocked entry, an absence, a close entry (`warnings.blocked`, `warnings.absence`, `warnings.close_preference`), matching date range, shift type and weekday, with the date taken as the shift's start date in its site zone (R2); `warnings.over_headcount` and `warnings.under_headcount`; `warnings.employee_inactive` for an assigned employee who is no longer active; no warning for open entries or for a clean assignment; each warning carries the employee id and every shift id involved
- [X] T032 [P] Failing property tests in `tests/property/test_rules_properties.py` (Hypothesis): for random shifts of one employee, every pair that overlaps or rests less than `min_rest` is reported and no other pair is; reported weekly totals equal the real elapsed hours per week; every assignment on a date and shift a blocked entry or an absence covers is reported, and none outside them (H4, Constitution IV)
- [X] T033 Failing purity test in `tests/unit/scheduling/test_purity.py`: importing `hds.scheduling` in a subprocess with only the standard library on the path succeeds, and its modules import nothing outside the standard library and the package (Constitution III)
- [X] T034 Implement `src/hds/scheduling/__init__.py`, `types.py` (`ShiftSlot(id, project_id, starts_at, ends_at, headcount, assigned)`, `Entry(kind, starts_on, ends_on, shift_type_id, weekday)`, `Settings(min_rest, max_weekly, week_zone)`, `Warning(code, employee_id, shift_ids)`, frozen dataclasses with standard-library types only) and `rules.py` (`validate`), and `local_span(date, start_local, end_local, crosses_midnight, zone)` for R4 (nonexistent local time moves forward by the gap; ambiguous takes the first occurrence)
- [X] T035 [P] DST tests in `tests/unit/scheduling/test_local_span.py`: 22:00–06:00 in `Asia/Jerusalem` is 8 hours on a normal night, 7 and 9 on the two change nights (SC-106); a start inside the spring gap moves forward
- [X] T036a Write `docs/adr/009-rule-checks-and-warnings.md`: every roster rule lives in the pure `hds.scheduling.validate`, shared with the future generator; manual changes answer warnings and are never refused for a rule (R1, FR-116); list it in `docs/README.md`
- [X] T036 Mutation checks on the rules: rest compared with `<=` instead of `<`; overlap ignoring touching ends; Monday-start weeks; close preference not reported; the date of a night shift taken from its end. Gates, both reviews, log; commit `feat(scheduling): pure validation rules for manual rostering (003 Group E)`

### Group F: migration 020 and the models

- [X] T037 Failing migration test `tests/integration/test_migration_020.py` on a scratch database at 019: upgrade creates the seven tables (including `idempotency_keys`, "primary key (`user_id`, `key`)") and the two `organisations` columns with the data-model constraints quoted exactly ("`min_rest_hours` SMALLINT NOT NULL DEFAULT 11", "`max_weekly_hours` SMALLINT NOT NULL DEFAULT 48", "`headcount` SMALLINT ≥ 1", "`pay_rate` NUMERIC(12,4) NULL (> 0 when set)", "`absence_kind` set if and only if kind is absence", "no two windows of one organisation overlap", "unique (`shift_id`, `user_id`)", "`ends_at` after `starts_at`"); each existing organisation gets morning 07:00–15:00, afternoon 15:00–23:00 and night 23:00–07:00 (crosses midnight); every check constraint refuses a bad row; downgrade removes all of it and upgrading again works; the revision id fits 32 characters
- [X] T038 Write `alembic/versions/020_availability_shifts.py` (revision id `020_availability_shifts`), creating `btree_gist` if absent and recording whether it did, so the downgrade removes it only then
- [X] T039 Models in `src/hds/models/{shift_type,availability,submission_window,shift,idempotency_key}.py` (`Shift` and `Assignment` share `shift.py`), and the `Organisation` settings columns; the three default shift types come from the migration's database trigger for every organisation, so no application code inserts them; `alembic check` clean
- [X] T040 Mutation checks on the migration (a constraint missing, the downgrade leaving a table, defaults not seeded). Gates including the migration round trip; both reviews; log; commit `feat(db): tables for availability, shift types and shifts (003 Group F)`

## Phase 9: US4 and US5, availability and windows (P1), Group G, P9

**Goal**: FR-101 to FR-109. **Independent test**: US4 and US5 independent tests in spec.md.

- [X] T041 [P] [US5] Failing integration tests in `tests/integration/test_windows.py`: Manager and Admin create, change the deadline of, and delete a window; Employee may list windows but not create one (403); Client 403; overlapping periods refused 409 `windows.overlap`; `ends_on` before `starts_on` refused 422; another organisation's window is 404
- [X] T042 [P] [US5] Failing concurrency test `tests/concurrency/test_window_overlap.py`: two Managers creating overlapping windows at once leave exactly one (the exclusion constraint), the other 409
- [X] T043 [P] [US4] Failing integration tests in `tests/integration/test_availability_entries.py`: inside an open window an Employee adds, changes and removes blocked, open and close entries (date, range, shift type, weekday); after the deadline or with no window each is refused 409 `availability.window_closed` or `availability.no_window` and nothing changes; an absence (leave, reserve duty, sick) is accepted at any time (FR-105a); an entry with a shift type or weekday on an absence is refused 422; a second entry for an overlapping date and the same or no shift type is refused 409 `availability.conflict` (FR-104)
- [X] T044 [P] [US5] Failing integration tests in `tests/integration/test_availability_roles.py`: Manager and Admin list every employee's entries for a range; Admin adds, changes and removes any employee's entries after the deadline; Manager changing another's entry 403 `availability.admin_required`; Employee reading or changing another's 404; Client 403 on every operation (FR-107 to FR-109)
- [X] T045 [P] [US4] Failing concurrency test `tests/concurrency/test_entry_conflict.py`: two requests adding conflicting entries for one employee at once leave exactly one (the user row lock, R6)
- [X] T046 [US4] Repositories `src/hds/repositories/{submission_window,availability}.py`; services `src/hds/services/availability.py` (window rule judged at request time against the deadline, absence exception, conflict check under `lock_user_row`, role rules); routers `src/hds/routers/availability.py` for the window and constraint operations of contracts/part2-api.md; error codes with `en` and `he` text in `src/hds/locales/`
- [X] T047 [US4] Contract: the window and constraint operations in `specs/001-foundation-auth-org/contracts/openapi.yaml`, `docs/hds-api.md` Availability row and error codes; drift check
- [X] T048 [US4] Mutation checks: deadline compared inclusively the wrong way; absence made window-bound; Manager allowed to edit another's entry; conflict check ignoring weekday; the window rule judged on the first date only. Gates, both reviews, log; commit `feat(api): availability entries and submission windows (003 Group G)`

## Phase 10: US8 and US6, shift types and the manual roster (P2, P1), Groups H and I, P10

### Group H: shift types (US8)

**Goal**: FR-110 to FR-112. **Independent test**: US8 independent test in spec.md.

- [ ] T049 [P] [US8] Failing integration tests in `tests/integration/test_shift_types.py`: a new organisation lists morning, afternoon and night; Manager and Admin add, edit and retire a type; Employee may list, not change (403); Client 403; a non-crossing type ending before it starts refused 422; a duplicate active name refused 409 `shift_types.duplicate_name`; `PUT /projects/{id}/shift-types` sets the complete set and refuses another organisation's type with 404; changing a type's times leaves existing shifts unchanged (FR-112, checked after Group I creates shifts, so add that case in T053)
- [ ] T050 [US8] Repository, service and router for shift types and project shift types (`src/hds/{repositories,services,routers}/shift_type*.py`), codes and locale texts; contract and `docs/hds-api.md`; mutation checks (crossing rule inverted, retire deleting, project set not scoped). Gates, reviews, log; commit `feat(api): shift types per project (003 Group H)`

### Group I: shifts and assignments with warnings (US6)

**Goal**: FR-113 to FR-120. **Independent test**: US6 independent test in spec.md.

- [ ] T051 [P] [US6] Failing integration tests in `tests/integration/test_shifts.py`: Admin creates a shift from one of its project's types on a date, with times in the site zone (R4) and status draft; a type the project does not use refused 422 `shifts.type_not_in_project`; headcount < 1 and a non-positive pay rate refused 422; Admin changes times, headcount and pay rate, and deletes a shift with its assignments; Manager create, change, delete 403 (owner decision); Manager and Admin list drafts and published with assignments, cursor-paginated (FR-120); Employee and Client 403 on `GET /shifts`
- [ ] T052 [P] [US6] Failing integration tests in `tests/integration/test_assignments.py`: Admin assigns and removes; each answer carries the warnings from `hds.scheduling.validate` for the cases of US6 scenarios 2 and 3 and every code in contracts/part2-api.md, and nothing is refused for a warning (FR-116, SC-103); warnings look across projects and drafts (FR-117); an inactive employee refused 409 `assignments.employee_inactive`; the same employee twice refused 409 `assignments.duplicate`; Manager 403; after an assigned employee is deactivated, listing and changing that shift warns `warnings.employee_inactive`
- [ ] T052a [P] [US6] Failing integration tests in `tests/integration/test_assignment_idempotency.py` (FR-115a, R8): a repeat with the same `Idempotency-Key` and body returns the first status and body and writes nothing; the same key with another body 422 `idempotency.key_reused`; the same key from another user is a separate key; a key longer than 255 characters 422; an expired key (over 24 hours) is treated as new; the purge script removes expired keys
- [ ] T053 [US6] Failing test in `tests/integration/test_shift_types.py` that editing a type's times leaves existing shifts unchanged (FR-112)
- [ ] T054 [P] [US6] Failing concurrency test `tests/concurrency/test_assignment_duplicate.py`: two identical assignments at once leave one, the other 409; two requests with one `Idempotency-Key` at once write once and both answer the same
- [ ] T055 [US6] Repositories `src/hds/repositories/{shift,assignment}.py` (the bounded read of R3, indexed by employee and start); service `src/hds/services/roster.py` (loads the R3 window, builds the pure inputs, calls `validate`, returns record plus warnings; pay rate Admin only, Decimal); router `src/hds/routers/shifts.py`; warning codes with `en` and `he` text
- [ ] T055a [US6] `Idempotency-Key` on `POST /shifts/{id}/assignments`: model and repository `src/hds/{models,repositories}/idempotency_key.py`, a service helper that reserves the key (insert on the primary key, so a concurrent repeat waits), runs the write, and stores the answer in the same transaction; the purge script `src/hds/scripts/purge_expired_records.py` also removes keys older than 24 hours; error code with `en` and `he` text
- [ ] T056 [US6] Contract (`{shift, warnings}`, `{assignment, warnings}`, `Warning`) and `docs/hds-api.md` Shifts and Assignments rows; drift check
- [ ] T057 [US6] Mutation checks: warnings computed from the current project only; drafts left out; a warning turned into a refusal; Manager allowed to create; pay rate accepted from a Manager; the stored answer not returned on a repeat; the body hash ignored; keys not scoped per user. Gates, both reviews, log; commit `feat(api): manual shifts and assignments with rule warnings (003 Group I)`

## Phase 11: US7, publishing and visibility (P1), Group J, P11

**Goal**: FR-121 to FR-125, SC-104. **Independent test**: US7 independent test in spec.md.

- [ ] T058 [P] [US7] Failing integration tests in `tests/integration/test_publish.py`: Manager and Admin publish a project's drafts for a date range (answers the count); Employee and Client 403; published shifts stay published when an Admin changes them and the change shows at once
- [ ] T059 [P] [US7] Failing integration tests in `tests/integration/test_my_shifts.py`: `GET /me/shifts` returns only published shifts the caller is assigned to, with date, times (UTC), project, site and the site's IANA time zone; no draft; the response schema has no field naming another employee, an assignment list or a headcount (checked on the generated schema and on the body)
- [ ] T060 [P] [US7] Failing sweep `tests/integration/test_roster_visibility_sweep.py`, in the style of `test_cross_organisation_sweep.py`: every Part 2 read, for every role and a second organisation, never returns another employee's entries or assignments to an Employee, any draft to an Employee, or anything to a Client (SC-104), and lists every Part 2 operation so a new one fails until it is added
- [ ] T061 [US7] Publish operation in `src/hds/services/roster.py` and `src/hds/routers/shifts.py`; `src/hds/routers/me_shifts.py` with its own schema (R7); contract and `docs/hds-api.md`; drift check
- [ ] T062 [US7] Mutation checks: drafts included in `/me/shifts`; co-workers' shifts included; publish allowed to an Employee; another organisation's project publishable. Gates, both reviews, log; commit `feat(api): publish shifts and show employees their own (003 Group J)`

## Phase 12: client, employee screens first (US4, US7), Group K, P12

**Goal**: the employee side of US4 and US7 in `apps/client`. **Independent test**: on the web build, an
employee signs in, adds an availability entry inside an open window, and sees their published shifts,
in English, with the layout mirrored when RTL is forced.

- [ ] T063 Install Node.js 22 LTS and pnpm (R10), or record in the log that the client gates run in CI only until they are installed; add the client jobs to `.github/workflows/ci.yml`: ESLint, `tsc --noEmit`, Jest with coverage, Playwright web with the RTL smoke test, and the generated-client drift check
- [ ] T064 Scaffold the Expo app in `apps/client` (Expo Router, TypeScript strict, React Native Web), keeping `src/locales/en.json`, `src/locales/he.json` and `tests/e2e/`; pin exact versions; ESLint with a literal-string rule for JSX (Constitution VIII)
- [ ] T064a Write `docs/adr/010-client-stack.md`: Expo Router with React Native Web, `openapi-typescript` and `openapi-fetch` for the generated client, `i18next`, and tokens in `expo-secure-store` on phones and in memory only on the web (R9), with the alternatives rejected; list it in `docs/README.md`
- [ ] T065 Generate `packages/api-client` from the OpenAPI document with `openapi-typescript`, with `openapi-fetch` as the typed client (R9); a `generate` script and a check that fails when the committed copy differs (Constitution V)
- [ ] T066 [P] i18n in `apps/client/src/i18n/` with `i18next` and `react-i18next`, English strings in `src/locales/en.json`; RTL through `I18nManager` and start/end styles only; a failing Jest test that a screen renders with no literal strings and that forced RTL flips its row direction
- [ ] T067 [US4] Failing Jest tests, then sign-in in `apps/client/app/sign-in.tsx` and `src/api/session.ts`: tokens in `expo-secure-store` on phones, in memory only on the web (R9); refresh on 401 once; the API's error codes shown through translation keys
- [ ] T068 [US4] Failing Jest tests, then "My availability" in `apps/client/app/(employee)/availability.tsx`: shows the open window and deadline, lists the employee's entries, adds, edits and removes blocked, open, close and absence entries, shows the API's refusal reasons (window closed, conflict) translated, and stays usable read-only after the deadline
- [ ] T069 [US7] Failing Jest tests, then "My shifts" in `apps/client/app/(employee)/shifts.tsx`: lists the caller's published shifts from `GET /me/shifts`, times shown in the site's zone, crossing midnight and DST correct, an empty state when none
- [ ] T070 [US4] Playwright on the web build in `apps/client/tests/e2e/test_us_availability.spec.ts` and `test_us_my_shifts.spec.ts` against a seeded API, plus the RTL smoke test; enable the skipped sign-in tests in `tests/e2e/test_us1_signin.spec.ts` that the new screens make runnable, and leave the rest skipped with their reason
- [ ] T070a Playwright smoke specs for the staff journeys in `apps/client/tests/e2e/test_us5_windows.spec.ts`, `test_us6_roster.spec.ts` and `test_us8_shift_types.spec.ts`, written against the planned screens and marked skipped "pending the staff screens", as 001 did for its journeys (Constitution IV: one per journey)
- [ ] T071 Update the README (client setup, scripts) and `docs/hds-api.md` where touched; gates (API and client), both reviews, log; commit `feat(client): employee availability and shifts screens (003 Group K)`

## Phase 13: Checkpoint P13

- [ ] T072 The Part 1 merge gate (all API gates, full suite, concurrency three times, timing, contract, drift, migration round trips 019 ↔ 020 on a seeded database) plus the client gates; every quickstart.md Part 2 row checked
- [ ] T073 Close `p8-log.md` with the results and open items; commit `docs(spec): close 003 Part 2`. Do not push.

## Dependencies (Part 2)

- T030, then Group E (T031 to T036) and Group F (T037 to T040); Group F needs no Group E code, but run E first so the rules exist when services need them.
- Group G (US4, US5) after F. Group H (US8) after F. Group I (US6) after E, F and H. Group J (US7) after I.
- Group K (client) after G and J for its screens; T063 to T066 can start after F.
- Checkpoint after all.

## Parallel examples (Part 2)

- Group E: T031, T032 and T035 (different test files).
- Group G: T041, T043 and T044; T042 and T045.
- Group I: T051, T052 and T054.
- Group J: T058, T059 and T060.
- Group K: T066 alongside T064 and T065 once the scaffold exists.

## Implementation strategy (Part 2)

The MVP of Part 2 is Groups E to J: the API for availability and the manual roster, usable by staff
through the API. Group K adds the employee app on top. Each group is committed on its own, so the
branch is in a good state after any of them.
