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

- [ ] T009 [US2] Failing tests in new `tests/integration/test_patch_unknown_fields.py`, parametrised over the four PATCH operations: an unknown field alone and with a valid field gives 422 `validation.request_invalid` and the stored row is unchanged; `/me` with an unknown field still gives 403 `profile.field_not_allowed` (`tests/integration/test_me.py:82` stays as is)
- [ ] T010 [US2] Add a shared request base with `extra="forbid"` in `src/hds/core/types.py` and use it for `UserUpdateRequest` (`src/hds/routers/employees.py`), `ClientUpdateRequest` (`src/hds/routers/clients.py`), `ProjectUpdateRequest` (`src/hds/routers/projects.py`) and `SiteUpdateRequest` (`src/hds/routers/sites.py`); `ProfileUpdate` in `src/hds/routers/me.py` keeps `extra="allow"` (research E2)
- [ ] T011 [US2] Fix any existing test that sent an unknown field to these four operations and relied on it being ignored; record each in the log with the reason
- [ ] T012 [US2] Contract: `additionalProperties: false` on the four request schemas in `specs/001-foundation-auth-org/contracts/openapi.yaml`; regenerate; drift check; `docs/hds-api.md` notes the refusal
- [ ] T013 [US2] Mutation checks: each of the four models back to the default `extra`; `ProfileUpdate` to `forbid` (must be caught by the `/me` 403 test)
- [ ] T014 [US2] Gates, both reviews, fixes, log; commit `fix(api): refuse unknown fields on change requests (003 Group B)`

## Phase 5: User Story 3 - Housekeeping that changes nothing users see (P3)

**Goal**: FR-006 to FR-010.

**Independent test**: the full suite and the drift check unchanged; the Client project path empty without a client; the migration round trip; the clean-up's plan uses the new index.

### Group C: the Client project path and the unreachable methods

- [ ] T015 [US3] Failing unit test in `tests/unit/test_project_client_scope.py`: the Client branch of `ProjectService.list_projects` reads through `ProjectRepository.list_by_client` with the status filter passed through, and never calls `list_by_org`; a repository test in `tests/integration/test_client_portal.py` (or the existing Client project test file) shows `list_by_client` honours the status filter
- [ ] T016 [US3] Add the status filter to `ProjectRepository.list_by_client` (`src/hds/repositories/project.py`) and use it in the Client branch of `ProjectService.list_projects` (`src/hds/services/project.py`); the early refusal of an account with no client stays (research E3)
- [ ] T017 [US3] For each rule pinned by the `promote_to_manager` / `promote_to_admin` unit tests in `tests/unit/test_employee_manager.py` (lines near 166, 179, 189, 225, 498), confirm a test of `edit_employee` pins the same rule; add any missing one first and see it fail under a mutation
- [ ] T018 [US3] Remove `EmployeeService.promote_to_manager` and `promote_to_admin` from `src/hds/services/employee.py` and their unit tests; log each removed test with the test that now covers its rule (research E4)
- [ ] T019 [US3] Mutation checks: the Client branch back to `list_by_org(client_id=...)` without the guard; the status filter dropped from `list_by_client`
- [ ] T020 [US3] Gates, both reviews, fixes, log; commit `refactor(api): narrow the client project read and drop unreachable promotes (003 Group C)`

### Group D: migration 019

- [ ] T021 [US3] Failing migration test `tests/integration/test_migration_019.py` on a scratch database, as in `test_migration_018.py`: after 019 both indexes exist on `expires_at` and the `users` columns are gone, users kept; after downgrade to 018 the indexes are gone and both columns are back as in 004 ("`failed_login_attempts SMALLINT NOT NULL DEFAULT 0`, `locked_until TIMESTAMPTZ NULL`") with every user at 0 and no lock; upgrade again succeeds
- [ ] T022 [P] [US3] Failing test in `tests/integration/test_expired_cleanup.py`: seed SC-003's volumes with `generate_series` (100,000 unexpired and 1,000 expired rows), `ANALYZE`, then `EXPLAIN (ANALYZE, FORMAT JSON)` the select each `delete_expired` runs (`src/hds/repositories/refresh_token.py`, `src/hds/repositories/invitation_token.py`): no sequential scan on the token table, and no more than 5,000 rows examined (SC-003)
- [ ] T023 [US3] Write `alembic/versions/019_expiry_indexes_retire_lock_columns.py`: create `idx_refresh_tokens_expires_at` and `idx_invitation_tokens_expires_at`; drop `users.failed_login_attempts` and `users.locked_until`; downgrade reverses all four (research E5, E6)
- [ ] T024 [US3] Declare the indexes in `src/hds/models/refresh_token.py` and `src/hds/models/invitation_token.py`; remove the two attributes from `src/hds/models/user.py`; `alembic check` clean
- [ ] T025 [US3] Update every test and document that names the dropped columns, including `tests/integration/test_one_address_count.py::test_a_registered_address_is_counted_in_the_throttle_table_only` (now asserts the columns do not exist) and the `LoginThrottle` docstring in `src/hds/models/login_throttle.py`; log each changed test with the reason
- [ ] T026 [US3] Mutation checks: downgrade without restoring a column; one index missing in the upgrade
- [ ] T027 [US3] Gates, migration round trip on a scratch database, both reviews, fixes, log; commit `feat(db): index token expiry and drop the retired lock columns (003 Group D)`

## Phase 6: Polish and checkpoint P7

- [ ] T028 Merge gate as at the end of 002: all gates, full suite, concurrency suite three times, timing tests, `scripts/contract_cli.sh` on a throwaway database, drift check, migration round trip 018 ↔ 019 on a seeded database; quickstart.md rows checked
- [ ] T029 Close the log with the results and anything left open; commit `docs(spec): close 003 Part 1`. Do not push.

## Dependencies

- Setup T001 first. Groups A, B, C and D are independent in code; they run in order A, B, C, D because A and B both edit `src/hds/routers/clients.py` and the contract.
- Within a group: tests, then code, then contract, then mutations, then gates and commit.
- T028 after all four groups.

## Parallel examples

- Group A: T002 and T003 (different test files).
- Group D: T021 and T022 (different test files).

## Implementation strategy

US1 first (the only new capability), then US2, then US3. Each group is committed on its own, so the branch is in a good state after any of them.
