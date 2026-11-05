# Tasks: Pay rates, pay to date and reports

**Input**: `specs/005-pay-reports/`: spec.md (US1 to US6, FR-001 to FR-033, SC-001 to SC-006), plan.md
(P1 to P7), research.md (R1 to R12), data-model.md, contracts/api.md, quickstart.md.

**Tests**: required (Constitution IV). Every rule gets a test that is run and seen failing for the right
reason before the code exists. Each group ends with mutation checks, the gates (ruff, format,
`mypy --strict src/ tests/`, interrogate, the import graph, `alembic check`), /hds-review and
/code-review high, the log (`p1-log.md`) and a commit. Tests run on the private database `hds_p3_test`.

**Organization**: one group per plan phase; each group ends green and is committed.

## Format: `[ID] [P?] [Story] Description`

## Phase 1: Foundational, P1, Group A: the pure pay package (blocks US1, US3, US5)

- [X] T001 [P] Failing purity test `apps/api/tests/unit/payroll/test_purity.py`: every module of `hds.payroll` imports only the standard library and nothing from `hds`; `import hds.payroll` in a bare interpreter loads no third-party module (research R2)
- [X] T002 [P] Failing example tests `apps/api/tests/unit/payroll/test_price.py`, rate 50 and the defaults (day 480 minutes, night 420, tier 120, 125%/150%, rest Friday 18:00 to Saturday 22:00 at 150%, increases 25 and 50 points), site `Asia/Jerusalem`: 10-hour weekday day shift 525.00 with parts 8 h at 100 and 2 h at 125; 8-hour Saturday 06:00 to 14:00 600.00; weekday night 22:00 to 06:00 412.50 (night length 7); Friday 22:00 to Saturday 06:00 612.50 (7 h at 150, 1 h at 175); Friday 16:00 to 24:00 split at 18:00; Saturday 18:00 to Sunday 02:00 split at 22:00; a 12-hour day shift (hours 9 and 10 at 125, 11 and 12 at 150); a 200% special day and its overtime at 225/250; a special day inside the rest window takes the higher; a project setting overriding a default; a DST night of 7 and of 9 real hours; a shift's own rate over the default; no rate gives `rate_missing` and no pay; half-up rounding to 0.01 once per line (FR-009 to FR-015, SC-003)
- [X] T003 [P] Failing property tests `apps/api/tests/property/test_price_properties.py`, at least 1,000 examples under the CI profile: random shifts (any start, 1 minute to 24 hours, any of several zones including DST changes), settings, rest windows and special days; the parts' hours add up to the shift's real hours; every part's rate is at least 100; overtime hours equal `max(0, hours − normal)`; pay equals Σ hours × base × rate/100 rounded half up to 0.01; with no rest window overlap, no special day and no overtime every part is at 100 (SC-002)
- [X] T004 `apps/api/src/hds/payroll/{__init__.py,settings.py,price.py}`: `PaySettings` (the twelve settings of data-model, with `RestWindow` of start and end weekday and time), `SpecialDay(day, rate_pct)`, `PayPart(rate_pct, microseconds)` with `hours`, `PayLine(microseconds, parts, pay | None, night)` with `hours` and `rate_missing`, and `price(starts_at, ends_at, timezone, base_rate, settings, special_days) -> PayLine` cutting the shift at the rest-window, special-day and overtime instants (research R3); night is at least 120 minutes between 22:00 and 06:00 local; module docstrings stating the layer and that it imports nothing from the app
- [X] T005 Mutation checks (for example: overtime counted from the start; night length ignored; rest and special day added instead of the higher; overtime on a rest day at 125 instead of 175; rounding per part; seconds lost at a DST change). Gates, both reviews, log; commit `feat(payroll): price a shift with overtime, rest and special-day premiums (005 Group A)`

## Phase 2: Foundational, P2, Group B: migration 022 and the models

- [ ] T006 Failing tests `apps/api/tests/integration/test_migration_022.py`: upgrade from 021 and downgrade back on a seeded database; organisations get the defaults (480, 420, 120, 125.00, 150.00, weekday 5 18:00 to weekday 6 22:00, 150.00, 25.00, 50.00); existing projects get `pay_enabled` true, `managers_see_pay` false and every override `NULL`; the checks refuse minutes outside 1 to 1440, percentages outside 100 to 300, increases not above 0 or above 200, weekdays outside 0 to 6, equal rest start and end; `special_days` refuses a rate outside 100 to 300 and a second organisation day or a second project day on one date; `project_managers` refuses a duplicate pair; `idx_shifts_org_published_start` exists
- [ ] T007 Migration `apps/api/alembic/versions/022_pay_reports.py` (states what it changes and why) and the models: the columns on `src/hds/models/organisation.py` and `src/hds/models/project.py`, `src/hds/models/special_day.py`, `src/hds/models/project_manager.py`, the index on `src/hds/models/shift.py` (data-model.md); `alembic check` clean. Gates, log; commit `feat(db): pay settings, special days and managers on projects (005 Group B)`

## Phase 3: US2 and US6, rates and settings (P1, P2), Group C, P3

**Goal**: FR-001 to FR-008a, FR-021, FR-022. **Independent test**: US2 and US6 independent tests in spec.md.

- [ ] T008 [P] [US2] Failing tests `apps/api/tests/integration/test_rate_admin_only.py`: an Admin sets, changes and clears an employee's default rate; a Manager sending `default_hourly_rate` (a value or `null`) on `POST /employees` or `PATCH /employees/{id}` gets 403 `users.rate_admin_only` and nothing changes; a Manager's change of other fields still works; an employee still reads their own rate. Change the 001 tests that had Managers set a rate to use an Admin, and log each change with its reason (research R7). FR-002 (the shift pay rate, Admin only) is already covered by the 003 tests in `apps/api/tests/integration/test_shifts.py` and the role matrix; cite them in the log
- [ ] T009 [P] [US2] Failing tests `apps/api/tests/integration/test_pay_settings.py`: Managers read and Admins change `/organisation/pay-settings`; out-of-range values 422 `pay.settings_invalid`, an empty or over-7-day rest window 422 `pay.rest_window_invalid`, unknown fields 422; `/projects/{id}/pay-settings` reads each setting with its `source`; an Admin sets an override and returns it to the default with `null`; `POST /projects` with `pay_settings` from an Admin is stored and from a Manager is 403 `projects.pay_settings_admin_only`; a default change reaches an active project that follows it and not one with its own value; ending a project fills its overrides from the defaults and copies the organisation days it does not override, so a later default or day change does not reach it; any change to an ended project 409 `pay.project_ended`; Employee and Client 403; another organisation's project 404
- [ ] T010 [P] [US2] Failing tests `apps/api/tests/integration/test_special_days.py`: Admin creates, changes and deletes organisation days (rate 150.00 by default, 100 to 300); a second day on one date 409 `special_days.duplicate`; `POST /projects/{id}/special-days` adds a project day and overrides an organisation day's rate; `GET /projects/{id}/special-days` merges them with `source`; a project day of an ended project 409 `pay.project_ended`; Managers read and are refused changes (403); Employee and Client 403; another organisation's day or project 404
- [ ] T011 [P] [US6] Failing tests `apps/api/tests/integration/test_project_managers.py`: an Admin replaces a project's managers with `PUT`; Managers and Admins read them; an Employee, a Client, an inactive user or another organisation's user 422 `project_managers.not_staff`; a Manager, Employee or Client trying to change them 403; another organisation's project 404
- [ ] T012 [P] [US2] Failing concurrency tests `apps/api/tests/concurrency/test_pay_settings_races.py`: ending a project while an Admin changes its overrides or a default leaves the project either frozen with the old values and the change refused, or changed and then frozen with the new ones, never a mix; ending a project while an Admin adds or changes an organisation or project special day leaves the ended project with either the day before or after the change, never without it; two `PUT /projects/{id}/managers` at once leave exactly one of the two sets
- [ ] T012a [P] [US2] Failing service unit tests with fake repositories (Constitution IV) `apps/api/tests/unit/test_pay_settings_service.py`, `test_special_day_service.py`, `test_project_manager_service.py`, and the Admin-only rate in `test_employee_manager.py`: role refusals, validation codes, effective settings from overrides and defaults, the freeze copying defaults and organisation days, the managers set rule
- [ ] T013 [US2] Admin-only rate in `apps/api/src/hds/services/employee.py` and the code `users.rate_admin_only` with `en` and `he` text
- [ ] T014 [US2] Repository and service `pay_settings` (organisation defaults, project switches and overrides, effective settings), the freeze on ending in `apps/api/src/hds/services/project.py` under the project's row lock, `pay_settings` on `POST /projects`, router `apps/api/src/hds/routers/pay_settings.py`; types `Percent` and `Hours` in `src/hds/core/types.py`; error codes with `en` and `he` text
- [ ] T015 [US2] Repository, service and router `special_day` (`src/hds/repositories/special_day.py`, `src/hds/services/special_day.py`, `src/hds/routers/special_days.py`)
- [ ] T016 [US6] Repository, service and router `project_manager` (`src/hds/repositories/project_manager.py`, `src/hds/services/project_manager.py`, `src/hds/routers/project_managers.py`), the set replaced under the project's row lock; the role matrix (`tests/integration/test_role_matrix.py`), the cross-organisation sweep and `tests/contract/unprovable_operations.json` extended for every operation of T008 to T011
- [ ] T017 [US2] Contract (`specs/001-foundation-auth-org/contracts/openapi.yaml`), `docs/hds-api.md`, drift check; mutation checks; gates, both reviews, log; commit `feat(api): admin-only rates, pay settings, special days and managers on projects (005 Group C)`

## Phase 4: US1 and US3, pay to date (P1), Group D, P4

**Goal**: FR-016 to FR-020, FR-029, and US3 end to end. **Independent test**: US1 and US3 independent tests in spec.md.

- [ ] T018 [P] [US1] Failing integration tests `apps/api/tests/integration/test_my_pay.py`: an employee with ended, current, future and draft shifts reads `/me/pay?month=`; only ended published shifts appear, each with date, project, start, end, hours, base rate, parts and pay; the total is the sum of the lines; a shift's own rate wins; a pay-off project's lines have `pay_enabled` false and no pay; no rate gives `rate_missing` and counts in `shifts_missing_rate`; the month defaults to the current month in the organisation's zone; a shift belongs to the month of its local start at its site; `month=2027-13` 422 `pay.month_invalid`; Managers and Admins see only their own; a Client 403
- [ ] T019 [P] [US3] Failing integration tests `apps/api/tests/integration/test_my_pay_premiums.py`: through the API, the spec's worked examples (525.00, 600.00, 412.50, 612.50), a project override, a project special day overriding an organisation day, and a default change reaching an active project's earlier shifts but not an ended project's
- [ ] T019a [P] [US1] Failing service unit tests with fake repositories `apps/api/tests/unit/test_pay_service.py`: the month window and default month, ended-only filtering, the pay-off and missing-rate lines, totals equal to the sum of lines
- [ ] T020 [US1] Read queries for pay to date in `apps/api/src/hds/repositories/pay_reports.py` (read-only; published, `ends_at <= now`, UTC window with a one-day margin, research R8), service `apps/api/src/hds/services/pay.py` building `hds.payroll` inputs from the effective settings and days, router `apps/api/src/hds/routers/my_pay.py` with `Amount` in `src/hds/core/types.py`; error codes with `en` and `he` text; matrix and sweeps extended
- [ ] T021 [US1] Contract, `docs/hds-api.md` (`/me/pay` replaces `/me/pay-forecast`), drift check; mutation checks; gates, both reviews, log; commit `feat(api): pay to date for the month (005 Group D)`

## Phase 5: US4 and US5, reports (P1, P2), Group E, P5

**Goal**: FR-023 to FR-028, FR-030 to FR-032, SC-004, SC-005. **Independent test**: US4 and US5 independent tests in spec.md.

- [ ] T022 [P] [US4] Failing integration tests `apps/api/tests/integration/test_work_log.py`: by project with no `from` starts at the project's start; by site per project; by group members only; `to` defaults to today; each person once per project with shifts, hours, first and last date; no pay field anywhere; deactivated and deleted employees and ended projects still appear; not exactly one scope 422 `reports.scope_invalid`; a site or group without `from` 422 `reports.start_required`; backwards or over 366 days 422 `reports.range_invalid`; Employee and Client 403; another organisation's project, site or group 404
- [ ] T023 [P] [US5] Failing integration tests `apps/api/tests/integration/test_hours_report.py`: by employee and by project with rows, group totals and an equal grand total; an Admin sees pay for every pay-on project; a Manager sees pay only for projects they are assigned to whose pay is on and open to Managers, and their pay totals cover only those; a demoted Manager sees none; missing rates counted, never zero; Employee and Client 403
- [ ] T024 [P] [US5] Failing property tests `apps/api/tests/property/test_report_totals.py`: for generated assignments, rates and settings, report totals equal the sum of rows and hours by employee equal hours by project (SC-002)
- [ ] T025 [P] [US5] Failing sweep `apps/api/tests/integration/test_pay_visibility_sweep.py`: every report and pay view read as every role, with every combination of the two project switches and assignment, never returns a `pay` figure the reader may not see (SC-004)
- [ ] T026 [P] [US5] Failing performance test `apps/api/tests/performance/test_report_performance.py`: 100 employees, 20 projects, a year of published shifts; each report under 3 seconds (SC-005)
- [ ] T026a [P] [US4] Failing service unit tests with fake repositories `apps/api/tests/unit/test_reports_service.py`: scope and range validation codes, the default `from` and `to`, grouping and totals, the pay-visibility rule for every combination of role, assignment and the two switches
- [ ] T027 [US4] Work-log and hours queries in `apps/api/src/hds/repositories/pay_reports.py` (read-only, using `idx_shifts_org_published_start`), service `apps/api/src/hds/services/reports.py` (read-only methods; the pay-visibility rule of research R6 and R10), router `apps/api/src/hds/routers/reports.py`; error codes with `en` and `he` text; matrix, sweeps and `unprovable_operations.json` extended
- [ ] T028 [US5] Contract, `docs/hds-api.md` (Reports), drift check; mutation checks; gates, both reviews, log; commit `feat(api): work-log and hours reports (005 Group E)`

## Phase 6: Screens, P6, Group F

- [ ] T029 Regenerate `packages/api-client` (`pnpm api-client:generate`) so the screens use the generated client only
- [ ] T030 [P] [US1] Failing Jest tests `apps/client/src/screens/__tests__/MyPayScreen.test.tsx`: lines, totals, a hidden pay, `rate_missing`, the month switch, Hebrew and English
- [ ] T031 [P] [US4] Failing Jest tests `apps/client/src/screens/__tests__/WorkLogScreen.test.tsx` and `HoursReportScreen.test.tsx` [US5]: filters, grouping, pay columns only when present
- [ ] T032 [P] Failing Playwright journeys `apps/client/tests/e2e/test_us_my_pay.spec.ts` (pay in at most three steps, SC-001), `test_us_work_log.spec.ts`, `test_us_hours_report.spec.ts`; extend `test_rtl.spec.ts` to the new screens; add `test_us_pay_settings.spec.ts` [US2] and `test_us_project_managers.spec.ts` [US6], skipped "pending the admin screens" as 004 did for journeys without screens (Constitution IV)
- [ ] T033 Screens `apps/client/src/screens/{MyPayScreen,WorkLogScreen,HoursReportScreen}.tsx`, routes `apps/client/app/(employee)/pay.tsx` and `apps/client/app/(employee)/reports/{work-log,hours}.tsx`, `src/components/EmployeeNav.tsx` (My pay for everyone; Reports for Managers and Admins), strings in `src/locales/{en,he}.json`; client gates (lint, typecheck, Jest, `api-client:check`, e2e) green; commit `feat(client): my pay and report screens (005 Group F)`

## Phase 7: Polish and checkpoint, P7, Group G

- [ ] T034 [P] ADR `docs/adr/012-pay-calculation.md` (the pure package, cutting at instants, defaults and freezing, hiding pay), listed in `docs/README.md`; the FIN-3 note in `docs/hds-spec.md` (pay to date, premiums, Proposed); `apps/api/README.md`
- [ ] T035 The merge gate (all API gates, full suite, concurrency three times, contract, drift, migration round trips 021 ↔ 022 on a seeded database, the property tests at the CI profile, the performance test) plus the client gates; every quickstart row checked; close `p1-log.md`; commit `docs(spec): close 005`. Do not push.

## Dependencies

- Group A before D and E. Group B before C. Group C before D. Group D before E (shared repository and
  pricing). Group F after D and E. Group G last.
- US2 and US6 (Group C) are independent of the pure package; US1 and US3 need A and C; US4 needs B and
  C; US5 needs A, C and D.

## Parallel examples

- Group A: T001, T002 and T003 (different test files).
- Group C: T008, T009, T010, T011 and T012.
- Group D: T018 and T019. Group E: T022 to T026. Group F: T030, T031 and T032.

## Implementation strategy

The pricing rules (A) carry most of the risk and are pure, so they come first with worked examples and
property tests. Settings (C) make the inputs real, pay to date (D) is the MVP an employee sees, and the
reports (E) reuse the same queries and pricing. Screens (F) come once the contract is stable.
