# 005 decision log

Owner instruction (2026-11-05): "Go with recommended for all, continue with the harness until it's time
to publish the PR". The standing rules apply: tests on the private database `hds_p3_test`, nothing else
using it; no push; no edit to committed migrations; no test deleted or weakened, and every test change
explained here; each group failing tests first, then code, mutation checks, the gates, /hds-review and
/code-review high, the log and a commit; stop on an unexplained failure, a change to a spec.md
requirement, a security regression, an unplanned migration, a push or anything destructive.

## Stages

- **Specify**: three owner answers (default rate Admin only, with pay switched per project; Managers see
  pay only on projects opened to them and assigned to them; reports count ended shifts), then the owner's
  direction on Israeli premiums and four follow-up answers (pay to date only; two switches per project;
  only assigned Managers; overtime per shift). The additions are marked Proposed in spec.md.
- **Clarify**: three questions (the weekly rest window Friday 18:00 to Saturday 22:00; organisation
  defaults reaching projects that follow them; special days per organisation with project overrides).
- **Plan**: research R1 to R12. R1: the prompt's `NUMERIC(12,2)` applies to money amounts, which are
  computed; the two existing rate columns stay `NUMERIC(12,4)` (Complexity Tracking).
- **Tasks**: T001 to T035 in groups A to G.
- **Analyze**, all recommendations applied:
  - C1: service unit tests with fake repositories added (T012a, T019a, T026a), as Constitution IV asks.
  - C2: skipped journeys for the admin-only stories US2 and US6 (T032).
  - I1: FR-003 now separates premium rates (100 to 300%) from overtime increases (above 0, at most 200
    points), matching data-model.md.
  - U1, U2: edge cases for reopening an ended project and deleting an organisation day that a project
    overrides.
  - C3: T012 also races a special-day change against ending a project.
  - A1: "today" is in the organisation's time zone. F1: FR-020 reworded.
  - C4: FR-002 is covered by the 003 tests in `tests/integration/test_shifts.py` and the role matrix.

## Group A: the pure pay package (T001 to T005)

- **Built:** `hds.payroll` (`settings.py`, `price.py`): `price()` cuts a shift at the rest-window
  ends, local midnights and the two overtime boundaries, gives each part one rate, and rounds the
  pay half up once. Standard library only; a purity test like scheduling's (`rglob`, so subpackages
  are checked too).
- **Tests:** 25 worked examples and edge cases in `tests/unit/payroll/test_price.py` (the spec's
  525.00, 600.00, 412.50 and 612.50, splits into and out of the rest, 200% days, DST nights of 7 and
  9 hours, rounding once); one Hypothesis property that compares `price` with an oracle walking the
  shift a minute at a time and following the site's clock (1,000 examples in about 2.5 seconds).
- **Mutation checks:** 16 mutants, all caught. The first rounding example passed one mutant by
  coincidence (per-part and per-line rounding gave the same 0.06); it was replaced before the commit
  with a case where they differ (0.00 against 0.01).
- **/code-review high**, 8 findings, all acted on:
  1. Rest-window ends at a time the clocks skip were read with `fold=0`, an hour late. Wall times now
     become "the first moment the clock shows that time or later" (`_first_showing`), for rest
     windows, nights and midnights alike; a unit test covers 02:30 on 2027-03-26.
  2. Part hours summed in 28 digits can differ from the line's hours. Lines now hold exact
     microseconds; `hours` is derived, and shown hours are rounded by whoever shows them, with totals
     summed from what is shown (documented on `PayLine`).
  3. The property test avoided 00:00 to 04:00 for rest-window ends, hiding finding 1. It now draws any
     minute of the week, adds zones whose clocks change at midnight (Havana, Santiago), and its
     oracle follows the clock forward instead of reading wall positions. **Test change, stronger.**
  4. Two special days on one date silently collapsed; `price` now refuses them, and callers merge a
     project day over the organisation's first (FR-008a).
  5. An unknown zone raised `KeyError`; it is now the documented `ValueError`.
  6. tasks.md T004 named a different shape; T004 now names the built one (`microseconds`, `night`).
  7. The purity test globbed one level; now `rglob`.
  8. `rate_missing` and `hours` are properties, `itertools.pairwise` replaces `zip`, and one helper
     turns wall times into instants.
- **After the commit:** a wider run replayed an example the mutation run had saved: a rest window of
  7 days less 1 minute (Sunday 23:47 to Sunday 23:46). `price` was right; the test's oracle was
  wrong, because one 15-minute step of its clock crossed both ends and it applied them in a fixed
  order. The oracle now applies them in the order the clock reaches them. **Test fix**, no code
  change; checked against every start minute of the week for that case.

## Group B: migration 022 and the models (T006, T007)

- **Built:** migration `022_pay_reports` and the models: the premium defaults on `organisations`,
  the switches and nullable overrides on `projects` (the bounds of both from one helper,
  `models/pay_columns.py`), `special_days`, `project_managers`, and the partial index
  `idx_shifts_org_published_start`. `alembic check` is clean.
- **Tests:** `tests/integration/test_migration_022.py` (26: defaults, bounds on both tables, the
  partial unique days, the managers' key, indexes, downgrade and upgrade again, offline SQL) and
  `tests/unit/test_pay_columns.py` (3).
- **Mutation checks:** 11, all caught.
- **/code-review high**, 9 findings:
  1. Projects already ended at the upgrade kept following the defaults, against FR-007. The upgrade
     now copies the defaults into them; a test covers it. **Fixed.**
  2. Nothing ties a special day's or a project manager's organisation to its project's. **Declined**:
     every organisation-owned table in the schema (staffing needs, shifts, assignments) relies on the
     service and repository scoping, which the cross-organisation sweep tests; a composite key would
     need a new unique constraint on `projects` for this one table.
  3. `idx_special_days_project` duplicated the partial unique index. **Removed.**
  4. `idx_shifts_org_published_start` overlaps `idx_shifts_org_start`. **Kept**: the plan prompt asks
     for indexed report queries and research R9 chose it; the performance test (T026) measures it.
     It now has the same `sqlite_where` as the other partial indexes.
  5. An unused `SETTINGS` tuple. **Removed.**
  6. and 7. The defaults and the bounds were written three times (pay package, models, migration),
     and `alembic check` does not compare check constraints. **Fixed by tests**: the models' defaults
     must equal `DEFAULT_SETTINGS`, and the migration's frozen bounds must equal the models'. Models
     do not import the pay package, which is for services only.
  8. The valid-override test did not read the values back. **Fixed.**
  9. A stale "depends on base only" docstring. **Fixed.**

## Groups C, D and E: rates and settings, pay to date, reports (T008 to T028)

One commit for the three groups: they share the composition root (`hds/wiring.py`), `hds/main.py`,
the contract, the role matrix and the sweeps, and D's pricing is what E reuses, so splitting the
working tree into three commits would have meant committing files that do not build alone. Each
group still had its own failing tests first, mutation checks and review.

- **Built:**
  - C: the Admin-only default rate (`guard_rate_change`); organisation premium defaults and each
    project's switches and overrides, frozen when a project ends (`PaySettingsService`); special
    days for the organisation and per project; managers on projects. `routers/projects.py` now gets
    its service from `hds.wiring`: the hook refuses a router that imports repositories, and the
    file had to change for `pay_settings` on create.
  - D: `GET /me/pay` (`PayService`, `repositories/pay_reports.py`), priced by `hds.payroll`.
  - E: `GET /reports/work-log` and `GET /reports/hours` (`ReportsService`, read-only repositories
    with plain-column queries); the pay-visibility rule.
- **Decisions:**
  - On `POST /employees`, a Manager's `default_hourly_rate: null` sets nothing and is allowed; on a
    change it would clear the rate and is refused (T008, the contract and docs/hds-api.md say so).
  - Times in the pay settings are written `HH:MM:SS`, as the shift types already are.
  - FR-030's 366-day limit applies to the work-log too. A project's default `from` is its start
    date, or 366 days before `to` when the project is older than that, so the request with only the
    project (SC-006) always answers; the response's `from` says where it began. **For the owner**:
    a work-log over a project's whole life, past a year, needs two requests today.
  - A hidden project's lines count no missing rates, so a Manager learns nothing about its rates.
- **Test changes, each for FR-001 (only Admins set a default rate):**
  - `tests/unit/test_employee_manager.py::test_create_employee_non_positive_hourly_rate_rejected`
    now uses an Admin: a Manager is refused before the rate is validated.
  - `tests/integration/test_patch_null_clears.py`: test employees are created by an Admin, and the
    rate is cleared by an Admin; the Manager still clears the phone.
  - `tests/unit/test_project_service.py`, `test_project_client_scope.py`: `ProjectService` now
    requires its pay settings collaborator (review finding below); the tests pass a mock.
  - `tests/roster_support.add_project` takes an optional `start_date` (default unchanged).
  - `tests/integration/conftest.py`: the other organisation has a special day, for the sweep.
- **Bugs found by the new tests:**
  - Ending a project reused a project row the session had loaded earlier, so it could overwrite an
    override committed while it waited for the lock (race test). Fixed with `lock_and_read`
    (`populate_existing` under the lock).
  - Copying two or more organisation days onto an ending project failed: SQLAlchemy evaluated the
    model's Python id default once for the whole `INSERT ... SELECT`. Fixed with
    `include_defaults=False`; the freeze test now copies two days.
  - Schemathesis: year `0000` passed `/me/pay`'s pattern but not the service, and the query
    validator has no look-ahead; the pattern now spells out years 0001 to 9999.
- **Mutation checks:** C 16, D 7, E 11; all caught after adding tests for three survivors (an ended
  project seeing a day added after it ended, and on the pricing path; a future shift in a report
  range; missing rates on hidden lines) and removing a duplicate start-date check that made one
  mutant equivalent.
- **/code-review high on C and D**, 10 findings:
  1. A PATCH with a rename and `status: ended` locked the project before the organisation, the
     reverse of every other path: a deadlock risk. The status now goes first; a race test covers it.
  2. Months and ranges at the calendar's ends overflowed to 500. Windows are clamped (`widen`);
     tests for `0001-01` and `9999-12`.
  3. A Manager's `null` rate on a create: kept as allowed, and the contract and docs were
     corrected to say so (decision above).
  4. `ProjectService` silently skipped the pay rules without its collaborator; it is now required.
  5. `Percent` and `Points` accepted `"1.25e2"`; they now match their published patterns. Tests added.
  6. A special day read before its lock could be stale; it is read again under the lock.
  7. T009 still described a per-setting `source`; it now describes `own`.
  8. A duplicated body conversion became `project_pay_changes`.
  9. A second `STAFF_ROLES` with another meaning became `ASSIGNABLE_ROLES`.
  10. Extra round trips: the per-project special-day list is now built once per report. Re-reads
      after `change_project` are **kept**: one row, an Admin action, and the read path stays shared.
- **Performance (SC-005):** a year of 26,000 assignments, 100 employees, 20 projects: the hours
  report about 1.15 s either way, a project's work-log about 40 ms.
- **/code-review high on C, D and E together**, 9 findings:
  1. A malformed month or setting is refused by the published schema with
     `validation.request_invalid`, so the service's `pay.month_invalid` and `pay.settings_invalid`
     are guards; docs/hds-api.md and the contract now say so. (The same holds for 004's limits.)
  2. A project older than a year could not be read with the default range: the default start is
     now clamped to the limit (decision above). Unit test added.
  3. A project whose pay may be seen but whose lines all lack a rate showed no pay key, which looks
     like hidden pay. Totals now carry pay wherever any line's pay may be seen, from zero, with the
     missing count. **Test change**: `test_a_project_without_pay_shows_hours_only_and_missing_rates_
     are_counted` now expects `"0.00"` on Omer's Alpha row instead of no pay key, for this reason.
  4. Shift counts counted people, not shifts. Totals now count distinct shifts (hours stay
     person-hours); the property test and a unit test check a shift of two people counts once.
  5. The safe order of ending and editing a project lived in the router; it is now
     `ProjectService.change_project`.
  6. Three groups in one commit: acknowledged above.
  7. `ReportScopeRepository` repeated existing lookups: removed for `ProjectRepository`,
     `SiteRepository` and a new `GroupRepository.member_ids`. The work-log's own hours helper
     **kept**: it reads one column pair and needs no pricing.
  8. The rest-window message now says weekday or time.
  9. The untyped `**narrow` became a typed `Scope`.

## Group F: the screens (T029 to T033)

- **Built:** My pay (month switch, a line per worked shift with its hours at each rate, the total,
  pay-off and missing-rate lines said in words), the work-log (a project, a site or a group; a site
  or group asks for a start date) and the hours report (by employee or project, shift counts, pay
  only where the answer has it, the missing-rate count). The bar shows My pay to everyone and the
  reports to Managers and Admins (from the signed-in user's role; the API refuses others anyway).
  `useLoad` takes inputs to reload on. English and Hebrew strings.
- **Where the tests are:** tasks.md named `src/screens/__tests__`; the client's Jest tests live in
  `apps/client/tests/unit`, so they went there (`pay.test.tsx`, `reports.test.tsx`, `nav.test.tsx`).
- **Journeys:** `test_us_my_pay` (pay in two steps from sign-in, and the Hebrew layout),
  `test_us_work_log`, `test_us_hours_report`; `test_us_pay_settings` and `test_us_project_managers`
  written and skipped pending the admin screens, as 004 did. e2e: 13 passed, 28 skipped.
- **Mutation checks:** 10 on the client, all caught (one after making its test wait long enough for
  the stale answer to arrive).
- **/code-review high**, 10 findings, all fixed:
  1. and 2. A late answer to an earlier month or grouping could replace the newer one, and old
     data stayed on screen while the new one loaded. `useLoad` now drops all but the latest answer
     and clears data when its inputs change. Tests with held-back answers cover both.
  3. and 4. The hours report hid the missing-rate count and the per-group shift counts; both shown.
  5. The e2e month check used the runner's zone; it now compares the shift's date at the site.
  6. A failed project list was silent; it now shows the error and a retry.
  7. The work-log only offered projects; it now offers sites and groups too (FR-023).
  8. A dead `undefined` check removed.
  9. The month switch is one component (`MonthSwitch`) with its own strings.
  10. The Hebrew singular forms now read "one shift", not "shift 1".
