# Implementation Plan: Pay rates, pay to date and reports

**Branch**: `005-pay-reports` | **Date**: 2026-11-05 | **Spec**: [spec.md](spec.md)

**Input**: spec.md (FR-001 to FR-033, SC-001 to SC-006) and the stored prompt
`.claude/hds/prompts/04-pay-reports/plan.md`: "Money as NUMERIC(12,2) in the database and Decimal in
Python, serialised as strings in the API. Reports as read-only service methods over repositories with
indexed queries. Add the report and forecast screens to the Expo app. Contract tests for every
role/endpoint pair (allowed and forbidden)."

## Summary

A pure package `hds.payroll` prices one employee on one shift: it cuts the shift at the weekly rest
window, special days and the overtime boundaries, gives each part its rate, and returns the hours at each
rate and the pay, rounded once to the agora. The API gains the organisation's premium defaults, each
project's pay switches and overrides (frozen when the project ends), special days for the organisation and
per project, Managers assigned to projects, the Admin-only default rate, the caller's pay to date, and two
read-only reports (work-log and hours) whose pay figures appear only to readers allowed to see them. The
Expo app gains "My pay" and the two report screens.

## Technical Context

**Language/Version**: Python 3.12 (API); TypeScript 5 with Expo (client), as 004

**Primary Dependencies**: as 004; no new library. `hds.payroll` uses the standard library only
(`decimal`, `datetime`, `zoneinfo`) (research R2)

**Storage**: PostgreSQL 16. Migration 022 adds the premium defaults to `organisations`, the switches and
overrides to `projects`, the tables `special_days` and `project_managers`, and a partial index on
published shifts. Money amounts are computed, not stored; rates keep `NUMERIC(12,4)` (research R1)

**Testing**: as 004 (pytest, Hypothesis, Schemathesis, concurrency tests on PostgreSQL, Jest, Playwright).
New: worked examples and property tests for `price`; property tests on report totals; a pay-visibility
sweep; a report performance test; three Playwright journeys

**Target Platform**: the API container and the Expo web build, as before

**Project Type**: web service and client in the existing monorepo

**Performance Goals**: SC-005: a work-log or hours report over one year for 100 employees and 20 projects
in under 3 seconds

**Constraints**:
- `Decimal` everywhere for money, rounded half up once per line; amounts as strings with two decimals
  (Constitution VII, prompt).
- Reports are read-only service methods over one read-only repository (prompt).
- A reader not allowed to see a project's pay receives no pay figure for it (FR-028).
- Shifts count when published and ended; periods by local start date at the site (research R8).

**Scale/Scope**: 15 new operations and 3 changed ones, 2 tables, 26 columns, 1 index, 1 pure package, 3
screens

## Constitution Check

| Principle | Check | Result |
| --- | --- | --- |
| I. Spec as source of truth | FIN-1, FIN-3, RPT-1, RPT-2, ADM-2 trace to the original; the owner's additions (pay switches, Managers on projects, premiums, pay to date) are marked Proposed in the spec | Pass |
| II. Layered backend | New routers, services, repositories and models; reports are service methods over `repositories/pay_reports.py`; `hds.payroll` is called by services only | Pass |
| III. Pure scheduling package | Unchanged; `hds.payroll` follows the same rule for pay and has its own purity test | Pass |
| IV. Test-first | Failing tests first in every group; property tests for `price` and the totals; Schemathesis; one Playwright journey per screen; mutation checks | Pass |
| V. API-first | The contract updated per group; the client regenerated, drift check green; screens use the generated client only | Pass |
| VI. Security | The role matrix and the pay-visibility rule enforced in services; an allowed and a forbidden test for every role and operation (prompt); 404 across organisations | Pass |
| VII. Money and time | `Decimal` only; amounts `NUMERIC(12,2)` in shape and strings in the API; rates and premiums positive by check and by service; UTC instants, local periods, DST-exact hours | Pass, noted (rates stay `NUMERIC(12,4)`, see Complexity Tracking) |
| VIII. Internationalisation | Every error code and screen string in `en` and `he`; RTL smoke covers the new screens | Pass |
| IX. Quality gates | Unchanged gates, plus the property tests at the CI profile and the performance test | Pass |
| X. Migrations only | Migration 022, reversible; no applied migration edited | Pass |
| XI. Documentation | Docstrings, the migration docstring, `docs/hds-api.md` (Finance, Reports, `/me/pay`), the contract, ADR 012 (pay calculation and premium settings), docs/hds-spec.md FIN-3 note | Pass |

**Post-design re-check**: the same result.

## Project Structure

### Documentation (this feature)

```text
specs/005-pay-reports/
├── spec.md, plan.md, research.md, data-model.md, quickstart.md
├── contracts/api.md
├── checklists/requirements.md
└── tasks.md            # /speckit-tasks
```

### Source Code

```text
apps/api/
├── alembic/versions/022_pay_reports.py
├── src/hds/
│   ├── payroll/
│   │   ├── __init__.py                     # price, PaySettings, SpecialDay, PayLine
│   │   ├── settings.py                     # PaySettings, RestWindow, the defaults
│   │   └── price.py                        # cutting a shift into parts and pricing them
│   ├── core/types.py                       # Amount, Percent, Hours
│   ├── models/{special_day,project_manager}.py   # organisation.py, project.py: new columns
│   ├── repositories/{special_day,project_manager,pay_reports}.py
│   ├── services/{pay_settings,special_day,project_manager,pay,reports}.py
│   │                                       # employee.py: Admin-only rate; project.py: freeze on end
│   ├── routers/{pay_settings,special_days,project_managers,my_pay,reports}.py
│   └── locales/{en,he}.json
└── tests/{unit/payroll,property,integration,performance}/...
apps/client/
├── app/(employee)/pay.tsx, app/(employee)/reports/{work-log,hours}.tsx
├── src/screens/{MyPayScreen,WorkLogScreen,HoursReportScreen}.tsx
├── src/components/EmployeeNav.tsx           # My pay; Reports for Managers and Admins
├── src/locales/{en,he}.json
└── tests/e2e/{test_us_my_pay,test_us_work_log,test_us_hours_report}.spec.ts
packages/api-client/                         # regenerated
docs/adr/012-pay-calculation.md
```

**Structure Decision**: the existing monorepo layout; the pay rules in a pure package next to
`hds.scheduling`.

## Phases

Each group: failing tests first, then code, the contract, mutation checks, gates, /hds-review and
/code-review high, the log, a commit.

- **P1, the pure pay package**: `PaySettings`, the rest window, `price` (research R3); worked examples
  from the spec; property tests; the purity test.
- **P2, migration 022 and the models**: columns, tables, checks and the index; round trip on a seeded
  database.
- **P3, rates and settings** (US2, US6):
  - the Admin-only default rate (research R7), with the 001 test changes logged;
  - organisation pay settings, project switches and overrides, the freeze when a project ends;
  - special days for the organisation and per project;
  - Managers on projects.
- **P4, pay to date** (US1, US3 end to end): `GET /me/pay`, priced by `hds.payroll`.
- **P5, reports** (US4, US5): the read-only repository, the work-log and hours services and routers, the
  pay-visibility rule, the property tests on totals, the performance test.
- **P6, screens**: My pay, work-log, hours report; translations; Jest and Playwright journeys; RTL smoke.
- **P7, wrap-up**: ADR 012, docs/hds-api.md and the FIN-3 note in docs/hds-spec.md, the generated client,
  the merge gate.

## Complexity Tracking

| Violation | Why Needed | Simpler Alternative Rejected Because |
| --- | --- | --- |
| The prompt says money is `NUMERIC(12,2)`, but the two existing rate columns stay `NUMERIC(12,4)` | They were created by applied migrations (004 in feature 001, and 020) and hold rates, not amounts; amounts in this feature are computed and take the `NUMERIC(12,2)` shape | Narrowing them needs a destructive two-release change (Constitution X) and could cut stored values, for no gain |
