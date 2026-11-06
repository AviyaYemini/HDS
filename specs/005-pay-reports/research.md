# Research: Pay rates, pay to date and reports

Each decision resolves an unknown of the plan's Technical Context. The plan prompt fixes: money as
`NUMERIC(12,2)` in the database and `Decimal` in Python, serialised as strings; reports as read-only
service methods over repositories with indexed queries; report and pay screens in the Expo app; contract
tests for every role and endpoint pair, allowed and forbidden.

## R1. Money columns and amounts

**Decision**: pay amounts are computed, never stored. Each line's amount is a `Decimal` rounded half up to
`0.01` once, which is the `NUMERIC(12,2)` shape; a new API type `Amount` serialises it as a string with
exactly two decimals (`"612.50"`), and totals are sums of the rounded lines. The two existing rate columns,
`users.default_hourly_rate` (001) and `shifts.pay_rate` (003), stay `NUMERIC(12,4)` and keep the existing
`Money` type: they are rates, not amounts, and narrowing them would edit the meaning of applied
migrations and could cut stored values (Constitution X). Any money amount stored later uses
`NUMERIC(12,2)`. Percentages are `NUMERIC(5,2)` (`"125.00"`), also strings in the API, also `Decimal`.

**Rationale**: the prompt's rule is met wherever a money amount exists; changing applied columns is a
destructive change the constitution routes through two releases, for no gain (a rate of 32.3000 is
already exact).

**Alternatives**: a migration narrowing both rates to `NUMERIC(12,2)` (rejected above); storing pay lines
(rejected: the spec computes from current values every time, FR-016).

## R2. Where the pay rules live

**Decision**: a pure package `hds.payroll`, standard library only (`decimal`, `datetime`, `zoneinfo`):
`price(shift, site_timezone, base_rate, settings, special_days) -> PayLine` with the parts of the shift
and their rates. Services build its inputs from repositories and call it; nothing else computes pay. A
purity test like `hds.scheduling`'s checks that it imports nothing from the app and nothing outside the
standard library.

**Rationale**: the premium rules (FR-009 to FR-015) are the riskiest part and need property tests over
many generated shifts (SC-002, SC-003); a pure function makes that cheap and keeps the rules in one place,
as Constitution III does for scheduling.

**Alternatives**: computing in SQL (hard to test, DST-unsafe); inside the service (mixes rules with data
access and needs a database for every example).

## R3. Splitting a shift into priced parts

**Decision**: the shift `[start, end)` is cut at every boundary that changes a rate, all as UTC instants:
the start and end of each weekly rest window that overlaps it (its weekday and time converted at the
site's zone for that week), each local midnight that starts or ends a special day, and the overtime
boundaries `end − (length − normal)` and that plus the first tier, measured in real elapsed time so that
overtime is the last hours of the shift (FR-011). For each part:

- hours = elapsed seconds / 3600, exact `Decimal`;
- day rate = the special day's rate on a special day, else the rest rate inside the window, else 100;
  the higher when both apply (FR-012);
- an overtime part at day rate 100 takes the overtime rate of its tier; above 100, the day rate plus the
  tier's increase (FR-013).

Pay = Σ hours × base rate × rate / 100, rounded half up to 0.01 once (FR-014). The line also returns the
hours at each rate, merged by rate (FR-015). A night shift is one with at least 120 minutes between 22:00
and 06:00 local time (FR-010).

**Rationale**: cutting at instants makes DST, overnight and window-crossing shifts one rule; exactness
comes from integer seconds. Worked examples in the spec (412.50, 525.00, 600.00, 612.50) are the first
tests.

**Alternatives**: whole-hour buckets (wrong for 7.5-hour shifts and DST); per-day overtime (rejected by the
owner).

## R4. Premium settings: defaults, overrides and freezing

**Decision**: the organisation row gets the premium defaults as columns (`pay_day_normal_minutes`,
`pay_night_normal_minutes`, `pay_tier1_minutes`, `pay_overtime1_pct`, `pay_overtime2_pct`,
`pay_rest_start_weekday`, `pay_rest_start_time`, `pay_rest_end_weekday`, `pay_rest_end_time`,
`pay_rest_pct`, `pay_increase1_points`, `pay_increase2_points`). Each project gets the same columns,
nullable: `NULL` follows the default (FR-006a). When a project's status becomes `ended`, the project
service, under the project's row lock, copies the current defaults into its `NULL` columns and the
organisation's special days into its own list, so later default changes do not reach it (FR-007,
FR-008a). Changes to an ended project's settings or days answer 409 `pay.project_ended`.

**Rationale**: one row read gives the effective settings (`COALESCE(project, organisation)`); freezing by
copy needs no history table.

**Alternatives**: a settings history with effective dates (more than the spec asks: it computes from
current values); a JSON column (no database checks).

## R5. Special days

**Decision**: one table `special_days` with an optional `project_id`: organisation days have none, project
days have one. Unique per (organisation, date) among organisation days and per (project, date) among a
project's days (two partial unique indexes). A project day on a date that also has an organisation day
overrides it for that project (FR-008a). Admins manage both; Managers read both.

**Alternatives**: two tables (the same shape twice).

## R6. Managers on projects

**Decision**: `project_managers (project_id, user_id)`, replaced as a set by `PUT /projects/{id}/managers`
(Admin). Each user must be an active Manager or Admin of the organisation (422
`project_managers.not_staff`). Whether a Manager sees a project's pay is decided on every read from the
live role, `is_active`, membership and the two project switches (FR-022), so a demotion takes effect at
once, like every role check since 001.

## R7. Default rate becomes Admin only

**Decision**: the employee service refuses `default_hourly_rate` in a create or change from a Manager with
403 `users.rate_admin_only`, whatever its value, including `null`; the Manager's other fields still work.
001 tests that had Managers set a rate change to use an Admin; each change and its reason go in the log.

## R8. Which shifts count, and in which period

**Decision**: published shifts with `ends_at <= now` (FR-029). The period of a shift is the local date of
its start at its site's zone. The repository selects by UTC with a margin of one day either side of the
range (no zone is more than 14 hours from UTC) and the service keeps the shifts whose local start date is
in range. The month of pay to date is `YYYY-MM`, default the current month in the organisation's time
zone.

## R9. Indexed read-only queries

**Decision**: one repository, `repositories/pay_reports.py`, read-only, one query per figure, joining
assignments, shifts, projects, sites and users, filtered by organisation, published status and the UTC
window. Migration 022 adds `idx_shifts_org_published_start` on `shifts (organization_id, starts_at) WHERE
status = 'published'`; `idx_assignments_user` (003) serves pay to date and `uq_assignments_shift_user`
serves the joins. A performance test seeds 100 employees, 20 projects and a year of shifts and checks
SC-005 (under 3 seconds).

## R10. Hiding pay

**Decision**: a reader without the right to a project's pay receives lines without the `pay` property, and
totals over the projects they may see only (FR-028); the schema marks `pay` optional. A line with no rate
has `rate_missing: true` and no `pay`; a project with pay off has `pay_enabled: false` and no `pay`.
Totals say `shifts_missing_rate`.

## R11. Screens

**Decision**: in the Expo app, a "My pay" screen for everyone who works shifts (month picker, lines,
total), and for Managers and Admins a "Reports" area with the work-log and the hours report (filters,
grouping, pay columns only when present). Admin settings (rates, project pay settings, special days,
managers on projects) are API only in this feature, as the prompt names only report and pay screens. One
Playwright journey per screen, plus the RTL smoke test extended.

## R12. Contract and access tests

**Decision**: every new operation is added to `openapi.yaml`, Schemathesis and the role matrix
(`tests/integration/test_role_matrix.py`), the cross-organisation sweep and, where a role check cannot be
proven by Schemathesis, `unprovable_operations.json`. A pay-visibility sweep reads every report as every
role and checks no forbidden pay figure appears (SC-004).
