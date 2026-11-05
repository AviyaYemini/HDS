# Quickstart: validating pay and reports

Prerequisites: as 004. Tests run on a private `_test` database. The client runs with `pnpm --filter
hds-client web` against the API; its journeys with `pnpm client:e2e`.

| Item | How to prove it | Expected |
| --- | --- | --- |
| Purity (research R2) | `tests/unit/payroll/test_purity.py` | `hds.payroll` imports only the standard library and nothing from the app |
| Worked examples (SC-003) | `tests/unit/payroll/test_price.py`: rate 50, defaults | 10-hour weekday day 525.00; 8-hour Saturday day 600.00; weekday night 22:00 to 06:00 412.50; Friday 22:00 night 612.50; a 200% special day; a DST night of 7 and 9 hours; own shift rate; no rate (missing) |
| Invariants (SC-002) | Hypothesis on `price` and on the report totals, at least 1,000 examples in CI | Parts' hours add up to the shift's hours; pay equals the sum of parts within rounding; no rate below 100%; totals equal the sum of lines; hours by employee equal hours by project |
| US2, FR-001 to FR-008a | Admin and Manager change a default rate; Admin sets project settings and days; end the project; change the defaults | Admin stored, Manager 403 `users.rate_admin_only`; figures change; the ended project's figures do not; 409 on changing it |
| US1, FR-017 to FR-020 | `GET /me/pay` as employee with ended, future and draft shifts and a pay-off project | Only ended published shifts; pay-off lines without pay; other people refused |
| US6, FR-021, FR-022 | `PUT /projects/{id}/managers`; open pay to Managers; demote the Manager | Assigned Manager sees pay; others do not; after demotion none |
| US4, FR-023 to FR-026 | Work-log by project without `from`, by site, by group, with a deactivated employee | Starts at the project start; per project; members only; history kept |
| US5, FR-027, FR-028 | Hours report by employee and by project as Admin and as Manager | Same grand total; Manager sees pay only for open, assigned projects |
| Access (SC-004, FR-031, FR-032) | Role matrix, cross-organisation sweep, pay-visibility sweep, Schemathesis | Every forbidden pair 403, foreign records 404, no forbidden pay figure anywhere |
| Performance (SC-005) | `tests/performance/test_report_performance.py`: 100 employees, 20 projects, one year | Each report under 3 seconds |
| Screens (SC-001) | Playwright: `test_us_my_pay`, `test_us_work_log`, `test_us_hours_report`, RTL smoke | Pay in at most three steps; reports render in Hebrew and English |
| Checkpoint | The 004 merge gate plus the new tests, migration round trips 021 ↔ 022 on a seeded database, the generated client and the client gates | Green |
