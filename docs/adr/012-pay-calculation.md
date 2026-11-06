# ADR 012: Pay worked out in one pure package, from settings that projects follow or override

**Date**: 2026-11-05
**Status**: Accepted.

## Context

Feature 005 shows an employee the gross pay they have earned so far in a month (FIN-3, original
scenario F) and gives Managers and Admins hours and pay reports (RPT-1, RPT-2). The owner asked for
pay to follow the basics of Israeli law: overtime per shift at 125% and then 150%, a weekly rest at
150%, and special days up to 200%, from defaults an Admin can change per organisation and per
project, frozen once a project ends. The plan prompt fixes money as `Decimal`, serialised as
strings, and reports as read-only service methods over repositories with indexed queries.

## Decision

**One pure package prices a shift.** `hds.payroll.price(start, end, zone, base_rate, settings,
special_days)` returns the hours at each rate and the pay. It imports only the standard library and
has its own purity test, as Constitution III asks of scheduling; services build its inputs from the
database and nothing else computes pay.

**A shift is cut at instants.** The cuts are the weekly rest window's ends, local midnights (special
days are local dates) and the two overtime boundaries, measured in real elapsed time so overtime is
the last hours of the shift. Each part takes one rate: the higher of the special day's and the
rest's, else 100%; overtime adds its increase on a day above 100%. A wall time becomes "the first
moment the site's clock shows it", which settles the days the clocks change; the rest window and
that search are exact to the microsecond, so a window's seconds count. Hours are exact
microseconds; pay is summed exactly and rounded half up to the agora once per line. The hours shown
at each rate are rounded by largest remainder, so they add up to the line's shown hours.

**Settings are defaults that projects follow or override.** The organisation row holds the
premium defaults; each project holds the same columns, `NULL` meaning "follow". Ending a project,
or deleting one that has not ended (its shifts still count), copies the defaults it follows and the organisation's special days into it, under the organisation's
lock and then the project's (the order schedule accept uses), so its pay stops changing. There is no
history of settings: figures use the values as they stand when read.

**Money amounts are computed, not stored.** They have the `NUMERIC(12,2)` shape and travel as
strings with two decimals. The two existing rate columns stay `NUMERIC(12,4)`: they are rates, and
narrowing applied columns is a destructive change.

**Who sees pay is decided on every read.** An Admin sees pay for every project with pay on; a
Manager only for a project with pay on, opened to Managers, and that they are assigned to, from the
live role. Where the reader may not see pay the answer has no pay key at all, and the line is not
priced. The same rule decides who is given a shift's own rate in the roster; a person's default
rate goes only to Admins and to that person, since rate times hours is pay (FR-022a). A demotion to
Employee removes a Manager's project assignments. A level of a report whose shown lines all lack a
rate has a count of missing shifts and no pay figure, never zero.

## Consequences

- Every pay figure, in pay to date and in reports, comes from one function with worked examples and a
  property test against a minute-by-minute oracle.
- Changing a default changes the pay of every active project that follows it, past shifts included;
  only ending or deleting a project fixes its pay. Recording worked hours (EMP-2) and payslips may later want pay
  frozen when earned; that would add a history, not change `price`.
- Reports price every line whose pay is shown on each request: at most about 1.2 seconds for a year
  of 26,000 assignments. Special days are read from the earliest start to the latest end of those
  shifts, so a shift longer than a day still meets its dates.

## Alternatives considered

- **Pricing in SQL.** Hard to test, and wrong around clock changes.
- **Whole-hour buckets.** Wrong for half hours and for nights of 7 or 9 hours.
- **A settings history with effective dates.** More than the spec asks; figures use current values.
- **Storing pay lines.** They would go stale whenever a rate or setting changes.
