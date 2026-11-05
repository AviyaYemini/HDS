# ADR 009: Roster rules live in one pure function, and manual changes only warn

**Date**: 2026-11-03
**Status**: Accepted.

## Context

Feature 003 lets an Admin build a roster by hand. The stored prompt says the system shows warnings, for
example too little rest between shifts, but does not block the Admin. The automatic generator (SCH-1)
comes later and must respect the same rules: no two shifts at once (H1), minimum rest (H2), weekly
hours (H3), blocked dates and shift types (H4). Constitution III says the `scheduling` package is pure
and is the only place scheduling rules live.

## Decision

**One pure function judges every change.** `hds.scheduling.validate` takes plain dataclasses (shift
slots, employees, availability entries, the organisation's limits) and returns warnings, each with a
stable code, the employee and every shift involved. It covers overlap, rest, weekly hours, blocked
dates, absences, close preferences, headcount and inactive employees. It imports only the standard
library; a test imports it in a bare interpreter, and another reads every import.

**Services feed it and report what it says.** A service loads the employee's shifts near the one being
changed (all projects, drafts included), the entries for those dates and the settings, calls
`validate`, and answers the stored record together with the warnings. A warning never turns into a
refusal (003 FR-116). Only data that cannot be stored is refused, for example an inactive employee or
the same employee twice on one shift.

**Local times become instants in the same package.** `local_span` turns a shift type's local times on a
date into UTC instants in the site's zone, so lengths and rest are real elapsed time across midnight and
daylight-saving changes.

**Weeks run Sunday to Saturday** in the organisation's zone, and a shift counts in the week and on the
date it starts.

## Consequences

- The generator will call the same function to check its result, so a manual roster and a generated one
  cannot be judged by different rules.
- Because the Admin is never blocked, a roster can be stored that breaks a hard rule. The warnings are
  in each answer only; nothing records them. A later "roster health" view would compute them again.
- The rules need every nearby shift of the employee, so a service must load them; the read is bounded by
  the longest shift plus the minimum rest, and the week.
- Shifts that clash with each other but not with the changed one are not reported, by design: an answer
  is about what this change does.

## Alternatives considered

- **Rules inside the services.** Simpler today, but the generator would need a second copy, and
  Constitution III forbids rules outside the package.
- **Refusing changes that break a hard rule.** Safer, but the prompt says the Admin decides, and real
  rosters sometimes need an exception.
- **ISO weeks (Monday start).** They do not match the Israeli working week the original describes.
