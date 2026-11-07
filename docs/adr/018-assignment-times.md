# ADR 018: Each assignment may carry its own times, and every rule reads the person's span

**Date**: 2026-11-07
**Status**: Accepted.

## Context

The owner's weekly roster must show "a half shift or two extra hours" (007, the owner's note S2). In
HDS a shift had times and an assignment only joined it to a person, so one person's half shift
inside a shared block had nowhere to live (research report S3). The owner chose to give each
assignment its own start and end (decision O7), reaching at most 4 hours before the shift starts
and after it ends (clarification Q3).

## Decision

**Nullable own times on the assignment.** `assignments.starts_at` and `ends_at` (migration 027),
both set or both null; null means the shift's times, so editing a shift still moves everyone on it.
The 4-hour reach is a service rule (`assignment.times_out_of_range`), because it compares with the
shift's row.

**One span per person, everywhere.** The pure package's `ShiftSlot` carries `own_times`, and
`span(person)` answers when that person works the shift. The rule checks judge rest, overlap and
weekly hours by each person's span; the shift's date (which week it counts in, which availability
entries cover it) and its headcount stay the shift's. The solver books already-assigned shifts by
the same spans. Pay, the hours and work-log reports and My shifts read
`coalesce(assignment, shift)` times. Coverage counts a person as one seat whatever their span.

**Concurrency.** Assignment changes touch the shift's `updated_at`, which is the shift's `ETag`, so
a reader's `If-Match` goes stale when anyone changes who works it or when (FR-041).

## Consequences

- A half shift can clear a rest warning the full shift would raise, and two extra hours can cause one.
- Proposals made before 007 keep their fingerprints: own times enter them only when set.
- Generated assignments use the shift's times; a manager sets own times by hand.

## Alternatives considered

- **Sub-shifts** (splitting a shift into pieces): changes headcount, coverage and the solver.
- **Offsets in minutes from the shift's start**: reads badly across daylight-saving changes; the
  week templates use offsets only inside one planned shift, where the length is fixed.
