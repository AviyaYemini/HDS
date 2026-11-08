# ADR 020: A proposal keeps a working copy, and every run an append-only record

**Date**: 2026-11-08
**Status**: Accepted. Extends the schedule jobs of feature 004.

## Context

Feature 004 stored a proposal as one JSON result on its schedule job and accepted it all or nothing,
refused as out of date when any input had changed (its fingerprint). Feature 008 lets a Manager or
Admin edit a proposal before accepting it, accept it in parts, lock assignments and run again
(spec FR-003 to FR-012, FR-018, FR-019), and asks for a record of every run that no one can change
(FR-030), which the audit log of feature 014 will show.

## Decision

- **The solver's result stays as it was.** `schedule_jobs.result` is never changed after the worker
  stores it. A second JSON, `current`, is the working copy: the same proposal, with each assignment
  marked as the solver's or a person's, locked or not, overridden (with the warning and reason) or
  not, and accepted or not.
- **A version guards the working copy.** `schedule_jobs.version` grows with every edit, undo, lock
  and accept; each write sends it in `If-Match` and is refused with 412 when it is stale, so two
  managers never overwrite each other.
- **A part is re-checked, not fingerprinted.** When the whole fingerprint no longer matches, a part is
  still accepted if none of its drafts changed and it breaks no rule on today's roster that the
  working copy does not already override. A per-day fingerprint would miss the weekly maximum and
  the rest rule, which cross days.
- **The record only grows.** `schedule_job_events` holds one row per step (created, finished,
  stopped, edited, undone, locked, overridden, accepted, discarded and so on), with actor, time and
  the working copy's version. Its repository can only insert and read, and a database trigger
  refuses UPDATE and DELETE. Each edit's step holds the people of the slots it touched before it,
  so undo restores them from the record and adds an `undone` step: there is no second history.

## Consequences

- Reading a proposal is still one row. A proposal's working copy is short-lived: it expires with the
  proposal after 7 days, while its record stays.
- Feature 014 reads `schedule_job_events` as one of its sources rather than duplicating it.
- Rows per proposal assignment were rejected: they would add a table for data that lives a week.
