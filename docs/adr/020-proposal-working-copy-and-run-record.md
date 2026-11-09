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
  and accept; a write that sends it in `If-Match` is refused with 412 when it is stale, so two
  managers never overwrite each other. The header is optional, as on the roster: a write without it
  is not guarded, and the app always sends it.
- **A part is re-checked, not fingerprinted.** Any accept but the whole of an untouched proposal is
  judged on today's roster, whether the fingerprint matches or not (it leaves out people placed by
  hand who were not candidates): a part is accepted if none of its drafts changed, their times
  included; each of its new slots still has its staffing need, with the same headcount; and it
  breaks no rule that the working copy does not already override. The whole of an untouched
  proposal is refused, as before 008, when the fingerprint no longer matches. An accept with
  nothing left open writes nothing and only closes the proposal as accepted, whatever changed
  since. A per-day fingerprint would miss the weekly maximum and the rest rule, which cross days.
- **The record only grows.** `schedule_job_events` holds one row per step, with actor, time and
  the working copy's version. From PR 1 it holds edits, undos, overrides (a reason given to keep
  a rule break), accepts and discards; the run's own steps (created, finished, stopped) and locks
  join it in later groups of 008 (PR 3 and PR 4). Its repository can only insert and read, and a database trigger
  refuses UPDATE and DELETE. Each edit's step holds the people of the slots it touched before it,
  so undo restores them from the record and adds an `undone` step: there is no second history.

## Consequences

- Reading a proposal is still one row. A proposal's working copy is short-lived: it expires with the
  proposal after 7 days, while its record stays.
- Feature 014 reads `schedule_job_events` as one of its sources rather than duplicating it.
- Rows per proposal assignment were rejected: they would add a table for data that lives a week.
