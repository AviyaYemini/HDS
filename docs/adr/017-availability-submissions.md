# ADR 017: Availability is submitted as a snapshot, and a missed deadline counts as unavailable

**Date**: 2026-11-07
**Status**: Accepted.

## Context

Until 007 each availability entry saved on its own and the scheduler read the live entries, so an
employee never saw a "done" state and a manager never knew who had finished (research report E2).
The owner asked for an explicit Submit (decision O4) and decided what happens to someone who misses
the deadline: "Count them as unavailable" (007 clarification Q4). Admins still edit anyone's
availability by hand at any time (003 FR-107).

## Decision

**A snapshot per employee and window.** Submitting stores the employee's blocked, open and close
entries for the window's dates, clipped to it and sorted, with its SHA-256
(`availability_submissions`, migration 026). Entries keep being saved through the 003 endpoints and
are the draft. The status is derived, never stored: Locked once the deadline has passed; otherwise
Submitted while the live entries hash to the snapshot, Draft after any change or entry, Not
submitted with neither.

**After the deadline scheduling reads what was submitted.** A pure helper
(`hds.services.availability_effective`) gives the scheduler and the manual-roster warnings the
entries that stand: inside a closed window, the snapshot; for an employee with none, one block over
every date of the window with reason `not_submitted`, so the solver leaves them out and a manual
assignment warns `warnings.not_submitted` (warnings never refuse, ADR 009). Absences, and every date
outside closed windows, keep the live entries.

**Admins.** An Admin's hand edit inside a closed window refreshes an existing snapshot, so it
reaches the scheduler, but never creates one: touching one day must not make a non-submitter
available for the rest of the window. To count such an employee an Admin submits on their behalf
(`POST /api/v1/employees/{id}/availability-submissions`), recorded in `submitted_by`.

**No reach into the past.** `submission_windows.requires_submission` is false for windows already
closed when migration 026 ran, so weeks that had no Submit keep their old meaning.

## Consequences

- Employees see a banner with the status and the deadline; managers get a submission tracker.
- A change after submitting must be submitted again, or the last submitted week stands.
- Reminders before the deadline come with feature 010.

## Alternatives considered

- **A status column flipped by a job at the deadline**: needs a scheduler and races with late edits.
- **A separate table of draft entries**: two sources of truth for the same week.
- **Using the draft at the deadline**: the owner chose to count a missed deadline as unavailable.
