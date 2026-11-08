# Implementation Plan: Auto-scheduler review

**Branch**: `008-auto-scheduler-review` | **Date**: 2026-11-08 | **Spec**: [spec.md](spec.md)

**Input**: spec.md (FR-001 to FR-036, SC-001 to SC-010, clarifications of 2026-11-08) and the stored
prompt `.claude/hds/prompts/07-auto-scheduler-review/plan.md`.

## Summary

The pure package `hds.scheduling` learns four things: locks (as the `kept` people a slot already
has), a starting point with fewest changes against it, the two run controls and the manager's chips
as ordered goals, and explanations that name each blocked person with numbers. Two new pure
functions answer "who else could cover" and the fairness panel. CP-SAT reports its best result while
it runs, and a stop flag ends it early with that result.

A proposal stays on its job, but it becomes a working copy: the solver's result stays as it was, and
a `current` copy takes a person's edits under a version number, so two managers never overwrite each
other. Accepting takes a part (days, projects or assignments) and re-checks only that part. Every step
is written to an append-only run record. Assignments gain a lock.

The client turns 007's read-only run page into the review: the week as a change against the roster,
an accept bar, edits from a person's menu, an empty slot's panel with reasons and "who else could
cover", the summary and the fairness panel, the progress with "best so far" and Stop, and the
record. The roster gains "Generate this week", "Find someone" and "Copy to next week".

## Technical Context

**Language/Version**: Python 3.12 (API, worker, scheduling); TypeScript with Expo and React Native
Web (client), as 007.

**Primary Dependencies**: no new runtime dependency (research P1). OR-Tools CP-SAT stays imported
only by `hds/scheduling/cpsat.py`.

**Storage**: PostgreSQL 16. Migration 034 adds the job's working copy, version, controls, chips,
starting point, progress and stop columns; `assignments.locked_by` and `locked_at`; and the
append-only table `schedule_job_events`, with a trigger that refuses UPDATE and DELETE (data-model.md).

**Testing**: pytest with Hypothesis for the scheduling package (no hard rule broken under any locks,
controls and chips; every lock kept; a rerun keeps locks and changes no more than needed); example
tests for each reason code and its numbers; service tests with fake repositories; integration tests
on `hds_p3_test`; Schemathesis; Jest and React Native Testing Library; Playwright specs in
`apps/client/tests/e2e/ux/` (Hebrew and English, desktop and phone, axe-core) run only through
`scripts/ops/e2e_ux.sh`.

**Target Platform**: the API and worker on Linux; the client on web, iOS and Android, as 007.

**Project Type**: web service and cross-platform client in the existing monorepo.

**Performance Goals**: SC-007: the best-so-far figure within 5 s of an improvement (the worker stores
it at most every 2 s); a stop yields a proposal within 5 s. An edit answers, rules checked, within
1 s for 30 employees and a week; "who else could cover" within 2 s.

**Constraints**:
- No hard rule broken by the solver; a person's override is the only way, with a reason (FR-012).
- The scheduling package stays pure (Constitution III): progress and stop pass in as plain callables.
- Accepting stays all or nothing within one accept; nothing is published (FR-003, FR-006).
- The run record only grows (FR-030): no update or delete path in code, and a trigger in the database.
- A Manager acts on a run only if they manage every project in it (007 T108).
- Times in UTC, days and weeks local (Constitution VII).

**Scale/Scope**: about 12 new or changed operations, 1 migration (1 table, 2 tables gain columns),
5 changed engine modules and 2 new pure functions, 6 client screens or panels, 4 pull requests.

## Constitution Check

| Principle | Check | Result |
| --- | --- | --- |
| I. Spec as source of truth | SCH-1, SCH-2 and section 5 are original; the rest is marked Proposed with its AS row or owner decision (O9, O17, O18, 2026-11-08) | Pass |
| II. Layered backend | New routers delegate to `ScheduleJobService`, `ProposalService` and `RosterService`; new repositories for events and locks | Pass |
| III. Pure scheduling package | Locks, starting point, controls, chips, explanations, cover and fairness all live in `hds.scheduling`, with standard-library types; progress and stop are callables | Pass |
| IV. Test-first | Each group's tests are written failing first: properties, examples, service fakes, contract, Playwright | Pass |
| V. API-first | Contract-first in `specs/001-foundation-auth-org/contracts/openapi.yaml`, additive under `/api/v1`, client regenerated | Pass |
| VI. Security | Role checks in the services; Manager scope as 007; cover never shows pay or another project's shift details (FR-016) | Pass |
| VII. Money and time | No money; minutes and UTC instants; weekends and weeks in the organisation's zone | Pass |
| VIII. i18n | Every reason, chip, summary and notice has Hebrew and English keys; the server sends codes and numbers, never sentences | Pass |
| IX. Quality gates | Scoped `make ci-local` per group; full suites in CI | Pass |
| X. Migrations | One new migration, 034; no applied migration edited | Pass |
| XI. Documentation | Module docstrings, endpoint summaries, ADR 020 (the proposal working copy and the append-only record), docs/hds-api.md | Pass |

Re-checked after Phase 1 design: unchanged.

## Project Structure

### Documentation (this feature)

```text
specs/008-auto-scheduler-review/
├── plan.md
├── research.md
├── data-model.md
├── quickstart.md
├── contracts/
│   └── api.md          # the operations, added to the 001 OpenAPI contract first
├── checklists/requirements.md
├── review-ledger.md    # from .claude/hds/review-ledger-template.md
└── tasks.md            # /speckit-tasks
```

### Source Code (repository root)

```text
apps/api/src/hds/
├── scheduling/
│   ├── problem.py       # Chip, Controls, Blocked (named people with numbers), Result.progress
│   ├── eligibility.py   # Roster.detail: a refusal's numbers (rest, week)
│   ├── engine.py        # generate(..., progress=, should_stop=); starting point; chips met
│   ├── cpsat.py         # goal order from controls and chips; solution callback; stop
│   ├── greedy.py        # chips as tie-breakers
│   ├── cover.py         # NEW: who else could cover, ranked
│   └── fairness.py      # NEW: hours, nights, weekends against the average; trade-offs
├── models/schedule_job.py, models/shift.py, models/schedule_job_event.py (NEW)
├── repositories/schedule_job.py, repositories/schedule_job_event.py (NEW), repositories/shift.py
├── services/schedule_job.py   # create with controls, chips, based_on; partial accept; stop
├── services/proposal.py       # NEW: working copy, edits, undo, locks in a proposal, cover, record
├── services/roster.py         # locks on the roster; Find someone
├── routers/schedule_jobs.py, routers/proposals.py (NEW), routers/shifts.py
├── worker.py                  # progress writes and the stop flag
└── alembic/versions/034_proposal_review.py

apps/client/src/
├── screens/runs/        # RunScreen → review; ProposalWeek with change marks; AcceptBar;
│                        # SlotPanel (reasons, cover); SummaryPanel; FairnessPanel; RecordPanel;
│                        # RunForm controls and ChipEditor; RunProgress (best so far, Stop)
├── screens/roster/      # Generate this week, Find someone, Copy to next week, lock marks
├── design/              # RunFinishedNotice (in-app, from job status)
└── locales/{he,en}.json

apps/client/tests/e2e/ux/i-proposal-review.spec.ts … l-run-flow.spec.ts (one per PR)
```

**Structure Decision**: the existing monorepo layout of 004 and 007; 008 adds two pure modules,
one service, one router and one model.

## Pull requests

As the plan prompt asks, the tasks fall into four pull requests, each ending in one `/code-review`
at PR-ready (owner's rules of 2026-11-08):

1. **Review as a change, partial accept, editing** (User Stories 1 and 2; FR-001 to FR-012):
   migration 034 entire, the working copy, versions, the record's first events, change marks.
2. **Empty-slot reasons and fixes** (User Story 3; FR-013 to FR-016): named reasons with numbers,
   cover, "Assign anyway".
3. **Steering, trade-offs and fairness** (User Stories 4 and 5; FR-017 to FR-026): controls, locks,
   starting point, chips, the summary in words, the fairness panel.
4. **Best so far, stop, the record and one flow** (User Stories 6 to 8; FR-027 to FR-034): progress,
   Stop, the in-app notice, the record screen, Generate this week, Find someone, Copy to next week.

FR-035 and FR-036 (access, language, accessibility) apply in every PR.

## Complexity Tracking

No violations.
