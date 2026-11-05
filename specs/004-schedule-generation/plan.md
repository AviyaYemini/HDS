# Implementation Plan: Schedule generation

**Branch**: `004-schedule-generation` | **Date**: 2026-11-05 | **Spec**: [spec.md](spec.md)

**Input**: spec.md (FR-001 to FR-027, SC-001 to SC-009) and the stored prompt
`.claude/hds/prompts/03-scheduling/plan.md`: "Implement the pure scheduling package with two solvers
behind one signature: the greedy baseline from HDS 1.0 and OR-Tools CP-SAT with a 20 second cap and an
`optimal` flag. Asynchronous jobs with statuses queued, running, succeeded, failed, run by a worker (use
a Postgres-backed queue, no extra broker). Property-based tests with Hypothesis proving the hard rules
hold for any generated input, and a benchmark comparing greedy and CP-SAT. Endpoints: POST
/schedule-jobs, GET /schedule-jobs/{id}, POST /schedule-jobs/{id}/accept."

## Summary

The pure package `hds.scheduling` gains `generate(problem, solver=..., time_limit=...)`. It has two
solvers behind that one signature: a greedy baseline rebuilt from the HDS 1.0 audit with its flaws
fixed, and a CP-SAT model whose objective puts the spec's goals in order. Both share the eligibility
test, the reasons for unfilled places and the summary. Their output is judged by the existing
`validate`. The API gains staffing needs per project, the organisation's rest and weekly settings, and
schedule jobs: a table that is also the queue, a worker process that claims jobs with
`FOR UPDATE SKIP LOCKED`, and accept and discard. Accept turns a proposal into draft shifts after
checking that nothing it was solved from has changed. No screens in this feature.

## Technical Context

**Language/Version**: Python 3.12, pinned for the API with `apps/api/.python-version` (research G1)

**Primary Dependencies**: as 003, plus `ortools` 9.15 (CP-SAT), imported only by
`hds/scheduling/cpsat.py` (research G2)

**Storage**: PostgreSQL 16. Migration 021 adds `staffing_needs` and `schedule_jobs` and
`assignments.source`, and bounds the organisation settings. The queue uses `LISTEN`/`NOTIFY` and
`SKIP LOCKED`, with no broker.

**Testing**: as 003 (pytest, Hypothesis, Schemathesis, concurrency tests on PostgreSQL). New: property
tests that run both solvers on generated problems and judge the results with `validate`; example tests
for each legacy flaw; worker tests for claiming, leases and retries; a `benchmark` marker, deselected by
default.

**Target Platform**: Linux server: the API, plus a worker process from the same image (a new compose
service)

**Project Type**: web service in the existing monorepo

**Performance Goals**: SC-001: 30 employees, several projects, one week, a proposal within 30 seconds of
the request (a 20-second solve plus queue and build time)

**Constraints**:
- No hard rule broken in any stored proposal: the solvers, and `validate` again before storing.
- The solve runs outside the API process.
- Accepting is all or nothing, and refused when out of date.
- Times are in UTC; days and weeks are local (Constitution VII).

**Scale/Scope**: 10 new operations, 2 tables and 1 column, 2 solvers, 1 worker

## Constitution Check

| Principle | Check | Result |
| --- | --- | --- |
| I. Spec as source of truth | Every FR traces to scenario B, SCH-1, SCH-2 (Proposed in the original), ORG-4 and docs/hds-spec.md section 5; the goal order, the 31-day range, the 6-day run and 7-day expiry are recorded in the spec's assumptions | Pass |
| II. Layered backend | New routers, services, repositories and models; the worker is a service caller like a router, through the same services | Pass |
| III. Pure scheduling package | `generate` and both solvers live in `hds.scheduling`, with no app, database or network import. `ortools` is a third-party library, imported only by `cpsat.py`, and not loaded by `import scheduling`. The purity test is widened for that one module, with the reason logged (research G2) | Pass, noted |
| IV. Test-first | Failing tests first in every group; property tests for every hard rule on both solvers (Constitution IV: "property-based tests for every hard constraint"); mutation checks; one Playwright smoke test per journey, written and skipped until the staff screens exist, as 003 did | Pass, noted |
| V. API-first | Contract updated per group; the generated client is regenerated and its drift check stays green | Pass |
| VI. Security | Manager and Admin only; another organisation's job 404; proposals carry ids only, no personal fields; an allowed and a forbidden test per operation | Pass |
| VII. Money and time | No money in this feature; times UTC, weeks and dates local, DST handled by 003's `local_span` | Pass |
| VIII. Internationalisation | Every new error code and reason code in `en` and `he` | Pass |
| IX. Quality gates | Unchanged gates, plus the property tests in CI and the benchmark run by hand before release, like the timing tests | Pass |
| X. Migrations only | Migration 021, reversible; no edit to an applied one | Pass |
| XI. Documentation | Docstrings, migration docstring, `docs/hds-api.md`, the contract, the README (worker), ADR 011 (solvers and the job queue) | Pass |

**Post-design re-check**: the same result.

## Project Structure

### Documentation (this feature)

```text
specs/004-schedule-generation/
├── spec.md, plan.md, research.md, data-model.md, quickstart.md
├── contracts/api.md
├── checklists/requirements.md
└── tasks.md            # /speckit-tasks
```

### Source Code

```text
apps/api/
├── .python-version                       # 3.12 (research G1)
├── alembic/versions/021_schedule_generation.py
├── benchmarks/compare_solvers.py
├── src/hds/
│   ├── scheduling/
│   │   ├── problem.py                    # Problem, Slot, Result, Unfilled, Reason, summaries
│   │   ├── eligibility.py                # who may take a slot against a roster (shared)
│   │   ├── greedy.py                     # standard library only
│   │   ├── cpsat.py                      # the only module importing ortools
│   │   └── generate.py                   # generate(): solve, repair, reasons, summary, changes
│   ├── models/{staffing_need,schedule_job}.py   # shift.py: Assignment.source
│   ├── repositories/{staffing_need,schedule_job,scheduling_inputs}.py
│   ├── services/{staffing_need,scheduling_settings,schedule_job,schedule_inputs}.py
│   ├── routers/{requirements,scheduling_settings,schedule_jobs}.py
│   ├── worker.py                         # python -m hds.worker
│   └── locales/{en,he}.json
└── tests/{unit/scheduling,property,integration,concurrency,benchmark}/...
infra/docker-compose.yml                  # a worker service
docs/adr/011-solvers-and-job-queue.md
```

**Structure Decision**: the existing monorepo layout; the worker shares the API's package and image.

## Phases

Each group: failing tests first, then code, the contract, mutation checks, gates, /hds-review and
/code-review high, the log, a commit.

- **P1, setup**: pin Python 3.12, add `ortools`, widen the purity test for `cpsat.py`.
- **P2, the pure engine**:
  - the types, the shared eligibility test, the greedy solver, the CP-SAT model, `generate` with the
    repair pass, the reasons and the summary;
  - property tests for both solvers;
  - example tests for every legacy flaw and every reason.
- **P3, migration 021 and the models.**
- **P4, staffing needs and settings** (US1, US7).
- **P5, jobs** (US2):
  - creating and reading jobs, with idempotency;
  - building the problem from the database (slots from needs and drafts, the surroundings, the
    candidates, the fingerprint);
  - the worker with claims, leases, retries and `LISTEN`.
- **P6, accept and discard** (US3, US6): the out-of-date check under locks, the draft writes, the
  concurrency tests.
- **P7, wrap-up**:
  - the benchmark;
  - ADR 011, the docs, the compose worker and the skipped staff journey specs;
  - the generated client;
  - the checkpoint merge gate.

## Complexity Tracking

| Violation | Why Needed | Simpler Alternative Rejected Because |
| --- | --- | --- |
| A third-party import (`ortools`) inside the pure package | The plan prompt and docs/hds-spec.md section 5 put CP-SAT in the engine; the model is the rules | An adapter outside the package would put rules outside it, which Constitution III forbids |
