---

description: "Task list for feature 008, the auto-scheduler review"
---

# Tasks: Auto-scheduler review

**Input**: Design documents from `specs/008-auto-scheduler-review/` (spec.md, plan.md, research.md,
data-model.md, contracts/api.md, quickstart.md)

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

**Tests**: Required (Constitution IV). In each group the tests are written first and fail: Hypothesis
properties and examples for the scheduling package, service tests with fake repositories,
integration tests on `hds_p3_test`, contract tests, Jest units, and the group's Playwright spec in
`apps/client/tests/e2e/ux/`, run only through `scripts/ops/e2e_ux.sh`.

**Organization**: one group per pull request (A to D), in the order they ship. Each task names its
user story (US1–US8 in spec.md). One session builds backend and client, so tasks carry no [UI] or
[CORE] mark.

**Each group ends with** (the owner's rules of 2026-11-08). Write it into each group's closing task:

1. the scoped `make ci-local` green: only the checks for the paths the group changed;
2. mutants only where the group touches risky code (sign-in, pay, or who can see what), named in
   the closing task; one at a time, with the hooks on; none otherwise;
3. one commit.

No `/code-review` at a group's close, and no log: an owner's choice or an accepted limit goes in the
commit message, `tasks.md` or `research.md`.

**When the pull request is ready to push, once**: one `/code-review`, each finding a row in the
feature's `review-ledger.md`, none open; the fixes in one commit. No `make ci-local-full`,
`make ci-local-db` or full e2e locally: the full suites run only in CI, and CodeRabbit on GitHub
catches line-level slips.

## Format: `- [ ] [ID] [P?] [Story] Description`

- **[P]**: can run in parallel (different files, no dependencies)
- **[Story]**: the user story in spec.md

---

## Phase 1: Setup

- [x] T001 Create `specs/008-auto-scheduler-review/review-ledger.md` from `.claude/hds/review-ledger-template.md`
- [x] T002 Add ADR 020 "A proposal's working copy and the append-only run record" in `docs/adr/` and its row in `docs/README.md` (research P8, P14)

---

## Phase 2: Group A — review as a change, partial accept, editing (PR 1) — US1, US2

**Goal**: a proposal reads as a change to the roster, is accepted in parts, and is edited before
accepting, with rules checked live and every step recorded.

**Independent test**: quickstart step 1.

### Foundation for every group

- [x] T003 Migration `apps/api/alembic/versions/034_proposal_review.py`: every column and table of data-model.md (job columns, `proposal_state` and `status` checks, `assignments.locked_by`/`locked_at` with their check, `schedule_job_events` with its index and append-only trigger), reversible; models in `apps/api/src/hds/models/schedule_job.py`, `models/shift.py`, new `models/schedule_job_event.py`
- [x] T004 [P] Repository `apps/api/src/hds/repositories/schedule_job_event.py` with insert and paged read only; an integration test that UPDATE and DELETE on `schedule_job_events` are refused by the trigger, in `apps/api/tests/integration/test_schedule_job_events.py`
- [x] T005 Contract first: every operation of `contracts/api.md` marked PR 1 (the proposal read, edits, undo, the accept part, the discard change, `partly_accepted`) in `specs/001-foundation-auth-org/contracts/openapi.yaml`; new cross-field or stored-data rules into `apps/api/tests/contract/unprovable_operations.json` (R110)

### Tests first

- [x] T006 [P] [US2] Hypothesis property in `apps/api/tests/property/test_proposal_edits.py`: any sequence of edits with overrides leaves a working copy whose every rule break carries an override, and undo back to zero edits gives the solver's result exactly
- [x] T007 [P] [US1] Service tests (built as integration tests on `hds_p3_test` in `apps/api/tests/integration/test_proposal_review.py`, which drive the real service and its locks; the pure copy in `apps/api/tests/unit/test_proposal_copy.py`): change marks (added, changed, removed, unchanged) and their counts; version bumps and 412 when stale; partial accept by day, by project and by assignments; the rest stays open; discard the rest gives `partly_accepted`; a Manager not managing every project is refused (403) for read-write operations. Every new operation of the group has an allowed test and a forbidden one: an Employee and a Client get 403, another organisation's job or assignment 404 (FR-035)
- [x] T008 [P] [US1] Integration tests on `hds_p3_test` in `apps/api/tests/integration/test_partial_accept.py`: an accept of Monday succeeds after Thursday's input changed; an accept of Thursday is refused with `schedule.part_out_of_date` naming the day and cause; the accept stays all or nothing within one call (research P9)
- [x] T009 [P] [US2] Integration tests in `apps/api/tests/integration/test_proposal_edits.py`: an edit that breaks rest without a reason is 422 `schedule.override_reason_required` with the warnings; with a reason it is kept and becomes a `rule_overrides` row on accept; an inactive employee is refused; two edits on one version: the second is 412
- [x] T010 [P] [US1] Playwright spec `apps/client/tests/e2e/ux/i-proposal-review.spec.ts` (Hebrew and English, desktop and phone, axe): marks with counts by shape and text; accept one day; edit with a swap and an override; undo; discard the rest

### Implementation

- [x] T011 [US1] The working copy: `current` and `version` written in `ScheduleJobService.succeed` from the result (`apps/api/src/hds/services/schedule_job.py`); `proposal_json` unchanged for `result`
- [x] T012 [US1] `ProposalService` in `apps/api/src/hds/services/proposal.py`: read with change marks against the drafts (FR-001, FR-002); scope check as 007 (FR-007); record events for every write (FR-030's first kinds)
- [x] T013 [US1] Partial accept in `ScheduleJobService.accept`: the body's part, the whole-fingerprint fast path, else the per-part re-check of research P9; `If-Match`; `partly_accepted` on discard or expiry with parts accepted (FR-003 to FR-006)
- [x] T014 [US2] Edits and undo in `ProposalService`: assign, unassign, move, swap on the working copy; `validate` over every project with the working copy; override reasons; undo by replaying the record (FR-008 to FR-012, research P13)
- [x] T015 [US1] Routers `apps/api/src/hds/routers/proposals.py` and the accept and discard changes in `routers/schedule_jobs.py`, with summaries and descriptions; regenerate `packages/api-client`
- [x] T016 [US1] Client: `ProposalWeek` with change marks (shape and text, not colour alone) and counts, per day and project filled over needed; `AcceptBar` (accept all, a day, a project, the selected; discard the rest) in `apps/client/src/screens/runs/`
- [x] T017 [US2] Client: a person's menu in the proposal (swap, move, unassign, assign) with the live rule answer and the override sheet asking for a reason; Undo; a screen-reader path for every edit (FR-036)
- [x] T018 [P] [US1] Locale keys in `apps/client/src/locales/he.json` and `en.json` for marks, the accept bar, edits, overrides and errors; `docs/hds-api.md` for the PR 1 operations

### Close

- [x] T019 Close group A as the template says. Mutants (≤ 3, who can see what): the proposal read letting a Manager who manages only some of the run's projects edit; the accept part skipping the scope re-check; the edit check ignoring shifts in projects outside the run. Done 2026-11-08: all three killed (the third needed a new test, an edit meeting another project's shift); the mutant on the accept part is `_open`'s scope check, which accept, discard and every edit share. Commit `feat(scheduling): review a proposal as a change, accept it in parts and edit it (…)`. **PR 1 ready**: the once-per-PR steps; the owner pushes

---

## Phase 3: Group B — why a slot is empty and how to fill it (PR 2) — US3

**Goal**: every empty slot explains itself with names and numbers, and "who else could cover" fills
it across every project.

**Independent test**: quickstart step 2.

### Tests first

- [x] T020 [P] [US3] Example tests in `apps/api/tests/unit/scheduling/test_explanations.py`: one week per reason code; rest and weekly maximum name each person with `have`, `need`, `week_minutes`, `slot_minutes`, `limit`; counts still add up to every candidate not on the slot (FR-013)
- [x] T021 [P] [US3] Hypothesis property in `apps/api/tests/property/test_cover.py`: every option marked free can be placed without breaking a rule across all projects, and every option not free names a rule that placing them breaks (SC-004)
- [x] T022 [P] [US3] Service and API tests (built as integration tests in `apps/api/tests/integration/test_proposal_cover.py`, which drive the real service): cover answers carry only codes and numbers, never pay, a site, or another project's shift (FR-016). Every new operation of the group has an allowed test and a forbidden one: an Employee and a Client get 403, another organisation's job or assignment 404 (FR-035)
- [x] T023 [P] [US3] Playwright spec `apps/client/tests/e2e/ux/j-empty-slots.spec.ts`: the slot panel's reasons and numbers; cover list order; picking a free person; "Assign anyway" with a reason

### Implementation

- [x] T024 [US3] `Roster.detail` in `apps/api/src/hds/scheduling/eligibility.py`; `Blocked` and `Reason.people` in `scheduling/problem.py`; `_unfilled` in `scheduling/engine.py` fills them (research P10)
- [x] T025 [US3] `scheduling/cover.py`: `options(problem, assigned, key)` ranked as research P11, across every project and kind (FR-014)
- [x] T026 [US3] Contract first for `GET /schedule-jobs/{id}/slots/{key}/cover`; `ProposalService.cover` builds the problem with the working copy; router; regenerate the client
- [x] T027 [US3] Client `SlotPanel` in `apps/client/src/screens/runs/`: reasons with counts, named people with numbers, the empty-slot list on the review, "Who else could cover" with reasons in words, picking places the person as an edit (FR-015); "Assign anyway" through the override sheet
- [x] T028 [P] [US3] Locale keys for every reason, number format and cover reason; `docs/hds-api.md`
- [ ] T028a CI (owner, 2026-11-09): the client e2e job, about 27 minutes, runs as four shards at once, each with its own PostgreSQL, like the database shards: each takes one theme of the accessibility sweep and a quarter of every other spec (`--shard`); the job's old name stays as the one required check over them, and a check fails on a sweep test in no shard

### Close

- [x] T029 Close group B as the template says (done 2026-11-09: the three mutants below all failed the cover tests; scoped `make ci-local` green; /code-review R24 to R33, eight fixed and two won't-fix). Mutants (≤ 3, who can see what): cover including another project's shift id or site in its answer; cover answering a Manager for a run they don't fully manage; the free check skipping shifts outside the run's projects. Commit `feat(scheduling): explain every empty slot and offer who else could cover (…)`. **PR 2 ready**: once-per-PR steps

---

## Phase 4: Group C — steering, trade-offs and fairness (PR 3) — US4, US5

**Goal**: runs are steered by plain controls, locks and the manager's chips, and every proposal says
what it guarantees, what it traded off, and how fair it is.

**Independent test**: quickstart step 3.

### Tests first

- [x] T030 [P] [US4] Hypothesis properties in `apps/api/tests/property/test_steering.py`: for any locks, controls and chips, no hard rule is broken and every lock is kept (SC-005); a rerun from a result with nothing changed changes no assignment; chips never outrank filling a place
- [x] T031 [P] [US4] Example tests in `apps/api/tests/unit/scheduling/test_chips.py`: each chip kind met and not met, with its cause; contradictory chips both reported; a chip for an inactive person or a project outside the run marked as no longer applying (FR-020 to FR-022)
- [x] T032 [P] [US5] Example tests in `apps/api/tests/unit/scheduling/test_fairness.py`: averages, the outlier thresholds of research P12, weekend shifts on Friday and Saturday in the site's zone, locked and overridden assignments counted; trade-off counts
- [x] T033 [P] [US4] Integration tests in `apps/api/tests/integration/test_locks.py`: lock and unlock on the roster within the projects a person may edit (403 otherwise); a locked draft is kept by the next run; a lock goes with its removed assignment. Every new operation of the group has an allowed test and a forbidden one: an Employee and a Client get 403, another organisation's job or assignment 404 (FR-035)
- [x] T034 [P] [US4] Playwright spec `apps/client/tests/e2e/ux/k-steering.spec.ts`: controls and chips on the form, lock marks, a rerun keeping locks, the guarantee line, the summary in words, the fairness panel's outliers by more than colour

### Implementation

- [x] T035 [US4] `Controls`, `Chip`, `Problem.start`, `Problem.controls`, `Problem.chips`, `Slot.site_id` in `scheduling/problem.py`; locks as `kept` (research P2); `Result.chips_met`
- [x] T036 [US4] CP-SAT goal order and chip terms in `scheduling/cpsat.py` (research P4, P5); hints from `start` (P6); greedy tie-breakers in `scheduling/greedy.py` (FR-017 to FR-019)
- [x] T037 [US5] `scheduling/fairness.py`: `panel` and `tradeoffs` (research P12)
- [x] T038 [US4] Contract first: `POST /schedule-jobs` with `controls`, `chips`, `based_on`; `PUT`/`DELETE /assignments/{id}/lock`; `GET /schedule-jobs/{id}/fairness`; lock and unlock as edit ops; `unprovable_operations.json`
- [x] T039 [US4] Services: `ScheduleInputService.build` passes locks as kept, the starting point for a rerun, controls and chips; `RosterService.lock`/`unlock` with scope (FR-018); chips copied by `based_on` (FR-021a); routers; regenerate the client
- [x] T040 [US4] Client: the run form's two controls and `ChipEditor` (chips in words, edit, remove; max 20), lock marks and lock and unlock on the roster and in the proposal, "Run again" from a proposal
- [x] T041 [US5] Client: the guarantee line (FR-023), `SummaryPanel` in words (FR-024), `FairnessPanel` (FR-025), all following edits at once (FR-026)
- [x] T042 [P] [US4] Locale keys for controls, chips, guarantees, the summary and fairness; `docs/hds-api.md`
- [x] T042a [US3] Review (owner, 2026-11-09): show no week row, and no day, that holds no slot of the run; a run's dates outside its weeks' slots (the calendar weeks before the range) add nothing. `weeksOf` in `apps/client/src/screens/runs/model.ts` keeps only the weeks with a slot; desktop columns keep the days of those weeks
- [x] T042b [US3] Review (owner, 2026-11-09): at the top of the review, the slots the run left with empty places, highlighted in red (with an icon and words, never colour alone): each opens its slot in the week, scrolled to and focused, with "Who else could cover" open on it (FR-014, FR-015); an RNTL test and a step in `j-empty-slots.spec.ts`
- [x] T042c Roster (owner, 2026-11-09): the roster's "Assign to the shift" sheet gets a search field over every active staff member, by name, so an open seat is filled without scrolling the whole list. Later (project teams, with 015's qualifications): the list shows only the people attached to the project or qualified for it, and search still finds anyone
- [x] T042d Roster (owner, 2026-11-09): an assignment that breaks a rule (two shifts back to back, 16 hours without rest, the weekly maximum…) still goes through, and a dialog then names each break in words with its numbers, never blocking; it closes with one button and focus goes back where it was
- [x] T042e Roster (owner, 2026-11-09): Publish on a week with no drafts showed a sheet of zeros (every shift there was already published, so nothing was to publish). With nothing to publish, Publish is disabled with the reason in words ("Every shift this week is already published"), and the sheet never opens on zeros
- [x] T042f Roster (owner, 2026-11-09): choosing someone already on an overlapping shift says so, naming that shift as far as the editor may see it, and offers: swap, when the seat is held (the two trade places); transfer, when the seat is empty (they move, and their first shift loses them); or keep both, with the overlap warning (never a block). Swap and transfer are offered only when the editor may change both shifts; otherwise only the warning. Each is one step, undone as one. Later: a stand-by role may hold overlapping shifts with no warning at all
- [x] T042g Roster (owner, 2026-11-09): a manual edit may carry a note, shown at once on the newly assigned employee's own shift page; and a choice of who is alerted: everyone on the edited roster, only the people the edit affects, or no one. 008 records the note and the choice with the edit, as publish records its notify choice; feature 010 delivers the alerts

### Close

- [x] T043 Close group C as the template says (done 2026-11-09: the three mutants below all failed the lock and chip tests; scoped `make ci-local` green; /code-review R37 to R46, all fixed). Mutants (≤ 3, who can see what): lock allowed on an assignment in a project the Manager doesn't manage; a chip naming a person outside the organisation accepted; `based_on` copying chips from another organisation's job. Commit `feat(scheduling): steer a run with controls, locks and chips, and show its trade-offs (…)`. **PR 3 ready**: once-per-PR steps

---

## Phase 5: Group D — best so far, stop, the record and one flow (PR 4) — US6, US7, US8

**Goal**: a run shows its best result while it works and can be stopped; every run has a record; the
roster and runs make one path.

**Independent test**: quickstart step 4.

### Tests first

- [x] T044 [P] [US6] Engine tests in `apps/api/tests/unit/scheduling/test_progress_stop.py`: `progress` is called with rising filled counts; `should_stop` ends the solve with the best result, still valid and marked stopped and not optimal
- [x] T045 [P] [US6] Worker tests in `apps/api/tests/integration/test_worker_progress.py`: progress stored at most every 2 s; Stop with a result ends `stopped` with a proposal; Stop before any result ends `cancelled` with none (SC-007)
- [x] T046 [P] [US7] Service tests: the record lists created, finished, edits, overrides, accepts and discards in order with actors; read scope as 007 (FR-031). Every new operation of the group has an allowed test and a forbidden one: an Employee and a Client get 403, another organisation's job or assignment 404 (FR-035)
- [ ] T047 [P] [US8] Playwright spec `apps/client/tests/e2e/ux/l-run-flow.spec.ts`: best so far and Stop; the in-app notice when a run ends on another screen; Generate this week prefilled; accept to the roster; Publish; Copy to next week offered; Find someone on an empty roster slot; the record panel

### Implementation

- [x] T048 [US6] `generate(..., progress=, should_stop=)` in `scheduling/engine.py` and the solution callback with `stop_search` in `scheduling/cpsat.py` (FR-027, FR-028, research P7)
- [x] T049 [US6] Worker: `needed`, `best_filled`, `progress_at` writes, the stop flag refreshed every 2 s, `ended` and `cancelled` in `apps/api/src/hds/worker.py` and `services/schedule_job.py`
- [x] T050 [US6] Contract first: `POST /schedule-jobs/{id}/stop`, the job's `progress`, `ended`, `stop_requested`; `GET /schedule-jobs/{id}/record`; `GET /shifts/{id}/cover`; routers; regenerate the client
- [ ] T051 [US6] Client `RunProgress`: best so far beside 007's elapsed bar; "Stop and use best so far", Cancel before any result; `RunFinishedNotice` in `apps/client/src/design/` polling a person's running runs every 5 s (FR-029, research P15)
- [ ] T052 [US7] Client `RecordPanel` on the run page, paged
- [ ] T053 [US8] Client: "Generate this week" on the roster, accept returning to the roster's accepted range, "Copy to next week" after a publish, "Find someone" on an empty roster slot opening the cover list (FR-032 to FR-034)
- [ ] T054 [P] [US6] Locale keys for progress, stop, notices, the record and the flow; `docs/hds-api.md`; replace 008's preview placeholder, if any, in `apps/client/src/screens/previews/`

### Close

- [ ] T055 Close group D as the template says. Mutants (≤ 3, who can see what): the record readable by a Manager who manages only some of the run's projects; Stop accepted from a Manager outside the run's projects; Find someone answering for a shift in a project the Manager doesn't manage. Commit `feat(scheduling): best so far and stop, the run record, and one scheduling flow (…)`. **PR 4 ready**: once-per-PR steps

---

## Dependencies & Execution Order

- Setup (T001, T002) first.
- Group A (PR 1) holds migration 034 and the working copy, which every later group uses.
- B depends on A (the working copy for cover); C depends on A (edits for lock ops) and B (the
  explanations' numbers feed the chips' causes); D depends on A and C (the starting point for "Run
  again", the cover function for Find someone).
- Within a group: tests first and failing → contract → backend → client → close.

## Parallel Opportunities

- Within a group's "Tests first", every [P] task touches its own file.
- Locale and documentation tasks [P] run beside the client tasks.

## Implementation Strategy

- MVP is PR 1 (US1, US2): reviewing as a change, accepting in parts and editing already remove the
  main reason managers redo proposals by hand.
- Each later PR ships on its own, and leaves the app working.
