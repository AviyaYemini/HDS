# Research: Auto-scheduler review

Decisions for 008's plan. Each gives the decision, why, and what else was weighed.

## P1. No new dependency

- **Decision**: build everything with the existing stack: OR-Tools CP-SAT, the worker and its
  Postgres-backed queue, 007's client and design system.
- **Rationale**: the plan prompt asks for none, and every need below fits what is there.
- **Alternatives**: a push or websocket channel for progress (rejected: the client already polls a
  running job; push belongs to feature 010).

## P2. Locks are `kept` people

- **Decision**: a locked assignment on a draft in the run's range enters the problem as one of its
  slot's `kept` people; a lock in an earlier proposal does the same for the rerun. Locked people take
  their places, and their hours, nights and rules count, exactly as `kept` already does (004).
- **Rationale**: the engine already handles people a slot must keep, in both solvers, in `validate`
  and in the summary. No new constraint is needed, so H6 cannot be broken by a solver.
- **Alternatives**: a fixed `x = 1` per locked pair in CP-SAT (rejected: greedy and the repair pass
  would need the same rule twice).

## P3. The two plain controls

- **Decision**: the spec's first control, "fill every slot, or avoid placing more people than a slot
  needs", cannot change anything: no solver ever places more people than a slot's headcount (004
  FR-017, a hard bound). It becomes **"Keep changes small" or "Rebalance freely"**: whether fewest
  changes against the starting point ranks right after the chips, or last as today. The second
  control, **how strictly to respect "would rather not"**, sets whether employees' open and close
  wishes rank before the balance of hours and nights ("strictly", today's order) or after it
  ("gently"). Neither ever ranks above filling every slot. FR-017 is amended to match.
- **Rationale**: both choices change a real trade-off, and both are words, not weights (AS6).
- **Alternatives**: exposing the weights (rejected by AS6), or a coverage target below 100%
  (rejected: the original's first goal is to cover every slot).

## P4. Goal order

- **Decision**: the lexicographic stages of `cpsat.solve` become, in order: fill every place; chips;
  fewest changes if "Keep changes small"; wishes and balance in the control's order; consecutive days;
  fewest changes if "Rebalance freely". Greedy keeps its order and uses chips as tie-breakers in its
  candidate ranking.
- **Rationale**: the clarification puts chips second; the stages already exist, and the order is a
  list.

## P5. Chips

- **Decision**: six kinds, each naming one person: at most N shifts, at most N nights, at most N hours
  in the range; keep on a site or project; keep off a weekday or shift type; prefer a weekday or shift
  type. In CP-SAT, a cap is a soft linear bound with one overflow variable; keep-on and keep-off
  penalise each assignment outside or inside; prefer rewards each assignment inside. The chip stage
  minimises the total, each chip weighted equally. A chip is met when its penalty is zero (prefer:
  when at least one assignment matches, or none could). Chips are stored on the job, never shared
  between runs; a rerun copies them (clarification).
- **Rationale**: a fixed set the manager reads as words, with no language model (O17); one stage keeps
  them below coverage and above wishes.
- **Alternatives**: chips as hard rules (rejected by the clarification).

## P6. Starting point and stability

- **Decision**: `Problem.current` stays the drafts; a new `Problem.start` (default: `current`) holds
  the starting point. For a rerun from a proposal, `start` is that proposal's working copy. CP-SAT
  gets `start` as solution hints; "fewest changes" counts against `start`.
- **Rationale**: AS7: a starting point cuts solve time and keeps a rerun stable.

## P7. Progress and stop

- **Decision**: `generate` takes two optional callables, `progress(filled, needed)` and
  `should_stop() -> bool`. In CP-SAT, a solution callback on the first stage calls `progress` with
  the places filled and checks `should_stop`, calling `stop_search` when it says so. A stop ends every
  later stage, keeps the best values reached, runs the repair pass and `validate` as usual, and marks
  the result not optimal and stopped. The worker passes a `progress` that writes to the job at most
  every 2 s (from the solver's thread, through the event loop), and a `should_stop` that reads a flag
  the worker refreshes from the job's `stop_requested_at` every 2 s. Before any result, Stop is
  Cancel: the job ends as cancelled, with no proposal.
- **Rationale**: plain callables keep the package pure; the 2 s rhythm meets SC-007.
- **Alternatives**: a separate progress table (rejected: one row per job is enough).

## P8. The working copy and its version

- **Decision**: `schedule_jobs.result` stays the solver's result, untouched. `current` holds the
  working copy: the same shape, plus per assignment `by` (solver or person), `locked`, `override`
  (warning code and reason) and `accepted`. `version` counts every change; edits and accepts send
  `If-Match` with it and are refused with 412 when stale. Undo replays the edits from the record minus
  the last one.
- **Rationale**: one JSON per job keeps reading a proposal one query; the version stops lost
  updates; the record already holds every edit, so undo needs no second store.
- **Alternatives**: rows per proposal assignment (rejected: proposals are short-lived, and expire
  after 7 days).

## P9. Accepting a part

- **Decision**: an accept names days, projects or assignments (or nothing, for all that is open).
  Under 004's locks, the service rebuilds the problem. If the job's whole fingerprint still matches,
  the part is stored as today. If not, the part is still accepted when (a) none of its drafts changed
  since the proposal and (b) applying it to today's roster breaks no rule that is not already an
  override in the working copy. Otherwise it is refused with `schedule.part_out_of_date`, naming each
  day and the cause. The other parts stay open.
- **Rationale**: FR-006: a change on Thursday must not block Monday. Checking the part against today's
  rules is exact where a per-day fingerprint would miss the weekly maximum, which spans the week.
- **Alternatives**: a fingerprint per day (rejected: the weekly maximum and rest cross days).

## P10. Explanations with names and numbers

- **Decision**: `Roster.detail(person, slot)` returns the refusal code with its numbers: for minimum
  rest, the rest they would have and the rest needed, in minutes; for the weekly maximum, the minutes
  already worked that week, the slot's minutes and the maximum. `Unfilled.reasons` keep their counts
  and gain `people` for rest and the weekly maximum. Each explanation is built step by step from these
  checks, never from CP-SAT's infeasibility core.
- **Rationale**: AS4: small, checkable steps; the core is unhelpful once an objective is set.

## P11. Who else could cover

- **Decision**: `scheduling.cover.options(problem, assigned, key)` checks every active candidate
  against the slot with today's rules over all their shifts in every project, the drafts and the
  working copy. People free come first, ranked by: no close wish on it; then nights over the
  average after it; then minutes over the average; then weekend shifts over the average; then name.
  People who would break a rule follow, with the rule and numbers. The API sends, per person, only
  the code and the numbers, never another project's shift, its site or its pay (FR-016).
- **Rationale**: the owner's decision of 2026-11-08: across both kinds of project, with every shift
  checked.
- **Alternatives**: ranking by pay cost (rejected: never pay, clarification).

## P12. Fairness and the trade-off summary

- **Decision**: `scheduling.fairness.panel(problem, assigned)` gives, per person in the run, minutes,
  nights and weekend shifts in the range, locked and overridden ones included, with the run's
  averages. An outlier is at least 2 nights or 2 weekend shifts over the average, or 20% over the
  average hours (and at least 4 h). A weekend shift starts on Friday or Saturday in the site's zone,
  the Israeli weekend. `fairness.tradeoffs` gives the counts the summary words: empty places, open
  wishes not given, close wishes given, the range of hours and of nights, chips not met. The server
  sends numbers; the client words them through the locale files.
- **Rationale**: AS10, AS11; numbers the manager can check, no score.
- **Alternatives**: standard deviations (rejected: teams of 5 to 30 make them noisy).

## P13. Edits and overrides

- **Decision**: an edit is assign, unassign, move, swap, lock or unlock, on the working copy. After
  each, `validate` runs over the person's shifts in every project with the working copy; the warnings
  come back with the answer. A warning the roster allows overriding (all but an inactive employee,
  003 FR-116) needs `override_reason` in the edit, or the edit is refused with
  `schedule.override_reason_required` and the warnings. On accept, an override becomes a
  `rule_overrides` row for the new assignment, as 007's roster override.
- **Rationale**: one rule set for the roster and the proposal (O10).

## P14. The run record

- **Decision**: `schedule_job_events` rows: job, kind, actor, time, and a small JSON body. Kinds:
  created, progress-milestone (only the final best), finished, stopped, cancelled, failed, edited,
  undone, locked, unlocked, overridden, accepted (with the part), discarded, expired. The repository
  has only insert and read; a trigger raises on UPDATE and DELETE. Read by an Admin, or a Manager of
  every project in the run.
- **Rationale**: FR-030; feature 014 shows these rows later (owner, 2026-11-08).
- **Alternatives**: the coming audit log (rejected: 014 comes later and reads this).

## P15. The in-app notice

- **Decision**: while the person has a run they started still queued or running, the client checks
  its status every 5 s from the app shell, and shows a toast that opens the proposal when it ends.
  Nothing when the app is closed.
- **Rationale**: FR-029; push is feature 010.

## P16. One flow

- **Decision**: the roster's "Generate this week" opens the run form with the week and the projects on
  screen the person may run; the run page leads to its review; accepting returns to the roster at the
  accepted range; Publish stays 007's; after a publish, 007's copy-week tool is offered for the next
  week; "Find someone" on an empty roster slot calls the same cover function for that shift.
- **Rationale**: O18, built from 007's pieces.
