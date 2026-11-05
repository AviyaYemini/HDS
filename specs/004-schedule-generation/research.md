# Research: Schedule generation

Each decision resolves an unknown of the plan's Technical Context. The plan prompt fixes the solvers
(greedy and OR-Tools CP-SAT behind one signature, a 20-second cap and an `optimal` flag), the job
statuses (queued, running, succeeded, failed), a worker over a Postgres-backed queue with no broker,
Hypothesis property tests, a benchmark, and the three endpoints `POST /schedule-jobs`,
`GET /schedule-jobs/{id}` and `POST /schedule-jobs/{id}/accept`.

## G1. Python version and OR-Tools

**Decision**: pin the API to Python 3.12 with `apps/api/.python-version`, and add `ortools` (9.15, the
current release, January 2026) as a runtime dependency.

**Rationale**: docs/hds-spec.md section 7 names Python 3.12, the API image is `python:3.12-slim`, and
mypy checks against 3.12. Nothing pinned the interpreter, so the local environment had drifted to 3.15,
for which OR-Tools publishes no wheel (9.15 ships wheels for 3.9 to 3.14). The pin makes local, CI and
the image agree.

**Alternatives**: building OR-Tools from source for 3.15 (slow, unsupported); a pure-Python solver such
as PuLP with CBC (an external binary, and no CP-SAT, which the prompt names).

## G2. OR-Tools inside the pure package

**Decision**: the CP-SAT solver lives in `hds/scheduling/cpsat.py`, the only module of the package that
imports `ortools`. `import scheduling` does not import it; `generate(..., solver="cpsat")` loads it when
called. The purity test changes in one way: `cpsat.py` may import `ortools`, and nothing else in the
package may import anything outside the standard library. The change and its reason are logged.

**Rationale**: Constitution III forbids imports from other application packages, database calls and
network calls, so a solver library is allowed. docs/hds-spec.md section 5 places CP-SAT in the engine,
and the plan prompt places it in the pure package. Keeping it out of `import scheduling` keeps the
bare-interpreter test meaningful, and the greedy solver and the rules stay standard library only.

**Alternatives**: a solver adapter outside the package (the model is the rules, so the rules would
live in two places, which III forbids).

## G3. One signature for both solvers

**Decision**: `generate(problem: Problem, *, solver: Literal["greedy", "cpsat"], time_limit: timedelta)
-> Result`. `Problem` holds the slots to fill, the fixed shifts of the surroundings (published shifts,
other projects' shifts, and draft assignments of non-candidates) with their employees, the candidates,
their availability entries, the current draft assignments (for "fewest changes"), and the settings.
`Result` holds the assignments per slot, the unfilled places with reasons, the per-employee summary,
the changes against the current drafts, and `optimal: bool` (always false for greedy).

**Rationale**: the benchmark and the property tests run both solvers on the same input, and the worker
chooses one by name. The existing `validate` stays the single judge of a roster: both solvers' outputs
are checked with it in the property tests, and the worker refuses to store a result `validate` finds a
hard-rule warning in (defence in depth).

## G4. The CP-SAT model

**Decision**:
- A boolean per (candidate, slot) pair that survives the eligibility filter (active, not blocked or
  absent that date and shift type, no overlap or short rest against a fixed shift).
- At most `headcount` per slot. Pairwise "at most one" for any two slots of one candidate that overlap
  or are closer than the minimum rest. Per candidate and week, minutes of fixed shifts plus chosen
  shifts at most the weekly maximum (integer minutes).
- The objective is a weighted sum with weights spaced so the order of FR-017 holds: each unfilled place
  costs more than any total of the lower goals. Preferences: each open shift given earns, each close
  shift given costs. Hours and nights: the spread between the largest and smallest totals across
  candidates, with two integer variables each. Runs: a worked-day boolean per candidate and date, and a
  cost for every 7-day window that is worked on all 7 days (runs longer than 6). Changes: a cost for each
  current draft assignment dropped or added.
- `max_time_in_seconds` is the time limit; `optimal` is true only when the status is `OPTIMAL`. The
  search is made repeatable for tests with a fixed seed and one worker; production uses the default
  workers.

**Rationale**: weights in tiers give the lexicographic order of FR-017 in one solve. A proven optimum
then means optimal for the whole order.

**Alternatives**: solving goal by goal (several solves within one 20-second budget; harder to bound).

## G5. The greedy baseline

**Decision**: a reconstruction of HDS 1.0's greedy algorithm from the legacy audit, with its flaws
fixed, because the 1.0 code is not in this repository. Slots are taken in start order. Each place goes
to the eligible candidate (by the same eligibility test as G4, against the roster so far) with, in
order: no close preference, an open preference, the fewest hours so far, the fewest nights so far, the
current draft holder, then the smallest id. It never breaks a hard rule; it may leave places unfilled
that CP-SAT fills.

**Rationale**: the baseline must be fair to compare. Keeping the 1.0 flaws (date-only rest, "close" as
a block) would only measure them.

## G6. Why a place stays unfilled

**Decision**: after either solver, a repair pass gives any unfilled place to a candidate who can take
it without breaking a rule against the final roster (a solver stopped by the time limit may leave
one). Every place still unfilled is then explained by testing each candidate, and the first reason in
this order counts that candidate:
1. unavailable: blocked or absent that date and shift;
2. weekly maximum, minimum rest or overlap against a fixed shift (published, another project's, or a
   kept draft);
3. needed on another slot: the same rules, but only against slots the proposal filled, so taking this
   place would mean giving up another.

With no candidates at all, the reason is "no active employee". The counts per reason add up to the
number of candidates.

**Rationale**: FR-020 asks for the rule and how many employees it excluded. A test against the final
roster is cheap, gives the same answer for both solvers, and is checkable in tests. The repair pass
means "a candidate was free" never needs a reason.

## G7. The job queue

**Decision**: a `schedule_jobs` table is the queue. `POST /schedule-jobs` stores a `queued` row and
answers 202. A worker process (`python -m hds.worker`) claims the oldest queued job with
`SELECT ... FOR UPDATE SKIP LOCKED`, marks it `running` with a lease of 2 minutes, builds the problem,
solves it, and stores the result and `succeeded`, or `failed` with a code. A job whose lease ran out
(the worker died) is claimed again, up to 3 attempts, then marked `failed` with `jobs.worker_lost`. The
worker waits on `LISTEN schedule_jobs` with a 5-second poll as the fallback, so a new job starts without
delay. One worker runs per deployment by default; more can run safely because of the claim.

**Rationale**: the prompt asks for a Postgres-backed queue with no extra broker. `SKIP LOCKED` is the
standard way to share a table queue between workers; the lease handles a crash.

**Alternatives**: a task framework (an extra dependency, against "no extra broker"); running jobs in the
API process (a 20-second CPU-bound solve would stall requests).

## G8. Idempotent job creation

**Decision**: `POST /schedule-jobs` takes an optional `Idempotency-Key`, through the existing
`IdempotencyService` (003): a repeat answers the first job, and another body with the same key is
refused with 422 `idempotency.key_reused`.

**Rationale**: FR-009 and docs/hds-api.md ("writes that can be retried, job creation and assignment,
accept an `Idempotency-Key`").

## G9. Out of date on accept

**Decision**: the worker stores a fingerprint of the problem it solved: a SHA-256 of a canonical form of
every input (slots, fixed shifts, candidates and their active state, their entries, the current drafts,
the settings, the projects' needs and shift types). Accepting rebuilds the problem from the database
and compares fingerprints; a difference refuses with 409 `schedule.proposal_out_of_date`. Before
rebuilding, accept locks the run's projects (`FOR NO KEY UPDATE`, as setting shift types does) and then
the candidates' rows in id order (`lock_user`, as availability changes and assignments do), so nothing
the fingerprint covers can change between the comparison and the writes.

**Rationale**: one rule covers every case of FR-024, and it compares what was solved with what would be
solved now, not a list of tables that might change later.

## G10. What accept writes

**Decision**: in one transaction, for each slot of the proposal: an existing draft shift keeps its id and
headcount and gets its assignments replaced by the proposal's (dropped ones deleted, new ones added); a
new slot becomes a new draft shift with the need's headcount and the project's shift type times, and its
assignments. Generated assignments record `source = 'generated'` and `assigned_by` the accepter. The job
moves to `accepted`. Nothing is published.

**Rationale**: FR-023, and the 003 model: an Admin then edits drafts by hand (warnings only) and a Manager
publishes.

## G11. Settings and staffing needs

**Decision**: `GET /api/v1/organisation/scheduling` (Manager, Admin) and `PATCH` (Admin) read and change
`min_rest_hours` (1 to 24) and `max_weekly_hours` (1 to 168), which exist since 020. Staffing needs are a
table `staffing_needs` (project, shift type, weekday, headcount; unique per project, shift type and
weekday), read and replaced as a whole with `GET`/`PUT /api/v1/projects/{id}/requirements`, the path
docs/hds-api.md already names. Removing a shift type from a project or retiring it deletes its needs in
the same transaction (FR-002).

## G12. Proposal state and expiry

**Decision**: the job row carries the run's `status` (queued, running, succeeded, failed) and, once
succeeded, the proposal's `proposal_state` (open, accepted, discarded). A proposal older than 7 days
reads as `expired` and cannot be accepted; nothing deletes it. `POST /schedule-jobs/{id}/discard`
(Manager, Admin) closes an open proposal.

## G13. Benchmark

**Decision**: `apps/api/benchmarks/compare_solvers.py` builds seeded random weeks at 10, 30 and 100
employees across 3 projects, runs both solvers, and prints coverage, the hours and nights spread, and the
runtime. A pytest marker `benchmark`, deselected by default like `timing`, asserts SC-001: 30 employees,
one week, under 30 seconds with CP-SAT, and that CP-SAT covers at least as many places as greedy.

## G14. Who sees what

**Decision**: jobs and proposals are visible to Managers and Admins of the organisation; another
organisation's job answers 404; Employees and Clients get 403 on every new operation. A proposal names
employees, so it never reaches the employee views; `GET /me/shifts` only reads published shifts, as
before.
