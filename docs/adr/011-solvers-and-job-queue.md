# ADR 011: Two solvers behind one function, and a table that is also the job queue

**Date**: 2026-11-05
**Status**: Accepted.

## Context

Feature 004 generates a roster for up to 31 days across every active project (original scenario B,
SCH-1). docs/hds-spec.md section 5 asks for a constraint-programming model (OR-Tools CP-SAT) with HDS
1.0's greedy algorithm kept as a baseline, a 20-second cap with an `optimal` flag, and generation as a
background job. The plan prompt adds a Postgres-backed queue with no extra broker. Constitution III
keeps every scheduling rule in the pure `scheduling` package; ADR 009 put the rules for manual
rostering there as one `validate` function.

## Decision

**One function, two solvers.** `hds.scheduling.generate(problem, solver=..., time_limit=...)` takes a
`Problem` (slots, fixed shifts, candidates, entries, limits) and returns a `Result`. Both solvers use
one eligibility test for the hard rules, then the same repair pass, the same reasons for empty places
and the same summary. `validate` judges every result before it leaves; a broken hard rule is a bug
and the result is never stored.

**The greedy baseline is rebuilt from the audit, with its flaws fixed.** The HDS 1.0 code is not in
this repository. The baseline fills slots in start order and never breaks a hard rule, so the
benchmark compares solvers, not bugs.

**CP-SAT solves the goals in order, not as one weighted sum.** Filling places, then preferences,
then the spread of hours, the spread of nights, long runs and changes: each stage keeps the best
value of the earlier ones as a constraint and starts from their solution. Tier weights strict enough
to keep the order needed coefficients near 10^15, which hurt the solver. `optimal` means every stage
was proven.

**OR-Tools inside the pure package, in one module.** Only `scheduling/cpsat.py` imports `ortools`;
`import scheduling` does not load it. The constitution forbids app, database and network imports,
not a solver library, and the model is the rules. The API is pinned to Python 3.12, which the image
already used, because OR-Tools has no wheel for 3.15.

**The jobs table is the queue.** `POST /schedule-jobs` stores a queued row and announces it with
`NOTIFY`. A worker (`python -m hds.worker`) claims the oldest job with `FOR UPDATE SKIP LOCKED`
under a 5-minute lease, builds the problem, solves it in a thread outside any transaction, and
stores the proposal only if the job is still its own (same attempt). A job whose lease runs out is
claimed again, up to three times.

**Accepting checks a fingerprint under locks.** The worker stores a SHA-256 of everything the problem
was built from. Accepting locks the job, the run's projects and the organisation's staff (in id
order, as the other paths do), rebuilds the problem and compares fingerprints before it writes the
drafts. Any change since is a 409, so a proposal is never applied to data it was not made from.

## Consequences

- Manual and generated rosters are judged by the same rules, in one package.
- A deployment runs at least one worker beside the API (`infra/docker-compose.yml` has one). Without
  one, jobs stay queued; nothing else breaks.
- The proposal is stored as JSON on the job, so a proposal can be read, accepted or discarded later
  without solving again; an open one expires after 7 days.
- Accepting briefly locks every staff account of the organisation. That is fine at the expected size
  (tens to a few hundred people) and is the price of the out-of-date check being exact.

## Alternatives considered

- **A task framework or a message broker.** An extra service to run and secure, against the plan's
  "no extra broker"; a table queue with `SKIP LOCKED` is enough at this volume.
- **Solving in the API process.** A 20-second CPU-bound solve would stall requests.
- **One weighted objective.** One solve, but the order of the goals is only approximate.
- **A CP-SAT adapter outside the package.** It would put rules outside `scheduling`.
