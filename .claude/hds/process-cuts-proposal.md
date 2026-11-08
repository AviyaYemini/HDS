# Proposal: faster pull requests (process cuts)

**Status**: Decided by the owner on 2026-11-07, after 006 PR 4. The proposal below is kept as it
was written; where a decision differs from it, the decision wins. **Superseded in part on
2026-11-08** by the owner's working rules in [CLAUDE.hds.md](CLAUDE.hds.md): one `/code-review` per
pull request at PR-ready and no reviewer session, mutants only for sign-in, pay and who can see
what, and only the changed tests locally, with the full suites in CI.

## The owner's decisions

1. **The ledger**: drop the "Fix commit" column.
2. **The log**: drop the log altogether. There is no `decisions.md` either: an owner's choice or an
   accepted limit goes where it already lands (the commit message, `tasks.md`, `research.md`).
   006's `p1-log.md` stays as it is.
3. **Reviews**: the reviewer session once per PR, as cut 3. No group is marked high-risk, so there
   is no per-group reviewer exception unless the owner names one later.
4. **Mutants**: by risk, at most three a group.
5. **The generated-client drift check**: kept in the scoped run for API changes.
6. **The working-agreement text**: agreed, as written below.

Where each one lives:
- **1**: `.claude/hds/review-ledger-template.md`, `ledger_counts` in `.claude/hooks/hds_hooks.py`
  (it reads both shapes), and `/hds-review`.
- **2 to 4**: the group close in `.specify/templates/overrides/tasks-template.md`.
- **5**: `scripts/ops/ci_local.sh`.
- **6**: `.claude/hds/CLAUDE.hds.md`.

None of this needs a constitution amendment. Principle IX requires the full suite on every pull
request and names CI as the authority, which is where it keeps running. Nothing in the constitution
requires the ledger's commit hashes, the narrative log, more than one review, or mutants: those come
from the working agreement (`.claude/hds/CLAUDE.hds.md`), the ledger template, and the "each group
ends with" block written into each feature's `tasks.md`.

- **Owner**: the cut changes the working agreement or the tasks convention. It waits for your yes.
- **Harness**: scripts, hooks or templates. I implement these once PR #13 has merged.

## Where PR 4's nine hours went

The I–L branch was created at 08:16, and the last commit the log covers was made at 17:16 (from
the branch's reflog).

| Stretch | Time | What happened in it |
| --- | --- | --- |
| Group I | 1 h 29 | 27 mutants (6 of them for `/code-review`'s fixes), both reviews, the log, a commit, a ledger commit |
| Group I, the reviewer's fixes | 21 min | R122 to R127, 5 more mutants, a fix commit, a ledger commit |
| Signing (T047c, Proposed) | 36 min | 8 mutants, a commit, a ledger commit |
| Group J | 1 h 55 | 10 mutants, the same closing steps |
| Group K | 1 h 18 | 16 mutants, the same closing steps |
| Group K, the reviewer's fixes | 26 min | R184 to R187, 4 mutants, a fix commit, a ledger commit |
| Merging main (#12) | 35 min | for the new `changes` job |
| Group L | 1 h 12 | the rolling-update proof, the closing steps |
| Group L, the reviewer's fixes | 1 h 08 | R200 to R205 (R205 from a review of the fixes), 4 rolling-update mutant runs (the first pair invalid), 6 commits, 3 of them bookkeeping |

What the nine hours produced, apart from the code:

- **Commits**: 22, of which 10 are `docs(spec)` bookkeeping: "record the commit in its ledger
  rows" after every commit, and log entries.
- **The ledger**: 110 new rows (R96 to R205), about 280 characters each. By source: the reviewer
  session 46, `/code-review high` 40, `/hds-review` 12, the session itself 12.
- **Mutants**: about 75. The log lists about 70 for I to K; L adds two pairs of rolling-update
  runs and the compatibility mutant.
- **The log**: `p1-log.md` grew by 370 lines (lines 596 to 965), 38% of the whole file, for one PR
  of four groups.
- **Gates**: the full `make ci-local` at each group's end and again after fixes (Group K's first
  run failed and ran again, R183), so about 7 runs. Main's script took 8 min 25 s on this Mac
  (measured 2026-11-07, warm caches, a busy machine), and I–L's adds five infra steps:
  - the Helm checks: 4 min 51 s;
  - the client: 1 min 51 s;
  - every API step together: 1 min 15 s;
  - the rest: under 30 s.

## The cuts

### 1. The ledger: no commit hashes (Harness, but it changes the format you approved in PR 9)

- **Today**: a fixed row names its fix commit, so every commit is followed by a second commit
  that writes its hash into the rows.
- **The hashes don't survive the merge**: main is squash-merged, so the hashes recorded for
  PRs 2 and 3 (`1271faf`, `884f31b`, `0cbda96`, `d0866bd` …) are on no commit of `main`. They live
  only on the PR branches.
- **Proposed**: drop the "Fix commit" column. A fix commit's message names the rows it fixes,
  for example `fix(infra): … (R122, R123)`, and the squash commit keeps that text, so
  `git log --grep R122` finds the fix. Statuses and "a group is done only when no row is open"
  stay.
- **Harness work**: the template, `ledger_counts` in the hooks (the status cell moves from
  second-to-last to last), its tests, and `/hds-review`'s text. Existing ledgers keep their column;
  the parser reads both shapes.
- **Saves**: the 10 bookkeeping commits and the hash edits, about 10 to 20 minutes a PR
  (estimate: each one is quick, but it comes after every commit).

### 2. The log: decisions only (Owner)

- **Today**: `p1-log.md` narrates every run: outputs, timings, every mutant, and a recap of
  findings that the ledger already holds.
- **Proposed**: a `decisions.md` per feature, holding only what no other file records:
  - your choices;
  - what was settled with the reviewer;
  - deviations from `tasks.md`;
  - accepted limits;
  - process slips.

  About 10 lines a group. The failing-first evidence goes in one line of the commit body ("failed
  first: `test_x`, on the missing check"). Run outputs stay in CI's logs. 006's `p1-log.md` stays
  as it is.
- **Saves**: about 45 to 90 minutes a PR (estimate: about 90 checked lines a group today, each one
  re-read against a run's output).

### 3. Reviews: one per group, the reviewer session once per PR (Owner)

- **Today**: each group gets `/hds-review` and `/code-review high`, then the reviewer session
  reviews the commit before the next group starts. Each review's fixes repeat the full closing
  steps: gates, mutants, ledger, log, a fix commit and a ledger commit. The reviewer then reviews
  the fixes too: R188 came from K's fix commit and R205 from L's.
- **Proposed**:
  - **per group**: `/code-review high` only. It made 40 of the two in-session reviews' 52
    findings.
  - **once per PR**: `/hds-review`, at PR-ready. Its 12 PR 4 findings were PR-level: the docs
    index, `infra/README.md`, ADR 013's scope. The mechanical rules are already in the hooks.
  - **the reviewer session**: once per PR, on the PR-ready branch, with all its fixes in one
    commit. Before a group starts it can still settle open questions (the "gaps settled first"
    talks), which is cheap and prevents rework.
  - **groups marked high-risk** in `tasks.md` (security, migrations, release trust) keep the
    reviewer's per-group review.
  - **the reviewer's additions** that are not defects against the spec or `tasks.md` (Group K's
    "additions 1 to 5", for example) become Proposed follow-ups instead of joining the group
    (Constitution I).
- **Risk**: a design defect found at the PR's end can mean rework across groups. The pre-group
  check and the high-risk marking are there to catch those.
- **Saves**: PR 4's post-commit review tails took 21, 26 and 68 minutes. The real fixes still
  happen, but the re-gate, ledger, log and commit cycle runs once instead of three times. Add three
  fewer `/hds-review` runs: about 45 to 75 minutes a PR (estimate).

### 4. Mutants: chosen by risk, capped, named up front (Owner)

- **Today**: each group's closing task says "mutation checks, where code was written". In practice
  every review fix got mutants too: about 75 in PR 4.
- **Proposed**: a mutant only where a passing test could be hollow:
  - a test that could not fail first (the behaviour already existed);
  - a security, money, time or data-loss rule.

  At most three a group, named in the group's closing task when `tasks.md` is written. A review fix
  gets none unless it falls into that class: its own test failing first is the evidence. One mutant
  per file at a time (R120 came from two mutants sharing a file), always with the hooks on.
- **Saves**: about 75 mutants become 12 at most. At 1 to 2 minutes each (edit, run, revert,
  check), that is about 1 to 2 hours a PR (estimate). A rolling-update run alone takes about
  3.5 minutes, and Group L ran four of them for its mutants.

### 5. Gates: scoped while building, full once at PR-ready (Harness; the tasks wording is Owner)

- **Per turn**: unchanged. The Stop hook runs ruff, mypy and the unit tests for what the session
  edited.
- **Group close**: `make ci-local` becomes scoped. It runs only the steps for the paths changed
  since the merge base with `origin/main`, using CI's `changes` mapping. The cheap repo-wide checks
  (gitleaks, commitlint, Markdown links) always run.
- **`alembic check` leaves the per-group gate list**: it needs a database. It runs in
  `make ci-local-db`, in CI, and in a group only when models or migrations changed.
- **Checked**: no hook, stored prompt or Spec Kit template asks for `make ci-local-db` per turn or
  per group. The Stop gate never runs a database. The only per-group database step anywhere is
  `alembic check` in the gate list of 006's `tasks.md`.
- **PR-ready, once**:
  - `make ci-local-full`;
  - `make ci-local-db`, only when `apps/api/` or the 001 contract changed (CI's own rule).
- **The full suite**: CI only.
- **One question for you**: the client job holds the generated-client drift check (Constitution
  V), which an API change can break. You asked for API-only changes to skip the client steps. I
  would keep that one check for API changes; it needs the client's install, about 1.5 minutes.
- **Saves** (from the measured step times):
  - **a docs-only change**: about 20 seconds of checks (links, gitleaks, commitlint) instead of
    8.5 minutes;
  - **an API-only change**: about 2 minutes, or 3.5 with the drift check;
  - **a chart change** (most of PR 4): the Helm checks still run, so it saves only the API and
    client steps, about 3 minutes a run, or 20 minutes over PR 4's 7 runs.

  The largest saving, `make ci-local-db` (about 25 minutes) no longer running at each group's
  end, came from your rule of 2026-11-07 and is not counted here.

### 6. The Helm checks in one container (Harness)

- **Today**: `infra/helm/tests/assert.sh` starts a container for every yq assertion, about 130 on
  main and more on I–L. That is most of its 4 min 51 s.
- **Proposed**: run all of the assertions in one container. This is the same change for CI's
  `chart` job, which runs the same script.
- **Saves**: about 4 minutes on every full run and every run that touches the charts. That is
  about 30 minutes over PR 4's 7 runs (estimate: a container start is about 2 seconds here).

## The group close, as `tasks.md` would carry it (Owner)

This replaces the five-step "each group ends with" block, for future features and for 006's
Group M.

> **Each group ends with**: the scoped `make ci-local` green; `/code-review high`, its findings in
> the ledger and none open; the risk mutants named in the closing task, if any; the group's
> decisions in `decisions.md`; one commit, whose message names the ledger rows it fixes.
>
> **When the PR is ready to push, once**: `make ci-local-full`; `make ci-local-db` if `apps/api/`
> or the 001 contract changed; `/hds-review`; the reviewer session; their fixes in one commit.

## The working agreement, as it would read (Owner)

In `.claude/hds/CLAUDE.hds.md`, the line "Implement one task phase at a time, run the tests, then
commit on the feature branch" becomes:

> Implement one task phase at a time. Test the phase and what it touches: the Stop gate, then the
> scoped `make ci-local`. Then commit on the feature branch. The full suite runs in CI. Locally,
> `make ci-local-full` and `make ci-local-db` run once, when the pull request is ready to push.

## The effect on a PR like PR 4 (estimates)

| Cut | Saved per PR |
| --- | --- |
| 1. No commit hashes in the ledger | 10 to 20 min |
| 2. Decisions instead of a narrative log | 45 to 90 min |
| 3. One review per group, the reviewer once per PR | 45 to 75 min |
| 4. Mutants by risk, at most three a group | 60 to 120 min |
| 5. Scoped gates | about 20 min (an infra PR); more for API or docs PRs |
| 6. The Helm checks in one container | about 30 min |
| **Total** | **about 3.5 to 6 hours of PR 4's 9** |

Only the gate times and the reflog's stretches are measured; the rest are estimates from the
counts above. The cuts overlap (a review cycle that no longer runs writes no log and runs no
mutants), so expect the lower end: a PR like PR 4 in about 5 hours rather than 9.

## What stays

- The constitution, unchanged. The hooks keep enforcing its mechanical rules.
- The test-database rules: `hds_p3_test` only, one run at a time.
- Failing tests first, and one commit per group.
- The ledger, with every finding a row, and a group done only when none is open.

## Decisions for you

1. **The ledger**: drop the "Fix commit" column (recommended), or fill it once at PR-ready.
2. **The log**: `decisions.md` from 006 Group M on, or keep `p1-log.md`.
3. **Reviews**: the reviewer session once per PR; and which kinds of group count as high-risk.
4. **Mutants**: by risk, at most three a group.
5. **The generated-client drift check**: keep it in the scoped run for API changes (recommended),
   or skip every client step as first asked.
6. **The working-agreement text** above.
