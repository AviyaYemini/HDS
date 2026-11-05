# 004 decision log

Owner instruction (2026-11-05): "Go with recommended to all - your suggestions are logical. Run until
youre stuck or ready to merge the feature - based on the existing harness." The standing rules of 003
apply: tests on a private `_test` database (`hds_p3_test`), nothing else using it; no push; no edit to
committed migrations; no test deleted or weakened, and every test change explained here; mail after the
response; each group failing tests first, then code, mutation checks, the gates, /hds-review and
/code-review high, the log and a commit; stop on an unexplained failure, a change to a spec.md
requirement, a security regression, an unplanned migration, a push or anything destructive.

## Stages

- **Specify**: two owner answers (slots from staffing needs; drafts only change), recorded in
  spec.md Clarifications.
- **Clarify**: four questions, the recommended option each time (Managers and Admins as candidates
  only with an open entry; no entries means free, listed; project dates bound the slots; the run's
  surroundings).
- **Plan**: research G1 to G14. G1: OR-Tools has no wheel for Python 3.15, and the local environment
  had drifted to 3.15 while the image, mypy and docs/hds-spec.md say 3.12, so the API is pinned to
  3.12. G2: `ortools` is imported by `cpsat.py` only; the purity test is widened for it.
- **Tasks**: T001 to T027 in groups A to G.
- **Analyze** (inline):
  - Every FR and SC has a task.
  - The only drift is in names: the spec's "waiting" and "finished" job states are the prompt's
    `queued` and `succeeded`, now noted in contracts/api.md.
  - No constitution conflict beyond the noted ortools import (plan, Complexity Tracking).

## Group A: Python 3.12 and OR-Tools (T001, T002)

- `apps/api/.python-version` pins 3.12. uv installed CPython 3.12.16 and recreated `.venv`, which had
  drifted to 3.15.2. `ortools>=9.15` resolved to 9.15.6755, published 2026-01-14.
- **Test change: `tests/unit/scheduling/test_purity.py`.** It allowed the standard library only in
  every module of the package. Now `cpsat.py` may also import `ortools`; every other module stays
  standard library only, and the bare-interpreter test also asserts that `import scheduling` loads no
  `ortools`. The reason: the plan prompt and docs/hds-spec.md section 5 put CP-SAT in the pure
  package. Constitution III forbids app, database and network imports, not a solver library
  (research G2, Complexity Tracking). The test is no weaker for every other module.

## Group B: the pure engine (T003 to T007)

- **Built:**
  - `problem.py`: types and reason codes.
  - `eligibility.py`: one `Roster.refusal` for both solvers and the reasons.
  - `greedy.py`: research G5.
  - `cpsat.py`: research G4.
  - `generate.py`: solve, repair pass, a final `validate` check, reasons, summary and changes.
  - `rules.entry_covers`: the one reading of "does an entry cover this shift", now shared by
    `validate` and the generator. A refactor; the 003 rule tests are unchanged and green.
- **Design change during the build: CP-SAT solves the goals in stages, not as one weighted sum.**
  Keeping FR-017's order strict with tier weights needs coefficients near 10^15, which hurt the
  solver's relaxation. Each stage now keeps the earlier best values as constraints and starts from
  their solution. `optimal` means every stage was proven. Research G4 is updated.
- **Reason order:** unavailable, then overlap, minimum rest and weekly maximum against fixed shifts,
  then "needed elsewhere" for clashes with slots the proposal filled (research G6, updated to match
  FR-020's list).
- **Tests:**
  - 81 unit tests: eligibility, and generation for both solvers.
  - 3 Hypothesis properties: both solvers break no hard rule; CP-SAT, when optimal, fills at least
    as many places as greedy. At 1,000 examples each they take about 14 seconds.
- **CI gap found and fixed:** `tests/property` was never run in CI. The unit job ran `tests/unit`
  only, so 003's rule properties never ran there either. The job now runs `tests/unit` and
  `tests/property`, with `HDS_GENERATE_EXAMPLES=1000`.
- **Mutations:**
  - All 19 caught (B1 to B19).
  - The first run left 8 uncaught. 6 were gaps in my tests and are now covered:
    - the repair pass (B7) and the `optimal` flag (B18) only matter when the search runs out of time:
      a zero-budget test, and a stage forced to report "feasible";
    - the runs (B10), hours (B11) and preference (B13) tests passed by the solver's own
      tie-breaking: they now use bigger cases, and the runs test runs both ways round;
    - greedy's preference order (B14): the preference test now runs both solvers;
    - fixed nights in the summary (B19): a summary test.
  - B17 (skipping the final `validate` check) is defence in depth: no correct solver can trigger it,
    so it has a direct test of `_check`.
  - B3 (the fixed-shift check skipped) changes only the reason given, never the roster, so the
    property tests cannot see it; the unit tests catch it.
- /hds-review: one finding, fixed. The package docstring still said "standard library only"; it now
  names the `cpsat.py` exception.
- /code-review high: ten findings. The tenth (Groups A and B uncommitted together) was already
  resolved: Group A was committed alone (`6f4357d`) before the review finished. Fixed, each with a
  failing test first except the clean-ups:
  1. **The repair pass ignored the minutes the solver had already given.** It balanced on fixed
     shifts only, so it could pile empty places on one person. It now counts every chosen slot
     (`worked_minutes`).
  2. **A draft that already holds non-candidates could be over-filled.** Their places were kept
     outside the slot. `Slot.kept` now holds them: they take places (`Slot.places`), count as fixed
     for their own rules, and are judged with the slot in the final check.
  3. **The `generate` function hid its own module**, so a patch of `hds.scheduling.generate._repair`
     silently did nothing. The module is now `engine.py`.
  4. **The time limit started after the model was built.** The clock now starts when `generate` is
     called, and CP-SAT takes a deadline.
  5. **Run days were read in the organisation's zone** for fixed shifts but in the site's zone for
     slots. Every booking now carries its own site-local day. No test: it needs two zones on one
     roster, which the services never build.
  6. **Minutes were rounded down for the weekly limit.** They are now rounded up (`problem.minutes`),
     so the limit is never crossed by seconds.
  7. **`Slot.day` was stored and could disagree with `starts_at`.** It is now derived.
  8. **The place-filling loop was copied.** Greedy and repair now share `eligibility.fill`.
  9. **Eligibility ran its checks twice.** It is now one pass that tells fixed clashes from chosen
     ones.
- Mutations after the fixes: 23 of 23 caught. New ones: B20 repair ignoring chosen minutes; B21 kept
  people taking no places; B22 minutes rounded down; B23 the final check ignoring kept people,
  defence in depth like B17, with a direct test.
- **The Group B commit (`3067ba1`) went in with one failing test (my mistake).** The commit ran in
  parallel with the unit and property run, before its result was read. The failing test was
  `test_time_money_invariants.py::test_source_has_no_float_annotations_or_calls_on_money`
  (Constitution VII bans the name `float` in `src`): the CP-SAT deadline was annotated `float`. The
  test is right, so the code changed: the deadline is now integer nanoseconds
  (`time.monotonic_ns()`). Fixed in the next commit; from here on, a commit waits for its tests.
