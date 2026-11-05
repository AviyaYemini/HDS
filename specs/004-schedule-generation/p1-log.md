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
