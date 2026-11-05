# 005 decision log

Owner instruction (2026-11-05): "Go with recommended for all, continue with the harness until it's time
to publish the PR". The standing rules apply: tests on the private database `hds_p3_test`, nothing else
using it; no push; no edit to committed migrations; no test deleted or weakened, and every test change
explained here; each group failing tests first, then code, mutation checks, the gates, /hds-review and
/code-review high, the log and a commit; stop on an unexplained failure, a change to a spec.md
requirement, a security regression, an unplanned migration, a push or anything destructive.

## Stages

- **Specify**: three owner answers (default rate Admin only, with pay switched per project; Managers see
  pay only on projects opened to them and assigned to them; reports count ended shifts), then the owner's
  direction on Israeli premiums and four follow-up answers (pay to date only; two switches per project;
  only assigned Managers; overtime per shift). The additions are marked Proposed in spec.md.
- **Clarify**: three questions (the weekly rest window Friday 18:00 to Saturday 22:00; organisation
  defaults reaching projects that follow them; special days per organisation with project overrides).
- **Plan**: research R1 to R12. R1: the prompt's `NUMERIC(12,2)` applies to money amounts, which are
  computed; the two existing rate columns stay `NUMERIC(12,4)` (Complexity Tracking).
- **Tasks**: T001 to T035 in groups A to G.
- **Analyze**, all recommendations applied:
  - C1: service unit tests with fake repositories added (T012a, T019a, T026a), as Constitution IV asks.
  - C2: skipped journeys for the admin-only stories US2 and US6 (T032).
  - I1: FR-003 now separates premium rates (100 to 300%) from overtime increases (above 0, at most 200
    points), matching data-model.md.
  - U1, U2: edge cases for reopening an ended project and deleting an organisation day that a project
    overrides.
  - C3: T012 also races a special-day change against ending a project.
  - A1: "today" is in the organisation's time zone. F1: FR-020 reworded.
  - C4: FR-002 is covered by the 003 tests in `tests/integration/test_shifts.py` and the role matrix.
