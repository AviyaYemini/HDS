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

## Group A: the pure pay package (T001 to T005)

- **Built:** `hds.payroll` (`settings.py`, `price.py`): `price()` cuts a shift at the rest-window
  ends, local midnights and the two overtime boundaries, gives each part one rate, and rounds the
  pay half up once. Standard library only; a purity test like scheduling's (`rglob`, so subpackages
  are checked too).
- **Tests:** 25 worked examples and edge cases in `tests/unit/payroll/test_price.py` (the spec's
  525.00, 600.00, 412.50 and 612.50, splits into and out of the rest, 200% days, DST nights of 7 and
  9 hours, rounding once); one Hypothesis property that compares `price` with an oracle walking the
  shift a minute at a time and following the site's clock (1,000 examples in about 2.5 seconds).
- **Mutation checks:** 16 mutants, all caught. The first rounding example passed one mutant by
  coincidence (per-part and per-line rounding gave the same 0.06); it was replaced before the commit
  with a case where they differ (0.00 against 0.01).
- **/code-review high**, 8 findings, all acted on:
  1. Rest-window ends at a time the clocks skip were read with `fold=0`, an hour late. Wall times now
     become "the first moment the clock shows that time or later" (`_first_showing`), for rest
     windows, nights and midnights alike; a unit test covers 02:30 on 2027-03-26.
  2. Part hours summed in 28 digits can differ from the line's hours. Lines now hold exact
     microseconds; `hours` is derived, and shown hours are rounded by whoever shows them, with totals
     summed from what is shown (documented on `PayLine`).
  3. The property test avoided 00:00 to 04:00 for rest-window ends, hiding finding 1. It now draws any
     minute of the week, adds zones whose clocks change at midnight (Havana, Santiago), and its
     oracle follows the clock forward instead of reading wall positions. **Test change, stronger.**
  4. Two special days on one date silently collapsed; `price` now refuses them, and callers merge a
     project day over the organisation's first (FR-008a).
  5. An unknown zone raised `KeyError`; it is now the documented `ValueError`.
  6. tasks.md T004 named a different shape; T004 now names the built one (`microseconds`, `night`).
  7. The purity test globbed one level; now `rglob`.
  8. `rate_missing` and `hours` are properties, `itertools.pairwise` replaces `zip`, and one helper
     turns wall times into instants.
- **After the commit:** a wider run replayed an example the mutation run had saved: a rest window of
  7 days less 1 minute (Sunday 23:47 to Sunday 23:46). `price` was right; the test's oracle was
  wrong, because one 15-minute step of its clock crossed both ends and it applied them in a fixed
  order. The oracle now applies them in the order the clock reaches them. **Test fix**, no code
  change; checked against every start minute of the week for that case.
