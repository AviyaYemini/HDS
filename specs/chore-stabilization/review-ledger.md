# Review ledger: stabilization after 008 (chore lane)

Every finding of a review of this feature (/code-review, /hds-review, the reviewer session), one
row each. Status is `open`, `fixed` or `won't-fix`; a won't-fix row says why in the finding. The
commit that fixes a row names its id in its message, for example `fix(api): … (R12)`, so
`git log --grep R12` finds it. A group is done only when no row is open. /hds-status shows the
count.

| ID | Source | Finding | Status |
| --- | --- | --- | --- |
| R1 | /code-review (PR 1) | `infra/tests/rolling_update.sh:42`: the exemption covered only compat.sh, but the rolling-update proof also runs the base's release on the head's schema, under load, so migration 037 fails it on the pull request and on main; for the exempt step it starts from the head's own release, keeping the cluster, upgrade, load and Flux checks | fixed |
| R2 | /code-review (PR 1) | `infra/tests/compat.sh:51`: the exemption skipped the check for any base without the exempt file, so another migration in the same range went unchecked; a shared `compat_exempt.sh` exempts only the exact step from the revised migration onto the listed one | fixed |
| R3 | /code-review (PR 1) | `.github/workflows/ci.yml:271`: a change to the exemption list or its script did not trigger the compatibility or rolling-update job; both path filters name them | fixed |
| R4 | /code-review (PR 1) | `apps/api/tests/integration/test_migration_037.py:57`: the test checked names only; a row from before now follows the round trip to its organisation, and a stored replay is checked too | fixed |
| R5 | /code-review (PR 1) | `specs/chore-stabilization/plan.md:25`: the plan said 38 columns across 19 models, index renames and "awaiting approval"; it says 22 columns with their foreign keys, no index renames, approved | fixed |
| R6 | /code-review (PR 1) | `specs/chore-stabilization/plan.md:5`: the chore lane skips specify, clarify and analyze, which the working agreement asks of a feature; the owner approved the chore lane for this series on 2026-11-09, and it adds no product feature | won't-fix |
| R7 | /code-review (PR 1) | `docs/adr/021-organisation-spelling-in-one-step.md:39`: idempotent replays stored before the migration kept the old field name; migration 037 renames it in them, and the downgrade back | fixed |
