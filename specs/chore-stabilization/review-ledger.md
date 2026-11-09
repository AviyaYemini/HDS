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
| R8 | CodeRabbit (PR 25) | `apps/api/src/hds/scripts/find_email_case_duplicates.py:62`: the script runs before migration 016, on a schema that still says `organization_id`, but read the renamed model's column; it reads whichever the user table has | fixed |
| R9 | /code-review (PR 2) | `apps/client/src/screens/runs/_rw_head.txt:1`: two scratch files from splitting ReviewWeek were committed in the source tree; deleted | fixed |
| R10 | /code-review (PR 2) | `apps/client/src/screens/runs/ReviewWeek.tsx:374`, `EmptyReasons.tsx:49`: doc comments left at the end of the files, describing code that moved; removed, each kept on its own declaration | fixed |
| R11 | /code-review (PR 2) | `apps/client/src/screens/roster/AssignSheet.tsx:46`: the sheet took over the cover and busy reads, against the plan's hook-holds-the-calls rule; `useAssignInputs` reads them, and the sheet takes them as props again | fixed |
| R12 | /code-review (PR 2) | `apps/client/src/screens/my-work/availability/AvailabilityScreen.tsx:67`: the constraints read was written twice (the hook's load and the screen's fresh read), and the writes stayed in the screen; the hook now holds the one read, the plan runner and the queued send. The cell edits, undo, copy and submit stay in the screen: they are woven into its draft, ledger and toast state, and moving them would change more than a split may | fixed |
| R13 | /code-review (PR 2) | `apps/client/src/screens/roster/RosterScreen.tsx:230`: the screen changed the edit hook's state through its raw setters; the hook offers `dropWarning`, `clearHistory` and `dismissStale` | fixed |
| R14 | /code-review (PR 2) | `apps/client/src/screens/runs/useRuns.ts:12`: the runs hook imported a type from the form view, and the pure `fromPreset` lived in it; `Offered`, `Again` and `fromPreset` are in `runs/model.ts`, where the test reads it | fixed |
| R15 | /code-review (PR 2) | `apps/client/src/screens/runs/RunsScreen.tsx:31`: the screen still read the "run again" job itself; `useAgain` in `useRuns.ts` reads it | fixed |
| R16 | /code-review (PR 2) | `apps/client/src/screens/roster/RosterScreen.tsx:22`: only the admin folder had an `index.ts`; roster, runs, account and availability have one, the routes import from it, and the plan records why roster and runs import each other's helpers directly | fixed |
| R17 | /code-review (PR 2) | `apps/client/src/screens/my-work/availability/AvailabilityScreen.tsx:75`, run and admin styles: a forwarding `runPlan` wrapper, and the `summary` and `checkbox` styles copied across files; the wrapper lives in the hook, the styles in `runs/runStyles.ts` and `admin/tableStyles.ts` | fixed |
| R18 | Owner (PR 2) | `apps/client/src/screens/roster/AssignSheet.tsx:120`: someone busy then read twice ("On another shift then" and the ranking's "Already on another shift") and never said where; the sheet says it once, with the project and the time when the reader may see that shift, the time alone otherwise, and the move or keep choice names the project too | fixed |
