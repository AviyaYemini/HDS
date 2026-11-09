# Review ledger: 008 auto-scheduler review

Every finding of a review of this feature (/code-review, /hds-review, the reviewer session), one
row each. Status is `open`, `fixed` or `won't-fix`; a won't-fix row says why in the finding. The
commit that fixes a row names its id in its message, for example `fix(api): … (R12)`, so
`git log --grep R12` finds it. A group is done only when no row is open. /hds-status shows the
count.

| ID | Source | Finding | Status |
| --- | --- | --- | --- |
| R1 | /code-review (PR 1) | `apps/api/src/hds/services/schedule_job.py:446`: an accept with any body, even `{}`, skipped the out-of-date refusal of an untouched whole proposal, because the dumped body is never empty | fixed |
| R2 | /code-review (PR 1) | `apps/api/src/hds/services/schedule_job.py:604`: the part check judged people deactivated or deleted since the run as active, so they could reach the roster | fixed |
| R3 | /code-review (PR 1) | `apps/api/src/hds/services/proposal.py:198`: an edit could place a soft-deleted account | fixed |
| R4 | /code-review (PR 1) | `apps/api/src/hds/services/proposal.py:220`: a Manager or Admin placed by an edit who is not a candidate had their other projects' shifts and their entries left out of the rule check (FR-009) | fixed |
| R5 | /code-review (PR 1) | `apps/api/src/hds/services/schedule_job.py:491`: a new draft took its headcount from the run, so a need changed since was neither caught nor used | fixed |
| R6 | /code-review (PR 1) | `apps/api/src/hds/services/proposal.py:220`: edits judged with the run's limits and accepts with today's, so they could disagree | fixed |
| R7 | /code-review (PR 1) | `apps/client/src/screens/runs/ReviewWeek.tsx:146`: a failed edit request left every button disabled | fixed |
| R8 | /code-review (PR 1) | `apps/client/src/screens/runs/review.ts:76`: the client's "part accepted" differed from the server's, so a half-accepted changed mark stayed editable and every edit there failed | fixed |
| R9 | /code-review (PR 1) | `apps/api/src/hds/routers/schedule_jobs.py:345`: `IfMatch` and `version_of` defined twice, one router importing another | fixed |
| R10 | /code-review (PR 1) | `apps/api/src/hds/services/proposal.py:167`: undo read the whole run record each time, under the job's lock | fixed |
| R11 | CI (PR 1) | `apps/api/tests/contract/unprovable_operations.json`: Schemathesis sent undo a made-up `If-Match` and counted the correct 412 `schedule.version_stale` as a valid request refused (ST202); undo is on the list now, and the accept, discard and edit entries name the 412 too | fixed |
| R12 | CI (PR 1) | `apps/api/tests/integration/test_role_matrix.py:123`: the matrix had no rows for reading, editing and undoing a proposal; all three are Manager or Admin | fixed |
| R13 | CI (PR 1) | `infra/tests/rolling_update.sh:238`: the rolling update queued a run in a new organisation with no project, which 007 refuses (422 `schedule.no_projects`); it makes a site, a client and a project first | fixed |
