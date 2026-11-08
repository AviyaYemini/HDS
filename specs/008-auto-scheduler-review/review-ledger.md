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
