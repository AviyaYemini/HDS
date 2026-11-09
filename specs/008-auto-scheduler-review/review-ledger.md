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
| R14 | CodeRabbit (PR 21) | `apps/api/src/hds/services/schedule_job.py:446`: a matching fingerprint skipped the part check, yet it leaves out people placed by hand who were not candidates, so a Manager placed then deactivated reached the roster; every accept but an untouched whole proposal is judged now | fixed |
| R15 | CodeRabbit (PR 21) | `apps/api/src/hds/services/proposal_copy.py:188`: naming only the newcomer of a changed pair left the person replaced on the draft too; naming either takes both | fixed |
| R16 | CodeRabbit (PR 21) | `apps/api/src/hds/services/schedule_job.py:590`: a draft whose times moved but whose people did not passed the part check, judged at the old times; it is a changed draft now | fixed |
| R17 | CodeRabbit (PR 21) | `apps/client/src/screens/runs/ReviewWeek.tsx:410`: staff kept on a draft (`kept`) filled places but were not shown; they are listed, unchanged and not editable | fixed |
| R18 | CodeRabbit (PR 21) | `apps/api/alembic/versions/034_proposal_review.py:126`: the downgrade failed on a job partly accepted or cancelled; 035 maps them to accepted and failed first (034 itself is committed) | fixed |
| R19 | CodeRabbit (PR 21) | `docs/adr/020-proposal-working-copy-and-run-record.md:22`, `docs/hds-api.md:30`: the version guards only writes that send `If-Match`, and the record holds edits and decisions until PR 4; both said more | fixed |
| R20 | CodeRabbit (PR 21) | `specs/008-auto-scheduler-review/contracts/api.md:5`, `:22`: the scope code is `roster.project_not_managed`, and `lock`/`unlock` come in PR 3 | fixed |
| R21 | CodeRabbit (PR 21) | `specs/008-auto-scheduler-review/quickstart.md:4`, `research.md:70`, `.claude/commands/hds-review.md:6`, `.claude/hds/process-cuts-proposal.md:7`: the quickstart assumed a larger seed; Stop is watched apart from solution callbacks; the review reads `git diff main...HEAD`; local checks are scoped to changed paths | fixed |
| R22 | CodeRabbit (PR 21) | `docs/adr/020-proposal-working-copy-and-run-record.md:28`: the ADR still listed the run's own steps in PR 1's record, and its part check predated R14 | fixed |
| R23 | CodeRabbit (PR 21) | `docs/hds-api.md:30`: the whole of an untouched proposal is refused with `schedule.proposal_out_of_date`, not `schedule.part_out_of_date` | fixed |
| R24 | /code-review (PR 2) | `apps/client/src/screens/runs/ReviewWeek.tsx:396`: when "who else could cover" could not be read, the assign sheet showed only a banner, so no one could be assigned; it offers a retry and the staff list now | fixed |
| R25 | /code-review (PR 2) | `apps/client/src/screens/runs/ReviewWeek.tsx:392`: the run's reasons sat beside the live cover list and could contradict it after edits; they are headed as the run's, "when the run was made" | fixed |
| R26 | /code-review (PR 2) | `apps/client/src/screens/runs/review.ts:132`: rest rounded to the nearest tenth of an hour, so 599 minutes against 600 read "10 h rest, needs 10 h"; what a person has rounds down and what is needed up | fixed |
| R27 | /code-review (PR 2) | `apps/api/src/hds/scheduling/cover.py:66`: cover counts a fixed shift by each person's own times, the run's summary (004's `_fixed_in_range`) by the whole shift. Cover's is the count the rules judge by (007 FR-027); aligning 004's summary is outside this PR | won't-fix |
| R28 | /code-review (PR 2) | `apps/api/src/hds/scheduling/eligibility.py:180`: `Roster.detail` judged the bookings again after its callers had `refusal`'s code; it takes that code now | fixed |
| R29 | /code-review (PR 2) | `apps/api/src/hds/scheduling/cover.py:142`: the cover request worked out every person's load twice; it is worked out once and given to both | fixed |
| R30 | /code-review (PR 2) | `apps/client/src/screens/runs/ReviewWeek.tsx:424`: the average counted idle Admins and Managers, pulling it down; it counts every Employee and anyone else with hours in the range | fixed |
| R31 | /code-review (PR 2) | `apps/api/src/hds/scheduling/cover.py:106`: someone who never submitted availability where it is required reads "Not available", and the reason sheet then names `warnings.not_submitted`. "Not available" is true of them, and the sheet names the exact rule before anything is kept | won't-fix |
| R32 | /code-review (PR 2) | `apps/api/src/hds/services/proposal.py:204`: cover built the whole review view only to drop it; it runs the read's access checks alone | fixed |
| R33 | /code-review (PR 2) | `.github/workflows/ci.yml:551`: a failed web build let the sweep fail every test against an empty site, burying the cause; the build is a step of its own and the sweep needs it to have worked | fixed |
| R34 | CI (PR 22) | `apps/client/src/locales/en.json:381`: an "I want" availability cell read "Want" but was named "Would like to work", failing axe's label-in-name check once a shard's earlier spec left such an entry; the state reads "Want to work" (the glossary's availability kind keeps its wording) | fixed |
| R35 | CI (PR 22) | `apps/api/tests/integration/test_proposal_cover.py:57`: the run could place the one free guard on the very slot, so they were not offered; two free guards, and the test checks whichever is left | fixed |
