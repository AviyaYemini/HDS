# Quickstart: Auto-scheduler review

How to see each pull request working, on the dev stack (`make up`, signed in as `admin@hds.test`).
The dev seed is small (two employees, two security projects), so first give the steps something
to work on: a few more employees with availability for next week, a need on each project (Projects,
Staffing needs), and a draft or two on next week's roster with people on them. The e2e spec
`apps/client/tests/e2e/ux/i-proposal-review.spec.ts` builds such a week through the API.

1. **Review and edit (PR 1).** As the Admin, generate next week for every project. Open the run:
   the week shows added, changed, removed and unchanged marks with counts. Swap two people; move one
   onto a shift that leaves 9 h of rest: the warning names the rule and asks for a reason. Accept
   Sunday only: it becomes drafts on the roster; the rest stays open. Discard the rest: the proposal
   is partly accepted.
2. **Empty slots (PR 2).** Block every employee but one on Saturday night and generate: the empty
   slot names the reasons with counts and the people with their numbers. "Who else could cover"
   lists the free person first, then the others with their rules; picking one assigns them.
3. **Steering (PR 3).** Lock four assignments, add "Noa: at most 2 nights", choose "Keep changes
   small", and run again from the proposal: the locks are kept and said so, the chip shows met or
   not, the summary words the trade-offs, and the fairness panel marks outliers.
4. **Progress and flow (PR 4).** Set a 60 s limit and generate a month: "best so far" rises; Stop
   gives a proposal labelled stopped early. From the roster, "Generate this week" opens the form set
   to the week; accept, publish, and "Copy to next week" is offered. An empty roster slot offers
   "Find someone". The run's record lists every step.

Checks per group: the scoped `make ci-local`; the group's Playwright spec through
`scripts/ops/e2e_ux.sh`; integration tests on `hds_p3_test` only. Full suites run in CI.
