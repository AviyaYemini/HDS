# Tasks: stabilization after 008 (chore lane)

The work of [plan.md](plan.md), one pull request per section, in this order. One commit per task.

## PR 1: one spelling, "organisation"

- [x] S01 The exemption record (ADR 021): a one-step rename, against Constitution V and X, while nothing is deployed; `infra/tests/compat.sh` skips a base older than the rename revision
- [x] S02 Migration 037: rename every `organization_id` column and foreign key, and the trigger function that names it, with a working downgrade; the models, repositories, services, routers, schemas, scripts and the API's tests follow in the same commit, since an attribute is renamed everywhere at once; the API fields become `organisation_id`
- [x] S03 The 001 contract, the regenerated `packages/api-client`, and the client follow
- [x] S04 Docs and specs follow; `git grep -i organization` lists only third-party names and applied migrations
- [x] S05 Close: scoped `make ci-local`, one `/code-review`, the ledger, the pull-request text

## PR 2: client structure

- [x] S11 Roster: the screen keeps its layout; its sheets, rows and data calls move beside it
- [x] S12 Admin: one file per screen
- [ ] S13 Runs: the review, the run page and the run list lose their parts and hooks to files of their own
- [ ] S14 Admin table, availability and account split the same way
- [ ] S15 Close: scoped `make ci-local`, one `/code-review`, the ledger, the pull-request text

## PR 3: expanded dev dataset

- [ ] S21 The wider records: people, managers, clients, sites, projects of both types, groups, qualifications, shift types, needs, pay settings, special days
- [ ] S22 Availability history and an open window; published past weeks and the current one through the scheduler and the services; a ready proposal; manual edits with overrides
- [ ] S23 Tests (every published shift passes validate; a re-run adds nothing) and the README section
- [ ] S24 Close: scoped `make ci-local`, one `/code-review`, the ledger, the pull-request text
