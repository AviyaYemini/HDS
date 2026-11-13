# CEO demo: tasks

See `plan.md`. **H** is the HDS helper session, on `demo/ceo`. **C** is the CLI session, on `demo/ceo-ui`.

## Data and API (H)

- [ ] D01 Git-ignore `demo-data/`. Write a workbook parser that writes `demo-data/roster.json` with clients, sites, projects, rates (and whether they are estimated), shift types, people (with aliases merged), temporary workers, shifts and assignments.
- [ ] D02 Temporary workers. Add the migration (employment type, period, `project_members`, `projects.terms_estimated`), the employee API fields, the filter and the rules, `AssignmentRead.temporary`, the roster warning `temporary_outside`, and the auto-scheduler's eligibility. Include tests.
- [ ] D03 The demo loader `hds.scripts.seed_demo`. It reads `demo-data/roster.json` into an empty database: the organisation, the Admin and two Manager sign-ins, and everything above.
- [ ] D04 `GET /api/v1/reports/overview`. Include tests for the sums, the visibility of Managers' projects and hidden pay.
- [ ] D05 Regenerate the OpenAPI document and the TypeScript client, then commit.

## Screens (C)

- [ ] U01 The snapshot dashboard (slide 14), as the landing screen for Admins and Managers.
- [ ] U02 The roster week in the sheet's layout (slide 7), with temporary chips and dashed empty places.
- [ ] U03 Staff control (slide 8): hours, the near-limit flag, temporary workers (filter, chip and form fields).
- [ ] U04 Hours and costs in the deck's style (slide 9).

## Close (H)

- [ ] Z01 Merge `demo/ceo-ui`. Load the demo data, then walk through the demo in Hebrew on desktop and phone widths.
- [ ] Z02 Write a short demo script for the meeting, and the PR text for the owner.
