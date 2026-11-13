feat(demo): ceo demo, the schedule, staff control and dashboards on the real roster

A demo for the CEO on the team's real roster workbook: the schedule, staff control and dashboards, in the style of Fable's features deck. Temporary workers come first, as the CEO asked.

This branch is a demo off main (1.0 POC). It is not meant to merge as is; cherry-pick what's useful.

## What's in it

- **Temporary workers.** An employee is permanent or temporary. A temporary worker is on one or more projects for at most three calendar months.
  - Migration 038 adds `project_members`, the start of project teams.
  - The employee API sets and shows the type, the dates and the projects, and the list filters by type. A Manager may add people only to projects they manage.
  - A schedule run never places a temporary worker outside their projects or dates. A manual assignment there is saved with `warnings.temporary_outside`.
- **The snapshot dashboard.** `GET /api/v1/reports/overview` is the landing screen for Admins and Managers. It shows:
  - billing and gross pay, actual and forecast;
  - margin, weekly bars, and a project table with coverage, margin and status;
  - this week's coverage;
  - the people block, including temporary workers.

  Managers see only their own projects, and money only where pay is open to them.
- **Charts on the overview:**
  - a donut of billing by client, the top five plus the rest;
  - a donut of permanent against temporary staff;
  - candles of daily billing for each week (open, close, high and low), from the overview's new `days[]`.
- **The roster week** in the layout the managers know from the sheet: projects as rows grouped by client, and days as columns. Empty places are dashed, and a "זמני" chip marks temporary workers.
- **Staff control:** hours this week and this month, a permanent/temporary filter and the engagement fields.
- **Phone layout:** below 768px the tables become cards, and the roster shows one day at a time.
- **No rule warnings anywhere in the demo screens:** the owner considers them out of scope for the app. That covers the roster sheet, the per-project editor (assign, move and swap simply save) and the publish review. `GET /api/v1/roster/warnings` stays in the API, unused, for a later cherry-pick.
- **Comparisons and next week on the overview:**
  - `previous`: the month, quarter or year before, with the same days so far;
  - `coverage_next`: the week being prepared;
  - a weekly series for each project's trend line.
- **Auto-scheduler on the real data:** staffing needs only for live tasks, so a run for a week fills every place in about two seconds. A hand edit in the proposal review that breaks a person rule is kept as made, with no reason demanded; an inactive employee and a part whose roster changed since the run are still refused.
- **Polish:**
  - sign-in in the deck's title-slide style;
  - a greeting, whole-shekel figures with ▲/▼ against the period before (like for like with where the roster stops), and numbers and charts that grow in;
  - trend lines in the project table, with rows that open the project's week;
  - hours and costs with full-name bars;
  - the roster with today shaded and a card on each name;
  - staff with avatars, sorted by this month's hours;
  - home without the setup card once the company is set up;
  - page titles, loading skeletons and a dark-mode pass;
  - the schedule-run page: numbered figures, the proposal as a roster week with changes marked, fairness as bars, and a clear action bar. A run over a range that already has drafts fills the gaps by default rather than reshuffling them.
- **Hours and costs** in the deck's style. The hours report now includes billing.
- **The demo data:**
  - `scripts/demo/parse_roster.py` reads the weekly Google Sheets export. It handles tasks, clients, managers, hours, sums, open places and merged spellings.
  - `hds.scripts.seed_demo` loads the result into an empty database.
  - `scripts/demo/run_demo.sh` runs everything with one command.

## The data stays off GitHub

The workbook holds real names and pay. The parsed data, the local overrides and the demo's signing key all live in the git-ignored `demo-data/` folder. No committed file, test or screenshot holds a name from it.

## Running it

```bash
scripts/demo/run_demo.sh "/path/to/the roster workbook.xlsx"
```

Open http://localhost:8081 and sign in as `admin@hds.test`, or as `manager1@hds.test` for a Manager's view. The walkthrough for the meeting is in `specs/demo-ceo/demo-script.md`.

## Testing

**Checked locally:**
- API: 1,707 unit, integration and contract tests for what changed (temporary workers, the overview, the hours report, employees, proposal review); lint and format are clean.
- The OpenAPI contract has no drift, and the generated client is up to date.
- Client: tsc, ESLint and all 401 unit tests.
- The demo run from the workbook, from a fresh database:
  - in Hebrew at 1440px and 390px, and in English at 390px;
  - the charts and the phone cards and roster;
  - saving a temporary worker's end date, and the three-month rule on the form.

**Left to CI:** the full suites, Schemathesis, and the end-to-end specs updated for the new screens (`test_us_hours_report`, `ux/g-reports`, `ux/d-roster`, `ux/h-a11y-sweep`, `ux/i-proposal-review`, `ux/j-empty-slots`, `ux/k-*`, `ux/l-run-flow`, `ux/o-owner-requests`).

**Not covered:** native iOS and Android builds.

Built by two Claude Code sessions: the HDS helper did the data and the API, and the CLI session did the screens.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
