# CEO demo (branch `demo/ceo`)

The owner, 2026-11-13: the CEO wants to see something feasible that works.

## The rules for this branch

- **One PR, opened by the owner.** It will probably not be merged; the owner cherry-picks what they like. Main is the 1.0 POC.
- **Focus:** the schedule, staff control and dashboards. The client portal waits.
- **Look:** Fable's deck, `decks/overview/HDS-features-deck-source/deck.html`:
  - slide 7 for the roster week;
  - slide 8 for the staff table;
  - slide 9 for hours and costs;
  - slide 14 for the snapshot dashboard.

  It uses the app's own light theme. The defining patterns are:
  - numbered key figures ("01 שעות" over a big tabular number);
  - a dark panel with a clipped corner;
  - grey and black bars;
  - tables with status chips.
- **Hebrew first.** Gender-neutral copy, and English kept working.
- **Speed first.** The Spec Kit stages are skipped. This file and `tasks.md` stand in for them. The hooks stay on.
- **Testing:** test what changed, plus the money and visibility paths. CI runs the rest.
- **Real data never enters git.** The roster workbook holds real names and pay.
  - The importer is committed.
  - The parsed file lives in `demo-data/`, which is git-ignored, and stays on the owner's Mac.
  - Tests use made-up names only.
- **Who does what:**
  - the HDS helper session does the data, the API, the migration and the generated client, on `demo/ceo`;
  - the CLI session does the screens, on `demo/ceo-ui`;
  - the helper merges `demo/ceo-ui` into `demo/ceo`.

## The data (the roster workbook)

The workbook is a Google Sheets export with one sheet per week. The layout has been the same since 6.9:
- **rows** are tasks;
- **columns** are the days, Sunday to Saturday, written right to left;
- **each cell** holds names, sometimes with hours or a sum;
- **then three columns:** the task name with its hours, the client (a security company) and the task manager.

How it maps onto HDS:
- the organisation is HDS;
- a client is a security company;
- a site is the place named in the task;
- a project is a task, of type security unless it is clearly training;
- a shift type is each distinct time window;
- shifts and assignments come from the cells;
- staffing needs come from the "how many are needed" rows and the usual headcount;
- the two task managers become Manager accounts on their tasks.

**Rates:**
- where the sheet states one (a billing/pay pair, an hourly rate, or a sum for a shift), it is used;
- elsewhere an estimate is used and the project carries `terms_estimated`, which the screens show.

**Temporary workers in the demo data:** none are inferred (the owner, 2026-11-13). Everyone is permanent until an Admin marks them temporary, at invitation, at creation or later in the worker's card; `demo-data/overrides.json` may name some for a rehearsal.

## API changes (helper)

### 1. Temporary workers (priority)

A temporary worker belongs to one or more projects for a period of at most 3 months, which may be shorter.

- **On users:**
  - `employment_type` is `permanent` (the default) or `temporary`;
  - `engaged_from` and `engaged_until` are dates;
  - `project_members` is a new table (project, user, from, until), the start of project teams (O15).
- **`EmployeeRead`** gains:
  - `employment_type`;
  - `engaged_from` (date or null);
  - `engaged_until` (date or null);
  - `project_ids` (the projects a temporary worker is on; empty for permanent staff).
- **Create and update** accept the same fields.
  - A temporary worker needs both dates, with `engaged_until` no more than 3 months after `engaged_from`: otherwise 422 `employees.temporary_period_invalid`.
  - They also need at least one project: otherwise 422 `employees.temporary_projects_required`.
  - Setting someone permanent clears all of these.
- **`GET /api/v1/employees`** takes `employment_type=temporary|permanent` as a filter.
- **`AssignmentRead`** gains `temporary: bool`, so the roster can show the chip.
- **The roster:** a hand assignment of a temporary worker outside their projects or dates is stored with a warning, code `temporary_outside`. It appears in the same warnings list as the other rules.
- **The auto-scheduler** never places a temporary worker outside their projects or dates.

### 2. The snapshot dashboard: `GET /api/v1/reports/overview?from=&to=`

Manager or Admin. A Manager sees only the projects they manage. Money is an `Amount` and hours are `HoursOut`, both strings with two decimals.

```text
from, to, today                       dates (organisation time zone)
billing_actual, billing_forecast      Amount: person-hours × the project's billing rate; actual =
                                      published shifts already ended in the range, forecast = actual
                                      + every shift (draft or published) still to come in the range
pay_actual, pay_forecast              Amount | null: gross pay the same way (null where pay is hidden)
margin_pct                            "12.50" | null: (billing_forecast − pay_forecast) / billing_forecast
hours_actual, hours_forecast          HoursOut
coverage                              the week holding today: week_start, filled, needed,
                                      empty_places, projects_with_gaps
people                                active, working_this_week, temporary, temporary_ending_soon
                                      (within 14 days), near_weekly_limit (this week within 4 h of
                                      the most hours in a week)
weeks[]                               week_start, hours, billing, pay|null, forecast (bool: the week
                                      is not over yet)
projects[]                            id, name, client_name, managers[] (names), type, hours, billing,
                                      pay|null, margin_pct|null, coverage_pct|null (this week; null
                                      without needs), empty_places (this week), temporary_workers,
                                      terms_estimated, status: "ok" | "gaps" | "loss" | "no_rate"
terms_estimated                       bool: some figure uses an estimated rate
```

The rest the screens need already exists:
- `GET /api/v1/shifts?from&to`: the whole week across projects, with who is on each shift;
- `GET /api/v1/reports/hours?group_by=employee|project|client`;
- `GET /api/v1/reports/work-log`;
- the employees, projects and clients lists;
- staffing needs.

## Screens (CLI session)

1. **The snapshot dashboard**, from slide 14. It replaces the preview at `app/(staff)/manage/overview.tsx`. It is where an Admin or Manager lands.
   - A month, quarter and year switch.
   - Four numbered key figures: billing so far, the forecast to the end of the period, this week's coverage, and people.
   - Weekly bars of billing against pay, with forecast weeks lighter.
   - The project table, with coverage, margin and a status chip.
   - A note when rates are estimated.
   - A temporary-workers line: how many, and how many end soon.
2. **The roster week**, from slide 7. It looks like the sheet the managers know:
   - projects as rows, grouped by client, with the days as columns;
   - names as blocks and empty places dashed;
   - a "זמני" chip on temporary workers, and the warnings strip;
   - the week switch, and copy last week.
3. **Staff control**, from slide 8. The staff table gains:
   - this week's and this month's hours, from the hours report by employee;
   - a near-the-weekly-limit flag;
   - a permanent or temporary filter;
   - a "זמני · עד 12.02" chip for temporary workers.

   The form gains the employment type, the dates (capped at 3 months) and the projects.
4. **Hours and costs**, from slide 9. The report screen in the deck's style: numbered key figures, bars by project and the table.
