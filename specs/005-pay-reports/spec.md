# Feature Specification: Pay rates, pay to date and reports

**Feature Branch**: `005-pay-reports`

**Created**: 2026-11-05

**Status**: Draft

**Input**: User description (the stored prompt `.claude/hds/prompts/04-pay-reports/specify.md`): "Admins set
each employee's default pay rate and can override pay on a single shift; an employee sees a monthly pay
forecast from scheduled shifts, and managers see work-log and hours reports. Use docs/hds-spec.md (FIN-1,
FIN-3, RPT-1, RPT-2) and original scenarios E and F. Effective pay for a shift is the shift's own rate if
set, otherwise the employee's default. Rates and times must be positive. Employees see only their own pay;
managers see hours but pay only where permitted by role. Work-log report: who worked at which project,
defaulting the start to the project start date. No technology details."

**Traces to**: original scenarios A, E and F, original entities 1, 2, 7 and 8, original rules 1 and 3;
FIN-1 (pay rate per employee, and a pay field on each shift that an Admin edits), FIN-3 (gross pay for the
month, per employee), RPT-1 (who worked at a site or group in a date range, or since the project was
created), RPT-2 (hours and pay reports per employee and per project), ADM-2 (times and rates must be
positive); docs/hds-spec.md section 2 (the role table) and section 6 (the money rules). Builds on 001
(employees and their default rate, projects, sites, groups) and 003 (shifts with an optional pay rate edited
by Admins only, assignments, draft and published shifts).

**Proposed** (added by the product owner on 2026-11-05, beyond the original): pay switched on or off per
project; Managers assigned to projects and allowed to see a project's pay; pay premiums for overtime, the
weekly rest and special days, following the basics of Israeli law; the month's figure is pay earned to
date rather than a projection.

## Clarifications

### Session 2026-11-05

- Q: Who sets an employee's default pay rate? → A: Admins only; Managers, who could set it since 001, are
  now refused. Pay is also optional per project: an Admin can switch it off for a project.
- Q: Do Managers see other employees' pay in reports? → A: Only for projects where an Admin has allowed it,
  and only Managers assigned to that project; they never edit it.
- Q: Which shifts do the reports count? → A: Only published shifts that have ended (worked).
- Q: What does an employee's monthly figure include? → A: Pay earned so far: only shifts of the month that
  have ended. Nothing is projected for the rest of the month.
- Q: What is the per-project switch? → A: Two switches per project, both set by Admins: pay on or off (on by
  default; when off, no pay is worked out for the project's shifts), and Managers see pay (off by default).
- Q: Which Managers may see a project's pay? → A: Only Managers an Admin has assigned to that project.
- Q: How is overtime counted? → A: Per shift: the hours of a shift beyond its normal length, the first 2 at
  125% and the rest at 150%, with defaults an Admin can change per project.
- Owner direction: weekly rest day and special-day premiums apply automatically from defaults (weekly rest
  150%; special days such as Yom Kippur or election day up to 200%), Admins can change them, and a project's
  pay settings are set when it is created and can be changed until it ends. All figures are before tax.

### Session 2026-11-05 (clarify)

- Q: When does the weekly rest start and end, for the rest-day premium? → A: A window set per project,
  from Friday 18:00 to Saturday 22:00 at the site by default; Admins can change its start and end day and
  time.
- Q: Can Admins change the organisation's premium defaults, and what happens to existing projects? → A: Yes;
  a change applies to every project that has not ended and has not given that setting its own value.
- Q: Are special days one list for the organisation, or can a project have its own? → A: The organisation's
  list applies to every project; an Admin can add days to a project or give a listed day its own rate there.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - An employee sees the gross pay they have earned this month (Priority: P1)

An employee opens their pay for a month and sees, for each shift of the month they have already worked, the
date, the project, the hours, the rate and any premium, and the pay; and the month's total so far. Every
figure is gross, before tax.

**Why this priority**: FIN-3 and original scenario F (MVP). It is the only money figure an employee sees.

**Independent Test**: an employee with a default rate of 50 has three published 8-hour day shifts this month:
two ended on weekdays and one still to come; one of the ended shifts has its own rate of 60. Their pay to
date shows 400 and 480, total 880, and does not show the shift still to come.

**Acceptance Scenarios**:

1. **Given** an employee with a default rate and ended published shifts in a month, **When** they read their
   pay for that month, **Then** each shift shows its hours, rate, premiums and pay, and the total is the sum
   of the lines.
2. **Given** a shift with its own pay rate, **When** the pay is read, **Then** that shift is paid at the
   shift's rate, not the employee's default.
3. **Given** shifts later in the month, or a shift under way, **When** the pay is read, **Then** they are not
   counted.
4. **Given** draft shifts the employee is on, **When** the pay is read, **Then** they are not counted.
5. **Given** a project whose pay is switched off, **When** the pay is read, **Then** its shifts show their
   hours with no pay and add nothing to the total.
6. **Given** an employee, **When** they ask for anyone else's pay, **Then** they are refused; a Manager or
   Admin reading their own pay sees only their own shifts.
7. **Given** a Client, **When** they ask for pay, **Then** they are refused.

---

### User Story 2 - Admins set pay rates and each project's pay settings (Priority: P1)

An Admin sets each employee's default hourly rate and, where one shift pays differently, that shift's own
rate. When creating a project, or later until it ends, the Admin decides whether the project has pay at
all, whether its assigned Managers may see that pay, and its premium settings; each starts from a default.

**Why this priority**: FIN-1 (MVP). Without rates and settings there is no pay to show.

**Independent Test**: an Admin changes an employee's default rate from 50 to 55; that employee's pay for a
month with two ended 8-hour weekday shifts at the default changes from 800 to 880, while a shift with its own
rate keeps its pay. A Manager trying the same change is refused.

**Acceptance Scenarios**:

1. **Given** an employee, **When** an Admin sets their default rate, **Then** every figure read afterwards
   uses it for shifts without their own rate.
2. **Given** an employee, **When** a Manager sets, changes or clears their default rate, **Then** the change
   is refused and nothing changes; the Manager can still edit the employee's other fields.
3. **Given** a shift, **When** an Admin sets or clears its pay rate, **Then** its employees are paid at that
   rate for it, or at their own default once it is cleared.
4. **Given** a rate or premium of zero or less, or one that is not a number, **When** it is sent, **Then** it
   is refused and nothing changes.
5. **Given** a new project, **When** an Admin creates it without pay settings, **Then** pay is on, Managers
   may not see it, and the premium defaults apply.
6. **Given** an active project, **When** an Admin changes its pay settings, **Then** every figure read
   afterwards uses them, including for its earlier shifts; **Given** an ended project, **When** anyone tries
   to change them, **Then** the change is refused.
7. **Given** an Employee, a Manager or a Client, **When** they try to change a project's pay settings,
   **Then** they are refused; an employee still sees their own default rate on their profile.

---

### User Story 3 - Premiums for overtime, the weekly rest and special days (Priority: P1)

Pay follows the basics of Israeli law without anyone working it out by hand: hours beyond a shift's normal
length are overtime, the first 2 at 125% and the rest at 150%; hours in the weekly rest, from Friday
evening to Saturday night, are paid at 150%; hours on a special day the Admin has listed, such as Yom Kippur or election day, at that day's rate,
up to 200%. Overtime on a rest or special day adds to that day's rate.

**Why this priority**: the owner asked for it; without it, pay to date understates what employees earn on
nights, long shifts and weekends.

**Independent Test**: with a rate of 50 and the defaults, a 10-hour weekday day shift pays 8 × 50 + 2 × 62.5
= 525; an 8-hour Saturday day shift pays 8 × 75 = 600; an 8-hour night shift (22:00 to 06:00) on a weekday
pays 7 × 50 + 1 × 62.5 = 412.5; the same night shift from Friday 22:00 pays 7 × 75 + 1 × 87.5 = 612.5.

**Acceptance Scenarios**:

1. **Given** a shift longer than its normal length, **When** its pay is worked out, **Then** the first 2
   extra hours are paid at the first overtime rate (125% by default) and any further hours at the second
   (150% by default).
2. **Given** a night shift (at least 2 of its hours between 22:00 and 06:00 at its site), **When** its pay is
   worked out, **Then** its normal length is the night length (7 hours by default) instead of the day length
   (8 hours by default).
3. **Given** a shift with hours in the weekly rest (Friday 18:00 to Saturday 22:00 at the site by default),
   **When** its pay is worked out, **Then** those hours are paid at the rest rate (150% by default) and the
   other hours at the normal rate; a Friday 22:00 to Saturday 06:00 shift is paid at the rest rate
   throughout.
4. **Given** a date the Admin listed as a special day with its rate (for example 200% for election day),
   **When** a shift has hours on it, **Then** those hours are paid at that rate; if they are also in the
   weekly rest, the higher rate applies.
5. **Given** overtime hours on a rest or special day, **When** they are paid, **Then** the overtime increase
   (25 points for the first 2 hours and 50 after, by default) is added to that day's rate: 175% and 200% on a
   150% weekly rest.
6. **Given** a project with its own value for a premium setting, **When** its shifts are paid, **Then**
   that value is used instead of the organisation default.
7. **Given** two active projects, one following the default overtime rate and one with its own, and an
   ended project that followed it, **When** an Admin changes the default from 125% to 130%, **Then** only
   the first project's figures change.
8. **Given** every pay line, **When** it is shown, **Then** it says how many hours were paid at each rate.
9. **Given** election day on the organisation's list at 200%, and project A giving it its own rate of 150%
   and adding a local holiday at 150%, **When** shifts on those days are paid, **Then** project A uses 150%
   for both, and every other project uses 200% for election day and nothing for the local holiday.

---

### User Story 4 - A Manager sees who worked at a project (Priority: P1)

A Manager picks a project, a site or a group, and a date range, and sees who worked there: each person, the
project, the number of shifts, the hours, and the first and last date they worked in the range. When a
project is picked and no start date is given, the range starts on the project's start date.

**Why this priority**: RPT-1 and original scenario A (MVP).

**Independent Test**: a project that started on 1 March with three employees who worked there in March and
one who worked only in May; the work-log for that project with no start date and an end of 31 May lists all
four with their shifts and hours, and a range of May only lists the one.

**Acceptance Scenarios**:

1. **Given** a project and a date range, **When** a Manager reads the work-log, **Then** every person who
   worked at least one ended published shift of that project in the range is listed once, with their shift
   count, hours, and first and last date in the range.
2. **Given** a project and no start date, **When** the work-log is read, **Then** the range starts on the
   project's start date.
3. **Given** a site, **When** the work-log is read, **Then** it covers every project at that site, listed per
   project.
4. **Given** a group, **When** the work-log is read, **Then** it lists only the group's members, at every
   project they worked at in the range.
5. **Given** an employee who has since been deactivated or deleted, or a project that has since ended,
   **When** a past range is read, **Then** their work still appears.
6. **Given** an Employee or a Client, **When** they read a work-log, **Then** they are refused.

---

### User Story 5 - Hours and pay reports per employee and per project (Priority: P2)

A Manager or Admin reads, for a date range, the hours each employee worked and the hours each project used,
with shift counts. An Admin also sees pay, per line and in total. A Manager sees pay only for projects they
are assigned to whose pay the Admin has opened to Managers.

**Why this priority**: RPT-2 (MVP). It builds on the same shifts, rates and premiums as stories 1 to 3.

**Independent Test**: two employees on two projects over a month, a Manager assigned to project A only, and
project A open to Managers; the Manager's report shows hours for both projects and pay for project A only;
the Admin's shows pay for both; the hours add up the same by employee and by project.

**Acceptance Scenarios**:

1. **Given** a date range, **When** the hours report is read by employee, **Then** each employee shows their
   hours and shift count per project and in total.
2. **Given** a date range, **When** the hours report is read by project, **Then** each project shows its
   hours and shift count per employee and in total.
3. **Given** the same range, **When** both views are read, **Then** the grand totals of hours are equal.
4. **Given** an Admin, **When** they read the report, **Then** pay is shown for every project whose pay is
   on.
5. **Given** a Manager, **When** they read the report, **Then** pay is shown only for projects they are
   assigned to and whose pay is open to Managers; for every other project they receive hours only, with no
   pay figure at all, and any pay total covers only the projects they may see.
6. **Given** an employee with no default rate on a shift with no rate of its own, **When** the report is
   read, **Then** that shift's hours count and its pay is marked as missing a rate, never shown as zero.
7. **Given** an Employee or a Client, **When** they read an hours report, **Then** they are refused.

---

### User Story 6 - Admins assign Managers to projects (Priority: P2)

An Admin assigns one or more Managers to a project and removes them. The assignment decides only which
Managers may see that project's pay; every Manager still runs every report for hours.

**Why this priority**: the owner asked that a Manager sees pay only for their own projects; there was no
link between Managers and projects.

**Independent Test**: assign Manager M to project A, open A's pay to Managers; M sees A's pay in the hours
report; remove M from A; M no longer sees it.

**Acceptance Scenarios**:

1. **Given** a project, **When** an Admin assigns a Manager to it, **Then** the Manager is listed on the
   project.
2. **Given** a user who is not a Manager or Admin, **When** an Admin tries to assign them, **Then** it is
   refused.
3. **Given** a Manager assigned to a project, **When** they are demoted, deactivated or deleted, **Then**
   they see no pay from then on.
4. **Given** a Manager, an Employee or a Client, **When** they try to assign or remove a Manager, **Then**
   they are refused; Managers and Admins can read who is assigned.

---

### Edge Cases

- A shift that starts on the last evening of a month belongs to the month in which it starts, at its site's
  local time; the same rule decides which range a shift falls in for the reports.
- A shift that crosses into or out of the weekly rest or a special day is split by the hour: only the hours on
  that day get its rate.
- Overtime hours are the last hours of a shift: in a shift that ends inside the weekly rest, the overtime
  hours get the rest rate plus the overtime increase.
- A shift from Friday 16:00 to 24:00 is paid at 100% until 18:00 and at the rest rate after; one from
  Saturday 18:00 to Sunday 02:00 at the rest rate until 22:00 and at 100% after.
- A shift on the night clocks change counts the hours that really pass (7 or 9), for hours and for pay.
- An employee with no default rate on a shift with no rate of its own: the hours count, the pay line is
  marked as missing a rate, and the totals say how many shifts have no rate.
- A project with pay switched off: its hours appear everywhere; no pay is worked out for it, and it is not
  shown as missing a rate.
- A rate or a project's premium settings changed during a month: figures always use the values as they are
  when read, so earlier shifts of an active project change too. Once a project has ended, its settings are
  fixed.
- An employee removed from a shift, or a shift deleted or moved to draft, drops out of every figure read
  afterwards.
- A range whose end is before its start is refused, and so is one longer than 366 days.
- A project with no ended shifts in the range, or a group with no members, gives an empty report, not an
  error.
- A project, site, group or Manager from another organisation is answered as if it did not exist.
- A month with no ended shifts shows a total of zero.
- An ended project made active again keeps the values it was frozen with as its own values; an Admin can
  return each to following the default.
- Deleting an organisation special day leaves a project's own entry for that date in place, as a project
  day.

## Requirements *(mandatory)*

### Functional Requirements

**Rates (FIN-1, ADM-2)**

- **FR-001**: An employee's default hourly pay rate MUST be set, changed and cleared by an Admin only; a
  Manager's attempt MUST be refused and change nothing, while the Manager's other employee edits still work.
- **FR-002**: A shift's own hourly pay rate MUST be set, changed and cleared by an Admin only (as in 003).
- **FR-003**: Every rate, normal length and overtime tier length MUST be greater than zero when set; a
  premium rate (overtime, weekly rest, special day) MUST be at least 100% and at most 300%; an overtime
  increase MUST be above 0 and at most 200 points; anything else MUST be refused and change nothing.
- **FR-004**: The base rate for an employee on a shift MUST be the shift's own rate if it has one, otherwise
  the employee's default; if neither is set, the pay MUST be reported as missing a rate, never as zero.

**Project pay settings (Proposed)**

- **FR-005**: Each project MUST have pay on or off (on by default) and Managers see pay on or off (off by
  default), both set by Admins.
- **FR-006**: The organisation MUST have premium defaults, which Admins change and Managers read: day
  normal length (8 hours), night normal length (7 hours), the length of the first overtime tier (2 hours),
  first and second overtime rates (125% and 150%), the weekly rest window (from Friday 18:00 to Saturday
  22:00, each end a weekday and a time of day) and its rate (150%), and the overtime increases added on a
  rest or special day (25 and 50 points).
- **FR-006a**: Each project MUST, for each premium setting, either follow the organisation default or have
  its own value. A change to a default MUST apply to every project that has not ended and follows that
  setting, including to its earlier shifts; projects with their own value, and ended projects, MUST be
  unaffected. An Admin MUST be able to give a setting its own value or return it to following the default.
- **FR-007**: An Admin MUST be able to set a project's pay settings when creating it and change them while
  it has not ended; changes to an ended project MUST be refused. When a project ends, the values it follows
  from the defaults MUST be kept as they are at that moment.
- **FR-008**: Admins MUST be able to keep a list of special days for the organisation, each a date with a
  name and a rate (150% by default, up to 200% or more within FR-003, for example 200% for election day).
  Managers MUST be able to read it.
- **FR-008a**: For a project that has not ended, an Admin MUST be able to add special days that apply to
  that project only, and give a day of the organisation's list its own rate for that project; the project's
  days and rates MUST override the organisation's for its shifts, a change to the organisation's list MUST
  apply to every project that has not ended unless the project overrides that day, and an ended project's
  special days MUST stay as they were when it ended. A project MUST NOT have two entries for one date.

**Working out pay (Proposed premiums; docs/hds-spec.md section 6)**

- **FR-009**: Hours MUST be the real time that passes between a shift's start and end, including across a
  change of clocks.
- **FR-010**: A shift MUST be a night shift when at least 2 of its hours fall between 22:00 and 06:00 at its
  site; its normal length is then the night length, otherwise the day length.
- **FR-011**: Hours of a shift beyond its normal length MUST be overtime, counted from the end of the
  shift: the first tier at the first overtime rate and the rest at the second.
- **FR-012**: Each hour of a shift MUST take the rate of when it falls at the site: the special day's rate
  on a special day, else the rest rate inside the weekly rest window, else 100%; when both apply, the
  higher. The parts of an hour that fall on either side of a boundary MUST be paid separately.
- **FR-013**: An overtime hour at a rate above 100% MUST be paid at that day's rate plus the overtime increase
  for its tier; on a 100% day, at the overtime rate.
- **FR-014**: The pay for an employee on a shift MUST be the sum of hours times base rate times the rate of
  each part; amounts MUST be exact and rounded to two decimal places only once per line, and every total MUST
  equal the sum of the lines shown.
- **FR-015**: Each pay line MUST show how many hours were paid at each rate.
- **FR-016**: Figures MUST use the rates, settings and special days as they stand when read.

**Pay to date (FIN-3, scenario F)**

- **FR-017**: Every Employee, Manager and Admin MUST be able to read their own pay for a chosen month, by
  default the current month.
- **FR-018**: It MUST list each ended published shift of theirs in that month with date, project, start and
  end, hours, base rate, the hours at each rate, and pay, and MUST total hours and pay; shifts not yet ended
  and draft shifts MUST NOT appear.
- **FR-019**: Shifts of projects with pay off MUST show hours and no pay, and add nothing to the pay total.
- **FR-020**: No one MAY read another person's pay through it; a Client MUST be refused.

**Managers on projects (Proposed)**

- **FR-021**: Admins MUST be able to assign Managers (and Admins) to projects and remove them; Managers and
  Admins MUST be able to read who is assigned; anyone else MUST be refused.
- **FR-022**: A Manager MUST see pay in reports only for projects they are assigned to whose pay is on and
  open to Managers, and MUST never be able to change any rate or setting; a demoted, deactivated or deleted
  Manager MUST see no pay.

**Work-log report (RPT-1, scenario A)**

- **FR-023**: Managers and Admins MUST be able to read a work-log for exactly one of a project, a site or a
  group over a date range.
- **FR-024**: For a project, a missing start date MUST default to the project's start date; for a site or a
  group, the start date MUST be given; a missing end date MUST default to today in the organisation's time
  zone.
- **FR-025**: The work-log MUST list each person once per project with shift count, hours, and first and last
  shift date in the range, and MUST show no pay.
- **FR-026**: A group's work-log MUST include only its current members; a site's MUST cover every project at
  the site; deactivated and deleted employees and ended projects MUST still appear for the periods worked.

**Hours and pay report (RPT-2)**

- **FR-027**: Managers and Admins MUST be able to read an hours report over a date range, grouped by employee
  or by project, with hours and shift counts per employee and project and in total; the grand total of hours
  MUST be the same either way.
- **FR-028**: An Admin MUST see pay for every project with pay on; a Manager only per FR-022. A reader not
  allowed to see a project's pay MUST receive no pay figure for it at all, and pay totals MUST cover only the
  projects shown with pay.

**Shared rules**

- **FR-029**: Reports and pay to date MUST count only published shifts that have ended, each in the month or
  range of its local start date at its site.
- **FR-030**: A report range MUST end on or after its start and cover at most 366 days, or be refused.
- **FR-031**: Employees and Clients MUST be refused every report and every pay setting; an Employee MUST never
  see another employee's hours or pay; a Client MUST never see employee fields (original rule 3).
- **FR-032**: Data from another organisation MUST never appear, and a project, site, group or user of another
  organisation MUST be answered as not found.
- **FR-033**: Every message a user can see MUST be available in English and Hebrew.

### Key Entities

- **Default pay rate**: an employee's hourly gross pay, optional, positive when set (exists since 001).
- **Shift pay rate**: a shift's own hourly gross pay, overriding the default for everyone on it (since 003).
- **Project pay settings**: pay on or off, Managers see pay on or off, and the premium settings of FR-006;
  each premium setting either follows the organisation default or has the project's own value;
  changeable until the project ends, then fixed.
- **Organisation premium defaults**: the values of FR-006 that projects follow unless they have their own.
- **Special day**: a date with a name and a rate, either on the organisation's list (applying to every
  project) or on one project's own list (added, or overriding the rate of an organisation day).
- **Project manager**: a Manager or Admin assigned to a project.
- **Pay line**: one employee on one ended published shift: date, project, hours, base rate (or "missing"),
  hours at each rate, and pay. Worked out when read; not stored.
- **Pay to date**: one person's pay lines for one month and their totals.
- **Work-log row**: one person at one project within a range: shift count, hours, first and last date.
- **Hours report row**: one employee and one project within a range: shift count, hours, and pay where the
  reader may see it.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: An employee reaches this month's gross pay to date in at most three steps from opening the app.
- **SC-002**: For any set of shifts, rates and settings, the pay total equals the sum of its lines, each
  line's hours at each rate add up to its hours, and the hours report's totals by employee and by project are
  equal (checked over many generated examples).
- **SC-003**: Every pay figure matches a hand calculation to the agora on each worked example: a shift with
  its own rate, one with no rate, a 10-hour day shift, a night shift, a Friday-to-Saturday night shift, a shift that crosses into or out of the weekly rest, a
  shift on a 200% special day, and a shift on a change of clocks.
- **SC-004**: In a test of every report and pay view with every role, an Employee or Client never receives
  another person's hours or pay, and a Manager never receives pay for a project they are not assigned to or
  that is not open to Managers.
- **SC-005**: A work-log or hours report over one year for 100 employees and 20 projects is ready in under 3
  seconds.
- **SC-006**: A Manager answers "who has worked at this project since it started" with one request that
  needs only the project.

## Assumptions

- Pay is gross only, before tax. Tax, deductions, pension, travel, holiday pay for days not worked and
  legal payslips are out of scope (docs/hds-spec.md section 1).
- The premium defaults follow common readings of the Israeli Hours of Work and Rest Law: overtime after 8
  hours (7 on a night shift), the first 2 overtime hours at 125% and the rest at 150%, work on the weekly
  rest day at 150%, and overtime on it at 175% then 200%. They are defaults to confirm before launch, not
  legal advice; every one can be changed per project.
- Overtime is counted per shift, as the owner chose; weekly overtime (beyond 42 hours) and daily overtime
  across two shifts on one day are not counted.
- The weekly rest is a fixed window each week at the site (Friday 18:00 to Saturday 22:00 by default),
  close to the Sabbath; it does not follow the exact times of sunset.
- There is no built-in calendar of holidays: Admins list the special days and their rates each year (for
  example Yom Kippur, or an election day at 200%).
- There is no record yet of hours actually worked (EMP-2 is a v1 item); a published shift that has ended is
  taken as worked.
- Figures are worked out from current values every time; there is no history of rates or settings, so a
  change also changes past months of an active project.
- All money is in the organisation's single currency, shown to two decimal places; a line is rounded half
  up to the agora.
- A shift's own rate applies equally to everyone on that shift.
- Group membership is read as it is now; past membership is not kept.

## Out of scope

- Projecting pay for the rest of the month from shifts still to come.
- Weekly overtime and per-day overtime across several shifts.
- A built-in Hebrew holiday calendar.
- Excel and PDF export (RPT-4), cash flow and billing (FIN-4, FIN-5), and the client portal, all v1.
- Recording real worked hours (EMP-2) and the monthly worked-hours report (EMP-3), both v1.
- Availability reports (RPT-3) and calendar feeds (RPT-5).
