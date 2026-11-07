# Feature Specification: UX foundation and redesign

**Feature Branch**: `007-ux-redesign`

**Created**: 2026-11-07

**Status**: Draft

**Input**: User description (the stored prompt `.claude/hds/prompts/06-ux-redesign/specify.md`): "Every
HDS user works in screens built for their role and shaped around their week, in the owner's approved
visual design, with no screen they can't use. Use docs/plans/ux-working-plan.md (sections 2, 3, 5 and
6: the decisions applied, the Option B design language, groups A to H, and the preview stubs) and the
rows it assigns to feature 007 in docs/research/ux-improvements-2026-11.md. The owner's six notes are
requirements: "Roster" and "Work log" in place of "מי עבד"; a weekly roster of shift blocks naming who
filled each one, flexible enough to show a half shift or two extra hours; calendar date pickers with
last day, week and month presets; an interactive availability week with an explicit Submit; My shifts
split into upcoming and past; clients see only what applies to them, never "You are not allowed to do
this." Roles allowed: employee, manager, admin, client. Business rules from the original: four
profiles with access as in docs/hds-spec.md section 2; a manager inherits every employee action;
clients see only their own projects and never employee fields; employees see published shifts only;
Hebrew RTL and English; times in the site's zone; money exact. Proposed and marked so: managers may
edit the roster of the projects they manage; each person's work in a shift has its own start and end;
availability has a submitted state; WCAG 2.2 AA. Placeholder screens for features 008 to 017 appear
only where previews are switched on, never in production. No technology details. Out of scope:
features 008 to 017 of the working plan, tenders, and anything in the legal-review rows L1 to L18."

Row IDs in brackets (N1, S2, E1, …) are the research report's rows that the working plan assigns to
feature 007; decision IDs (O1 to O14) are the plan's section 2. **Owner** marks one of the owner's six
notes. **Proposed** marks anything the original product spec does not state. Everything else changes
only how an original capability looks or behaves.

## Clarifications

### Session 2026-11-07

- Q: Who approves an absence request when feature 016 builds it? → A: Any manager of a project the
  employee works on, or an admin (the plan's default, approved with "apply all").
- Q: Are tenders in scope for 007's client portal? → A: No; no screen or preview mentions them.
- Q: How far may one person's own times in a shift reach past the shift itself? → A: Up to 4 h before
  the shift starts and up to 4 h after it ends; the rest and weekly-cap checks flag anything beyond
  policy.
- Q: An employee has not submitted by the deadline (with or without a draft). What does scheduling
  use? → A: "Count them as unavailable." They count as unavailable for that window and any draft is
  not used. Derived: the auto-scheduler does not assign them in that window; a manager may still
  assign them by hand, with a warning; the submission tracker shows who hasn't submitted; the screen
  tells the employee before the deadline that not submitting means they won't be scheduled; reminders
  come with feature 010.
- Q: How long does a phone stay signed in with biometric unlock? → A: 30 days rolling on phones,
  unlocked by biometrics; the web stays at 7 days.
- Q: (Reviewer, 2026-11-07, defaults accepted) Publish notifications, the shift leader's contact and
  approved hours? → A: Publish records one notification per person for feature 010 to deliver; the
  project manager's contact stands in for the shift leader; approved hours are a feature 011 preview.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Each role sees only what it can use (Priority: P1)

A person signs in and lands on a home screen made for their role. An employee sees "My work"
(availability, my shifts, my pay). A manager sees the same "My work" plus "Manage" (the roster,
reports, schedule runs and the management screens they are allowed to use). An admin additionally sees
the screens only admins may use. A client sees a separate set: Projects, Billing, Requests and
Contact. Nobody meets a tab, button or menu item that leads to a refusal. [Owner: clients see only what
applies to them, never "You are not allowed to do this." N1–N9, N12, C1, C6]

**Why this priority**: It fixes the defect the owner named (clients land on staff screens and are
refused) and every other story's screens hang off this navigation.

**Independent Test**: Sign in as each of the four roles and list the destinations offered; open every
one and confirm none shows a refusal. Follow a link to a page the role may not see and confirm a
"no access" page with a way home.

**Acceptance Scenarios**:

1. **Given** a client user, **When** they sign in, **Then** they see Projects, Billing, Requests and
   Contact, and never My availability, My shifts, My pay, the roster or any employee field.
2. **Given** an employee, **When** they sign in, **Then** they see My work and nothing from Manage or
   Admin.
3. **Given** a manager who also works shifts, **When** they sign in, **Then** one navigation shows both
   My work and Manage, with no profile switch.
4. **Given** any signed-in user, **When** they open a saved link to a page their role may not see,
   **Then** a full "You don't have access to this page" page offers Go home and Contact your manager,
   names no record, and refreshes their permissions and navigation.
5. **Given** an action the user may normally take but is blocked for now (publishing with unfilled
   slots, editing availability after the deadline), **When** they look at it, **Then** it is disabled
   or explained with the reason, rather than hidden.
6. **Given** any screen, **When** the user looks for sign-out, help, contact and the accessibility
   statement, **Then** each is in the same place on every screen.
7. **Given** a client, **When** they open Contact, **Then** they see the company's phone, e-mail and
   opening hours.

---

### User Story 2 - An employee fills in and submits the availability week (Priority: P1)

The employee opens the week that needs submitting and sees it as a grid: Sunday to Saturday, with a
cell for each shift template plus an all-day cell. A tap cycles a cell between unmarked (available),
open (would like to work), close (would rather not) and blocked (can't work). A banner states where the
week stands and the deadline. The employee reviews the week and presses Submit. They can change it
until the deadline; then it locks. [Owner: an interactive availability week with an explicit Submit.
E1–E4, E6, E16; O4; Proposed: the submitted state]

**Why this priority**: It is one of the owner's notes, and the roster can only be as good as the
availability behind it.

**Independent Test**: As an employee with an open window, mark cells, see the banner move from Not
submitted to Draft, review, submit, see Submitted with the time, change a cell, and after the deadline
see Locked.

**Acceptance Scenarios**:

1. **Given** an open availability window, **When** the employee taps a cell, **Then** its state moves
   to the next of unmarked, open, close and blocked, shown by icon, label and colour together.
2. **Given** a week with no changes, **When** the employee first changes a cell, **Then** the banner
   moves from "Not submitted, deadline …" to Draft, and the draft survives leaving the screen or losing
   the connection.
3. **Given** a draft, **When** the employee chooses Submit, **Then** a Review step summarises the week
   before it is sent, and afterwards the banner shows Submitted with the time and "can be changed until
   the deadline".
4. **Given** a submitted week before the deadline, **When** the employee changes a cell, **Then** the
   change is allowed and the week shows it must be submitted again.
5. **Given** the deadline has passed, **When** the employee opens the week, **Then** it shows Locked,
   cells cannot change, and the screen says why.
6. **Given** an employee who has not submitted, **When** they open the week before the deadline,
   **Then** the banner says that if they don't submit by the deadline they count as unavailable and
   won't be scheduled for that window; **given** the deadline passes without a submit, **then** the
   week shows "Not submitted, counted as unavailable" and any draft is not used.
7. **Given** any cell change, **When** the employee chooses Undo, **Then** the previous state returns;
   there is no reset that cannot be undone.
8. **Given** the grid, **When** the employee reads it, **Then** the rules are stated: unmarked means
   available, open and close are wishes rather than promises.
9. **Given** last week's availability, **When** the employee chooses Copy last week, **Then** the
   grid fills from it and can still be changed cell by cell.

---

### User Story 3 - A manager runs the week on the roster (Priority: P1)

The manager's main screen is the weekly roster ("Roster" / "סידור עבודה"): columns Sunday to Saturday,
rows by shift template. Each block lists who filled it with their own times, so a half shift draws as a
partial bar and two extra hours draw past the block's edge. Past days show who worked; future days show
who is scheduled. The manager assigns and moves people by tapping (dragging is a shortcut), sees
coverage and warnings as they go, and publishes through a review that says exactly what will go live.
A manager may edit the roster only for the projects they manage; an admin may edit all of it. [Owner:
"Roster" in place of "מי עבד"; a weekly roster of shift blocks naming who filled each one, flexible
enough to show a half shift or two extra hours. S1–S15, S19, S21; O7, O9; Proposed: managers' roster
edits and each assignment's own start and end]

**Why this priority**: Two of the owner's notes, and the screen managers use most.

**Independent Test**: As a manager of one project, open a week that has a half shift, an extended
shift and an overnight shift; confirm each draws as described; assign someone by tapping; confirm a
broken rest rule shows its rule and numbers; publish and confirm the review's counts; confirm a project
the manager doesn't manage is read-only.

**Acceptance Scenarios**:

1. **Given** a week with assignments, **When** the manager opens the roster, **Then** each block names
   its people with their own start and end times, and coverage reads filled over required (for
   example 2/3).
2. **Given** an assignment from 06:00 to 10:00 in a 06:00–14:00 block, **When** the roster draws it,
   **Then** it shows as a partial bar; **given** 14:00–24:00 in a 14:00–22:00 block, **then** the two
   extra hours draw past the block's edge.
3. **Given** a shift that crosses midnight, **When** the roster draws it, **Then** it sits on the day it
   starts with a continuation marker on the next day, and its end time carries "+1".
4. **Given** an unfilled seat, a draft shift, a published shift and a shift changed since publishing,
   **When** the roster draws them, **Then** each is distinguishable by shape as well as colour (dashed
   empty slot, striped draft, solid published, a changed marker).
5. **Given** a person and a shift, **When** the manager selects the shift and chooses Assign to or Move
   to, **Then** the change happens without dragging, and the people who prefer the shift are listed
   first with their availability shown.
6. **Given** an assignment that breaks a rule, **When** it is placed, **Then** the block shows a count
   badge, and opening it names the person, the rule and the numbers (for example "Dana: 11 h rest
   required, 8 h scheduled"); a warning can be overridden only with a reason, and a hard stop cannot.
7. **Given** unpublished changes, **When** the manager chooses Publish, **Then** a review states the
   exact scope, the number of shifts and people affected, coverage, unfilled slots and open warnings,
   before anything goes live.
8. **Given** unpublished changes, **When** the manager chooses Undo, **Then** the changes come back
   off one step at a time.
9. **Given** a manager and a project they don't manage, **When** they open its roster, **Then** they
   can see it but not change it.
10. **Given** an assignment with its own start and end, **When** pay and the rule checks run, **Then**
    they use the assignment's times; **given** none were set, **then** the shift's times apply.
11. **Given** an open availability window, **When** the manager opens the submission tracker, **Then**
    it lists who has and hasn't submitted.
12. **Given** a phone, **When** a manager opens the roster, **Then** a day agenda offers the same
    tap-to-assign and publishing.

---

### User Story 4 - An employee sees upcoming and past shifts, and pay (Priority: P2)

My shifts has two parts: Upcoming, grouped by week, with the next shift as a card at the top; and
Past, grouped by pay period. Each shift opens its details, and can be added to the person's calendar.
My pay offers a pay-period picker, an Hours view and a Pay view whose breakdown by rate adds up to the
total and is labelled an estimate. [Owner: My shifts split into upcoming and past. E8–E12]

**Why this priority**: An owner note, on the screen employees open most after availability.

**Independent Test**: As an employee with past and future published shifts, confirm the split, the
grouping, the next-shift card and "+1" on an overnight shift; open My pay and confirm the breakdown sums
to the total.

**Acceptance Scenarios**:

1. **Given** past and future published shifts, **When** the employee opens My shifts, **Then**
   Upcoming and Past are separate, Upcoming grouped by week and Past by pay period.
2. **Given** a next shift, **When** the employee opens My shifts or their home, **Then** a card shows
   its date, times (with "+1" when it ends the next day), site and a countdown.
3. **Given** draft shifts, **When** the employee looks at any screen, **Then** they never see them.
4. **Given** a shift, **When** the employee opens it, **Then** they see its details and can add it to
   their calendar.
5. **Given** a pay period, **When** the employee opens My pay, **Then** the Hours and Pay views show
   the period's figures, the breakdown by rate adds up exactly to the total, and the figures are
   labelled "estimate, before tax".

---

### User Story 5 - Every date is picked from a calendar with presets (Priority: P2)

Wherever a date or a range is entered, the user can type it (any separator) or pick it from a
calendar, and ranges offer presets: Today, Yesterday, This week, Last week, Last 7 days, Last 30 days,
This month, Last month and Custom; reports add This pay period and Last pay period. Weeks start on
Sunday and every preset is calculated in the site's time zone. [Owner: calendar date pickers with last
day, week and month presets. D1–D7]

**Why this priority**: An owner note, used by reports, schedule runs and admin screens.

**Independent Test**: In a report, pick each preset and confirm the dates shown under it and the
highlighted range agree; type a date with dots, slashes and dashes; try an end before the start.

**Acceptance Scenarios**:

1. **Given** a range field, **When** the user picks a preset, **Then** the actual dates appear under
   it and the calendar highlights the same range.
2. **Given** a preset is chosen, **When** the user changes a day by hand, **Then** the preset becomes
   Custom.
3. **Given** a date field, **When** the user types 7/11/2026, 7.11.2026 or 7-11-2026, **Then** each is
   read as 7 November 2026.
4. **Given** a range, **When** the end is before the start, **Then** it is refused with a reason.
5. **Given** a report, **When** it opens, **Then** the range defaults to the current pay period and
   cannot reach into the future; a schedule run's range cannot start in the past.
6. **Given** the calendar opened by keyboard, **When** the user uses the arrow, Page Up, Page Down and
   Escape keys, **Then** it moves by day, week and month and returns focus to the field, and the chosen
   date is announced.
7. **Given** any date shown, **When** it is numeric, **Then** it reads dd/MM/yyyy, and headers spell
   out Hebrew or English month and weekday names.

---

### User Story 6 - The whole app in the approved design, accessible in both languages (Priority: P2)

Every screen is drawn in the owner's approved design language (Option B, the working plan's section 3):
its colours, type, square shapes, status chips and layouts, in light, dark and two high-contrast themes.
Every screen works in Hebrew right to left and in English, on phones and on the web, and meets WCAG 2.2
AA. [X1–X17, D8–D13; Proposed: WCAG 2.2 AA]

**Why this priority**: The design is what the owner approved; accessibility is a floor, and is
cheapest when built in from the first screen.

**Independent Test**: Open every screen in Hebrew and English, light and dark, at phone and desktop
sizes; run an automated accessibility check on each; check colour contrast of every text and icon pair
in the design values.

**Acceptance Scenarios**:

1. **Given** any screen, **When** it is viewed in Hebrew, **Then** the layout, back and next arrows,
   week arrows and progress bars are mirrored, and numbers, phone numbers, times, charts and the clock
   icon are not.
2. **Given** a Hebrew sentence that contains a name, phone number, time or amount, **When** it is
   shown, **Then** the inserted value keeps its own direction and does not reorder the sentence.
3. **Given** any amount of money, **When** it is shown, **Then** it is exact to the agorot and formatted
   by the user's language, never rounded for display without saying so.
4. **Given** any status, **When** it is shown, **Then** it uses an icon, a label and a colour together,
   and is still distinguishable in grayscale.
5. **Given** a phone, **When** the user taps any control, **Then** its target is at least 44 pt / 48 dp;
   in dense desktop tables never under 24 px.
6. **Given** keyboard or screen-reader use, **When** the user moves through a screen, **Then** focus is
   always visible, never hidden behind sticky bars or toasts, and status messages are announced.
7. **Given** the language switch on the Account screen, **When** the user changes language, **Then**
   the direction and all text change, with a notice if the app must restart.
8. **Given** Hebrew copy, **When** it addresses the user, **Then** it is gender-neutral (nouns and
   infinitives on buttons, plurals in sentences).
9. **Given** the sign-in screen, **When** the user signs in, **Then** paste and password managers work
   and there is no memory or puzzle test.

---

### User Story 7 - Managers and admins work through tables, forms and reports (Priority: P3)

Staff, clients, sites, projects, shift templates, staffing needs, special days and pay settings use
one table pattern (sorting, filtering, saved views, a side panel for details) and one form pattern
(validation on leaving a field, an error summary, an explicit Save). Money changes confirm in specific
words. The work log ("Work log" / "יומן עבודה") and the hours report open with key figures and bars
that open into rows. Schedule runs keep today's behaviour in the new design, labelled with how good the
result is. [Owner: "Work log" in place of "מי עבד". A1–A11, A13, A19, N10, AS9, S1]

**Why this priority**: These screens work today; the redesign makes them consistent and safe for
money, after the owner's notes are met.

**Independent Test**: Sort and filter staff, save a view, reload and confirm the filters stay; submit a
form with errors and confirm the summary takes focus; change a pay rate and read the confirmation;
open the work log and confirm the key figures match the table.

**Acceptance Scenarios**:

1. **Given** a table, **When** the user sorts, filters or saves a view, **Then** the web address keeps
   the filters, view and open record, so a reload or a shared link shows the same thing.
2. **Given** a picker for employees, projects, clients or sites, **When** the user types part of a
   Hebrew or English name, **Then** matching records appear with a second line and recent items first.
3. **Given** a form with errors, **When** the user saves, **Then** an error summary at the top takes
   focus and each field shows its error, which clears as soon as it is fixed.
4. **Given** a pay-rate change, **When** the user saves, **Then** the confirmation names the change
   ("Change the hourly rate for 14 employees from 45.00 to 48.00 from 1 Dec"), and money is never saved
   without an explicit Save.
5. **Given** unsaved changes, **When** the user tries to leave, **Then** they are asked first.
6. **Given** a list loading for the first time, **When** data has not arrived, **Then** placeholder rows
   show, never "No records".
7. **Given** a screen with nothing in it, **When** it is first used, **Then** an empty state names one
   next action; an admin with an empty organisation sees a short setup checklist.
8. **Given** the work log or hours report, **When** it opens, **Then** a row of key figures matches the
   table below it, and each bar opens its rows.
9. **Given** a schedule run, **When** the manager generates it with a preset range, **Then** progress
   shows while it runs, and the proposal is labelled "Optimal" or "Best found in N s"; accept and
   discard work as today.

---

### User Story 8 - Previews of later features, never in production (Priority: P3)

Each screen that a later feature (008 to 017) fills in already exists as a clearly marked preview,
placed where the real screen will go and shown only to roles that will use it. It says what it will
do and which feature brings it, and nothing on it pretends to work. Previews appear only where they
are switched on (development and the shared test environment) and never in production. [Plan section
6; Proposed]

**Why this priority**: Lets the owner see the whole shape early, without real users ever meeting a
placeholder.

**Independent Test**: With previews on, confirm each preview in section 6's list sits in its place for
its roles; with previews off, confirm none is reachable.

**Acceptance Scenarios**:

1. **Given** previews switched on, **When** a role opens its navigation, **Then** each preview for that
   role appears where its feature will put the real screen, marked as a preview, naming its feature.
2. **Given** previews switched off, **When** any user browses or follows a link to a preview, **Then**
   no preview is shown or reachable.
3. **Given** a preview, **When** the user interacts with it, **Then** nothing on it acts as if it
   worked.

---

### User Story 9 - The app feels finished: motion, loading and words (Priority: P2)

Screens move with short, sharp transitions that suit Option B's square look; waiting shows a thin
copper-and-tan bar under the header instead of a frozen screen; every word, in Hebrew and English,
comes from one glossary and one voice. The app opens on a branded splash, and sign-in, the 404 page,
the offline banner and toasts all look like the same product. [Owner, 2026-11-07: "a polished, well
structured design and language wise product, with a full set of sub features like tasteful
animations and custom loading bar." Proposed. X8, E17, AS8's visible part]

**Why this priority**: The owner asked for polish as the goal of the sprint; motion and loading
feedback also tell users the app is working.

**Independent Test**: Delay one route and confirm the bar appears after about 300 ms and not for a
fast route; emulate reduce-motion and confirm nothing animates; run the scheduler and watch the bar's
value rise; open the splash, 404 and offline banner in both languages; compare both locale files.

**Acceptance Scenarios**:

1. **Given** a route or data load longer than about 300 ms, **When** it is pending, **Then** a thin bar
   under the header shows indeterminate progress; **given** a load under that, **then** no bar flashes.
2. **Given** a schedule run, **When** it is running, **Then** the bar is determinate, showing elapsed
   time against the time limit and the best result so far, and its value is announced.
3. **Given** the device's reduce-motion setting is on, or switched on while the app is open, **When**
   any screen changes, **Then** nothing animates and the bar shows a still state.
4. **Given** a sheet or side panel, **When** it opens, **Then** it slides from the leading edge
   (mirrored in Hebrew), and input is never blocked while anything animates.
5. **Given** a phone, **When** the user submits availability or assigns a shift, **Then** a light
   success haptic confirms it.
6. **Given** the two locale files, **When** they are compared, **Then** they have the same keys, no
   empty or placeholder strings, and every domain term matches the glossary.
7. **Given** a first launch, an unknown address or a lost connection, **When** it happens, **Then** the
   Option B splash, 404 page or offline banner shows in the user's language.

---

### Edge Cases

- A person's role changes while they are signed in (promoted to manager, made admin, resigns): the next
  refused action refreshes their permissions and redraws the navigation, without naming the record
  behind the refusal.
- A client user follows a link to a staff screen, or to another client's project: the no-access page,
  with nothing about the other client.
- The deadline passes and the employee never submitted (with or without a draft): they count as
  unavailable for the whole window, the draft is not used, the auto-scheduler leaves them out, and a
  manager who assigns them by hand sees a warning.
- An employee submitted, then changed a cell and did not submit again before the deadline: the last
  submitted week stands and the unsent change is dropped; the screen says so.
- The device is offline while filling availability: the draft is kept on the device and sent when the
  connection returns; My shifts shows the last loaded list with "updated hh:mm".
- An assignment's own times start more than 4 h before its shift starts or end more than 4 h after
  it ends: refused with a reason. Within those 4 h the rest and weekly-cap checks still warn. An end
  after midnight shows "+1" rather than reading as an end before the start.
- A daylight-saving change makes a night 7 or 9 real hours: the shift's duration shows the real hours
  and says why.
- Two managers edit the same project's roster at once: the second save is told the roster changed and
  shown the current version, never silently overwriting.
- A manager manages some, but not all, of the projects on one site: they can change only their
  projects' blocks; the others are read-only in the same view.
- A Hebrew name, an English name and a phone number appear in one line: each keeps its own direction.
- Publishing would cover zero shifts, or shifts hidden by a filter: the review states the exact count
  and scope, including what the filter leaves out.
- A schedule run's proposal arrives after the manager hand-edited the same week: accept and discard
  behave as today.
- A screen reader user meets a drag-only gesture: there is none; every drag has a tap path and a
  screen-reader action.

## Requirements *(mandatory)*

### Functional Requirements

**Roles and navigation (group B)**

- **FR-001**: The system MUST decide everything a signed-in user is offered (destinations, screens,
  buttons, menu items) from one map of what that person may do, the same map the system uses to allow
  or refuse their actions, so the two never disagree. [N2]
- **FR-002**: The system MUST give the four profiles access exactly as in `docs/hds-spec.md` section 2:
  a manager inherits every employee action; managers create and edit employees, clients, sites and
  projects; only admins delete them, edit pay on a shift and change admin-only settings.
- **FR-003**: Staff (employee, manager, admin) MUST have one navigation with a "My work" group for
  everyone, a "Manage" group for managers and admins, and the admin-only screens for admins, with no
  profile switch. [N4, O8]
- **FR-004**: Clients MUST have their own navigation (Projects, Billing, Requests, Contact) and MUST
  never see a staff destination or any employee field. [N3, C1, CLI-1]
- **FR-005**: Phones MUST use a bottom bar of at most five destinations and the web a persistent side
  navigation with text labels, both mirrored in Hebrew. [N5, N6]
- **FR-006**: Each role MUST land on its own home screen: employees on their next shift and the
  availability deadline; managers on today's coverage, unfilled slots and who hasn't submitted; clients
  on one summary per project. [N7]
- **FR-007**: No destination the user is offered may end in a refusal. When a refusal still happens
  (stale permissions, a deep link), a screen MUST show a full no-access page with Go home and Contact
  your manager, an action MUST show an inline banner, the user's permissions and navigation MUST
  refresh, and the message MUST never name the record behind the block. The phrase "You are not
  allowed to do this." MUST not appear anywhere. [N1, N8]
- **FR-008**: Actions blocked only for now MUST be shown disabled or explained with the reason; actions
  a role may see but never change MUST be shown read-only; actions a role can never take MUST be hidden.
  [N9]
- **FR-009**: Every role MUST have one Account screen with the language switch, active sessions with
  "sign out of other devices", and a sign-out that is visible on every screen. Help, contact and the
  accessibility statement MUST sit in the same place on every screen. [N12, X10]
- **FR-010**: Sessions on phones MUST stay signed in for 30 days, renewed with each use, and unlock
  with the device's biometrics where available; web sessions keep today's 7 days. A session that does
  end MUST say so visibly. [E15; Proposed]
- **FR-011**: An invitation MUST open the set-password screen in the invitee's language, then the
  role's home. [N11]
- **FR-012**: Clients MUST have a contact card with the company's phone, e-mail and opening hours.
  [C6]

**Availability (group C)** — *Proposed: the submitted state*

- **FR-013**: Employees MUST fill availability on a week grid, Sunday to Saturday, with a cell for each
  shift template plus an all-day cell, one week at a time with arrows to the other weeks of the open
  window. A tap MUST cycle unmarked, open, close and blocked, and a sheet with the choices MUST also be
  available. [Owner, E1]
- **FR-014**: Each employee's availability for each window MUST have a status: Not submitted, Draft,
  Submitted (with the time, changeable until the deadline) and Locked (after the deadline). A change
  after submitting MUST return the week to Draft until it is submitted again. Scheduling MUST use the
  employee's last submitted availability. An employee with no submission for a window when its
  deadline passes MUST count as unavailable for the whole window; any draft MUST not be used; the
  auto-scheduler MUST not assign them in that window; a manager MAY still assign them by hand, with a
  warning. Before the deadline the grid MUST tell the employee that not submitting means they won't
  be scheduled. Reminders come with feature 010. [E2, O4; Proposed]
- **FR-015**: Submit MUST pass through a Review step that summarises the week. [E2]
- **FR-016**: The grid MUST state its rules: unmarked means available, open and close are wishes, not
  promises. [E3]
- **FR-017**: Employees MUST be able to copy last week into the grid and change it from there. [E4]
- **FR-018**: Each cell change MUST be undoable; there MUST be no reset that cannot be undone. [E6]
- **FR-019**: A draft MUST survive leaving the screen and a lost connection, and My shifts MUST be
  readable offline with the time it was last updated. [E16]
- **FR-020**: Until feature 016 brings absence requests, today's absence entries MUST show on the grid
  as full-day bands. [O3]

**My shifts and pay (group C)**

- **FR-021**: My shifts MUST split Upcoming (grouped by week) from Past (grouped by pay period, with
  total hours and estimated pay), and MUST show published shifts only. [Owner, E8, EMP-1]
- **FR-022**: The next shift MUST lead as a card with its date, times ("+1" for overnight), site, post
  and a countdown. [E9]
- **FR-023**: Each shift MUST open its details (site address, map links, post instructions where they
  exist, and the project manager's contact in place of a shift leader, whom HDS does not record) and offer "Add to calendar" on phones and a calendar file on the
  web. [E10, E11]
- **FR-024**: My pay MUST offer a pay-period picker, an Hours view (scheduled, and worked as the past published shifts; approved hours appear as a
  preview until feature 011) and a
  Pay view (estimated gross, a breakdown by rate that adds up exactly to the total, the change since
  the last period), labelled "estimate, before tax" until payroll is final. [E12, EMP-3]

**The roster (group D)** — *Proposed: managers' roster edits; each assignment's own start and end*

- **FR-025**: The weekly screen MUST be named "Roster" / "סידור עבודה", and the long-range report
  "Work log" / "יומן עבודה". The words "מי עבד" MUST not name any screen. [Owner, S1, O13]
- **FR-026**: The roster MUST show columns Sunday to Saturday and rows by shift template; each block
  MUST name who filled it with their own times, drawing a half shift as a partial bar and extra hours
  past the block's edge. Past days MUST show who worked and future days who is scheduled. [Owner, S2]
- **FR-027**: Each assignment MUST have its own start and end, defaulting to its shift's, starting no
  more than 4 h before the shift starts and ending no more than 4 h after it ends. Pay, the rule checks
  and the reports MUST use the assignment's times. [S3, O7; Proposed]
- **FR-028**: Managers MUST be able to edit the roster of the projects they manage; admins MUST be able
  to edit all of it; anyone else who may see a roster MUST see it read-only. [O9; Proposed, changing
  the original's admin-only hand edits]
- **FR-029**: The roster MUST switch its span (day, week, month) and grouping (by shift template, by
  employee, by project and site), and keep a range mode for "since the project started". [S4]
- **FR-030**: Status MUST be shown by shape as well as colour: draft striped, published solid, a
  changed-since-publish marker, and unfilled seats as dashed empty slots. [S5]
- **FR-031**: Each block MUST show coverage as filled over required, each day a coverage summary, and
  each person's hours against their weekly cap. [S6]
- **FR-032**: An overnight shift MUST be drawn on the day it starts with a continuation marker on the
  next day. [S7]
- **FR-033**: Every drag MUST have a tap path (select, then Assign to or Move to) and a screen-reader
  action. [S8, X3]
- **FR-034**: Rule warnings MUST be layered: a count badge on the block; the rule, the person and the
  numbers on opening it; totals per person; a list of warnings with "Show on roster". Warnings MUST be
  overridable with a recorded reason; hard stops MUST not be overridable. [S9, S10, O10]
- **FR-035**: The roster MUST show each person's availability (unavailable, prefers, would rather not,
  absence) and list those who prefer a shift first when picking. [S11]
- **FR-036**: Publishing MUST go through a review stating the exact scope, the number of shifts and
  people affected, coverage, unfilled slots and open warnings; one notification for each affected person
  MUST be recorded for delivery per publish, however many shifts it covers, with the publisher's
  choice to notify or publish silently; feature 010 delivers them. [S12]
- **FR-037**: Unpublished changes MUST be undoable step by step. [S13]
- **FR-038**: Managers MUST be able to copy last week (choosing what happens on conflicts), save and
  apply week templates, and bulk edit with a preview that states the scope. [S14, S15]
- **FR-039**: On phones, managers MUST have a day agenda with the same tap-to-assign and publishing.
  [S19]
- **FR-040**: Managers MUST have a submission tracker for the open window, showing who has and hasn't
  submitted, and after the deadline who counts as unavailable for not submitting. [S21]
- **FR-041**: Two people changing the same roster MUST never silently overwrite each other; the later
  save MUST be told and shown the current version.

**Schedule runs (group E)**

- **FR-042**: The run list, the generate form (with the date presets) and the proposal view MUST be
  drawn in the new design, and every proposal MUST be labelled "Optimal" or "Best found in N s".
  Accept and discard MUST work as today. [AS9]

**Admin screens (group F)**

- **FR-043**: Staff, clients, sites, projects, shift templates, staffing needs, special days and pay
  settings MUST share one table pattern (a readable first column, sorting, search, a few filters with
  "More filters", a marker when filters are on, saved views, pagination, details in a side panel, and a
  bulk-action bar that states the count and scope). [A1–A5]
- **FR-044**: Long lists of employees, projects, clients and sites MUST be chosen through one
  searchable picker that matches Hebrew and English names, shows a second line and puts recent items
  first; two to five options MUST use a segmented control. [A6]
- **FR-045**: Forms MUST validate a field when it is left and clear the error once fixed, show an error
  summary at the top that takes focus, have one Save that is never disabled, never save money
  automatically, and ask before leaving with unsaved changes. [A7]
- **FR-046**: Money changes MUST be confirmed in words that name the change, its scope and its date;
  archive and remove MUST offer undo; typed confirmation only for the rarest destructive actions. [A8]
- **FR-047**: Loading MUST match the wait (nothing under a second, placeholder rows on a list's first
  load, progress with step text beyond ten seconds), never showing "No records" while loading.
  Non-critical news MUST use short notices; critical messages MUST stay inline and never time out;
  nothing touching pay may appear saved before the system confirms it. [A9, A11]
- **FR-048**: Every empty screen MUST name one next action, and an admin with an empty organisation
  MUST see a short setup checklist (site, project, shift templates, invite staff, first roster). [A10,
  N10]
- **FR-049**: On the web, filters, views, date ranges and the open record MUST live in the web address.
  [A19]

**Reports (group G)**

- **FR-050**: The work log and the hours report MUST open with a row of key figures that match the
  table, bars that open into their rows, pay-period presets defaulting to the current period, and the
  date picker. [A13, D4]

**Dates, times, languages and money (group A)**

- **FR-051**: Every date and range input MUST accept typing with any separator and offer a calendar
  with today marked, impossible dates disabled, an end before the start refused, and the presets of
  User Story 5, calculated in the site's time zone with Sunday weeks. The dates a preset stands for
  MUST appear under it and match the highlighted range. [Owner, D1, D2, D4]
- **FR-052**: The calendar MUST be fully usable by keyboard (day, week and month movement, Escape back
  to the field) and announce the chosen date. [D3]
- **FR-053**: Shift times MUST be 24-hour HH:mm with "+1" when the end is not after the start, show the
  duration live, and explain a 7- or 9-hour night caused by a daylight-saving change. All times MUST be
  shown in the site's time zone. [D5]
- **FR-054**: Weeks MUST start on Sunday everywhere, Friday shown as a short day and Saturday as rest,
  with Israeli holidays and their eves marked on the roster, the availability grid and the pickers.
  [D6]
- **FR-055**: Numeric dates MUST read dd/MM/yyyy, with month and weekday names spelled out in headers.
  [D7, O13]
- **FR-056**: Names, e-mails, phone numbers, IDs, times and amounts inside sentences MUST keep their
  own direction. [D8]
- **FR-057**: In Hebrew, layout and directional arrows MUST mirror; numbers, phone numbers, charts and
  the clock icon MUST not. [D9, O13]
- **FR-058**: Money MUST be shown exactly as the system holds it, formatted by the user's language,
  never computed or rounded on screen. [D10]
- **FR-059**: Changing language MUST switch text and direction, explaining and performing a restart
  where the device needs one. [D12]
- **FR-060**: Hebrew copy MUST be gender-neutral. [D13, O13]

**Design and accessibility (groups A and H)** — *Proposed: WCAG 2.2 AA*

- **FR-061**: Every screen MUST follow the approved design language (the working plan's section 3:
  colours, type, shapes, chips, page titles with breadcrumbs, header and tab treatments, density), in
  four themes: light, dark (following the device with an in-app override) and high-contrast light and
  dark. [X11, X12, X15]
- **FR-062**: Every text and icon colour pair in the design values MUST meet WCAG contrast, checked
  automatically. [X13]
- **FR-063**: Every control MUST have the same set of states: default, hover, pressed, focused,
  selected, disabled but legible, busy and error. [X14]
- **FR-064**: Every screen MUST meet WCAG 2.2 AA (with the Israeli standard's section-heading change):
  targets of 44 pt / 48 dp on phones and never under 24 px in dense tables; a single-pointer
  alternative to every drag; status messages announced; focus never hidden by sticky bars, tab bars or
  notices, with a visible focus ring; status by icon, label and colour; respect for the device's text
  size; and sign-in that allows paste and password managers with no memory test. [X1–X7]
- **FR-065**: Every screen MUST be checked in Hebrew and English, light and dark, on phone and desktop,
  with an automated accessibility check and a manual screen-reader pass on phones in both languages.
  [X17, H]
- **FR-066**: The accessibility statement page MUST exist, with its text supplied by the owner. [L15
  only as a page; its legal content is out of scope]
- **FR-067**: A gallery of the building blocks with every state MUST be available outside production,
  so they can be checked without data. [Plan group A]

**Previews (section 6)** — *Proposed*

- **FR-068**: Each screen in the working plan's section 6 MUST exist as a preview in the role and place
  its later feature will use, gated by the same permission map, saying what it will do and which
  feature brings it, with nothing that pretends to work.
- **FR-069**: Previews MUST appear only where they are switched on (development and the shared test
  environment) and MUST be unreachable in production. Each later feature replaces its previews.

**Motion, loading and finish** — *Proposed (the owner's note of 2026-11-07)*

- **FR-070**: The design values MUST include motion: three durations (about 120, 200 and 320 ms), a
  standard and an emphasised easing, and no bounce or overshoot.
- **FR-071**: Transitions MUST accompany screen changes, sheets and side panels (sliding from the
  leading edge, mirrored in Hebrew), list rows entering and leaving, status-chip changes, assigning
  and dragging on the roster, feature panels revealing their clipped corner, and loading
  placeholders (a shimmer). No animation may block input. [X14]
- **FR-072**: All motion MUST follow the device's reduce-motion setting, including a change made
  while the app is open, on phones and on the web. [X8]
- **FR-073**: Phones MUST give a light haptic on availability cell changes and a success haptic on
  submitting availability, assigning and publishing. [E17, moved into scope]
- **FR-074**: A loading bar of the design's own MUST sit under the header: thin, copper and tan, with
  the clipped-corner motif; indeterminate for route and data loads and shown only after about 300 ms;
  determinate for schedule runs, showing elapsed time against the time limit and the best result so
  far. It MUST be exposed as a progress bar with its value announced, and MUST show a still state
  under reduce-motion. [AS8's visible part]
- **FR-075**: A glossary MUST fix the Hebrew and English name of every domain term (for example Roster /
  סידור עבודה, Work log / יומן עבודה, Availability / זמינות), and a short voice-and-tone guide MUST set
  plain, short, gender-neutral copy, nouns and infinitives on Hebrew buttons, and one format each for
  dates, times and money. All copy, including every empty state and error, MUST follow both. [D13]
- **FR-076**: The Hebrew and English locale files MUST have the same keys and no empty or placeholder
  strings, checked automatically.
- **FR-077**: The app MUST have an Option B splash screen, app icon and favicon, and a branded sign-in,
  404 page, offline banner and toasts, with visible focus rings and every theme finished.

### Key Entities *(include if feature involves data)*

- **Capability map**: for a signed-in person, the set of things they may do (see, change, approve),
  derived from their profile and, for managers, the projects they manage. Drives both what they are
  offered and what the system allows.
- **Availability submission** *(Proposed)*: one per employee and availability window; status (Not
  submitted, Draft, Submitted, Locked), when it was submitted, and the entries it covers. Locks at the
  window's deadline.
- **Assignment** *(changed, Proposed)*: joins a shift and a person, now with its own start and end
  (defaulting to the shift's), which pay, rule checks and reports use.
- **Rule override** *(Proposed)*: a manager's or admin's decision to keep an assignment despite a
  warning, with who, when and the reason.
- **Week template** *(Proposed)*: a saved pattern of shifts and assignments a manager can apply to a
  week.
- **Saved view**: a named set of filters and sorting on a table, for one user.
- **Organisation contact details**: the phone, e-mail and opening hours a client's contact card shows.
- **Preview**: a placeholder screen with its role, its place, and the feature that replaces it; shown
  only where previews are switched on.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: For each of the four roles, 100% of the destinations, buttons and menu items offered
  open without a refusal, and the phrase "You are not allowed to do this." appears nowhere.
- **SC-002**: A client user is offered exactly Projects, Billing, Requests and Contact (plus Account),
  and no screen they can reach shows an employee field.
- **SC-003**: An employee can fill and submit a week of availability on a phone in under two minutes,
  and copy last week and submit in under 30 seconds.
- **SC-004**: Every employee can tell at a glance whether their week is submitted: the status is shown
  on the grid at all times, in words.
- **SC-005**: A manager can find who works a given shift on a given day in under 10 seconds from the
  roster, and assign a person to an open seat in three taps or fewer, without dragging.
- **SC-006**: A half shift, a shift with two extra hours and an overnight shift each draw distinctly
  and correctly on the roster, and pay for them matches their own times exactly.
- **SC-007**: Every date or range input in the app offers the presets, and a preset's stated dates and
  highlighted range agree in 100% of cases.
- **SC-008**: Every screen passes an automated WCAG 2.2 AA check with no serious issues, in Hebrew and
  English, light and dark, at phone and desktop sizes, and every text and icon colour pair passes its
  contrast check.
- **SC-009**: In production, no preview is reachable; with previews on, each preview listed in the
  working plan's section 6 is present in its place.
- **SC-010**: Every amount of money shown matches the system's exact value to the agorot.
- **SC-011**: No loading bar appears for loads under 300 ms; with reduce-motion on, no element on any
  screen keeps a transition; during a schedule run the bar's value only rises.
- **SC-012**: The two locale files have identical key sets with no empty or placeholder strings, and
  every domain term in them matches the glossary.

## Assumptions

- **Absence approval (O3)**: absence becomes a request with approval in feature 016, approved by any
  manager of a project the employee works on, or an admin (confirmed in clarify). Until then 007
  draws today's absence entries as full-day bands.
- **Tenders (O6)**: out of scope; no screen or preview mentions them (confirmed in clarify).
- **Client content**: in 007 the client shell carries its navigation, the contact card and the home
  screen with one summary per project; Billing and Requests are previews filled by feature 009, and
  coverage for clients will show counts only, never names (O5).
- **Approved design**: the working plan's section 3 is the full statement of the design language; the
  owner's canvas is not needed to build it.
- **Building-block choice**: which set of ready-made building blocks to use is decided in the plan
  stage after a one-day right-to-left trial (O1, O2).
- **Channels**: "Notify everyone" on publish uses the notification channels that exist today; the
  inbox and push come in feature 010 (O14).
- **Rule severities**: which rules are warnings and which are hard stops stays as today's rules define
  them; legal limits wait for the legal review (O10).
- **Pay rules**: one rule set until feature 017 (O11).
- **Out of scope**: features 008 to 017 (their screens appear only as previews), tenders, and legal
  rows L1 to L18. The research's "Could" rows S18 and X9 may be taken if they fit their groups; E17 (haptics) is in scope (FR-073).
- **Shipping**: 007 ships in five steps (A; B; C; D with E; F to H), each usable on its own.
