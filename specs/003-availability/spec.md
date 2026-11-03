# Feature Specification: Availability (Part 1: leftovers from 002; Part 2: availability and manual shifts)

**Feature Branch**: `003-availability`

**Created**: 2026-11-03

**Status**: Draft

**Input**: User description: "Part 1 of feature 003 (availability), done first: four leftovers from 002-security-hardening, which the owner chose on 2026-11-03. (1) A Manager or Admin can change a client's contract start date; the rule that the contract end is not before the start is checked against the pair as it will be after the change, and a pair that already breaks the rule does not block an edit that does not touch either date. (2) Small corrections with no new capability: remove service operations that no request can reach (promoting to Manager or Admin, which role editing already covers); a Client account's project list never treats a missing client as "no filter"; a change request that names a field the operation does not accept is refused instead of silently ignored. (3) The bulk clean-up of expired sign-in tokens and one-time links finds expired records without reading every record. (4) The two account fields that held the sign-in failure count and lock, which nothing has used since 002 moved that count to one shared store, are removed. No behaviour visible to users changes except (1) and the refusal of unknown fields in (2). Availability itself is Part 2, specified later from the stored prompt. No technology details."

**Scope note**: This feature is built in two parts. **Part 1** (this text) finishes four items left over
from feature 002 and is implemented first. **Part 2**, availability and manual shifts (docs/hds-spec.md
AVL-*, EMP-1 and the manual SCH rows), is specified from the stored prompt and appended below.
Part 1 items are *Proposed* (Constitution I): they correct existing behaviour or finish work 002 began,
and add no capability beyond editing a client's start date.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Correct a client's contract start date (Priority: P1)

A Manager or Admin finds that a client was saved with the wrong contract start date and corrects it,
without deleting and re-creating the client, which is often impossible because projects belong to it.

**Why this priority**: Today a wrong start date cannot be fixed at all, and it can stop a correct end
date from being saved, because an end before the start is refused. It is the only change in Part 1
that users can see.

**Independent Test**: Create a client with a start date, change only the start date, and confirm it is
stored; then try a start date later than the stored end date and confirm it is refused and nothing
changes.

**Acceptance Scenarios**:

1. **Given** a client with start 2026-01-01 and no end date, **When** a Manager changes the start to
   2026-02-01, **Then** the start is stored as 2026-02-01.
2. **Given** a client with start 2026-01-01 and end 2026-06-30, **When** a Manager changes the start
   to 2026-07-01, **Then** the change is refused with the client date-order error and nothing changes.
3. **Given** a client with start 2062-01-01 (a typing error) and no end date, **When** a Manager sends
   start 2026-01-01 and end 2026-12-31 in one change, **Then** both are stored, because the rule is
   judged on the pair as it will be after the change.
4. **Given** a client whose stored pair already breaks the rule, **When** a Manager renames it without
   sending either date, **Then** the rename is stored.
5. **Given** any client, **When** an Employee or a Client account tries to change its start date,
   **Then** it is refused as for any other client change.

---

### User Story 2 - Edits never silently ignore a field (Priority: P2)

Someone who sends a change with a field the operation does not accept, for example a misspelt field
name or a field that cannot be edited there, is told so, instead of receiving a success that did not
do what they meant.

**Why this priority**: A silent success hides mistakes. Feature 002 found that a cleared value sent
under a name the operation does not accept was answered as a success and changed nothing.

**Independent Test**: Send a change to a client, an employee, a project and a site that includes one
unknown field, and confirm each is refused and nothing is stored.

**Acceptance Scenarios**:

1. **Given** any record that can be edited, **When** a change names a field the operation does not
   accept, **Then** the change is refused with the request validation error and nothing is stored,
   including the fields that were valid.
2. **Given** a user editing their own profile, **When** the change names a field they may not edit,
   **Then** the existing refusal for a field not allowed on one's own profile still applies.

---

### User Story 3 - Housekeeping that changes nothing users see (Priority: P3)

The organisation's data stays correct and tidy as it grows: unused account fields are gone, the
clean-up of expired credentials stays quick however many records are kept, internal operations that
no request can reach are removed, and a Client account without a client can never be shown everyone's
projects by any route.

**Why this priority**: None of these changes what a user sees today, but each removes a way for a
later change to go wrong, and the clean-up would otherwise slow down as records accumulate.

**Independent Test**: The whole existing test suite passes unchanged, and the published API document
changes only where Stories 1 and 2 say it does.

**Acceptance Scenarios**:

1. **Given** a large number of unexpired credentials and a few expired ones, **When** the clean-up
   runs, **Then** it removes exactly the expired ones, and its work grows with the number of expired
   records, not with the total.
2. **Given** the retired account fields, **When** this feature is installed, **Then** they no longer
   exist, and every sign-in, lock and reset behaves exactly as before.
3. **Given** a Client account that has no client, **When** any route lists or reads projects for it,
   **Then** it gets nothing, whichever internal path serves the request.

### Edge Cases

- A change that sends the start date unchanged together with a new end date is judged on the new pair.
- Clearing the end date is always allowed; an open-ended contract has no end to compare.
- The start date cannot be cleared: a client always has one.
- A change that sends only unknown fields is refused, not treated as an empty change.
- A field name that differs only in letter case from an accepted one is unknown.
- The removed account fields may hold values left from before feature 002; they are discarded, since
  nothing has read them since 002.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: A Manager or Admin MUST be able to change a client's contract start date in the same
  change operation as the client's other fields.
- **FR-002**: When a change sets the start date, the end date or both to a value other than the
  stored one, the contract end MUST NOT be earlier than the start, judged on the pair as it will be
  after the change (using the stored value of any date not sent); otherwise the change is refused with
  `clients.date_range_invalid` and nothing is stored.
- **FR-003**: A change that sets neither date, or sends both back as stored, MUST NOT be refused
  because of the stored pair, even if that pair already breaks the rule.
- **FR-004**: The contract start date MUST NOT be clearable; a request to clear it is refused with the
  request validation error.
- **FR-005**: A change that names a field the operation does not accept MUST be refused with the
  request validation error and MUST change nothing. Editing one's own profile keeps its existing
  refusal, `profile.field_not_allowed`.
- **FR-006**: Any listing or read of projects for a Client account MUST return nothing when that
  account has no client, through every internal path, not only the one used today.
- **FR-007**: Internal operations that no request can reach MUST be removed, with no change in what
  any request does.
- **FR-008**: The clean-up of expired sign-in credentials and one-time links MUST find expired records
  without examining every stored record, so that its work grows with the number of expired records.
- **FR-009**: The two account fields that held the sign-in failure count and lock MUST be removed.
  Their values are discarded; nothing has read them since feature 002 moved the count to one store.
- **FR-010**: Apart from FR-001 to FR-005, nothing a user or a client of the API can observe MUST
  change.

### Key Entities

- **Client**: an organisation the company works for. Its contract has a start date, which is
  required and now editable, and an optional end date, which may not precede the start.
- **Account**: a user. It loses two fields that no longer hold anything.
- **Sign-in credential and one-time link**: records that expire and are removed by the clean-up.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A Manager can correct a client's wrong start date in one change, including when the
  stored end date is earlier than the new start would allow on its own and the end is changed in the
  same request.
- **SC-002**: Of change requests that name an unknown field, 100% are refused and none changes any
  stored value.
- **SC-003**: With 100,000 unexpired credentials and 1,000 expired ones, the clean-up removes exactly
  the 1,000 and examines no more than a few times that many records.
- **SC-004**: After installation, no stored account has the retired fields, and every existing test
  of sign-in, lockout and reset passes unchanged.
- **SC-005**: The published API document changes only to add the start date to the client change and
  to declare that unknown fields are refused on change operations.

## Assumptions

- Nothing is deployed yet, so removing the retired account fields in the release after the one that
  stopped using them meets the two-step rule for destructive changes (Constitution X).
- "Change operations" are the partial edits of a profile, an employee, a client, a project and a site,
  and the renaming of a group and a qualification.
  Requests that create records keep their current handling of unknown fields; they are not in scope.
- The client date-order error and the request validation error already exist and keep their meaning.
- Removing unreachable internal operations is judged by the existing tests and the published API
  document staying the same.

## Out of scope (Part 1)

- Availability, shift templates, shifts and assignments: Part 2.
- Changing which roles may edit a client.
- Any other leftover from 002, including the occasional false alarm in the contract fuzzing script.

---

# Part 2: Availability and manual shifts

**Input**: the stored prompt `.claude/hds/prompts/02-availability/specify.md`: "Employees tell the
company when they can and cannot work, and managers/admins can build and publish shifts by hand. Use
docs/hds-spec.md section 4 (AVL-*, EMP-1, manual SCH rows) and docs/hds-api.md (Availability, Shifts
and assignments). Availability types: blocked, open, close, and absence (leave, reserve duty, sick).
Open means the employee would like to work the shift and close means they would rather not; both are
preferences, not blocks. Employees edit their own availability inside a submission window set by
managers; admins can edit anyone's. Shift templates define the shift types per project. Admins create,
edit and delete shifts and assignments; the system shows warnings (for example too little rest between
shifts) but does not block the admin. Published shifts are visible to employees, who see only their own
assignments, never co-workers'. Drafts are visible to managers and admins only. No technology details.
Out of scope: automatic schedule generation."

## Clarifications (Part 2)

### Session 2026-11-03

- Q: May a Manager build and publish shifts by hand? → A: Managers may publish drafts but not create,
  change or delete shifts or assignments; building the roster by hand stays an Admin permission.
- Q: May an employee report an absence after the window has closed? → A: Yes, absences (leave, reserve
  duty, sick) can be reported at any time; blocked, open and close follow the window.

**Traces to**: AVL-1, AVL-2, AVL-3 (submission window), AVL-4, ORG-4 and ORG-5 (shift types per project),
SCH-3 (manual roster), SCH-4 (warnings), EMP-1 (personal roster), FIN-1 (shift pay field), and the role
table in docs/hds-spec.md section 2. SCH-4 and ORG-5 are *Proposed* in the original list.

## User Scenarios & Testing (Part 2)

### User Story 4 - An employee submits availability for the coming period (Priority: P1)

An employee opens the period whose submission window is open and marks the dates and shift types they
cannot work (blocked), the shifts they would like (open), the shifts they would rather not have (close),
and any absence (leave, reserve duty or sick) with its dates. After the deadline the period is read-only
for them.

**Why this priority**: the roster follows what employees submit (original rule 2); without it, nothing
else in this part has input.

**Independent Test**: with a window open for next week, an employee adds a blocked date, an open shift,
a close shift and a leave absence, sees them listed, changes one and removes one; after the deadline the
same changes are refused and the entries are still visible.

**Acceptance Scenarios**:

1. **Given** a window open for next week, **When** an employee blocks Tuesday's night shift, **Then** it
   is stored and listed as theirs.
2. **Given** the same window, **When** the employee marks Thursday morning open and Friday night close,
   **Then** both are stored as preferences.
3. **Given** the window has closed, **When** the employee tries to add, change or remove an entry for
   that period, **Then** it is refused with a reason that says the window is closed, and nothing changes.
4. **Given** any employee, **When** they ask for another employee's availability, **Then** they are
   refused.

---

### User Story 5 - Managers open a submission window and see who is available (Priority: P1)

A Manager or Admin opens a submission window for a period of dates, with a deadline. Managers and Admins
see every employee's submitted availability for the period. An Admin can correct any employee's
availability at any time, including after the deadline.

**Why this priority**: the window is what lets employees submit at all (AVL-3), and the overview is what
a manager builds the roster from.

**Independent Test**: a Manager opens a window for next week with a deadline; employees submit; the
Manager lists everyone's entries for that week; an Admin adds a blocked date for an employee after the
deadline; a Manager's attempt to edit an employee's entry is refused.

**Acceptance Scenarios**:

1. **Given** no window for next week, **When** a Manager opens one with a deadline, **Then** employees
   can submit for that week until the deadline.
2. **Given** submitted entries, **When** a Manager lists availability for the week, **Then** they see
   every employee's entries, grouped by employee.
3. **Given** the deadline has passed, **When** an Admin changes an employee's entry, **Then** it is
   stored.
4. **Given** any time, **When** a Manager changes another employee's entry, **Then** it is refused,
   because editing others' availability is an Admin permission (docs/hds-spec.md section 2).

---

### User Story 6 - An Admin builds a roster by hand, with warnings (Priority: P1)

For a project, an Admin creates shifts from the project's shift types on chosen dates, adjusts times,
headcount and the pay field, and assigns employees. Each change answers with warnings, for example too
little rest between two of an employee's shifts, two shifts at the same time, too many hours in a week,
or an assignment against a blocked date, an absence or a close preference. The warnings never stop the
Admin.

**Why this priority**: manual rostering (SCH-3) is the MVP path until automatic generation exists.

**Independent Test**: an Admin creates a week of shifts for one project, assigns an employee to two
shifts eight hours apart and to a shift on a date the employee blocked; each assignment is stored, and
each answer lists the matching warning.

**Acceptance Scenarios**:

1. **Given** a project with shift types morning, afternoon and night, **When** an Admin creates a night
   shift for Monday, **Then** a draft shift exists with the night type's times in the site's time zone.
2. **Given** an employee assigned to Monday night (22:00 to 06:00), **When** the Admin assigns them to
   Tuesday morning (07:00 to 15:00), **Then** the assignment is stored and the answer warns that the rest
   between them is 1 hour, below the organisation's minimum.
3. **Given** an employee who blocked Wednesday, **When** the Admin assigns them on Wednesday, **Then** it
   is stored with a warning naming the blocked entry.
4. **Given** a shift with assignments, **When** the Admin deletes the shift, **Then** the shift and its
   assignments are gone.
5. **Given** a Manager, **When** they try to create, edit or delete a shift or assignment, **Then** it is
   refused; a Manager may publish drafts (User Story 7) but not change them.

---

### User Story 7 - Publishing, and who sees what (Priority: P1)

Shifts start as drafts that only Managers and Admins see. A Manager or Admin publishes them. Once
published, an employee sees their own
assigned shifts (date, times, project and site) and never who else is on them. Clients see none of it.

**Why this priority**: EMP-1, and the original's rule that employees never see other employees' rosters.

**Independent Test**: with a draft week, an employee sees no shifts; after publishing, the employee sees
exactly their own assignments, with no co-worker names or counts; a Client sees nothing.

**Acceptance Scenarios**:

1. **Given** draft shifts, **When** an employee lists their roster, **Then** none appear.
2. **Given** the week is published, **When** an employee lists their roster, **Then** they see only the
   shifts they are assigned to, and no other employee on any of them.
3. **Given** a published shift, **When** an Admin changes its time, **Then** the employee assigned sees
   the new time.
4. **Given** a Client account, **When** it asks for shifts or availability, **Then** it is refused.

---

### User Story 8 - Shift types per project (Priority: P2)

A Manager or Admin manages the organisation's shift types (name, start and end time, whether it crosses
midnight), starting from morning, afternoon and night, and chooses which of them each project uses.

**Why this priority**: shifts are created from a project's shift types; the three defaults let a
project start without setup, so this can follow the P1 stories.

**Independent Test**: a Manager adds a "long day" type (07:00 to 19:00), adds it to one project, and an
Admin creates a shift of that type there; another project that does not use it cannot get one.

**Acceptance Scenarios**:

1. **Given** a new organisation, **When** a Manager lists shift types, **Then** morning, afternoon and
   night exist.
2. **Given** a type that crosses midnight, **When** a shift of it is created, **Then** it ends on the
   next day.
3. **Given** a type already used by shifts, **When** its times change, **Then** existing shifts keep
   their times.

---

### Edge Cases (Part 2)

- An absence reported after the deadline, or for dates with no window (for example, sick today):
  allowed. Absences do not wait for a window; blocked, open and close entries do.
- Two entries of the same employee for the same date and shift type that contradict each other (blocked
  and open, or open and close): refused; one entry per employee, date and shift type.
- A shift that crosses midnight or a daylight-saving change: its length is the real elapsed time (for
  example 22:00 to 06:00 is 8 hours, 7 or 9 on the nights the clocks change).
- An employee deactivated with future assignments: the assignments stay; listing the roster warns about
  them. An inactive employee cannot be newly assigned.
- Assigning more employees than a shift's headcount: stored with a warning; fewer is a warning on the
  shift too.
- The same employee assigned twice to one shift: refused.
- Warnings look across every project of the organisation, drafts included, because one employee can
  work several projects (rule H1 holds between them).
- Windows whose periods overlap: refused; one window per date.
- Deleting a project's use of a shift type that existing shifts have: allowed; the shifts stay.

## Requirements (Part 2)

### Functional Requirements

**Availability**

- **FR-101**: An employee MUST be able to record availability entries of four kinds: *blocked* (cannot
  work), *open* (would like to work), *close* (would rather not work) and *absence* with a kind of
  leave, reserve duty or sick.
- **FR-102**: An entry MUST apply to a date or a range of dates, and blocked, open and close entries MAY
  be limited to one shift type or one weekday within that range.
- **FR-103**: *Open* and *close* MUST be preferences only: they never prevent an assignment.
- **FR-104**: An employee MUST NOT hold two entries for the same date and shift type; a second one is
  refused with a reason.
- **FR-105**: An employee MUST be able to add, change and remove their own blocked, open and close
  entries only for dates in a period whose submission window is open, and only until its deadline.
  Outside that, the change is refused with a reason that names the closed or missing window, and nothing
  changes. Their entries stay visible to them.
- **FR-105a**: An employee MUST be able to add, change and remove their own absences at any time, with
  or without an open window.
- **FR-106**: A Manager or Admin MUST be able to open a submission window for a period of dates, with a
  deadline, and change the deadline. Periods of two windows MUST NOT overlap.
- **FR-107**: An Admin MUST be able to add, change and remove any employee's entries at any time,
  including outside a window.
- **FR-108**: Managers and Admins MUST be able to see every employee's entries; a Manager MUST NOT change
  another employee's entries.
- **FR-109**: An employee MUST NOT see another employee's entries, and a Client MUST NOT see any.

**Shift types**

- **FR-110**: The organisation MUST have shift types, each with a name, a start and an end time, and
  whether it crosses midnight. A new organisation starts with morning, afternoon and night. A Manager or
  Admin MUST be able to add, edit and retire shift types.
- **FR-111**: Each project MUST list the shift types it uses; a shift MUST be of one of its project's
  shift types.
- **FR-112**: Changing a shift type's times MUST NOT change shifts already created from it.

**Shifts and assignments**

- **FR-113**: An Admin MUST be able to create a shift for a project from one of its shift types on a
  date, with a headcount (at least 1) and an optional hourly pay rate (positive when set). Its times come
  from the shift type in the site's time zone and MAY be adjusted. A new shift is a draft.
- **FR-114**: An Admin MUST be able to change a shift's times, headcount and pay rate, and delete a
  shift, which removes its assignments.
- **FR-115**: An Admin MUST be able to assign an active employee to a shift and remove an assignment. An
  employee is assigned to a shift at most once; an inactive employee cannot be assigned.
- **FR-116**: Every creation or change of a shift or an assignment MUST answer with the warnings it
  causes and MUST NOT be refused because of them. Warnings cover at least: rest between two of an
  employee's shifts below the organisation's minimum; two of an employee's shifts overlapping; an
  employee's hours in a calendar week above the organisation's maximum; an assignment on a blocked entry
  or an absence; an assignment on a close entry; more or fewer employees than the headcount. Each
  warning names its rule, the employee and the shifts involved.
- **FR-117**: Warnings MUST consider all of the employee's shifts in the organisation, across projects
  and including drafts.
- **FR-118**: Shift lengths and rest MUST be measured in real elapsed time, correct across midnight and
  daylight-saving changes, and shown in the site's time zone.
- **FR-119**: The shift pay rate MUST be editable by an Admin only (FIN-1).
- **FR-120**: Managers MUST be able to see all shifts and assignments, drafts and published, and MUST
  NOT create, change or delete them.

**Publishing and visibility**

- **FR-121**: A Manager or Admin MUST be able to publish the draft shifts of a project for a range of
  dates. A published shift stays published when an Admin changes it, and the change is visible at once.
- **FR-122**: Draft shifts MUST be visible to Managers and Admins only.
- **FR-123**: An employee MUST see only published shifts they are assigned to, with date, times, project
  and site, and MUST NOT see who else is assigned, how many, or any draft.
- **FR-124**: A Client MUST NOT see shifts, assignments or availability.
- **FR-125**: Every rule above MUST hold however the request is made, not only in the screens, and only
  within the caller's organisation.

### Key Entities (Part 2)

- **Availability entry**: an employee's blocked, open, close or absence entry; dates, optional shift type
  or weekday, absence kind.
- **Submission window**: a period of dates and a deadline, opened by a Manager or Admin.
- **Shift type**: name, start and end time, crosses midnight; organisation-wide, chosen per project.
- **Shift**: a project, a shift type, start and end, headcount, optional pay rate, draft or published.
- **Assignment**: an employee on a shift.
- **Warning**: a rule a change breaks, with the employee and shifts involved; shown, never stored as a
  refusal.

## Success Criteria (Part 2)

### Measurable Outcomes

- **SC-101**: An employee can submit a week's availability (five entries) in under 3 minutes.
- **SC-102**: An Admin can build and publish one project's week of 21 shifts with assignments in under
  15 minutes.
- **SC-103**: Of manual changes that break a rest, overlap, weekly-hours, blocked, absence, close or
  headcount rule, 100% answer with a warning naming the rule and the employee, and 0% are refused for it.
- **SC-104**: In a test of every role against every shift, assignment and availability read, an employee
  sees no other employee's assignments or entries and no draft, and a Client sees none: 0 leaks.
- **SC-105**: Of employee changes to blocked, open or close entries outside an open window, 100% are
  refused and none is stored; absences and Admin changes at any time succeed.
- **SC-106**: A 22:00 to 06:00 shift counts 8 hours, 7 or 9 on the nights the clocks change, and rest is
  measured the same way.

## Assumptions (Part 2)

- Shift types belong to the organisation (docs/hds-spec.md section 6) and each project chooses which it
  uses; that is how "shift templates define the shift types per project" is read.
- The minimum rest and weekly maximum come from the organisation's settings, 11 and 48 hours by default
  (docs/hds-spec.md section 5, H2 and H3).
- One submission window covers a period of consecutive dates; the weekly cycle (AVL-3) is a window per
  week, opened by a Manager or Admin, not automatically.
- A weekday-limited entry within a date range covers the recurring case for that range; a standing
  pattern with no end date (AVL-5) is not in this part.
- Staffing needs per project (ORG-4) are not used to create shifts automatically here; the Admin
  creates them.

## Out of scope (Part 2)

- Automatic schedule generation and unfilled-slot reasons (SCH-1, SCH-2), locked assignments (SCH-5) and
  shift swaps (SCH-6).
- Notifications when a roster is published or changed (MOB-2), and offline access (MOB-3).
- Employees reporting worked shifts (EMP-2), pay forecasts and reports.
- Qualifications required on a shift.
- A standing weekly availability pattern without an end date (AVL-5).
