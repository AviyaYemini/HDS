# Feature Specification: Schedule generation

**Feature Branch**: `004-schedule-generation`

**Created**: 2026-11-05

**Status**: Draft

**Input**: User description (the stored prompt `.claude/hds/prompts/03-scheduling/specify.md`): "A manager
asks the system to generate a roster for a date range across all active projects at once, reviews the
proposal, and accepts it as draft shifts. Use docs/hds-spec.md section 5 and docs/hds-api.md (Schedule
generation, Scheduler contract), and original scenario B. Hard rules: no overlapping shifts, minimum rest
between shifts (default 11 hours, configurable), maximum weekly hours (default 48, configurable), respect
blocked dates and absences. Goals: fill every slot, honour preferences, balance hours and night shifts
across employees, avoid long runs of consecutive days, and minimise changes to an existing roster. Slots
that cannot be filled are listed with the reason. The result says whether it is provably optimal or the
best found within the time limit. No technology details."

**Traces to**: original scenario B and rule 2; SCH-1 (generate with a preview before saving); SCH-2
(unfilled slots with reasons, Proposed in the original); docs/hds-spec.md section 5 (hard rules H1 to H4,
soft goals, behaviour); the legacy scheduler flaws in section 3, which become test cases; ORG-4 (staffing needs per project by
shift type and weekday), which the slots come from. Builds on 003: availability entries, submission
windows, shift types per project, draft and published shifts, and the rule warnings of manual rostering.

## Clarifications

### Session 2026-11-05

- Q: Where do the slots the generator fills come from? → A: From staffing needs per project (ORG-4),
  added in this feature: Managers and Admins set each project's headcount per shift type and weekday, and
  generation makes the slots from them, using a draft shift that already exists for that project, shift
  type and date instead of making a second one.
- Q: When a range already has a roster, what may generation change? → A: Drafts only. Published shifts
  and their assignments are kept exactly as they are and count for overlap, rest and weekly hours; draft
  assignments may change, as few as possible.
- Q: Should Managers and Admins be assigned by the generator like Employees? → A: Only when they
  submitted at least one open entry within the range; every active Employee is always a candidate.
- Q: Is an Employee who submitted no availability for the range free to work any shift? → A: Yes, free
  for every shift, and the proposal lists who submitted nothing for the range.
- Q: Should a project get slots only on dates within its own start and end dates? → A: Yes; a project
  with no end date is open-ended.
- Q: Which shifts outside the range count as "next to" it, for the rules and the out-of-date check? → A:
  Every shift in a calendar week the range touches, plus any shift that starts or ends within the
  minimum rest of those weeks (the run's surroundings).

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Managers set each project's staffing needs (Priority: P1)

A Manager says how many people each project needs on each of its shift types, per weekday: for example,
two on the night shift every day and one on the morning shift on weekdays only. These needs are what the
generator fills.

**Why this priority**: ORG-4 (MVP in docs/hds-spec.md). Without staffing needs there are no slots to
generate, except shifts an Admin created one by one.

**Independent Test**: set a project's needs to two people on nights every day and one on mornings Sunday
to Thursday, read them back, and generate a week: the proposal has seven night slots of two and five
morning slots of one for that project.

**Acceptance Scenarios**:

1. **Given** a project that uses the morning and night shift types, **When** a Manager sets two people on
   nights for every weekday and one on mornings for Sunday to Thursday, **Then** the needs are stored and
   read back exactly, and replace the project's previous needs as a whole.
2. **Given** a shift type the project does not use, or a retired one, **When** a need names it, **Then**
   the change is refused and nothing changes.
3. **Given** a headcount of zero or less, or a weekday outside Sunday to Saturday, **When** it is sent,
   **Then** it is refused and nothing changes.
4. **Given** an Employee or a Client, **When** they read or change a project's needs, **Then** they are
   refused.

---

### User Story 2 - A Manager generates a roster proposal across all active projects (Priority: P1)

A Manager picks a date range, for example next week, and asks for a roster. The system works across every
active project at once, so the same employee is never booked twice and their hours are balanced across
projects. When it finishes, the Manager sees a proposal: who works which shift, which slots stay empty
and why, and whether the result is the best possible or the best found in the time allowed. Nothing is
saved to the roster yet, and employees see nothing.

**Why this priority**: this is original scenario B and SCH-1, the reason the feature exists. Everything
else reviews or refines its output.

**Independent Test**: with two active projects, five employees with submitted availability and a week of
slots, a Manager generates a proposal; it breaks no hard rule, fills every slot that can be filled, and
the stored roster is unchanged until it is accepted.

**Acceptance Scenarios**:

1. **Given** two active projects and enough available employees for every slot next week, **When** a
   Manager generates a roster for next week, **Then** the proposal fills every slot, no employee is on
   two shifts at once, every employee has at least the minimum rest between shifts, no one exceeds the
   weekly maximum, and no one is placed on a date or shift they blocked or on a day of absence.
2. **Given** an employee who finishes a night shift at 06:00, **When** the proposal is made, **Then**
   that employee is not given a morning shift starting at 06:00 or 07:00 the same day (legacy flaw: rest
   was checked by date only).
3. **Given** a generation still running, **When** the Manager leaves and comes back, **Then** they see
   whether it is waiting, running, finished or failed, and the proposal once it is finished.
4. **Given** the time limit is reached before the best roster is proven, **When** the run ends, **Then**
   the proposal is the best found so far and says it is not proven optimal; when it is proven, it says
   so.
5. **Given** a finished proposal, **When** an employee or a Client asks for it, or for their own
   roster, **Then** they are refused the proposal and their roster shows no change.

---

### User Story 3 - The Manager accepts the proposal as draft shifts (Priority: P1)

The Manager reviews the proposal and accepts it. Its shifts and assignments become drafts in the roster,
where an Admin can still change them by hand with the usual warnings and a Manager publishes them, as in
003. A proposal can also be discarded.

**Why this priority**: SCH-1 asks for a preview before saving. Without accepting, a proposal has no
effect; without the preview, a bad run would overwrite the roster.

**Independent Test**: accept a proposal and confirm its shifts and assignments now exist as drafts, none
is published, employees still see nothing new, and accepting it a second time is refused.

**Acceptance Scenarios**:

1. **Given** a finished proposal, **When** the Manager accepts it, **Then** every proposed shift and
   assignment is stored as a draft in one step, all or nothing, and nothing is published.
2. **Given** a proposal, **When** an employee's availability, a shift or assignment in the run's
   surroundings (FR-016), an employee's active status or the rules' settings change after it was generated, **Then**
   accepting it is refused as out of date and the Manager is told to generate again; nothing changes.
3. **Given** an accepted proposal, **When** anyone accepts it again, **Then** it is refused and nothing
   changes.
4. **Given** a proposal the Manager does not want, **When** they discard it, **Then** it can no longer be
   accepted and the roster is unchanged.

---

### User Story 4 - Slots that cannot be filled say why (Priority: P2)

When a slot stays empty, the proposal lists it with the reason: for example, every employee
blocked that date, the only free employees would break the rest rule or go over the weekly maximum, or
there are not enough active employees at that time.

**Why this priority**: SCH-2. A Manager who knows why a slot is empty can fix the cause (ask someone to
change availability, add staff, adjust the rules) instead of guessing.

**Independent Test**: build a week where one night slot cannot be filled because both possible employees
blocked it, and another because the only free employee is at the weekly maximum; the proposal lists both
slots, each with its reason.

**Acceptance Scenarios**:

1. **Given** a slot every employee blocked or is absent for, **When** the proposal is made, **Then** the
   slot is listed as unfilled with the reason "unavailable", naming how many employees that excluded.
2. **Given** a slot only one employee could take, and taking it would break their minimum rest or weekly
   maximum, **When** the proposal is made, **Then** the slot is listed with that rule as the reason.
3. **Given** a slot that needs three people and only two can take it, **When** the proposal is made,
   **Then** the slot is listed with one place unfilled and its reason.

---

### User Story 5 - Preferences and fairness (Priority: P2)

Among the rosters that respect the hard rules and fill the most slots, the system prefers one that gives
employees the shifts they marked open, avoids the ones they marked close, spreads hours and night shifts
evenly, and avoids long runs of consecutive working days.

**Why this priority**: original rule 2 says the roster follows what employees submit; the legacy
scheduler balanced shift counts, not hours, ignored nights, and treated "would rather not" as a block.

**Independent Test**: with two employees who can each work every slot, generate a week; their hours
differ by at most one shift's length and their night shifts by at most one, and each gets the shifts
they marked open when that costs nothing else.

**Acceptance Scenarios**:

1. **Given** a slot only one employee can fill and they marked it close, **When** the proposal is made,
   **Then** they are assigned to it, because filling slots comes first (legacy flaw: a disliked shift
   acted as a block), and the proposal shows the preference was not met.
2. **Given** employees who could share the nights evenly, **When** the proposal is made, **Then** the
   difference between the most and fewest night shifts is at most one.
3. **Given** an employee who could work seven days in a row, **When** another arrangement fills the same
   slots with a shorter run, **Then** the shorter run is chosen.

---

### User Story 6 - Regenerating changes as little as possible (Priority: P3)

A Manager regenerates a range that already has a roster, for example after one employee reported sick.
The new proposal keeps every assignment it can and changes only what it must.

**Why this priority**: a roster people have planned around should not be reshuffled for one change.

**Independent Test**: accept a proposal, generate the same range again with nothing changed, and see zero
changes; then mark one employee absent for a day and see only that day's affected slots change.

**Acceptance Scenarios**:

1. **Given** an accepted roster and no change to anything it depends on, **When** the same range is
   generated again, **Then** the proposal changes nothing.
2. **Given** one employee becomes absent for one day, **When** the range is generated again, **Then** only
   the slots that employee held that day are reassigned, unless more changes are needed to keep the hard
   rules.
3. **Given** published shifts in the range, **When** a proposal is made, **Then** they and their
   assignments are unchanged, and they count for overlap, rest and weekly hours; a published shift with
   fewer people than its need is reported, not changed.
4. **Given** a draft shift an Admin created by hand for a project, shift type and date that has a need,
   **When** a proposal is made, **Then** it is used as that slot, with its own headcount, and no second
   shift is proposed beside it.

---

### User Story 7 - Admins set the rest and weekly limits (Priority: P3)

An Admin sets the organisation's minimum rest between shifts and maximum weekly hours. Generation and the
warnings of manual rostering both use them.

**Why this priority**: the prompt asks for both limits to be configurable; the 11 and 48 hour defaults are
placeholders to confirm against labour rules (docs/hds-spec.md section 5).

**Independent Test**: an Admin lowers the weekly maximum to 40 hours; a new proposal gives no one more
than 40 hours, and a manual assignment past 40 hours now warns.

**Acceptance Scenarios**:

1. **Given** an Admin, **When** they set the minimum rest to 12 hours and the weekly maximum to 40,
   **Then** both are stored, and later proposals and manual-rostering warnings use them.
2. **Given** a Manager, **When** they try to change either limit, **Then** they are refused; they can read
   both.
3. **Given** a limit of zero, a negative number or more hours than a week has, **When** it is sent,
   **Then** it is refused and nothing changes.

---

### Edge Cases

- No active projects, or no slots in the range: the run finishes with an empty proposal that says so.
- A project that starts or ends inside the range: it gets slots only on its own dates.
- No active employees: every slot is listed as unfilled with that reason.
- A range that ends before it starts, or longer than 31 days: refused.
- Shifts just before or after the range (in the run's surroundings, FR-016), in any project and whether
  draft or published, count for overlap, rest and weekly hours, so a proposal never breaks a rule against
  them.
- A week that straddles the range: hours already rostered in the part of the week outside the range count
  toward the weekly maximum.
- Night shifts across midnight and the two daylight-saving nights: lengths and rest are real elapsed time
  at the site, as in 003.
- An existing manual assignment that already breaks a rule (stored with a warning in 003): the proposal
  does not make it worse, and reports it.
- Two Managers generate overlapping ranges: both may run; the second to be accepted is refused as out of
  date if the first changed anything it depended on.
- A run that fails (for example, it cannot finish at all): it is marked failed with a reason, and nothing
  changes.
- An employee deactivated while a run is in progress: the proposal is out of date and cannot be accepted.
- The same generation request sent twice by a retry: it starts one run, not two.
- A Manager or Admin who is not a candidate (no open entry in the range) but already has a draft
  assignment there: the assignment is kept unchanged, as a published one is, and counts for their rules.

## Requirements *(mandatory)*

### Functional Requirements

**Staffing needs**

- **FR-001**: A Manager or Admin MUST be able to read and replace a project's staffing needs as a whole:
  a list of (shift type, weekday Sunday to Saturday, headcount of at least 1), at most one per shift type
  and weekday. The shift type MUST be one the project uses and not retired; otherwise the change is
  refused and nothing changes.
- **FR-002**: Removing a shift type from a project, or retiring it, MUST remove it from the project's
  staffing needs; existing shifts of that type are unchanged.
- **FR-003**: Employees and Clients MUST be refused reading and changing staffing needs.

**Generating**

- **FR-004**: A Manager or Admin MUST be able to request a roster for a date range (first and last date,
  inclusive, as dates at each project's site). By default it covers every active project of the
  organisation; the requester MAY limit it to some active projects.
- **FR-005**: A range that ends before it starts, or covers more than 31 days, MUST be refused, and
  nothing is started.
- **FR-006**: The slots to fill MUST be, for each project in the run and each date in the range that lies
  within the project's start and end dates (no end date: open-ended), one per
  staffing need of that project for that date's weekday, needing the need's headcount. Where a draft shift
  already exists for that project, shift type and date, it MUST be the slot, with its own headcount,
  rather than a new shift. A draft shift with no matching need MUST still be staffed as a slot. A
  published shift MUST NOT be a slot: it and its assignments are kept unchanged, and a published shift
  with fewer people than its need MUST be reported in the proposal.
- **FR-007**: Every active Employee of the organisation MUST be a candidate. A Manager or Admin MUST be a
  candidate only in a run whose range holds at least one open entry of theirs (they sometimes work shifts,
  docs/hds-spec.md section 2, and an open entry is how they say so). Clients and inactive accounts never
  are. An Admin can still assign anyone by hand (003).
- **FR-008**: Generation MUST run in the background. The requester, and any Manager or Admin of the
  organisation, MUST be able to see each run's state (waiting, running, finished, failed), when it was
  asked for and by whom, its range and projects, and the settings it used.
- **FR-009**: Sending the same request again with the same request key MUST NOT start a second run; it
  answers with the first.
- **FR-010**: A run MUST stop at a time limit (20 seconds by default) and return the best proposal found.
  Every proposal MUST say whether it is proven optimal or the best found within the limit.

**Hard rules (never broken in a proposal)**

- **FR-011**: No employee on two shifts that overlap in time (H1).
- **FR-012**: At least the organisation's minimum rest (default 11 hours) between the end of one of an
  employee's shifts and the start of the next (H2).
- **FR-013**: No more than the organisation's weekly maximum (default 48 hours) for an employee in a
  calendar week, Sunday to Saturday in the organisation's time zone, counting a shift in the week it
  starts (H3).
- **FR-014**: No assignment on a date or shift an employee blocked, or on a day of their absence (H4).
- **FR-015**: Only active employees (H5, without qualifications; see Out of scope).
- **FR-016**: FR-011 to FR-015 MUST hold against every shift of the employee, in every project, in the
  run's surroundings, not only against the proposal's own shifts. The surroundings are every calendar
  week (Sunday to Saturday, in the organisation's time zone) that the range touches, plus any shift that
  starts or ends within the minimum rest before or after those weeks.

**Goals (traded off, in this order of importance)**

- **FR-017**: The proposal MUST, among those that keep the hard rules, prefer in this order: (1) fill as
  many slot places as possible; (2) give employees shifts they marked open and avoid shifts they marked
  close; (3) balance total hours across employees; (4) balance night shifts across employees; (5) avoid
  runs of more than 6 consecutive working days; (6) change as few existing assignments as possible. A
  close preference MUST never stop a slot being filled when no one else can take it.
- **FR-018**: A night shift is one whose type crosses midnight.

**The proposal**

- **FR-019**: A proposal MUST list, per slot, the employees assigned; and per employee, their hours and
  night shifts in the range, the open preferences met and the close preferences not met. It MUST also
  list the candidates who have no availability entry of any kind for any date in the range; they are
  treated as free for every shift, as no entry means no block.
- **FR-020**: Every slot place left unfilled MUST be listed with at least one reason naming the rule that
  blocked it: unavailable (blocked or absent), overlap with another shift, minimum rest, weekly maximum,
  no active employee, or every employee who could take it is needed on another slot. Each reason MUST say
  how many employees it excluded.
- **FR-021**: A proposal MUST list what it changes against the existing roster in the range: assignments
  added and removed.
- **FR-022**: A proposal MUST NOT change the stored roster, and MUST NOT be visible to Employees or
  Clients; Managers and Admins of the organisation see it.

**Accepting**

- **FR-023**: A Manager or Admin MUST be able to accept a finished proposal. Its shifts and assignments
  are stored as drafts, all or nothing, and nothing is published; publishing stays as in 003.
- **FR-024**: Accepting MUST be refused as out of date, changing nothing, when anything the proposal used
  has changed since it was made: availability entries of a candidate for those dates, shifts and
  assignments in the run's surroundings (FR-016), an employee's active status, the projects or their shift types,
  or the rest and weekly settings.
- **FR-025**: A proposal MUST be accepted at most once. A Manager or Admin MAY discard it; a discarded,
  accepted or out-of-date proposal cannot be accepted. A proposal not accepted within 7 days expires.

**Settings**

- **FR-026**: An Admin MUST be able to set the minimum rest (whole hours, 1 to 24) and the weekly maximum
  (whole hours, 1 to 168). Managers MUST be able to read them. Generation and the warnings of 003 both use
  the stored values.

**Access**

- **FR-027**: Employees and Clients MUST be refused every generation, proposal, staffing-need and
  settings-change operation. Runs and proposals of another organisation MUST NOT be found.

### Key Entities

- **Generation run**: who asked and when, the range, the projects, the settings used, its state, and when
  it finished; it produces at most one proposal.
- **Proposal**: the proposed shifts and assignments, the unfilled slot places with their reasons, the
  per-employee summary, the changes against the existing roster, whether it is proven optimal, and
  whether it was accepted, discarded or expired.
- **Staffing need**: a project, a shift type it uses, a weekday and a headcount.
- **Slot**: a shift of a project and shift type on a date, with the number of employees it needs; made
  from a staffing need, or an existing draft shift.
- **Organisation settings**: minimum rest and weekly maximum, used by generation and by manual rostering.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: For 30 employees across several projects over one week, a proposal is ready within 30
  seconds of the request (docs/hds-spec.md section 1 target).
- **SC-002**: Across at least 1,000 varied generated inputs, 0 proposals break any hard rule (FR-011 to
  FR-016).
- **SC-003**: For inputs built so that a full roster exists, 100% of slot places are filled.
- **SC-004**: 100% of unfilled slot places carry at least one reason, and in scenarios built to fail for a
  known reason, the reason given matches it in 100% of cases.
- **SC-005**: Each legacy scheduler flaw (section 3) has a scenario that passes: rest across a night and a
  morning, a close preference yielding to coverage, hours rather than shift counts balanced, nights
  balanced, and all projects generated together.
- **SC-006**: When employees can share the work evenly, the spread of total hours is at most one shift's
  length and the spread of night shifts at most one.
- **SC-007**: Generating an accepted range again with nothing changed proposes 0 changes.
- **SC-008**: For a project with staffing needs, a generated week has exactly one slot per need and date,
  each with the need's headcount (or an existing draft's), and none for a weekday without a need.
- **SC-009**: After accepting, 100% of the proposal's shifts and assignments exist as drafts, 0 are
  published, and no Employee's roster view changes until a Manager publishes.

## Assumptions

- Weeks run Sunday to Saturday in the organisation's time zone, and shift times come from the project's
  shift types at the site, as in 003.
- Hours are real elapsed time, so a shift across a daylight-saving change counts 7 or 9 hours.
- The weights between goals are fixed in the order of FR-017 for this feature; changing them per
  organisation (docs/hds-spec.md section 5) is out of scope.
- The time limit is 20 seconds for every organisation in this feature.
- A Manager or Admin with no open entry in the range is left out of the run entirely, so their hours do
  not count toward the balance of hours and nights.
- Generation is a Manager action (docs/hds-spec.md section 2: "Generate a roster across all active
  projects"); accepting a proposal is part of generating, so Managers may accept, while changing the
  resulting drafts by hand stays Admin only (003).
- A proposal holds at most 31 days, which covers the weekly and monthly cycles the original describes.
- Long runs of consecutive days are a goal, not a rule; 6 days is the default length beyond which a run
  counts against the proposal.

## Out of scope

- Locking chosen assignments so regeneration keeps them (SCH-5) and shift swaps (SCH-6).
- Qualifications required on a slot (the qualification half of H5).
- Changing the weights between goals or the time limit per organisation.
- Notifications when a run finishes or a roster changes (MOB-2); the requester checks the run's state.
- The staff screens for generating and reviewing; this feature delivers the capability, and the screens
  follow with the other staff screens.
