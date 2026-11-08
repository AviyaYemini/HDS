# Feature Specification: Auto-scheduler review

**Feature Branch**: `008-auto-scheduler-review`

**Created**: 2026-11-08

**Status**: Draft

**Input**: User description: "Managers trust and steer the auto-scheduler: they see a proposal as a
change to the current roster, accept it in parts or edit it before accepting, and learn why every slot
is empty and how to fill it. Use docs/hds-spec.md section 5 and rows SCH-1, SCH-2 and SCH-5, and
docs/research/ux-improvements-2026-11.md rows AS1 to AS8 and AS10 to AS13. Roles allowed: manager,
admin. Business rules from the original: generation never breaks hard rules (rest hours, weekly caps,
blocked dates); the solve is capped in time and runs in the background with a notification; managers
generate, and (Proposed, owner decision O9) may edit proposals for the projects they manage. Proposed
(owner decisions O17 and O18): managers and admins also steer a run with their own preferences as
editable chips, without a language model, and creating and changing a schedule is one continuous flow
in which the step that usually comes next is one tap away. No technology details. Out of scope: a
language-model assistant (AS15), employee preference comparisons (AS16), qualification checks (feature
015).

Note from 007 (AS8): the "best so far" figure during a run needs the worker to publish interim
results; 007 shows only elapsed time against the time limit.

Builds on 007, without specifying again what it shipped: a run's page already shows its proposal
read-only, marked added or removed against the drafts (007 FR-080); a run already covers chosen
projects (FR-081); progress already shows elapsed time against the time limit (FR-074).

The owner's decisions of 2026-11-08 on this feature:
- Neighbouring features keep their parts: an empty slot's reasons do not include "not qualified"
  (feature 015), its fixes do not include posting an open slot (feature 016), a finished run is
  announced in the app only, not by push (feature 010), and 008 keeps its own record of runs, which
  the audit log (feature 014) shows later.
- Generation never relaxes a hard rule. The only way past one is a person's: a manager or admin
  assigns someone anyway with a logged reason, as 007's override does, and the proposal marks it.
- "Who else could cover" looks across every project, of both kinds: an employee may be both a
  trainer and a guard. A person counts as free only after checking all their shifts in every project
  (no overlap, rest hours, weekly cap, blocked dates); guards on other projects who have no shift
  elsewhere at that time are offered too."

**Scope marks**: requirements from the original product (docs/hds-spec.md section 5, SCH-1, SCH-2)
are unmarked; everything else is marked *Proposed*, with the owner decision or research row it comes
from in brackets.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Review a proposal as a change to the roster and accept it in parts (Priority: P1)

A manager opens a finished run and sees its proposal laid over the week's current roster: every
assignment is marked as added, changed, removed or unchanged. They accept Sunday to Wednesday and the
North Gate project now, leave Thursday for later, and discard what is left at the end of the day.

**Why this priority**: accepting all or nothing is the main reason managers redo a proposal by hand.
Seeing exactly what changes, and taking only the parts they trust, is the smallest step that makes the
scheduler usable week to week.

**Independent Test**: run a week with an existing roster, accept one day and one project from the
proposal, and check that only those assignments became drafts, the rest of the proposal is still
open, and the roster outside them is untouched.

**Acceptance Scenarios**:

1. **Given** a finished proposal over a week with existing drafts, **When** the manager opens it,
   **Then** the week shows each assignment as added, changed (the same slot, another person), removed or
   unchanged, each in its own style that does not rely on colour alone, with a count of each.
2. **Given** an open proposal, **When** the manager accepts one day, one project, or a chosen set of
   assignments, **Then** exactly those become drafts, nothing is published, and the proposal shows them
   as accepted and the rest as still open.
3. **Given** a partly accepted proposal, **When** the manager chooses "Discard the rest", **Then**
   nothing more is accepted and the proposal is closed as partly accepted.
4. **Given** an open proposal, **When** something it used changes for one day only (an employee blocks
   that date), **Then** that day can no longer be accepted, saying why, and the other days still can.
5. **Given** a Manager who does not manage every project in a run, **When** they open it, **Then** they
   can neither accept nor discard any part of it, as in 007.

---

### User Story 2 - Edit a proposal before accepting it (Priority: P1)

The manager sees that the proposal gives Noa the Friday night she asked to avoid. They swap her with
Dana, unassign a second guard on a quiet shift, and lock Yossi on the Sunday morning so a rerun keeps
him. Each change is checked against the rules as they make it.

**Why this priority**: people use an imperfect algorithm far more readily when they can change its
output (research row AS3). Editing in place saves accepting a proposal only to rework it on the
roster.

**Independent Test**: edit a proposal with one swap, one unassignment and one assignment that breaks
the rest rule, and check the live rule answers, the logged reason, and that accepting stores exactly
the edited result.

**Acceptance Scenarios**:

1. **Given** an open proposal, **When** a manager of every project in it swaps two people, moves a
   person to another slot, unassigns or assigns someone, **Then** the proposal changes at once, marked as
   edited by a person, and the stored roster does not change until it is accepted.
2. **Given** an edit that would break a rule (rest, weekly maximum, blocked date or absence, overlap),
   **When** it is made, **Then** the rule, the person and the numbers are shown at once ("Noa: 9 h rest,
   needs 11 h"); a rule the roster lets a person override needs a reason, and one it does not allow
   cannot be made.
3. **Given** an edit made with an override, **When** the proposal is accepted, **Then** the override and
   its reason are kept with the assignment, as 007's roster overrides are.
4. **Given** an edited proposal, **When** the manager chooses "Undo", **Then** their edits are undone one
   at a time, back to the run's own result.
5. **Given** an Admin, **When** they open any proposal, **Then** they can edit it; a Manager can edit only
   proposals whose projects they all manage [O9].

---

### User Story 3 - Learn why a slot is empty and fill it (Priority: P1)

A Saturday night slot is empty. Opening it shows: "No one free: 6 blocked the date, 2 would be under
11 h rest (Noa: 9 h), 1 is at 48 h this week." Below are the people who could cover, best first: "Dana,
free, it would be her 3rd night this week" and "Avi, from the Training Centre project, free that
night". The manager picks Dana; she is placed in the proposal at once.

**Why this priority**: SCH-2 is in the original product's minimum: an empty slot without a reason is a
dead end. A reason with a fix turns it into an action (AS4, AS5).

**Independent Test**: build a week with slots left empty for each reason, and check each reason, its
numbers, the people it names, and that each offered fix fills the slot without breaking a rule
silently.

**Acceptance Scenarios**:

1. **Given** a proposal with an empty slot place, **When** the manager opens it, **Then** it shows every
   reason that applies from a fixed list (no active employee, blocked or absent, overlapping shift,
   minimum rest, weekly maximum, everyone who could take it is needed elsewhere), how many people each
   reason excluded, and, for each person excluded by rest or the weekly maximum, the numbers.
2. **Given** an empty slot, **When** the manager asks "Who else could cover", **Then** the list includes
   every active employee of the organisation, whichever project or kind of project they usually work
   on, who is free after checking all their shifts in every project and the proposal itself; it is
   ranked so that people who break no rule come first, then by the smallest effect on fairness and
   preferences, each with a plain reason ("her 3rd night this week", "marked would rather not").
3. **Given** the "Who else could cover" list, **When** the manager picks someone free, **Then** they are
   placed in the proposal as an edit (User Story 2).
4. **Given** an empty slot no one free can take, **When** the manager chooses "Assign anyway", **Then**
   they pick a person, see the rule it breaks with the numbers, and give a reason; the proposal marks it.
   The solver itself never places anyone against a rule.
5. **Given** the list of empty slots, **When** the manager opens the proposal, **Then** every empty slot
   is listed with its main reason, and each opens on the week.

---

### User Story 4 - Steer a run: plain controls, locks and own preferences (Priority: P2)

Before running the week, the manager chooses "Fill every slot" over "Avoid overstaffing", sets
"Respect would rather not: strictly", and adds two preferences of their own: "Noa: at most 2 nights"
and "Keep Dana on North Gate". After looking at the proposal they lock four assignments they like and
run again; the locked ones stay, and the rest of the week changes as little as it can.

**Why this priority**: steering cuts the hand edits that follow every run (AS6, AS7, O17), but the first
three stories already make a single run usable.

**Independent Test**: run the same week twice with the same input, once with a lock and a preference
chip; check that the lock is kept, the chip is shown and followed, and unlocked assignments change no
more than the rules and chips require.

**Acceptance Scenarios**:

1. **Given** the run form, **When** the manager sets it up, **Then** besides the projects and dates of 007
   they may choose one or two plain controls in words (fill every slot, or avoid placing more people than
   needed; how strictly to respect "would rather not"), never raw weights, and the run uses them.
   *Proposed* [AS6]
2. **Given** an assignment on the roster or in a proposal, **When** the manager locks it, **Then** every
   later run over that range keeps it, counts it toward hours, nights and fairness, and says so on the
   proposal ("4 locked assignments kept"). [SCH-5, AS7]
3. **Given** a rerun over a range with an existing roster or an earlier proposal, **When** it finishes,
   **Then** it changes as few unlocked assignments as the rules and the controls allow, and the proposal
   lists what it changed against that starting point. [AS7]
4. **Given** the run form, **When** the manager adds a preference of their own from a fixed set (a person:
   at most N shifts, nights or hours in the range; keep a person on a site or project; keep a person off
   a day or shift type; prefer a person on a day or shift type), **Then** it appears as a chip in words
   they can edit or remove before running; the proposal shows each chip as met or not met, and why.
   *Proposed* [O17]
5. **Given** a chip that cannot be met without breaking a hard rule, **When** the run finishes, **Then**
   the hard rule wins, and the chip is shown as not met, naming the rule.
6. **Given** a chip that names a person or project, **When** that person is deactivated or the project
   leaves the run, **Then** the chip is shown as no longer applying, and the run does not fail.

---

### User Story 5 - Trust the result: guarantees, trade-offs and fairness (Priority: P2)

On the proposal, the manager reads: "Every proposal respects rest hours, weekly maximums, blocked
dates and absences." Below: "3 wishes not met; nights range from 2 to 4 per person; 2 slots empty." A
fairness panel shows each person's hours, nights and weekend shifts against the team's average, and
marks Avi's 5 nights as an outlier.

**Why this priority**: distrust of auto-fill centres on rules broken silently (AS12); words and a panel
let the manager check fairness instead of trusting a score (AS10, AS11).

**Independent Test**: produce a proposal with known preference misses and an uneven night spread, and
check the summary sentences and the panel's numbers and outlier marks against a hand count.

**Acceptance Scenarios**:

1. **Given** any proposal, **When** it is shown, **Then** it states the rules every proposal keeps, and,
   if a person's override is in it, how many assignments break a rule by a person's choice.
2. **Given** a proposal, **When** it is shown, **Then** a summary in words gives the slots left empty, the
   wishes not met (open not given, close given), the range of hours and of nights per person, and the
   chips not met, with no raw score. [AS10]
3. **Given** a proposal, **When** the manager opens the fairness panel, **Then** each person in the run
   shows hours, nights and weekend shifts in the range against the average of the people in the run,
   locked and overridden assignments included, with outliers marked by more than colour. [AS11]
4. **Given** a proposal edited by hand, **When** any figure changes, **Then** the summary and the panel
   reflect the edited result at once.

---

### User Story 6 - Follow a run and stop it early (Priority: P2)

A large month-long run is working. Its progress shows elapsed time against the limit and "Best so
far: 76 of 82 filled". At 40 seconds the manager chooses "Stop and use best so far" and lands on the
proposal, labelled as stopped early. Later, from another screen, an in-app notice says a background run
has finished.

**Why this priority**: the original asks for a capped, background solve with a notification; the live
figure tells the manager whether waiting is worth it (AS8).

**Independent Test**: start a run with a long limit, watch the figure rise, stop it, and check the
proposal holds the best result reached and says it was stopped early.

**Acceptance Scenarios**:

1. **Given** a run in progress, **When** the solver finds a better result, **Then** the progress shows
   the slots filled out of those needed within a few seconds, alongside 007's elapsed time.
2. **Given** a run in progress, **When** the manager chooses "Stop and use best so far", **Then** the run
   ends with the best result reached, which is a proposal like any other, labelled "stopped early"; if
   nothing was found yet, the run ends with no proposal, saying so.
3. **Given** a run finishes while the person who started it is on another screen, **When** it does,
   **Then** an in-app notice says so and opens the proposal; no push or e-mail is sent (feature 010).

---

### User Story 7 - A record of every run (Priority: P3)

An admin asked why last month's roster gave Dana so many nights opens the run record: who ran it,
when, over what, with which controls, chips and locks, how it ended, what was accepted, by whom, and
every hand edit and override with its reason.

**Why this priority**: algorithmic scheduling draws regulators' attention (AS13), and the audit log of
feature 014 will show this record; it is needed before the record can be shown there.

**Independent Test**: run, edit, partly accept and discard a proposal, then check the record lists
each step, in order, with who did it.

**Acceptance Scenarios**:

1. **Given** any run, **When** an Admin, or a Manager of every project in it, opens its record, **Then**
   it lists its inputs (range, projects, controls, chips, locks), its end (finished, stopped early, timed
   out, failed), its result's summary, each part accepted and each discard, and each hand edit and
   override with its reason, each with who and when.
2. **Given** a record, **When** anyone tries to change or remove it, **Then** they cannot: the record
   only grows.

---

### User Story 8 - One continuous scheduling flow (Priority: P3)

From the roster the manager taps "Generate this week", already set to the project and week on screen.
The run's progress leads to its review; Accept leads back to the roster, which offers Publish; after
publishing, "Copy to next week" is offered. An empty slot on the roster offers "Find someone".

**Why this priority**: each step exists by the end of the earlier stories; this joins them, so the
usual next step is always one tap away (O18).

**Independent Test**: from an empty roster week, reach a published week through generate, review,
accept and publish, counting that each step is one tap from the last.

**Acceptance Scenarios**:

1. **Given** a roster week, **When** the manager chooses "Generate this week", **Then** the run form
   opens set to that week and the projects on screen they manage.
2. **Given** a run in progress, **When** it finishes, **Then** its page leads to its review in one tap,
   and the review to the roster after accepting.
3. **Given** a roster week with drafts, **When** it is shown after accepting, **Then** Publish is one tap
   away; after a publish, "Copy to next week" is offered.
4. **Given** an empty slot on the roster, **When** the manager taps "Find someone", **Then** the same
   "Who else could cover" list of User Story 3 opens for that slot, and picking a person assigns them
   on the roster with today's warnings.

### Edge Cases

- A proposal whose range overlaps another open proposal: accepting a part of one marks the overlapping
  parts of the other as out of date, never both silently accepted.
- A swap that leaves a person with two overlapping shifts in different projects: shown as an overlap,
  with both shifts, and handled like any other rule break (User Story 2).
- A Manager's "Who else could cover" names an employee whose other shift is in a project the Manager
  does not manage: the list says the person is busy or free and the rule numbers, without showing more
  about that other shift than today's roster warnings do.
- A lock on an assignment the roster later removes: the lock goes with it; a rerun does not bring it
  back.
- A lock that breaks a rule today (made by override): kept, counted, and shown as an override in the
  next proposal.
- Locks that leave no room (every slot locked): the run finishes at once, with nothing to change.
- Two chips that contradict each other ("Noa: at most 2 nights" and "prefer Noa on nights"): both are
  shown; the proposal names which was not met.
- Stopping a run that has just finished: the finished result is kept; nothing is lost.
- A proposal edited by one manager while another accepts a part: the second action sees the latest
  proposal, or is refused as out of date, never overwriting the first.
- A proposal that expires (after 7 days, as in 004) with parts accepted: the accepted parts stay as
  drafts; the rest closes as expired.
- Every text, reason and chip in Hebrew and English, right-to-left in Hebrew, gender-neutral as 007's
  voice-and-tone guide requires.

## Requirements *(mandatory)*

### Functional Requirements

**Reviewing a proposal as a change**

- **FR-001**: A proposal MUST be shown over the current roster of its range, each assignment marked
  added, changed (a slot given to another person), removed or unchanged, by shape or text as well as
  colour, with a count of each. This extends 007's read-only view (007 FR-080). *Proposed* [AS1]
- **FR-002**: The review MUST show, per day and per project, how many slot places are filled out of
  those needed, and list every empty slot place with its main reason.

**Accepting in parts**

- **FR-003**: A Manager or Admin MUST be able to accept a whole proposal, a day, a project, or a chosen
  set of its assignments. What is accepted becomes drafts; nothing is published. *Proposed* [AS2]
- **FR-004**: A proposal MUST stay open after a partial accept, its accepted parts marked, until the
  rest is accepted, discarded or expires.
- **FR-005**: "Discard the rest" MUST close the proposal with nothing more accepted. Parts already
  accepted stay as drafts, removable on the roster as any draft is.
- **FR-006**: The out-of-date rule of 004 (FR-024) MUST apply per part: a part is refused when anything
  it used has changed, saying what, and the other parts stay acceptable. Accepting MUST stay all or
  nothing within one accept.
- **FR-007**: Only an Admin, or a Manager of every project in the run, MAY accept or discard any part
  of it, as 007 decided.

**Editing a proposal**

- **FR-008**: An Admin, or a Manager of every project in the run, MUST be able to edit an open
  proposal: assign, unassign, move a person between slots, and swap two people. Edits change only the
  proposal until accepted. *Proposed* [AS3, O9]
- **FR-009**: Every edit MUST be checked at once against the same rules and across all of the person's
  shifts in every project, drafts and the proposal included; each break names the rule, the person and
  the numbers.
- **FR-010**: A rule break the roster allows overriding MUST need a reason, and is kept with the
  assignment when accepted; a break the roster refuses MUST be refused in the proposal too. Which rules
  are which stays as today's rules define them (007). [O10]
- **FR-011**: Edits MUST be undoable one at a time back to the run's own result, and every edited
  assignment MUST be marked as a person's, not the solver's.
- **FR-012**: The solver MUST never place a person against a hard rule (rest, weekly maximum, blocked
  date or absence, overlap, inactive). Only a person's override, with a reason, may.

**Why a slot is empty, and how to fill it**

- **FR-013**: Every empty slot place MUST open its reasons from a fixed list: no active employee;
  blocked or absent; overlapping shift; minimum rest; weekly maximum; everyone who could take it is
  needed on another slot. Each reason MUST say how many people it excluded, and name each person
  excluded by rest or the weekly maximum with the numbers ("9 h rest, needs 11 h"; "48 h this week, 8 h
  more"). "Not qualified" is not a reason until feature 015. [SCH-2; AS4]
- **FR-014**: "Who else could cover" MUST list the active employees of the organisation who could take
  the slot, whatever project or kind of project they work on, after checking all their shifts in every
  project, the roster's drafts and the proposal. People who break no rule MUST come first, ranked by the
  smallest effect on fairness (hours, nights, weekends against the average) and on their preferences,
  each with a reason in words; people who would break a rule follow, each with the rule and numbers.
  *Proposed* [AS5; owner decision of 2026-11-08]
- **FR-015**: Picking a person from that list MUST place them as an edit (FR-008 to FR-011); picking a
  person who breaks a rule is an override (FR-010).
- **FR-016**: The list MUST never rank by pay, and MUST never show a Manager a person's pay rate, or
  more about a shift in a project they do not manage than today's roster warnings show.

**Steering a run**

- **FR-017**: The run form MUST offer, besides 007's projects and dates, plain controls in words: fill
  every slot, or avoid placing more people than a slot needs; and how strictly to respect "would rather
  not" (gently, or strictly while it leaves no slot empty). The organisation's defaults apply when
  untouched; raw weights MUST never be shown. *Proposed* [AS6]
- **FR-018**: A Manager or Admin MUST be able to lock and unlock an assignment on the roster or in a
  proposal, within the projects they may edit. Every later run MUST keep locked assignments, count them
  toward hours, nights, the weekly maximum and fairness, and state how many it kept. [SCH-5; AS7]
- **FR-019**: A rerun MUST start from the current roster and proposal for its range and change as few
  unlocked assignments as the rules, controls and chips allow; the proposal lists its changes against
  that starting point. [AS7]
- **FR-020**: A Manager or Admin MUST be able to add their own preferences to a run from a fixed set,
  each shown as a chip in words they can edit or remove before running: a person at most N shifts,
  nights or hours in the range; a person kept on a site or project; a person kept off a day or shift
  type; a person preferred on a day or shift type. No language model is used. *Proposed* [O17]
- **FR-021**: Chips MUST never override a hard rule. The proposal MUST show each chip as met or not met,
  naming what prevented it.
- **FR-022**: A chip naming a person who is no longer active, or a project no longer in the run, MUST
  be shown as no longer applying, and MUST NOT fail the run.

**Trust**

- **FR-023**: Every proposal MUST state the rules it keeps ("respects rest hours, weekly maximums,
  blocked dates and absences") and how many of its assignments break a rule by a person's override.
  *Proposed* [AS12]
- **FR-024**: Every proposal MUST summarise its trade-offs in words: slots left empty, wishes not met,
  the range of hours and of nights per person, and chips not met, with no raw score. *Proposed* [AS10]
- **FR-025**: A fairness panel MUST show, per person in the run, hours, nights and weekend shifts in the
  range against the average of the people in the run, locked and overridden assignments included, and
  mark outliers by more than colour. *Proposed* [AS11]
- **FR-026**: The summary and panel MUST follow every hand edit at once.

**Progress and finishing**

- **FR-027**: A run in progress MUST show the best result so far as slots filled out of those needed,
  updated within a few seconds of the solver improving it, beside 007's elapsed time. [AS8]
- **FR-028**: "Stop and use best so far" MUST replace Cancel while a result exists: the run ends with
  the best result reached, labelled "stopped early", reviewable as any proposal. Before any result, the
  choice is Cancel, ending with no proposal.
- **FR-029**: When a run finishes, the person who started it MUST get an in-app notice that opens the
  proposal, wherever they are in the app. No push or e-mail (feature 010).

**The run record**

- **FR-030**: Every run MUST keep a record: who ran it and when, the range, projects, controls, chips
  and locks, how it ended, its summary, each part accepted or discarded, and each hand edit and
  override with its reason, each with who and when. The record MUST only grow; no one may change or
  remove it. *Proposed* [AS13]
- **FR-031**: An Admin MUST be able to read every run's record; a Manager, the records of runs whose
  projects they all manage. Feature 014 shows it in the audit log later.

**One continuous flow**

- **FR-032**: The roster MUST offer "Generate this week", opening the run form set to the week and the
  projects on screen the person may run. *Proposed* [O18]
- **FR-033**: A run's progress MUST lead to its review in one step; accepting MUST lead to the roster
  range accepted, where Publish is one step away; after a publish, "Copy to next week" MUST be offered.
- **FR-034**: An empty slot on the roster MUST offer "Find someone", opening FR-014's list for that slot;
  picking a person assigns them on the roster with today's warnings and overrides.

**Access, language and accessibility**

- **FR-035**: Employees and Clients MUST be refused every operation of this feature; runs, proposals,
  locks, chips and records of another organisation MUST NOT be found.
- **FR-036**: Every text, reason, chip and summary MUST be in Hebrew and English, follow 007's glossary
  and voice-and-tone guide, and every screen MUST meet 007's accessibility bar (WCAG 2.2 AA), including
  a screen-reader path for every edit, lock and accept.

### Key Entities

- **Proposal**: a run's result over a range (004), now editable: its assignments, each marked as the
  solver's or a person's, locked or not, and accepted or open; its empty slot places with reasons; its
  summary; its state (open, partly accepted, accepted, discarded, expired, stopped early).
- **Proposal edit**: one change a person made to a proposal: what, by whom, when, and the override
  reason if a rule was broken.
- **Lock**: an assignment on the roster or in a proposal that later runs keep; who locked it and when.
- **Run controls**: the plain choices a run used (coverage over overstaffing, strictness of "would
  rather not").
- **Preference chip**: a person's own preference for a run, from a fixed set, with its values and
  whether the proposal met it.
- **Empty slot reason**: for one unfilled slot place, a rule from the fixed list, how many it excluded,
  and the people with their numbers.
- **Run record**: the append-only history of one run, from its inputs to its last accept or discard.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: In a usability test, managers say correctly whether a given assignment is added, changed,
  removed or unchanged for at least 95% of the assignments they are asked about.
- **SC-002**: A manager can accept a single day of a week's proposal in 3 steps or fewer from opening it.
- **SC-003**: 100% of empty slot places in any proposal open at least one reason, with numbers for every
  person excluded by rest or the weekly maximum.
- **SC-004**: In generated test weeks, no person in "Who else could cover" listed as free breaks a rule
  when placed, across all projects, in 100% of cases.
- **SC-005**: For any random valid input, with any locks, controls and chips, no proposal breaks a hard
  rule except by a recorded override, and every lock is kept.
- **SC-006**: A rerun after locking changes no locked assignment, and changes at most as many unlocked
  assignments as the first run's result needs to keep the rules.
- **SC-007**: The best-so-far figure updates within 5 seconds of the solver improving its result, and
  "Stop and use best so far" yields a proposal within 5 seconds.
- **SC-008**: From an empty roster week to a published week through generate, review, accept and
  publish, each step is one tap from the last.
- **SC-009**: Every run's record lists every accept, discard, edit and override, with who and when, in
  100% of test runs.
- **SC-010**: Every screen of this feature passes 007's automated accessibility check in both languages,
  all four themes and both sizes.

## Assumptions

- **What a proposal's parts are**: a day, a project, or any set of assignments; a "part" for the
  out-of-date rule is what is being accepted at that moment.
- **Overrides**: the split between warnings a person may override and hard stops stays as today's rules
  define it (003 FR-116, 007 FR-034, O10); 008 adds no new rule and relaxes none.
- **Fairness and ranking**: "cost" in AS5 means effect on fairness and preferences, never pay; Managers
  do not see pay rates (as today).
- **Visibility across projects**: who is free is checked across every project, but a Manager sees no
  more of another project's shifts than today's roster warnings show them.
- **The kinds of project**: training and security projects are scheduled together when a run covers
  both, as the owner allows; "Who else could cover" crosses kinds (owner decision of 2026-11-08). Shift
  types per kind and project teams come in a later feature.
- **Controls**: two plain controls only; the organisation's weights stay as 004 set them and are not
  shown.
- **Interim results**: the solver publishes improved results while running, often enough for the
  figure and the stop to meet SC-007.
- **Notices**: in the app only, through 007's toasts; the inbox and push come with feature 010.
- **Record**: kept as long as the organisation's roster history; feature 014 shows and searches it.
- **Expiry**: proposals still expire after 7 days (004 FR-025); accepted parts stay as drafts.
- **Out of scope**: a language-model assistant (AS15), employee preference comparisons (AS16),
  qualification checks and "not qualified" reasons (feature 015), posting an open slot (feature 016),
  push and e-mail notices (feature 010), the audit log screen (feature 014), "Explain this assignment"
  (AS14), and changes to which rules are warnings and which are hard stops (legal review).
