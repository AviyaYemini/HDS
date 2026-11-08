# UX working plan

**Status:** Proposed. Written on 2026-11-07 from the owner's decision to "apply all" of the UX
research, with Option B as the design language. Everything here beyond the original product spec
is Proposed. Under the working agreement it goes through the Spec Kit stages before any code is
written.

**Inputs:**
- the research report, [ux-improvements-2026-11.md](../research/ux-improvements-2026-11.md): 155
  suggestions, 14 decisions, and legal findings L1 to L18. The IDs below (N1, S2, E1 and so on)
  are its rows.
- the owner's notes on the design canvas "HDS screens", page "Option B · hdssec.com" (a private
  claude.ai design canvas the owner keeps). Its design language is written out in section 3, so
  this plan stands on its own.
- the original spec, [hds-spec.md](../hds-spec.md), and the shipped features 001 to 006.

## 1. What changes, in one paragraph

HDS keeps its data and rules and changes their shape. Each role gets its own navigation, built from
one capability map, so nobody meets "You are not allowed to do this." again. The list screens
become week-shaped: availability is a week grid with an explicit Submit, My shifts splits upcoming
from past, and the manager's main screen is a weekly roster of shift blocks that shows partial and
extended work. Every date input becomes a calendar with presets. Everything is drawn in Option B's
visual language, accessible to WCAG 2.2 AA in Hebrew and English. Screens for the features that
follow 007 exist from the start as clearly marked previews (section 6).

## 2. Decisions applied

The owner asked to apply the report's recommendations. These are its 14 decisions as applied.
Rows marked "clarify" go to 007's clarify stage with the default shown.

| ID | Decision | Applied |
| --- | --- | --- |
| O1 | Component library | Chosen in 007's plan stage after the one-day Hebrew RTL spike (X16). Shortlist: a Uniwind kit (React Native Reusables or HeroUI Native), Tamagui, or Unistyles with HDS's own components. It must be able to draw section 3's design language exactly. |
| O2 | Date-picker library | Tied to O1: react-native-ui-datepicker or react-native-paper-dates, with HDS's own presets (D1). Not react-native-calendars for ranges. |
| O3 | Absence as a request | Yes: a request with approval. **Clarify** who approves; the default is any manager of a project the employee works on, or an admin. Built in feature 016; until then 007 draws today's absence entries as full-day bands. |
| O4 | Explicit Submit for availability | Yes: Not submitted, Draft, Submitted (changeable until the deadline), then Locked, with a Review step (E2). In 007. |
| O5 | Client-portal scope | C2 billing breakdown, C3 requests, C6 contact card and C7 client home. C4 coverage shows **counts only, no names**, which keeps the original's "clients never see employee fields". C5, C8 and C9 follow; C10 and C11 later. The shell and C6 come in 007; the rest in feature 009. |
| O6 | Tenders | Out of scope until the owner confirms; **clarify**. No stub. |
| O7 | Half shifts and extra hours | Each assignment gets its own start and end, defaulting to the shift's (S3). In 007, because the owner's weekly view (S2) needs it. Pay and the rule checks use the assignment's times. |
| O8 | Navigation for managers | One merged navigation (N4). |
| O9 | Hand edits of the roster | Managers too, for the projects they manage, not only admins. This changes the original's split and is marked Proposed. Applies to the roster (007) and to editable proposals (008). |
| O10 | Overriding rules | Warnings can be overridden with a logged reason; hard stops can't. Legal limits are never overridable once the legal review confirms them. |
| O11 | Pay rules by employee group | One rule set until the legal review (L1 to L12) is back; then feature 017. |
| O12 | Qualification expiry | Its own feature, 015: expiry checked on the shift's date, blocking armed shifts. |
| O13 | Wording and formats | "Roster" / "סידור עבודה" for the weekly view and "Work log" / "יומן עבודה" for the long-range report (S1). Numeric dates dd/MM/yyyy, with Hebrew month and weekday names in headers. The clock icon is not mirrored. Gender-neutral Hebrew only, with no "address me as" setting for now (D13). |
| O14 | Channels and AI | Push, e-mail and an in-app inbox. No SMS or WhatsApp. No language-model features (AS15 waits). |
| O15 | Project teams | Who works on which project is its own feature after 007, built together with the training/security separation; 007 builds no employee–project link (owner, 2026-11-08). |
| O16 | Gig board (IDEA-1) | Feature 016. A gig is either an open seat on a project's shift (a hole in the roster), which may carry a higher pay through the shift's own pay (FIN-1), or a stand-alone job outside any project. An employee reaches the manager in charge through the contact details shown on the gig, and the manager assigns by hand on the roster; there's no in-app request for a gig. A manager sets who sees a gig: all employees, or named employees; by trust level or qualifications later, with 015. Channels stay as O14: no SMS or WhatsApp sending (owner, 2026-11-08). |

## 3. Design language: Option B

Taken from the canvas's Option B artboards (Sign in, My availability, Adding an entry, My shifts in
English and Hebrew, Schedule, Reviewing a proposal, and Staffing needs and limits). These become
007's design tokens (X11). The library's theme is generated from them, and the contrast test (X13)
checks every text and icon pair.

**Colour, light theme**

| Token | Value | Used for |
| --- | --- | --- |
| `bg.canvas` | `#DDE1DD` | page background |
| `bg.selected` | `#CCD3CA` | active tab, selected row |
| `border.subtle` | `#BFC7C0` | row and header rules |
| `border.strong` | `#899585` | table header rule, neutral chips |
| `text.primary` | `#151E1A` | body and headings; dark button background |
| `text.secondary` | `#4F5952` | labels, meta text, eyebrows |
| `accent` | `#834B30` | links, active indicators, numbering |
| `accent.pressed` | `#5E3420` | link hover and pressed |
| `accent.onSelected` | `#713F26` | active tab text |
| `cta.bg` / `cta.text` | `#D7A57E` / `#141813` | the one primary action on a screen; "ready" chips |
| `inverse.bg` | `#101413` | feature panels (next shift, submission window, new run) |
| `inverse.text` / `inverse.textSecondary` | `#F5F3ED` / `#B9BFB8` | text on inverse panels |
| `inverse.border` / `inverse.field` | `#3A403B`, `#566059` / `#1A201D` | rules and inputs on inverse panels |
| `status.warning` | `#7A5A00` | running, would rather not |
| `status.success` | `#2F6B4F` | accepted, would like to work |
| `status.danger` | `#A3261B` | failed, can't work |
| `status.info` | `#3C5A8A` | absence |

**Themes:** light as above. Dark for night shifts is built from the inverse set (X12), with
`cta.bg` as its accent. High-contrast light and dark aim at 7:1 for text. Status is always shown by
icon, label and colour together (X6), and each status colour differs from its neighbours in
lightness.

**Type:** IBM Plex Sans for Latin and IBM Plex Sans Hebrew for Hebrew, weights 400, 500 and 600.
This replaces the report's D15 candidates, because Option B is the owner's choice. Hebrew line
height is generous and Hebrew text never gets letter-spacing.

| Style | Web | Phone |
| --- | --- | --- |
| Display (page title) | 48 px, 500, tracking -0.045em, line height 1.05 | 30 px, 500, -0.03em (Hebrew -0.01em) |
| Section title | 22–24 px, 500, -0.02em | 22 px |
| Body | 15–16 px, 400 | 16 px |
| Meta | 13–14 px, `text.secondary` | 13–14 px |
| Eyebrow | 12 px, 500, uppercase, 0.12–0.14em (Latin); 13 px, 0.04em (Hebrew) | same |
| Numbers | tabular figures for times, counts and money; times isolated LTR inside Hebrew (D8) | same |

**Shape and layout**
- Square corners everywhere. Feature panels have one clipped corner (18–20 px, on the leading top
  corner, mirrored in Hebrew). The primary action has a 14 px clipped corner.
- Status chips are square: a 1 px border, a 6 px square dot and a 12 px uppercase label. The
  "ready" state is a filled `cta.bg` chip.
- Eyebrow breadcrumbs such as "HDS / SCHEDULING" above every page title.
- Web: a 64 px header whose active item has a 2 px `accent` top rule on `bg.selected`. Content is
  at most 78 rem wide with 32 px gutters. Tables have uppercase 12 px headers over a
  `border.strong` rule and rows separated by `border.subtle`.
- Phone: a 60 px bottom tab bar of at most five items with the same active treatment, 20 px side
  padding, and targets of at least 44 pt / 48 dp (X2).
- Density: comfortable on phones, compact for desktop rosters and tables (X15).

## 4. Roadmap

Each feature is a Spec Kit feature with its stored prompt in
[`.claude/hds/prompts/`](../../.claude/hds/prompts/README.md). "Prompt" names its folder.

| Feature | Prompt | What | Rows | Depends on |
| --- | --- | --- | --- | --- |
| **007 UX foundation and redesign** | `06-ux-redesign` | The design system, role shells, the employee app, the roster, admin screens, reports, the client shell, and previews of everything below | N1–N12, S1–S19, S21, E1–E4, E6–E12, E15–E16, A1–A14, A19, D1–D13, X1–X17, C1, C6, AS9 | the harness PR, this plan |
| 008 Auto-scheduler review | `07-auto-scheduler-review` | Proposal as a diff, partial and in-place acceptance, a reason and a fix for every empty slot, progress, fairness | AS1–AS8, AS10–AS13 | 007 D |
| 009 Client portal | `08-client-portal` | Billing breakdown, requests, coverage counts, client home, the monthly e-mail, several users per client | C2–C5, C7–C9, NT6 | 007 B |
| 010 Notifications and native builds | `09-notifications` | Inbox, push, preferences, reminders before deadlines and shifts, store builds | NT1–NT5, NT7, MOB rows | 007 B |
| 011 Hours worked | `10-hours-worked` | Reporting and confirming worked hours, "I can't make it"; clock-in later | E13, E18 (later), EMP-2 | 007 C |
| 012 Billing, cash flow and overview | `11-billing-overview` | The manager overview, cash flow and the hours-and-cost strip | A12, S17, OVR-1, FIN-4, FIN-6 | 007 G |
| 013 Calendar and exports | `12-calendar-exports` | Calendar feed, Excel, CSV and PDF everywhere, scheduled reports | E10 (feed), A14, A15, RPT-4, RPT-5 | 007 G |
| 014 Audit log | `13-audit-log` | History tabs, the admin audit log, run records, client logins | A16, S16, AS13, C9, ADM-1, L10 | 007 F |
| 015 Qualifications and security workflows | `14-qualifications` | Expiry on the shift's date, required qualifications, refreshers, filters, post orders | SI1–SI10 | 007 D |
| 016 Requests | `15-requests` | Absence requests with approval, swaps, giveaways, open-slot offers | E5, E14, S20, NT8 | 007 C, 010 |
| 017 Pay compliance | `16-pay-compliance` | Whatever the legal review confirms: rest windows, overtime by the week, rules by employee group, late cancellation, minimum wage | L1–L12, SI11–SI15, O11 | the legal review |

The "Could" rows not listed (S18, E17, A17, A18, D14, D16, N13, X9, AS14 to AS16, C10, C11, NT9)
stay in the report's backlog. 007 may take S18, E17 and X9 if they fit its groups.

## 5. Feature 007 in groups

Each group ends the way the harness's group close says: the scoped `make ci-local` green,
`/code-review high` with no open ledger row, at most three risk mutants named up front, and one
commit. **Each group starts with its Playwright specs, failing first** (section 7). Old e2e specs
that a group's screens replace are updated in the same group. 007 ships as five pull requests:
A, then B, then C, then D and E, then F to H.

### A. Design foundation
- Tokens from section 3 in DTCG 2025.10 JSON (X11), with the theme generated for the library O1
  picks. Four themes (X12), the density token (X15), and the contrast unit test (X13).
- The one-day RTL spike first (X16, O1, O2). Its result and the library choice go in research.md.
- Primitives, each with every state (X14): button and CTA, text field, segmented control, status
  chip, sheet, side panel, table, toast and inline banner, empty state (A10), skeleton (A9), the
  searchable picker (A6), and the date and range picker with presets (D1 to D4).
- Locale plumbing: FormatJS Intl polyfills (D11), one bidi-isolate helper (D8), money from Decimal
  strings (D10), dates as decided in O13 (D7), Sunday weeks and Israeli holidays (D6), and 24-hour
  times with "+1" (D5).
- Accessibility basics: X1 to X7, plus the X17 test stack.
- A development-only component gallery route, so the specs can test primitives without data.

### B. Role shells and navigation
- One capability map (N2). `GET /me` returns the signed-in user's capabilities: a contract change
  in the 001 OpenAPI, then the client regenerated. Tabs, routes, buttons and menus all read it.
- Shells:
  - the staff shell with "My work", "Manage" and "Admin" groups (N4);
  - the client shell with Projects, Billing, Requests and Contact (N3, C1);
  - bottom tabs on phones and a sidebar on web, mirrored in Hebrew (N5, N6);
  - a home screen per role (N7).
- No dead ends: the refusal page and banner (N8); disabled-with-a-reason only for temporary blocks
  (N9).
- Account screen (N12): the language switch (D12), sessions, and a sign-out that is always
  visible. Help, contact and the accessibility statement in one consistent place (X10, L15).
- Long-lived sessions with biometric unlock (E15). Invitations open set-password in the
  invitee's language (N11).
- The preview stubs of section 6, behind their switch.
- The client contact card (C6).

### C. Employee: My work
- **Availability (owner, E1):** a week grid with a cell per shift template plus an all-day cell. A
  tap cycles the states. A status banner and an explicit Submit with Review, Draft, Submitted and
  Locked (E2, O4). Rules stated on screen (E3), copy last week (E4), undo (E6), and drafts and
  offline reading (E16).
- **API:** an availability submission per window and employee, with status and time, locked at the
  deadline.
- **My shifts (owner, E8):** Upcoming grouped by week and Past grouped by pay period, a next-shift
  card (E9), add to calendar on phones and an .ics file on web (E10), and shift details (E11).
- **My pay (E12):** a pay-period picker, Hours and Pay views, the breakdown by rate, and an
  "estimate" label.

### D. The roster (manager's main screen)
- Renamed (owner, S1).
- **The weekly view (owner, S2):** columns Sunday to Saturday and rows by shift template. Each
  block names who filled it, with their real times, so a half shift draws as a partial bar and two
  extra hours draw past the block's edge. Past days show who worked; future days show who is
  scheduled.
- **API:** assignments get their own start and end, defaulting to the shift's (S3, O7), and pay
  and the rule checks use them. Managers may edit the roster for the projects they manage (O9).
- The span and grouping switches (S4); status by shape (S5); coverage and hours against the cap
  (S6); overnight shifts (S7); a tap path for every drag (S8, X3).
- Layered warnings (S9) with two severities and an override reason (S10); availability shown
  inside the roster (S11).
- A publish review (S12); undo (S13); copy last week and templates (S14); bulk edit with a preview
  (S15).
- A phone roster for managers (S19), and the submission tracker (S21).

### E. Schedule runs, as they are today
- The run list, a generate form with the date presets, and the proposal view, in Option B, with the
  honesty label (AS9).
- Accept and discard work as today. The diff, partial acceptance, editing and reasons come in 008
  and show here as preview stubs.

### F. Admin screens
- Staff, clients, sites, projects, shift templates, staffing needs, special days and pay settings,
  built on the table pattern (A1 to A5), side panels (A4), the form rules (A7) and money
  confirmations (A8).
- Loading and feedback states (A9, A11). Filters, views and open records live in the web address
  (A19).
- Empty states and the short admin setup checklist (N10).

### G. Reports
- The work log (renamed, S1) and the hours report: a row of key figures, bars that open into
  rows, pay-period presets and the date picker (A13, D4).

### H. Polish and accessibility pass
- The whole app checked in Hebrew and English, light and dark, on phone and desktop.
- A VoiceOver and TalkBack pass (X17), and a grayscale check (X6).
- The accessibility statement page's text, to be filled in by the owner (L15).

## 6. Preview stubs for later features

Every screen a later feature fills in exists in 007 as a stub. Each stub:
- sits under the role and navigation group where the real screen will go;
- is gated by the capability map like any other screen;
- shows an Option B empty state: what it will do, the feature that brings it, and nothing
  clickable that pretends to work.

Stubs show only while previews are switched on. The switch is on in development and the shared
test environment, and **off in production**, so real users never meet a placeholder (the spirit of
N1). Each later feature replaces its stubs and removes their entries.

| Stub | Role | Where | Filled by |
| --- | --- | --- | --- |
| Billing | client | client shell | 009 |
| Requests | client; managers' queue | client shell; Manage | 009 |
| Inbox and notification preferences | all | header; Account | 010 |
| Report worked hours / "I can't make it" | employee | My shifts | 011 |
| Overview | manager, admin | Manage › Home | 012 |
| Hours-and-cost strip | manager, admin (pay viewers) | Roster | 012 |
| Exports and calendar feed | all | Reports; My shifts | 013 |
| History and audit log | admin | each record; Admin | 014 |
| Qualification expiry and refreshers | admin, manager | Staff; Roster pickers | 015 |
| Absence requests and swaps | employee; manager approvals | My work; Manage | 016 |
| Proposal diff, partial acceptance, reasons | manager | Schedule runs | 008 |
| Pay rules by employee group | admin | Pay settings | 017 |

## 7. Playwright tests for each group

Every group's first task, a [CORE] task (section 9), writes its specs under
`apps/client/tests/e2e/ux/`. They fail until the group's [UI] tasks make them pass. They run on the web
build like the existing specs, in CI, and locally only when that group's files change (the testing
rule). Each spec runs in Hebrew and English. Each screen spec runs at a desktop viewport and at a
390 × 844 phone viewport, with an axe-core check (X17) on every screen it visits.

| Group | Spec | It asserts |
| --- | --- | --- |
| A | `a-design-system.spec.ts` | On the gallery route: tokens render with Option B's colours and fonts; the RTL layout and arrows are mirrored and numbers are not; the date picker types any separator, keyboard-navigates per the W3C pattern, and keeps presets in sync with the highlighted range; contrast and target sizes pass axe |
| B | `b-roles.spec.ts` | For each of the four roles, the exact set of tabs. Clients never see My availability, My shifts or My pay; no reachable screen shows "not allowed"; a deep link to a forbidden page shows the refusal page with a way home; previews are hidden when switched off; the language switch flips direction; sign-out is on every screen |
| C | `c-my-work.spec.ts` | Availability: a tap cycles a cell's state; the banner moves Not submitted → Draft → Submitted → Locked after the deadline; Review lists the week; undo restores. My shifts: Upcoming and Past are split, an overnight shift shows "+1", the next-shift card is first. My pay: the breakdown by rate adds up to the total |
| D | `d-roster.spec.ts` | The week grid draws each block with the names in it. A half shift draws partial and two extra hours draw past the edge; an overnight shift carries on to the next day; coverage reads filled over required; a tap assigns without dragging; a broken rest rule shows its badge, rule and numbers; the publish review counts the right shifts and people; a manager can edit only their own projects |
| E | `e-schedule-runs.spec.ts` | Generating with a preset range; progress while running; the proposal's honesty label and filled count; accept and discard as today; the preview stubs are present only while previews are on |
| F | `f-admin.spec.ts` | Tables sort, filter and save views; the searchable picker finds an employee by Hebrew and English names; a form shows inline errors and an error summary that takes focus; a pay-rate change confirms in specific words; leaving with unsaved changes asks first; the address keeps filters on reload |
| G | `g-reports.spec.ts` | Presets default to the current pay period; key figures match the table; a bar opens its rows; the work log is named as decided |
| H | `h-a11y-sweep.spec.ts` | Every screen, in both languages and both themes, passes axe with no serious issues, and focus is never hidden behind sticky bars |

## 8. Legal review, in parallel

Before 017, the owner takes rows L1 to L18 to an employment lawyer, and L13 to L18 to an
accessibility adviser. Nothing in 007 to 016 depends on the answer except the warning severities
(O10). 007 builds to WCAG 2.2 AA anyway (X1), which covers L13 and L14.

## 9. Who does what

The owner's split (2026-11-07): **Fable 5.1 builds the UI, and everything else is the
implementer session's.**

- **The implementer session** (the owner's CLI session):
  - creates the 007 branch and runs the Spec Kit stages: specify, clarify, plan, tasks and
    analyze;
  - marks each task in tasks.md as **[UI]** (Fable's) or **[CORE]** (its own);
  - does the [CORE] tasks: the API contract and migrations, the backend changes (capabilities on
    `/me`, assignment times, availability submissions, managers' roster edits), the regenerated
    client, the design-token JSON and its contrast test, CI, and **every group's Playwright specs,
    written failing first, before that group's [UI] tasks**;
  - runs each group's close and commits.
- **Fable 5.1** (a separate Code session the owner starts with Fable 5.1 chosen in its model
  menu): the [UI] tasks only, which means the RTL spike, components, screens, styling, copy and the
  preview stubs, until that group's specs pass. It never changes the API, the contract, the
  migrations or the specs. If a spec seems wrong, it asks the implementer session.
- **The reviewer** ("HDS helper"): settles questions before a group starts, and reviews once per
  pull request.
- **The owner:** answers clarify questions, pushes and merges.

Within a group the order is: [CORE] specs and API, then [UI], then the close. One group at a time
on the one branch, so the two sessions never edit at once.

The brief for the Fable session, once tasks.md exists:

> You build the UI of HDS feature 007, the UX redesign, on the branch the implementer session
> created. Read `docs/plans/ux-working-plan.md` (section 3 is the design language),
> `specs/007-*/` (spec, plan, tasks), `.claude/hds/CLAUDE.hds.md` and the constitution. Do only
> the tasks marked [UI], one group at a time, after the implementer session has committed that
> group's [CORE] tasks and failing Playwright specs. You are done with a group when its specs
> pass. Never change the API, the contract, migrations or specs; ask the implementer session
> (hds-29) if a spec looks wrong. Never push.

## 10. Next steps

1. The owner pushes and merges the harness PR (`chore/harness-fast-prs`). 007's tasks stage needs
   its new group close, so the 007 branch merges main after that.
2. The implementer session creates the 007 branch from this plan's commit and runs specify,
   clarify and plan. Tasks and analyze follow once the harness PR is on main. The plan ships with
   007's first pull request.
3. Once tasks.md exists, the owner starts the Fable 5.1 session with the brief above.
4. The owner sends L1 to L18 for legal review.
5. Feature 006's Group M (PR 5) goes ahead whenever the owner has time. It doesn't touch the
   client, so it can run alongside 007.
