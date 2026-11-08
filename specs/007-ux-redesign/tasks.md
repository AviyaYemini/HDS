---

description: "Task list for feature 007, the UX foundation and redesign"
---

# Tasks: UX foundation and redesign

**Input**: Design documents from `specs/007-ux-redesign/` (spec.md, plan.md, research.md,
data-model.md, contracts/, quickstart.md)

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/

**Tests**: Required. Constitution IV and the plan's prompt: each group's Playwright spec is written
first and fails before that group's [UI] tasks; services get fake-repository unit tests; the scheduler
gets Hypothesis properties; Schemathesis covers every new endpoint.

**Organization**: One phase per working-plan group (A to H), in the order they ship. Each task names
its user story (US1–US8 in spec.md) and **who does it**:

- **[CORE]** — the implementer session: contract, migrations, backend, regenerated client, design
  tokens and their contrast test, CI, each group's Playwright specs, and the group close.
- **[UI]** — the Fable 5.1 session: the RTL spike, components, screens, styling, copy and preview
  stubs, until the group's specs pass. It never changes the API, the contract, migrations or specs;
  if a spec looks wrong it asks the implementer session.

**Order within a group**: [CORE] specs and API, committed → [UI] → close. One group at a time on
the branch, so the two sessions never edit at once.

**Each group ends with** (HDS, the owner's decision of 2026-11-07 in
`.claude/hds/process-cuts-proposal.md`). Written into each group's closing task, naming that
group's mutants:

1. the scoped `make ci-local` green: the checks for the paths the branch changed;
2. `/code-review high`, each finding a row in the feature's `review-ledger.md`, none open;
3. the mutants named in the closing task, if any: at most three, and only where a passing test
   could be hollow (a test that could not fail first, or a security, money, time or data-loss
   rule); one at a time, with the hooks on;
4. one commit, whose message names the ledger rows it fixes, for example `(R12, R13)`.

No log: an owner's choice or an accepted limit goes in the commit message, `tasks.md` or
`research.md`.

**When the pull request is ready to push, once**: `make ci-local-full`; `make ci-local-db` if
`apps/api/` or `specs/001-foundation-auth-org/contracts/openapi.yaml` changed; `/hds-review`; the
reviewer session; their fixes in one commit. The full suite runs only in CI.

## Format: `- [ ] [ID] [P?] [Story] [CORE|UI] Description`

- **[P]**: can run in parallel (different files, no dependency on an unfinished task)
- **[Story]**: the spec's user story (US1–US8)
- Paths: `apps/api/src/hds/…`, `apps/api/tests/…`, `apps/client/…`, `packages/api-client/…`,
  `specs/001-foundation-auth-org/contracts/openapi.yaml` (below: **the 001 contract**)

---

## Phase 1: Setup

**Purpose**: wiring both sessions need before group A.

- [x] T001 [CORE] Merge `origin/main` into `007-ux-redesign` if it has moved since 25779ce (a merge, not a rebase), and confirm `/hds-status` shows feature 007
- [x] T002 [CORE] Add `apps/client/tests/e2e/ux/` with a shared helper `apps/client/tests/e2e/ux/support.ts`: sign in as each seeded role through the UI, switch language (he, en), the two viewports (desktop 1280 × 800, phone 390 × 844), and `checkA11y(page)` built on `@axe-core/playwright` (fails on serious and critical issues); add `@axe-core/playwright` to `apps/client/package.json`
- [x] T003 [CORE] Add Playwright projects `ux-desktop` and `ux-phone` in `apps/client/playwright.config.ts` for `tests/e2e/ux/*.spec.ts`; keep the existing `chromium` project for the old specs
- [x] T004 [CORE] Run the `ux/` specs in CI's "Client e2e" job (same job name, so the main ruleset's required check is unchanged): `pnpm e2e` builds with `EXPO_PUBLIC_PREVIEWS=1` and the `ux-desktop` and `ux-phone` projects pick the specs up; the previews-off run is added in group B (T025). Locally, `scripts/ops/e2e_ux.sh [filter]` runs them in CI's pinned image: the API on the host against hds_p3_test (one run at a time), the build and Playwright in the pinned Playwright image on a copy of the tree. `make ci-local` stays database-free, so it does not run them
- [x] T005 [CORE] Extend `hds.scripts.seed_dev` (`apps/api/src/hds/scripts/seed_dev.py`) with what the ux specs need: a manager of one project only, a second project they don't manage, an open and a past availability window, a half shift, an extended shift and an overnight shift, a published and a draft week, and one employee with no submission — done at the start of group B, the first group whose specs need data. Done in B for what B's schema holds; the half and extended shifts' own times and `employee@hds.test`'s submission (so `extra@hds.test` is the one without) are added when migrations 027 and 026 merge, in groups D and C

---

## Phase 2: Group A — design foundation (PR 1) — US6, US5 foundations

**Goal**: Option B's tokens, the four themes, the primitives with every state, the date picker with
presets, and the locale plumbing, on a gallery route.

**Independent Test**: `ux/a-design-system.spec.ts` on `/dev/gallery` and the token contrast test.

### [CORE] first

- [x] T006 [US6] [CORE] Write `apps/client/tests/e2e/ux/a-design-system.spec.ts`, failing: on `/dev/gallery`, in he and en, desktop and phone — tokens render Option B's colours (`bg.canvas` #DDE1DD, `accent` #834B30, `cta.bg` #D7A57E) and the Plex families; RTL mirrors layout and arrows but not numbers, times or the clock icon; the date picker accepts 7/11/2026, 7.11.2026 and 7-11-2026, keyboard-navigates per the W3C pattern (arrows, Page Up/Down, Escape back to the field), keeps the preset in sync with the highlighted range and turns it to Custom on a hand change; targets ≥ 44 px on phone; the loading bar appears for a route delayed by interception (> 300 ms) and not for a fast one, with `role=progressbar`; with reduce-motion emulated no element keeps a transition (computed `transition-duration` and `animation-duration` are 0); axe clean (FR-072, FR-074)
- [x] T007 [P] [US6] [CORE] Write the DTCG 2025.10 token source from the working plan's section 3 in `apps/client/src/design/tokens/base.tokens.json`, `semantic.tokens.json` (light, dark, high-contrast light, high-contrast dark; each text and icon token lists the backgrounds it sits on in `$extensions.hds.on`) and `component.tokens.json`, with the `density` token (comfortable, compact)
- [x] T008 [P] [US6] [CORE] Write the contrast unit test `apps/client/tests/unit/tokens.contrast.test.ts`: every text pair ≥ 4.5:1, icons and large text ≥ 3:1, text in high-contrast themes ≥ 7:1; it must fail first on a deliberately bad fixture, then pass on T007 (FR-062)
- [x] T009 [P] [US9] [CORE] Add the motion tokens to `apps/client/src/design/tokens/base.tokens.json` and `component.tokens.json`: durations `fast` 120 ms, `base` 200 ms, `slow` 320 ms; easings `standard` cubic-bezier(0.2, 0, 0, 1) and `emphasized` cubic-bezier(0.3, 0, 0, 1); no spring or overshoot; a unit test in `apps/client/tests/unit/tokens.motion.test.ts` that every easing's control points stay in [0, 1] (no overshoot) (FR-070, research R14)
- [x] T010 [P] [US9] [CORE] Write `docs/ux/glossary.md` (every domain term in he and en, with the terms to avoid, starting from Roster / סידור עבודה, Work log / יומן עבודה, Availability / זמינות, My shifts, My pay, Submit / הגשה, Draft, Locked, Publish, Unfilled) and `docs/ux/voice-and-tone.md` (plain, short, gender-neutral Hebrew; nouns and infinitives on buttons; one format each for dates, times and money; empty-state and error patterns) (FR-075, research R16)
- [x] T011 [P] [US9] [CORE] Write `apps/client/tests/unit/locales.parity.test.ts`: `he.json` and `en.json` have identical keys; no empty value, no value equal to its key, no `TODO`/`TBD`/`FIXME`/`lorem`/`xxx`; the glossary's keys carry the glossary's terms. It fails first on a fixture with a missing key, then passes on today's files after any fix (FR-076, SC-012)
- [x] T012 [US6] [CORE] Add the previews switch: read `EXPO_PUBLIC_PREVIEWS` in `apps/client/src/config/previews.ts`, register `/dev/gallery` only when it is on (FR-067) (`apps/client/app/dev/gallery.tsx` as an empty shell for [UI]), and add `pnpm web:previews` to `apps/client/package.json` (R11)
- [x] T013 [CORE] Commit group A's [CORE] tasks (`test(client): add group a's failing design-system spec and tokens`), so the [UI] session can start

### [UI] then

- [x] T014 [US6] [UI] Run the one-day Hebrew RTL spike (a list, a form, tabs, a select, a switch, a bottom sheet, a date-range picker) for the R1 shortlist and the R2 pickers, on the web plus whatever runs unattended tonight (the iOS Simulator if Xcode is installed, an Android emulator if the SDK is); record exactly which platforms were and were not checked, and where native could not be checked prefer the candidate with the strongest documented native RTL; write the result into `specs/007-ux-redesign/research.md` R1 and R2 and `docs/adr/014-component-library.md`
- [x] T015 [US6] [UI] Install the chosen library and picker, `expo-font` and the IBM Plex Sans and IBM Plex Sans Hebrew fonts (400, 500, 600) in `apps/client`; load fonts at the entry point
- [x] T016 [US6] [UI] Write the theme generator `apps/client/scripts/build-theme.ts` (tokens JSON → the library's theme in `apps/client/src/design/theme/`), with `pnpm theme:check` failing on drift; the four themes follow the device (dark, contrast) with an in-app override
- [x] T017 [P] [US6] [UI] Locale plumbing in `apps/client/src/locale/`: FormatJS Intl polyfills (he, en, Asia/Jerusalem) loaded before i18next; `isolate()` (FSI/LRI … PDI); `formatDate` (dd/MM/yyyy, spelled-out month and weekday in headers); `formatMoney` from the Decimal string via `Intl.NumberFormat`, never through arithmetic; `formatTimeRange` (24-hour, "+1"); Sunday weeks; `@hebcal/core` holidays and eves; with unit tests in `apps/client/tests/unit/locale.test.ts` (FR-053–FR-058; the clock icon unmirrored, FR-057); and the copy: fill the 59 keys `he.json` lacks today and bring the keys `docs/ux/glossary.md` names to its words ("Who worked" → "Work log" / "יומן עבודה", "Sign Out" → "Sign out"), so `tests/unit/locales.parity.test.ts` (T011) passes
- [x] T018 [US6] [UI] Primitives in `apps/client/src/design/components/`, each with default, hover, pressed, focused, selected, disabled-but-legible, busy and error states (X14): Button and the CTA (14 px clipped corner), TextField, SegmentedControl, StatusChip (icon + label + colour, square, 6 px dot), Sheet, SidePanel, Table (sortable headers, frozen first column on the leading side, density), Toast and InlineBanner, EmptyState (FR-063), Skeleton, SearchablePicker (second line, recent first), FeaturePanel (clipped leading top corner, mirrored), PageTitle with eyebrow breadcrumb
- [x] T019 [US5] [UI] DateField and DateRangeField in `apps/client/src/design/components/date/`: typed entry with any separator, a calendar button, today marked, impossible dates disabled, end-before-start refused, presets (Today … Last month, Custom; pay-period presets as an option) as a side list on web and chips on phone, dates shown under the preset, W3C keyboard pattern, announcement of the chosen date, a two-month range layout that does not jump (FR-051, FR-052)
- [x] T020 [US6] [UI] Fill the gallery `apps/client/app/dev/gallery.tsx` with every primitive in every state, both themes, both languages, until `a-design-system.spec.ts` passes; accessibility basics X1–X7 in the primitives (targets, focus ring ≥ 2 px at 3:1, announcements, text size)
- [x] T021 [US9] [UI] Motion layer in `apps/client/src/design/motion/`: the generated duration and easing tokens, `useReducedMotion()` following the setting live on native and web (every duration 0 under it), and the transitions of FR-071 in the primitives (sheet and side panel from the leading edge, mirrored; list row enter and leave; status-chip change; feature-panel clipped-corner reveal; skeleton shimmer); screen transitions in the expo-router layouts; nothing blocks input (FR-071, FR-072)
- [x] T022 [US9] [UI] `LoadingBar` in `apps/client/src/design/components/LoadingBar.tsx` and its loading store: 3 px under the header, `cta.bg` track and `accent` fill with a clipped leading end; indeterminate after 300 ms and kept ≥ 400 ms once shown; a determinate mode with a value and side text; progress-bar role and value on web and native, announced at 25 % steps; a still state under reduce-motion; wired to route changes and the API client's pending requests (FR-074, research R15)
- [x] T023 [P] [US6] [UI] Unit tests with React Native Testing Library for the primitives' roles, names and states in `apps/client/tests/unit/design/*.test.tsx`

### Close

- [x] T024 [CORE] Close group A as the template says. Mutants (≤ 3): the contrast test's ratio threshold (4.5 → 4.4 must fail a pair), `formatMoney` given a Decimal string with 4 places (must not round through `Number`), and the date parser's day/month order (7/11 must not read as July 11). Commit `feat(client): add the option b design foundation (…)`. **PR 1 ready**: the once-per-PR steps; owner pushes

---

## Phase 3: Group B — role shells and navigation (PR 2) — US1, US8

**Goal**: one capability map from the API drives every role's navigation; the client shell; no
dead ends; account, sessions, contact card, previews.

**Independent Test**: `ux/b-roles.spec.ts`.

### [CORE] first

- [x] T025 [US1] [CORE] Write `apps/client/tests/e2e/ux/b-roles.spec.ts`, failing: for each of the four roles, the exact tab set (contracts/ui-routes.md) on phone and desktop; a client never sees My availability, My shifts, My pay, the roster or any employee field; no reachable screen shows "not allowed"; a deep link to a forbidden page shows `screen-no-access` with Go home; the language switch flips `dir`; sign-out and the accessibility link on every screen; the contact card shows phone, e-mail and hours; the splash, the 404 page (an unknown address) and the offline banner (network emulated offline) render in he and en (FR-077); with previews on each US8 stub sits in its place for its roles, and in the previews-off run none resolves
- [x] T026 [US1] [CORE] Change the 001 contract: `GET /me` adds `capabilities` and `managed_project_ids`; `POST /auth/login` adds `client_kind`; new `GET /me/sessions`, `DELETE /me/sessions/{id}`, `GET /organisation/contact`, `PATCH /organisation/contact` (contracts/api-changes.md, group B)
- [x] T027 [US1] [CORE] Write the failing API tests: `apps/api/tests/unit/test_capabilities.py` (each role's list equals contracts/capabilities.md; a manager's `:project` capabilities follow `project_managers`), `apps/api/tests/integration/test_me_capabilities.py`, `test_sessions.py` (native tokens last 30 days and roll; web 7; rotation and reuse detection unchanged; a web sign-in without `client_kind` gets 7 days), `test_organisation_contact.py` (admin edits; others 403; every role reads)
- [x] T028 [US1] [CORE] Write the pure map `apps/api/src/hds/core/capabilities.py` and `apps/api/src/hds/services/capabilities.py`; re-express `require_staff`, `require_manager`, `require_admin` and `require_manager_or_client` in `apps/api/src/hds/core/deps.py` through the map without changing who they admit; add `capabilities` and `managed_project_ids` to `UserRead`-for-me in `apps/api/src/hds/routers/me.py`; ADR `docs/adr/015-capability-map.md` (FR-001, FR-002)
- [x] T029 [P] [US1] [CORE] Migration `apps/api/alembic/versions/025_org_contact_sessions.py` (data-model 025): `organisations.contact_phone`, `contact_email`, `opening_hours`; `refresh_tokens.client_kind`; models, repository, service and router `apps/api/src/hds/routers/organisation.py`
- [x] T030 [P] [US1] [CORE] Phone sessions: `REFRESH_TOKEN_TTL_DAYS_NATIVE` (30) in `apps/api/src/hds/core/config.py`; `client_kind` through `services/auth.py` and `repositories/refresh_token.py`; `GET/DELETE /me/sessions`; ADR `docs/adr/016-phone-sessions.md` (R10), which records that `client_kind` is self-declared — a browser can claim `native` and get 30 days — and why that is acceptable: rotation and reuse detection still apply, the web keeps tokens in memory (ADR 010), and biometrics are a convenience gate, not a security boundary
- [x] T031 [US1] [CORE] Regenerate the client (`pnpm api-client:generate`), add the new error codes' locale keys to `apps/client/src/locales/{en,he}.json` and `apps/api/src/hds/locales/`, and a client unit test `apps/client/tests/unit/capabilities.contract.test.ts` asserting every capability name the UI uses exists in the generated schema's documented list
- [x] T032 [US1] [CORE] Client capability layer `apps/client/src/capabilities/`: `useCapabilities()` from `/me`, `<Gate need=…>`, a route guard that renders the no-access page and refreshes `/me` after any 403; `client_kind: native` sent from phones in `apps/client/src/api/session.ts`
- [x] T033 [US1] [CORE] Update or retire the old specs this group replaces: `apps/client/tests/e2e/test_us1_signin.spec.ts`, `test_rtl.spec.ts`, `test_us5_client_portal.spec.ts` (skipped today; its checks move into b-roles), `test_us2_profile.spec.ts`
- [x] T097 [CORE] Make the secret-scan step of `scripts/ops/ci_local.sh` work from a git worktree and never pass vacuously: mount the common git folder (`git rev-parse --git-common-dir`) into the container at its own path, and fail when the scanner reports "not a git repository" or scans no commit of a non-empty range; check it on a worktree and on the main tree (found in PR 1)
- [x] T034 [CORE] Commit group B's [CORE] tasks (`feat(api): serve capabilities on me and add the contact card and phone sessions`, `test(client): add group b's failing roles spec`)

### [UI] then

- [x] T035 [US1] [UI] Staff shell `apps/client/app/(staff)/_layout.tsx`: bottom tabs on phone (≤ 5, per role), side navigation on web with My work, Manage and Admin groups, mirrored in Hebrew, 64 px header with the 2 px accent rule; replaces `apps/client/src/components/EmployeeNav.tsx` (FR-003, FR-005)
- [x] T036 [US1] [UI] Client shell `apps/client/app/(client)/_layout.tsx` with Projects, Billing, Requests, Contact; client home `apps/client/app/(client)/index.tsx` (one card per project from the client-scoped projects list, no employee fields) and project page (FR-004)
- [x] T037 [US1] [UI] Home per role `apps/client/src/screens/home/`: employee (next shift, availability deadline banner), manager (today's coverage, unfilled slots, who hasn't submitted — reading what group C and D expose, placeholders until then), admin (the setup checklist entry) (FR-006). The manager home's coverage, unfilled and not-submitted cards are finished in T074, once groups C and D serve that data; until then they show skeletons, never invented numbers
- [x] T038 [US1] [UI] No-access page and inline refusal banner `apps/client/src/screens/no-access/`; disabled-with-reason and read-only patterns in the primitives (FR-007, FR-008); remove the string "You are not allowed to do this." from both locale files
- [x] T039 [US1] [UI] Account screen `apps/client/app/account.tsx`: language switch (FR-059) (restart notice on native; immediate `dir`/`lang` on web), active sessions with revoke and "sign out of other devices", profile; sign-out, help, contact and the accessibility link in the same place on every screen (FR-009)
- [x] T040 [US1] [UI] Biometric unlock on phones with `expo-local-authentication` in front of the stored refresh token; the visible "session ended" notice (FR-010)
- [x] T041 [US1] [UI] Set-password screen for invitations in the invitee's language, then the role's home (FR-011); sign-in with paste, password managers, `autoComplete`/`textContentType`, no puzzle (FR-064)
- [x] T042 [US1] [UI] Contact card `apps/client/app/(client)/contact.tsx` and the admin's contact-details form (FR-012)
- [x] T043 [US8] [UI] Preview stubs for every row of contracts/ui-routes.md's preview table in `apps/client/src/screens/previews/`, each an Option B empty state naming its feature, gated by its capability, registered only with the previews switch (FR-068, FR-069)
- [x] T044 [US9] [UI] Finishing touches: an Option B splash screen (`expo-splash-screen`), app icon and favicon in `apps/client/assets/` and `app.json`; the branded sign-in; the 404 page `apps/client/app/+not-found.tsx` with a way home; the offline banner; styled toasts; focus rings; all copy so far written against `docs/ux/glossary.md` and `docs/ux/voice-and-tone.md` (FR-075, FR-077)
- [x] T045 [P] [US1] [UI] Unit tests for the shells' tab sets per capability list in `apps/client/tests/unit/shells.test.tsx`

### Close

- [x] T046 [CORE] Close group B as the template says. Mutants (≤ 3): the client role given `my_work.read` in `core/capabilities.py` (b-roles and the unit test must fail), a manager's `roster.edit:project` granted for an unmanaged project, and the native refresh lifetime falling back to 7 days. Commit `feat(client): add role shells built from the capability map (…)`. **PR 2 ready**: once-per-PR steps, including `make ci-local-db`

---

## Phase 4: Group C — employee: My work (PR 3) — US2, US4

**Goal**: the availability week with an explicit Submit and "not submitted counts as unavailable";
My shifts split Upcoming and Past; My pay.

**Independent Test**: `ux/c-my-work.spec.ts`.

### [CORE] first

- [x] T047 [US2] [CORE] Write `apps/client/tests/e2e/ux/c-my-work.spec.ts`, failing: a tap cycles unmarked → open → close → blocked; the banner moves Not submitted → Draft → (Review) Submitted with the time → Draft after a change → Locked after the deadline; before the deadline the banner warns that not submitting means not being scheduled; a past window with no submission shows "counted as unavailable"; Review lists the week; Undo restores; Copy last week fills the grid. My shifts: Upcoming and Past split, overnight shows "+1", the next-shift card first, a detail page. My pay: the breakdown by rate adds up to the total and carries "estimate, before tax"
- [x] T048 [US2] [CORE] Change the 001 contract (contracts/api-changes.md, group C): `GET/POST /me/availability-submissions`, 409 `availability.window_closed` on constraint writes after the deadline, `GET /me/shifts` going back in time with effective times, `GET /me/shifts/{id}`
- [x] T049 [US2] [CORE] Write the failing API tests: `apps/api/tests/unit/test_availability_submission_service.py` (derived status table, snapshot hash, resubmit replaces), `apps/api/tests/integration/test_availability_submissions.py` (lock after the deadline; absences stay editable; another employee's submission is never visible), `apps/api/tests/unit/test_schedule_inputs_not_submitted.py` (a past window without a submission yields a synthetic blocked entry for every date; with one, the snapshot replaces live entries), and a Hypothesis property in `apps/api/tests/property/test_not_submitted.py` (no generated roster assigns a not-submitted employee inside the window)
- [x] T050 [US2] [CORE] Migration `apps/api/alembic/versions/026_availability_submissions.py`, model `models/availability_submission.py`, repository, service `services/availability_submission.py` (status derivation, snapshot, lock), router additions in `routers/availability.py` and `routers/me_shifts.py`; the lock check in `services/availability.py`; ADR `docs/adr/017-availability-submissions.md`
- [x] T051 [US2] [CORE] Scheduling inputs: in `services/schedule_inputs.py` read snapshots for windows past their deadline and synthesise `not_submitted` blocks; in `hds/scheduling/types.py` and `rules.py` carry the reason and emit the warning `warnings.not_submitted` from `validate` for manual assignments (no new imports in the package)
- [x] T052 [US4] [CORE] `GET /me/shifts` Past range and `GET /me/shifts/{id}` details in `services/roster.py` (published only; the caller's own; effective times once group D adds them, shift times until then)
- [x] T053 [US2] [CORE] Regenerate the client; locale keys for the new codes; update or retire `apps/client/tests/e2e/test_us_availability.spec.ts`, `test_us5_windows.spec.ts`, `test_us_my_pay.spec.ts` and the unit tests `apps/client/tests/unit/{availability,shifts,pay}.test.tsx` that the new screens replace
- [x] T054 [CORE] Commit group C's [CORE] tasks (`feat(api): add availability submissions and count a missed deadline as unavailable`, `test(client): add group c's failing my-work spec`)

### [UI] then

- [x] T055 [US2] [UI] Availability week `apps/client/src/screens/my-work/availability/`: Sunday–Saturday grid, a cell per shift template plus all-day, ≥ 44 pt cells, tap to cycle and a sheet with the choices, week arrows across the window, absences as full-day bands, holidays marked, the rules stated (FR-013, FR-016, FR-020, FR-054)
- [x] T056 [US2] [UI] Status banner, Review and Submit, the "not submitting means you won't be scheduled" notice, Locked and "counted as unavailable" states (FR-014, FR-015)
- [x] T057 [US2] [UI] Undo for each cell change (FR-018), a light haptic on each cell change and a success haptic on Submit with `expo-haptics` (FR-073), Copy last week, the device draft in async storage with sync on reconnect (`@react-native-async-storage/async-storage`) (FR-017–FR-019)
- [x] T058 [US4] [UI] My shifts `apps/client/src/screens/my-work/shifts/`: Upcoming by week and Past by pay period with totals; the next-shift card (FeaturePanel, FR-022) with "+1" and a countdown; the detail page with map links; Add to calendar via `expo-calendar` on phones and an `.ics` download on web; offline list with "updated hh:mm" (FR-021–FR-023, FR-019)
- [x] T059 [US4] [UI] My pay `apps/client/src/screens/my-work/pay/`: pay-period picker, Hours and Pay views, breakdown by rate, change since last period, the estimate label; money only through `formatMoney` (FR-024)
- [x] T060 [P] [US2] [UI] Unit tests for the grid's cycle, banner states and undo in `apps/client/tests/unit/my-work/*.test.tsx`

### Close

- [x] T061 [CORE] Close group C as the template says. Mutants (≤ 3): the lock comparison `now >= deadline` turned into `>` (lock at the exact deadline must hold), the synthetic `not_submitted` block skipped for the window's last date, and Submitted derived without comparing the snapshot hash. Commit `feat(client): add the availability week and my shifts split (…)`. **PR 3 ready**: once-per-PR steps, including `make ci-local-db`

---

## Phase 5: Group D — the roster (PR 4, with E) — US3

**Goal**: the weekly roster with each assignment's own times, managers editing their projects, layered
warnings with overrides, the publish review, undo, copy week, templates, bulk edit, the phone
agenda and the submission tracker.

**Independent Test**: `ux/d-roster.spec.ts`.

### [CORE] first

- [x] T062 [US3] [CORE] Write `apps/client/tests/e2e/ux/d-roster.spec.ts`, failing: the week grid draws each block with its names; a 06:00–10:00 assignment in a 06–14 block is partial and a 14:00–24:00 one draws past the edge; an overnight shift continues on the next day with "+1"; coverage reads filled/required; shapes for draft, published, changed and unfilled; a tap assigns without dragging; a rest-rule break shows its badge, rule and numbers and an override asks for a reason; a not-submitted employee assigned by hand shows the `warnings.not_submitted` warning; the publish review counts the right shifts and people; undo steps back; a manager edits only their own project (the other is read-only); a stale save shows "the roster changed"; the submission tracker lists who hasn't submitted; the phone day agenda assigns and publishes; assigning a person to an open seat takes at most three taps (SC-005)
- [x] T063 [US3] [CORE] Change the 001 contract (contracts/api-changes.md, group D): widened gates, assignment `starts_at`/`ends_at`, `PATCH /assignments/{id}`, `ETag`/`If-Match`/412, overrides, publish preview, `notify` on publish, copy week, week templates, bulk, the submission tracker
- [x] T064 [US3] [CORE] Write the failing API tests: `apps/api/tests/unit/test_effective_span.py`; `apps/api/tests/integration/test_assignment_times.py` (±4 h bounds, pairing, reset); `test_manager_roster_edits.py` (a manager edits managed projects, is refused on others, cannot set `pay_rate`; employees and clients refused); `test_roster_concurrency.py` (stale `If-Match` → 412; no header behaves as today); `test_rule_overrides.py`; `test_publish_preview.py` (counts match what publish then publishes; one `publish_events` row per person); `test_copy_week.py` and `test_week_templates.py` (dry run equals the applied change); `test_submission_tracker.py`; pay with partial and extended times in `apps/api/tests/unit/test_price_assignment_span.py`; a Hypothesis property in `apps/api/tests/property/test_assignment_span_rules.py` (rest and weekly hours are computed on the effective span)
- [x] T065 [US3] [CORE] Migration `apps/api/alembic/versions/031_assignment_times.py` (data-model 031, written as 027) and the effective-span helper in `services/roster.py`; carry the span through `hds/payroll/price.py`, `hds/scheduling/types.py`/`rules.py` (via plain fields), `services/pay.py`, `services/reports.py` and `services/roster.py` (My shifts); ADR `docs/adr/018-assignment-times.md` (FR-027)
- [x] T066 [US3] [CORE] Managers' roster edits: widen the six admin routes in `routers/shifts.py`, check `roster.edit:project` in `services/roster.py`, refuse `pay_rate` from managers with `shift.pay_rate_admin_only` (FR-028); ETag from `updated_at` and `If-Match` → 412 `roster.changed`
- [x] T067 [US3] [CORE] Migration `apps/api/alembic/versions/032_roster_extras.py` (data-model 032, written as 028) with models, repositories and services for `rule_overrides`, `week_templates`, `publish_events`; routes in `apps/api/src/hds/routers/roster_extras.py` (overrides, publish preview, copy week, templates, bulk) and the tracker in `routers/availability.py`; `notify` on publish
- [x] T098 [US3] [CORE] Owner's decision of 2026-11-08, publishing per project: `roster.publish` becomes `roster.publish:project` (an Admin publishes any project, a Manager only the projects they manage) in `core/capabilities.py`, contracts/capabilities.md, the 001 contract and the client; record in spec.md which way a publish covering an unmanaged project is refused; role matrix and `test_capabilities.py`
- [x] T099 [US1] [CORE] Owner's decision of 2026-11-08, web sign-in per tab: the web token store keeps the tokens in sessionStorage, read and removed on load and written back only on `pagehide`, so a reload or a deep link in the same tab stays signed in and a duplicated tab signs in again without a refresh-token reuse; ADR 019 amending ADR 010 (a Content-Security-Policy as Proposed or a follow-up); a unit test of the store and e2e for the reload, the deep link and the duplicated tab
- [x] T100 [US4] [UI] My work on each person's own times (carried from PR 3's review): My shifts, the shift detail, the period minutes, the countdown, the Upcoming and Past split and the calendar event use the caller's effective span from `/me/shifts`
- [x] T101 [US3] [CORE] The submission tracker shows an Admin's hand change beside the submission (`changed_by`, `changed_at` from migration 030)
- [x] T068 [US3] [CORE] Regenerate the client; locale keys; update or retire `apps/client/tests/e2e/test_us6_roster.spec.ts` and `test_us8_shift_types.spec.ts` where the roster replaces them
- [x] T069 [CORE] Commit group D's [CORE] tasks (`feat(api): give assignments their own times and open roster edits to project managers`, `test(client): add group d's failing roster spec`)

### [UI] then

- [x] T070 [US3] [UI] Roster week `apps/client/src/screens/roster/`: Sunday–Saturday columns, rows by shift template, blocks with people and their bars (partial, extended), overnight continuation, shapes for status, coverage per block and per day and hours against the cap in row headers (FR-031), holidays, compact density, the span and grouping switches and the range mode, all in the web address (FR-025, FR-026, FR-029–FR-032)
- [x] T071 [US3] [UI] Tap path for every drag (select → Assign to / Move to sheet, people who prefer first, availability shown), drag as a shortcut with a qualification hint, and screen-reader actions; assign and drag feedback in motion (lift, target highlight, settle) and a success haptic on assign and publish (FR-071, FR-073); edit an assignment's own times with the live duration and the DST notice (FR-033, FR-035, FR-053)
- [x] T072 [US3] [UI] Layered warnings: badge, detail with rule and numbers, totals per person, the warnings list with "Show on roster", the override sheet with a reason; the stale-save banner (FR-034, FR-041)
- [x] T073 [US3] [UI] Publish review (scope, counts, coverage, unfilled, open warnings, Notify everyone / Publish silently), the multi-step undo next to Publish (FR-037), copy last week, week templates and bulk edit with their previews (FR-036–FR-038)
- [x] T074 [US3] [UI] Phone day agenda for managers with tap-to-assign and publishing, and the submission tracker screen (FR-039, FR-040); finish the manager home's coverage, unfilled and not-submitted cards from T037 (FR-006)
- [x] T075 [P] [US3] [UI] Unit tests for the bar geometry (partial, extended, overnight, RTL) in `apps/client/tests/unit/roster/*.test.tsx`

### Close

- [x] T076 [CORE] Close group D as the template says. Mutants (≤ 3): the +4 h bound turned into +5 h, pay priced on the shift's span instead of the assignment's, and the project check in `roster.edit:project` dropped. Commit `feat(client): add the weekly roster (…)`

---

## Phase 6: Group E — schedule runs as they are today (PR 4) — US7

**Goal**: runs list, generate form with presets, proposal with the honesty label; accept and discard
unchanged; 008's previews in place.

**Independent Test**: `ux/e-schedule-runs.spec.ts`.

- [x] T077 [US7] [CORE] Write `apps/client/tests/e2e/ux/e-schedule-runs.spec.ts`, failing: generate with a preset range (no past start); progress while running; the honesty label ("Optimal" / "Best found in N s") and filled count; accept and discard as today; the loading bar is determinate during the run and its `aria-valuenow` only rises, labelled "up to N s" (FR-074, SC-011); the `proposal-diff` preview present only with previews on
- [x] T078 [US7] [CORE] If `GET /schedule-jobs/{id}` lacks `optimal` or `elapsed_seconds`, add them to the 001 contract and `routers/schedule_jobs.py` with an integration test; regenerate the client; update `apps/client/tests/e2e/test_us_generate.spec.ts`; commit (`test(client): add group e's failing schedule-runs spec`)
- [x] T079 [US7] [UI] Runs list, generate form (DateRangeField with presets, limits), proposal view with the honesty label, accept and discard, in `apps/client/src/screens/runs/`, and the determinate `LoadingBar` fed by the job's elapsed time and limit with "best so far: n / m filled" (FR-042, FR-074); give the loading store's `track` an opt-out, `{ background: true }`, and use it for the run-status poll and the token refresh, so background requests never keep the bar showing (the determinate run bar sets its own progress) (PR 1 review)
- [x] T080 [CORE] Close group E as the template says (no mutants: no security, money or time rule changes beyond D's). Commit `feat(client): redraw schedule runs (…)`. **PR 4 ready** (D and E): once-per-PR steps, including `make ci-local-db`

---

## Phase 6b: Group O — the owner's requests of 2026-11-08 (PR 5, first) — US6, US7

**Goal**: the owner's four requests of 2026-11-08 (spec FR-078 to FR-081, Proposed), before F–H.
Each starts with its failing spec. The alignment and language items come first.

**Independent Test**: `ux/o-owner-requests.spec.ts` and `expectNothingOverflows` across the ux specs.

- [x] T102 [US6] [CORE] (Owner, 2026-11-08) Add `expectNothingOverflows(page)` to `apps/client/tests/e2e/ux/support.ts`: for every element with a visible border or panel background, every visible descendant's box lies inside the element's box, within 1 px. Call it on each screen the ux specs already visit, at 390 px, in he and en. It must fail on today's Admin `/home` (the "Set up HDS" card's second row of buttons crosses its border). Commit failing (`test(client): check that nothing overflows its card`) (FR-079)
- [x] T103 [US6] [UI] (Owner, 2026-11-08) The setup card gets its own style: `styles.card`'s `flexBasis: 220` was written for ManagerCards in a row, and in a column it becomes a height. Then audit the app for the same pattern (`flexBasis` or `flexGrow` on an item whose parent is a column; fixed heights on boxes whose content wraps: cards, FeaturePanel, EmptyState and InlineBanner actions, ClippedBox users) until T102 passes (FR-079)
- [x] T104 [US6] [CORE] (Owner, 2026-11-08) Write the failing specs: a unit test that the current path and query are saved before a language restart and restored after it; and in `ux/o-owner-requests.spec.ts`, on the web, switch the language on `/account` and on a deep path (`/my/shifts?view=past`), from the top bar, Account and sign-in: the switching state shows at once in the target language, and the person lands on the same path in the new direction, still signed in (FR-078)
- [x] T105 [US6] [UI] (Owner, 2026-11-08: "as long as youre not going back to the home screen and view a loading/please wait animation - yes / the user has to know the app didnt crach") The language switch restarts the app on every platform: (1) at once, a full-screen "Switching to English…" / "עוברים לעברית…" state in the target language, with the Option B loading animation; (2) the current path and query saved first and restored after the restart, signed in, never home; (3) on the web, `location.reload()` once the choice is stored; on phones, expo-updates' `reloadAsync` (check it works in a Release build with remote updates off; use the configuration that keeps it working), with the native splash held until the session and the screen are restored, and the restart notice as the fallback if it throws. Until the restart, no Txt runs in a different direction from its container; Txt's default direction (R76) reads i18n's current language. expo-updates is a native module: tell the reviewer the commit, for the simulator rebuild. The native pass goes on the owner's device checklist (T092) (FR-078)
- [x] T106 [US7] [CORE] (Owner, 2026-11-08) Write the failing spec: open the demo's accepted run and see a named assignment for a known shift, the places left empty and the people without availability by name, and "Open in roster" for its range; a discarded run shows the same view, labelled. Extend `GET /schedule-jobs/{id}` in the 001 contract if the proposal lacks the names or slots the view needs, with tests; regenerate (FR-080)
- [x] T107 [US7] [UI] (Owner, 2026-11-08) The run's proposal as D's roster week, read-only, with the run's changes marked added or removed against the drafts; names for the empty places and for the people without availability; "Open in roster" for an accepted run (FR-080)
- [x] T108 [US7] [CORE] (Owner, 2026-11-08) Write the failing spec: generate for one project, and the proposal holds only that project's slots; a Manager is offered only the projects they manage. Then the API scope (reviewer ruling, 2026-11-08, from O9 and T098; do it right after T104, before T106): create refuses any `project_ids` entry a Manager does not manage with 403 `roster.project_not_managed` and creates no job; an empty list means the Manager's managed projects (none: 422 `schedule.no_projects`) or, for an Admin, every active project; the resolved list is stored on the job. Accept and discard re-check that the acting Manager manages every project of the job, and refuse the whole of it otherwise, never part. The runs list and a run's page stay readable by any Manager. 001 contract with the new codes; integration tests for create (a foreign id; an empty list as a Manager and as an Admin), accept after the Manager loses a project, and discard; regenerate (FR-081)
- [ ] T109 [US7] [UI] (Owner, 2026-11-08) A multi-select SearchablePicker of projects in the generate form, all by default, each with its type (the owner's note: training and security projects don't mix); a Manager sees only the projects they manage, matching publish per project (T098), and an Admin sees all; the chosen projects show on the run's row and page (FR-081)
- [ ] T110 [CORE] Close group O as the template says. Mutants (≤ 3): accept's re-check of the Manager's projects dropped, the generate form offering a Manager a project they don't manage, and the language switch reloading before the choice is stored. Commit `feat(client): the owner's requests of 2026-11-08 (…)`

---

## Phase 7: Group F — admin screens (PR 5) — US7

**Goal**: one table pattern and one form pattern across the admin screens, money confirmations,
loading and empty states, filters in the address, the setup checklist.

**Independent Test**: `ux/f-admin.spec.ts`.

- [ ] T081 [US7] [CORE] Write `apps/client/tests/e2e/ux/f-admin.spec.ts`, failing: tables sort, filter and save views; the address keeps filters and the open record on reload; the searchable picker finds an employee by Hebrew and English names; a form shows inline errors and an error summary that takes focus; a pay-rate change confirms in specific words; leaving with unsaved changes asks first; skeleton rows, never "No records", on first load; the empty organisation shows the setup checklist; a manager sees create and edit but not delete
- [ ] T082 [US7] [CORE] Change the 001 contract for `GET/POST/DELETE /me/saved-views` and any missing `q`/`sort`/`page`/`page_size` on the admin list endpoints; migration `apps/api/alembic/versions/029_saved_views.py` (data-model 029), model, repository, service, router `routers/saved_views.py`, with tests `apps/api/tests/integration/test_saved_views.py`; regenerate the client; update `apps/client/tests/e2e/test_us4_clients_projects.spec.ts`, `test_us6_invite.spec.ts`, `test_us_pay_settings.spec.ts`; commit (`feat(api): add saved views`, `test(client): add group f's failing admin spec`)
- [ ] T083 [US7] [UI] Admin table screens in `apps/client/src/screens/admin/` for staff, clients, sites, projects, shift templates, staffing needs, special days and pay settings, on the Table, SidePanel, bulk bar and SearchablePicker (FR-043, FR-044, FR-049)
- [ ] T084 [US7] [UI] Form pattern (validate on leave, error summary with focus, one Save never disabled, no autosave of money, unsaved-changes guard in the app and on tab close), money confirmations naming change, scope and date, undo toasts for archive and remove (FR-045, FR-046)
- [ ] T085 [US7] [UI] Loading, feedback and empty states, and the admin setup checklist (FR-047, FR-048)
- [ ] T086 [CORE] Close group F as the template says. Mutants (≤ 3): a manager's view showing the delete action (capability check removed), and the pay-rate confirmation saving before the confirm. Commit `feat(client): add the admin tables and forms (…)`

---

## Phase 8: Group G — reports (PR 5) — US7

**Independent Test**: `ux/g-reports.spec.ts`.

- [ ] T087 [US7] [CORE] Write `apps/client/tests/e2e/ux/g-reports.spec.ts`, failing: presets default to the current pay period and can't reach the future; key figures match the table; a bar opens its rows; the work log is titled "Work log" / "יומן עבודה" and no screen says "מי עבד"; add `totals` to `GET /reports/hours` and `GET /reports/work-log` in the 001 contract and `services/reports.py` if missing, with tests; regenerate; update `apps/client/tests/e2e/test_us_work_log.spec.ts` and `test_us_hours_report.spec.ts`; commit
- [ ] T088 [US7] [UI] Work log and hours report in `apps/client/src/screens/reports/`: key figures, bars by employee, project or client that open into rows, pay-period presets, pagination (FR-050, FR-025)
- [ ] T089 [CORE] Close group G as the template says (mutant: totals summed on the client instead of read from the server). Commit `feat(client): redraw the work log and hours report (…)`

---

## Phase 9: Group H — polish and accessibility (PR 5) — US6

**Independent Test**: `ux/h-a11y-sweep.spec.ts`.

- [ ] T090 [US6] [CORE] Write `apps/client/tests/e2e/ux/h-a11y-sweep.spec.ts`, failing until the sweep is done: every route in contracts/ui-routes.md for every role, in he and en, light and dark, desktop and phone, passes axe with no serious issues, and focus is never hidden behind sticky bars, tab bars or toasts; commit
- [ ] T091 [US6] [UI] The sweep (FR-065): fix every finding across the app; finish all four themes and every motion and reduce-motion path; review every string against the glossary and the voice guide (FR-061, FR-071, FR-075, FR-077); grayscale check of the roster and the grid; the accessibility statement page `apps/client/app/accessibility.tsx` with placeholder text for the owner (FR-061–FR-066)
- [ ] T092 [US6] [UI] Write the owner's morning device checklist for the manual VoiceOver (iOS device) and TalkBack (Android device) pass in Hebrew and English, and the Intl polyfills on a physical Android phone; with a place to record the results in `specs/007-ux-redesign/research.md` under a new "H. Manual checks" entry (FR-065); check gender-neutral Hebrew copy across both locale files (FR-060) and status shown by icon, label and colour (FR-030, FR-064); the roster week scrolls at 60 fps on a mid-range Android phone (plan, Performance Goals) These device checks need a person at the phones: they do not block group H's close or the PR 5 marker branch.
- [ ] T093 [CORE] Close group H as the template says (no mutants). Commit `feat(client): finish the accessibility pass (…)`

---

## Phase 10: Polish & cross-cutting

- [ ] T094 [P] [CORE] Documentation: `apps/api/README.md` (new endpoints and settings), `docs/hds-api.md` (Proposed rows: managers' roster edits, assignment times, submissions), `docs/README` index for ADRs 014–018, and `.claude/hds/` working-plan pointers
- [ ] T095 [CORE] Remove what the redesign retired: `apps/client/src/components/EmployeeNav.tsx`, the old `(employee)` routes and screens, and their locale keys, once no spec or route uses them
- [ ] T096 [CORE] Run quickstart.md's checks for every group. **PR 5 ready** (F, G, H): once-per-PR steps, including `make ci-local-db`

---

## Dependencies & Execution Order

- **Setup (T001–T005)** before group A's spec.
- **A → B → C → D → E → F → G → H**, one group at a time on the branch. Within each: [CORE] spec and
  API, committed → [UI] → close.
- **B depends on A** (primitives, theme). **C depends on B** (shell, capability gating). **D depends
  on C** (the not-submitted warning, the submission model for the tracker) and **on B** (capabilities
  for managers). **E depends on A's date picker**. **F and G depend on A and B**. **H depends on all**.
- Migrations are numbered in shipping order: 025 (B), 026 and 030 (C), 031 and 032 (D, written as
  027 and 028), 033 (F, written as 029).
- My shifts' effective times (T052) read shift times until T065 lands, then the effective span.

### User Story → groups

| Story | Groups | Tasks |
| --- | --- | --- |
| US1 roles see only what they can use | B | T025–T042, T045–T046 |
| US2 availability week | C | T047–T051, T053–T057, T060–T061 |
| US3 roster | D | T062–T076 |
| US4 my shifts and pay | C | T052, T058–T059 |
| US5 date pickers | A (built), E, F, G (used) | T019, T077, T079, T087 |
| US6 design and accessibility | A, H | T006–T024, T090–T093 |
| US7 admin, reports, runs | E, F, G | T077–T089 |
| US8 previews | B | T043 (and checks in T025, T077) |
| US9 motion, loading, language, finish | A, B, C, D, E, H | T009–T011, T021–T022, T044, and parts of T006, T025, T057, T071, T077, T079, T091 |

### Parallel opportunities

- Group A: T007 and T008 together; T017 and T023 alongside T018.
- Group B: T029 and T030 together after T028.
- Within any group, [P] tests and unit-test tasks next to their implementation.
- [CORE] and [UI] never run at once on the branch.

## Implementation strategy

- **MVP**: groups A and B (PR 1 and PR 2): the design system and role shells end the owner's worst
  complaint (clients refused on staff screens).
- **Then** C (availability, my shifts), D with E (roster, runs), F to H (admin, reports, the
  accessibility pass), each a pull request usable on its own.

## Notes

- R12: 007 records one `publish_events` row per affected person and sends nothing; feature 010
  delivers them. Flagged for the reviewer against FR-036.
- R9: four libraries beyond the prompt's list (`expo-local-authentication`, `expo-calendar`,
  `@react-native-async-storage/async-storage`, `expo-font`), each added in the group that needs it.
- Never push; the owner pushes and merges.
