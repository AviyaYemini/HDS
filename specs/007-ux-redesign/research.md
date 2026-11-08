# Research: UX foundation and redesign

Phase 0 of the plan. Each entry gives the decision, the rationale and the alternatives. R1 and R2
stay open until group A's spike; the spike's result is written into them and into an ADR.

## R1. Component library — decided by the spike: HDS's own components on React Native's primitives

- **Decision** (2026-11-07, group A task T014, ADR 014): HDS draws its own components on React
  Native's primitives (`View`, `Text`, `Pressable`, `TextInput`, `Modal`), styled with `StyleSheet`
  from a theme context generated from the DTCG tokens; React Native's `Animated` on the native
  driver for motion; `react-native-svg` for icons and clipped corners; `expo-font` with the Expo
  Google Fonts packages for IBM Plex Sans and IBM Plex Sans Hebrew. No styling library is added.
- **What the spike checked**: a probe screen (list, form, tabs, select, switch, bottom sheet,
  range picker) per candidate, in a throwaway copy of the app, exported for the web and measured by
  Playwright at 390 × 844 in Hebrew and English. On the web, React Native primitives mirror rows,
  tabs, the select's chevron, the switch and text alignment under `dir="rtl"`; logical start/end
  styles resolve only through React Native Web's `LocaleProvider`, which a `dir` prop on the root
  View provides (read in `react-native-web/dist/exports/createElement`), so the root screen carries
  `dir`. Tamagui 3.2.3 did not start with its v5 or v6 default config (`createTamagui` error 008, a
  token name colliding with a CSS keyword) on this Expo SDK's web export. HeroUI Native 1.0.11 on
  Uniwind 1.13.2 builds for the web after nine peer packages are pinned, and Tailwind's logical
  utilities mirror correctly; it needs Nitro Modules, so a development build. Unistyles 3 was not run
  (also Nitro plus a development build and a Babel plugin); its benefits are small at HDS's size.
- **iOS, on a development build** (`expo run:ios`, iPhone 16 simulator, iOS 18.3, the device in
  Hebrew, after the owner installed Xcode and Node): React Native primitives mirror everything on
  the device too (rows, tabs, the select's chevron, the switch, `paddingStart`, the range picker's
  Sunday-first headers and its month arrows), so the web result holds natively. HeroUI Native on
  Uniwind also renders and mirrors there (`flex-row` and `ps-10`), so it is a credible later
  option. Tamagui needs a per-platform animation driver (`animations-rn` on native, a CSS or
  Reanimated driver on the web) and, with the native driver set, the probe crashed on an undefined
  component; it was not pursued further. react-native-paper-dates renders right to left on iOS in
  Material style.
- **What the spike could not check**: Android RTL. The Android emulator (API 36.1, Expo Go 58)
  rendered the primitives probe in English, but Expo Go cannot switch layout direction, and an
  Android development build (Gradle with the installed JDK 25) was not attempted tonight. Android
  rests on React Native's own `I18nManager` and start/end handling, which the iOS build verified,
  and is checked on a device in group H (FR-065).
- **Why own components**: Option B is square, with one clipped corner, 1 px chips and a 2 px rule;
  every kit on the shortlist ships rounded, Material- or shadcn-like defaults that would be
  overridden everywhere, and none adds RTL beyond what React Native already does. Own components
  keep Expo Go working and add no native module.
- **Alternatives kept open**: Reanimated behind the same `src/design/motion/` API if a screen needs
  gesture-driven motion; HeroUI Native if it gains a square theme.

## R2. Date picker — decided by the spike: HDS's own, on `src/locale/plainDate.ts`

- **Decision**: `DateField` and `DateRangeField` are HDS's own (`src/design/components/date/`),
  built on the pure date module: typed entry with any separator (day first), the W3C grid keyboard
  pattern with `gridcell` roles, presets as a side list on the web and chips on phones, Sunday weeks,
  Israeli holidays from `@hebcal/core`, two months side by side that do not jump.
- **What the spike saw**: react-native-ui-datepicker 3.3.0 in range mode renders Sunday-first weeks
  with Hebrew month and weekday names and mirrors on the web, but has no keyboard grid, typed entry,
  presets or accessible roles. react-native-paper-dates 0.24.1 renders right to left on the web and on the iOS build with
  Hebrew registered by hand, but looks Material, needs react-native-paper's provider, and its range
  header swaps the two dates in Hebrew. react-native-ui-datepicker also mirrored on iOS (see R1).
- **Rationale**: the spec's picker behaviour (FR-051, FR-052) is mostly what a wrapper would have to
  add; building the grid on the same primitives as the rest of the design costs about the same and
  draws Option B exactly.

## R3. Design tokens

- **Decision**: DTCG 2025.10 JSON in three tiers (`base`, `semantic`, `component`) under
  `apps/client/src/design/tokens/`, transcribed from the working plan's section 3; four themes as
  semantic overrides (light, dark from the inverse set with `cta.bg` as accent, high-contrast light
  and dark targeting 7:1 for text); a `density` token (comfortable, compact). A small script
  generates the chosen library's theme from the JSON; the generated file is checked for drift in CI
  like the API client.
- **Contrast test**: a Jest unit test reads the semantic tier, pairs every text and icon token with
  every background it is declared for (a `$extensions.hds.on` list), and asserts WCAG 2.x contrast
  ≥ 4.5:1 for text, ≥ 3:1 for icons and large text, ≥ 7:1 for text in the high-contrast themes.
- **Rationale**: keeps the source of truth independent of R1, and makes the contrast floor
  mechanical (X11–X13).

## R4. Capability map

- **Decision**: one pure module, `hds.core.capabilities`, maps a role (and, for managers, the set of
  projects they manage) to named capabilities such as `my_work.read`, `roster.read`,
  `roster.edit:project`, `pay.edit_shift`, `staff.manage`, `staff.delete`, `client_portal.read`.
  The existing `require_staff`, `require_manager`, `require_admin` and
  `require_manager_or_client` become thin wrappers that ask the map, and project-scoped checks
  (`roster.edit:project`) are made in services. `GET /me` adds `capabilities` (a sorted list) and
  `managed_project_ids`. The client gates tabs, routes, buttons and menus only from that list
  (`contracts/capabilities.md`).
- **Rationale**: N2. The UI and the API can no longer drift: there is one list, and a contract test
  asserts that every capability the UI reads exists in the map and every role's list equals the
  table in `contracts/capabilities.md`.
- **Alternatives**: deriving tabs from the role in the client (today's cause of N1); per-endpoint
  probing (slow, and still refuses).

## R5. Each assignment's own start and end

- **Decision**: nullable `starts_at` and `ends_at` on `assignments` (UTC), both null meaning "the
  shift's times" (so editing a shift still moves everyone on it) or both set. The service refuses a
  start more than 4 h before the shift's start or an end more than 4 h after its end
  (`assignment.times_out_of_range`, 422), and an end not after the start. An "effective span" helper
  is the only way services read an assignment's times; pay (`hds.payroll.price`), the rule checks
  (`hds.scheduling.rules` via `validate`), reports and My shifts all take the effective span.
  Coverage counts an assignment as filling one seat whatever its span.
- **Rationale**: O7 (each assignment gets its own start and end) is simpler than sub-shifts: one row
  per person per shift, as today, and the solver keeps placing whole shifts.
- **Alternatives**: sub-shifts (a shift split into pieces), which changes headcount, coverage and the
  solver; storing offsets in minutes, which reads badly across daylight-saving changes.

## R6. Availability submissions and "not submitted counts as unavailable"

- **Decision**: a row per employee and window in `availability_submissions` with `submitted_at` and
  a snapshot of the employee's blocked, open and close entries for the window's dates at the time of
  submitting. Entries keep being saved through today's endpoints and form the draft. Status is
  derived: no row and no entries in the window → Not submitted; entries changed after the snapshot
  or no row → Draft; snapshot equals the live entries → Submitted; now past the deadline → Locked
  (whatever the rest). After the deadline the live entries for the window are frozen
  (`availability.window_closed`, 409, the code 003 already uses), except absences, which stay editable; an Admin's change there refreshes an existing snapshot but never creates one (reviewer note): an Admin counts a non-submitter only by submitting on their behalf, which records who did it and when. Windows already closed when 026 runs keep the old rule (`requires_submission` false).
- **Scheduling**: `schedule_inputs` reads the snapshot for dates inside a window whose deadline has
  passed, not the live entries. An employee with no submission for such a window gets one synthetic
  blocked entry per date of the window with reason `not_submitted`, so the solver never assigns
  them there. For a window still open, the solver reads the last snapshot (or, with none, treats the
  employee as not submitted); the manager sees this in the run's inputs. `validate` turns a
  `not_submitted` block into the warning `warnings.not_submitted` for manual assignments, so a
  manager may still assign by hand, with a warning (ADR 009: manual changes only warn).
- **Rationale**: the owner's answer to Q4 ("count them as unavailable") and FR-014.
- **Alternatives**: a status column flipped by a job at the deadline (needs a scheduler and races
  with late edits); copying entries into a separate "draft" table (two sources of truth).

## R7. Managers' roster edits

- **Decision**: the six admin-only routes in `routers/shifts.py` (create, edit, delete shift; add and
  remove an assignment; publish is already manager) move to `require_staff_roster` and the service
  checks `roster.edit:project` (an admin, or a manager in `project_managers` for that project). The
  pay field on a shift stays admin-only: a manager's PATCH carrying `pay_rate` is refused with 403
  `shift.pay_rate_admin_only`. Shifts gain optimistic concurrency: responses carry an `ETag`
  (from `updated_at`), and an optional `If-Match` on PATCH, DELETE and assignment changes is answered
  412 `roster.changed` when stale (FR-041). Clients that send no `If-Match` behave as today.
- **Rationale**: O9 and FR-028, with no breaking change to existing callers.

## R8. Rule overrides, warnings and hard stops

- **Decision**: warnings stay as ADR 009 defines them (they never refuse). Overriding one records a
  row in `rule_overrides` (assignment, warning code, reason, who, when) through
  `POST /assignments/{id}/overrides`. The publish preview counts a warning as open until it has an
  override. Hard stops are today's refusals (data that cannot be stored), which have no override.
- **Rationale**: S9, S10 and O10 without changing ADR 009; legal limits wait for the legal review.

## R9. Libraries the spec needs beyond the prompt's list

The prompt names the stack above; four more are needed to meet the spec and are listed here for the
reviewer: `expo-local-authentication` (FR-010), `expo-calendar` (FR-023; the web gets an `.ics` file
built by a small helper), `@react-native-async-storage/async-storage` (FR-019), `expo-font` (the
Plex families), `expo-haptics` (FR-073) and `expo-splash-screen` (FR-077). Each is an Expo-maintained or de-facto standard package and is added in the group
that first needs it.

## R10. Phone sessions

- **Decision**: `POST /auth/login` accepts an optional `client_kind` (`native` or `web`, default
  `web`). Refresh tokens issued to `native` last 30 days and roll on every refresh; `web` keeps
  `REFRESH_TOKEN_TTL_DAYS` (7). A new setting `REFRESH_TOKEN_TTL_DAYS_NATIVE` (30) holds the phone
  value; rotation and reuse detection (ADR 006) are unchanged; access tokens stay ≤ 15 min.
  Biometric unlock is a client gate in front of the stored refresh token; no biometric data leaves
  the device.
- **Accepted limit**: `client_kind` is self-declared, so a browser can claim `native` and get a
  30-day token. Acceptable because rotation and reuse detection still apply, the web keeps tokens in
  memory (ADR 010), and biometrics are a convenience gate, not a security boundary. Recorded in ADR
  016; a test pins that a web sign-in without `client_kind` gets 7 days.
- **Rationale**: the owner's answer to Q5.

## R11. Previews

- **Decision**: a build-time switch `EXPO_PUBLIC_PREVIEWS` (on in development and the shared test
  environment's build, unset in production). Preview routes are registered only when it is on, and
  each preview declares its role capability, so the capability map still gates it. A Playwright
  test runs against a build with the switch off and asserts no preview route resolves. The
  development gallery route (`/dev/gallery`) uses the same switch.
- **Rationale**: FR-068 and FR-069; a build-time switch means production bundles do not even contain
  the preview screens.

## R12. Publish notifications

- **Decision**: the publish review counts the people a publish affects and records one publish
  event per affected person (`publish_events`, data-model.md), which feature 010 delivers through
  the inbox and push. In 007 no message is sent, because HDS has no channel for it yet; the review's
  "Notify everyone / Publish silently" choice is stored on the event.
- **Rationale**: the channels arrive in 010 (O14). Accepted by the reviewer on 2026-11-07: FR-036
  now reads "recorded for delivery".

## R13. Locale plumbing

- **Decision**: FormatJS polyfills (`intl-getcanonicallocales`, `intl-locale`, `intl-pluralrules`,
  `intl-numberformat`, `intl-datetimeformat` with he and en data and the Asia/Jerusalem zone) loaded
  at the entry point before i18next, checked on a physical Android phone; one `isolate()` helper
  (FSI … PDI for text of unknown direction, LRI … PDI for times, phones and amounts); dates as
  dd/MM/yyyy built from `Intl` parts; money via `Intl.NumberFormat` from the Decimal string, never
  through `Number` arithmetic; `@hebcal/core` with `il: true` for holidays and eves; Sunday weeks;
  24-hour times with "+1". The clock icon is not mirrored (O13).

## R14. Motion

- **Decision**: three durations (120, 200, 320 ms) and two easings (standard
  cubic-bezier(0.2, 0, 0, 1), emphasised cubic-bezier(0.3, 0, 0, 1)) as DTCG `duration` and
  `cubicBezier` tokens; no springs. Enter uses `base`, exit `fast`, large surfaces (side panels, the
  clipped-corner reveal) `slow`. Motion runs on the UI thread where the library allows it, and no
  gesture or press waits for an animation to end. Reduce-motion is read live and sets every duration
  to 0.
- **Rationale**: the owner's note; Option B's sharp shapes read best with short, decelerating moves
  and no bounce; WCAG 2.3.3 and X8.
- **Alternatives**: springs (overshoot fights the square look); CSS-only on web (two code paths).

## R15. The loading bar

- **Decision**: one `LoadingBar` fed by a small loading store: route changes and any request still
  pending after 300 ms raise it, and once shown it stays at least 400 ms. Indeterminate is a moving
  copper segment on a tan track; determinate (schedule runs) is a fill to elapsed / `time_limit_seconds`,
  polled from the job and labelled "up to N s"; it completes at once when the run ends early, and
  a greedy run or one without a limit shows the indeterminate bar. "Best so far" needs the worker to
  publish interim results, which comes with 008 (reviewer ruling). Under reduce-motion the indeterminate state is a
  still, half-filled bar with "Loading…" text for screen readers.
- **Rationale**: the owner's note, A9 (nothing under a second flashes), AS8's visible part.

## R16. Glossary, voice and locale parity

- **Decision**: `docs/ux/glossary.md` and `docs/ux/voice-and-tone.md` are the source for all copy; a
  Jest test compares `he.json` and `en.json` key sets and refuses empty values, values equal to their
  key, and the markers `TODO`, `TBD`, `FIXME`, `lorem`, `xxx`. A second test checks the glossary's
  terms are used for their keys (for example `nav.roster` is "Roster" / "סידור עבודה").
- **Rationale**: FR-075, FR-076, D13.

## H. Manual checks

The checks a person has to do on the phones (task T092, FR-060, FR-064, FR-065). They do not block
group H's close or the PR 5 marker. Record each result in the table below: the date, the device
and its system version, the language, pass or what failed.

**Before you start**: a Release build of 007 on an iPhone (VoiceOver) and a mid-range Android
phone (TalkBack), signed in once as an employee and once as a manager. On each phone, do the
whole list in Hebrew and again in English.

1. **Screen reader, every tab**: with VoiceOver or TalkBack on, move through Home, Availability,
   My shifts, Pay, More (employee) and Home, Roster, My work, Reports, More (manager). Every
   control says what it is and what it does; no control is read twice or skipped; the order
   follows the screen's reading direction (right to left in Hebrew).
2. **Availability week**: change a cell, undo it, and submit. Each change is announced; the
   submitted state is read out.
3. **Roster**: open a block, assign someone, open a warning, override it with a reason, publish.
   Every step can be done without dragging.
4. **Schedule run**: generate a run and wait. The progress bar's value is read out at each
   quarter; the result ("Optimal" or "Best found in N s") is read when it arrives.
5. **Language switch (T105)**: from the top bar, switch to the other language on a deep screen
   (My shifts, Past). You see "Switching to …" in the new language, then the splash, then the same
   screen in the new direction, still signed in, with no second Face ID or fingerprint prompt.
   Switch back. If the phone shows the "restart to finish" notice instead, note it: the reload
   did not happen.
6. **Intl on Android**: dates, times and money read correctly in both languages (07/11/2026,
   07:00–15:00, ₪48.00), and the week starts on Sunday.
7. **Status without colour**: with the phone's grayscale filter on, every status (availability
   cells, roster coverage, run status, chips) is still told apart by its icon and its words.
8. **Roster scrolling**: on the Android phone, scroll the roster week up and down and across for
   ten seconds. It keeps up with the finger (60 frames a second, no visible stutter).

**Done in the code already (no device needed)**: Hebrew copy uses plural or noun forms rather
than a gendered singular (checked across `he.json` in group H, seven strings reworded); every
screen passes axe in both languages, light and dark, at desktop and phone sizes (h-a11y-sweep);
status chips carry a mark and a label beside their colour.

| Date | Device and system | Language | Check | Result |
| --- | --- | --- | --- | --- |
| | | | | |
