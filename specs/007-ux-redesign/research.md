# Research: UX foundation and redesign

Phase 0 of the plan. Each entry gives the decision, the rationale and the alternatives. R1 and R2
stay open until group A's spike; the spike's result is written into them and into an ADR.

## R1. Component library — open until the spike

- **Decision**: A one-day Hebrew RTL spike (group A's first task, [UI]) builds a list, a form, tabs,
  a select, a switch, a bottom sheet and a date-range picker on iOS, Android and web with each
  candidate, then picks one. **Shortlist:** a Uniwind kit (React Native Reusables or HeroUI Native),
  Tamagui, or Unistyles with HDS's own components.
- **Pass criteria** (in this order): mirrors correctly on all three platforms without per-component
  fixes; can draw Option B exactly (square corners, the clipped corner, 1 px chips, the 2 px
  active rule); themes generated from the DTCG tokens with four themes and a density token; screen
  reader roles and focus on web; bundle and start-up cost on a mid-range Android phone.
- **Rationale**: RTL quality differs most between candidates, and the only evidence so far is their
  2026 GitHub activity (research report X16). NativeWind 4's `rtl:` variants are broken on native;
  React Native Paper looks like Material and has an open RTL issue on web, so both are off the list.
- **Records**: the result goes here and in `docs/adr/014-component-library.md`. The spike runs on the web and on whatever simulators or emulators run unattended; R1 and R2 record which platforms were and were not checked..

## R2. Date picker — open until the spike

- **Decision**: react-native-ui-datepicker or react-native-paper-dates, chosen in the same spike,
  with HDS's own preset list, typed entry and keyboard handling on top (D1–D4).
- **Rationale**: ui-datepicker is unstyled, starts weeks on Sunday and takes a time zone but
  documents no RTL; paper-dates has Hebrew and RTL but pulls in Paper. Not react-native-calendars,
  whose RTL issues were closed as stale.

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
  (`availability.window_locked`, 409), except absences, which stay editable as in 003.
- **Scheduling**: `schedule_inputs` reads the snapshot for dates inside a window whose deadline has
  passed, not the live entries. An employee with no submission for such a window gets one synthetic
  blocked entry per date of the window with reason `not_submitted`, so the solver never assigns
  them there. For a window still open, the solver reads the last snapshot (or, with none, treats the
  employee as not submitted); the manager sees this in the run's inputs. `validate` turns a
  `not_submitted` block into the warning `availability.not_submitted` for manual assignments, so a
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
  copper segment on a tan track; determinate (schedule runs) is a fill to elapsed / limit, polled
  from the job, with "best so far" text beside it. Under reduce-motion the indeterminate state is a
  still, half-filled bar with "Loading…" text for screen readers.
- **Rationale**: the owner's note, A9 (nothing under a second flashes), AS8's visible part.

## R16. Glossary, voice and locale parity

- **Decision**: `docs/ux/glossary.md` and `docs/ux/voice-and-tone.md` are the source for all copy; a
  Jest test compares `he.json` and `en.json` key sets and refuses empty values, values equal to their
  key, and the markers `TODO`, `TBD`, `FIXME`, `lorem`, `xxx`. A second test checks the glossary's
  terms are used for their keys (for example `nav.roster` is "Roster" / "סידור עבודה").
- **Rationale**: FR-075, FR-076, D13.
