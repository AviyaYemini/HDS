# Implementation Plan: UX foundation and redesign

**Branch**: `007-ux-redesign` | **Date**: 2026-11-07 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/007-ux-redesign/spec.md`, and the stored prompt
`.claude/hds/prompts/06-ux-redesign/plan.md`:

> The existing Expo app (expo-router, i18next, the generated API client). Group A's first task is a
> one-day Hebrew RTL spike on iOS, Android and web: a list, a form, tabs, a select, a switch, a bottom
> sheet and a date-range picker. Pick the component library (a Uniwind kit such as React Native
> Reusables or HeroUI Native, Tamagui, or Unistyles with HDS's own components) and the date picker
> (react-native-ui-datepicker or react-native-paper-dates) by its result, recording the choice in
> research.md and an ADR; until then the plan names the shortlist. Design tokens in DTCG 2025.10 JSON
> from the working plan's section 3, generated into the library's theme, with a unit test for WCAG
> contrast. Fonts: IBM Plex Sans and IBM Plex Sans Hebrew. FormatJS Intl polyfills on Hermes,
> @hebcal/core for Israeli holidays, Unicode isolates for mixed-direction text. API changes go
> contract-first in the 001 OpenAPI, then the client is regenerated, with Alembic migrations: user
> capabilities on /me, assignment start and end, availability submissions, managers' roster edits.
> Playwright specs for every group under apps/client/tests/e2e/ux/, in Hebrew and English, desktop and
> phone viewports, with axe-core; React Native Testing Library for units.
> Every task in tasks.md is marked [UI] (screens, components, styling, copy, the RTL spike and the
> preview stubs, built by a separate UI session) or [CORE] (contract, migrations, backend, regenerated
> client, tokens and their contrast test, CI, and each group's Playwright specs, written failing first
> before that group's [UI] tasks).

## Summary

HDS keeps its data and rules and changes their shape. The client (`apps/client`, one Expo app for
iOS, Android and web, ADR 010) is rebuilt on a design system generated from Option B's tokens, with
role shells driven by a capability map that the API serves on `GET /me` and enforces itself. Four
backend changes carry the Proposed rules: capabilities on `/me` (one map, shared by the UI and the
role checks); each assignment's own start and end, which pay, the rule checks and reports read;
availability submissions per window, with "not submitted by the deadline counts as unavailable"; and
roster edits opened to managers for the projects they manage. A handful of smaller additive endpoints
serve the new screens (submission tracker, publish preview, rule overrides, copy week, week templates,
saved views, the organisation's contact card, phone session length). The work ships in five pull
requests, groups A; B; C; D with E; F to H (working plan section 5), each group opening with its
failing Playwright specs ([CORE]) before its screens ([UI]).

## Technical Context

**Language/Version**: TypeScript 5 strict on Expo SDK (the app's pinned SDK), React Native with
Hermes, React Native Web; Python 3.12 for the API (unchanged).

**Primary Dependencies**:
- Existing: expo-router, i18next / react-i18next, `@hds/api-client` (openapi-typescript +
  openapi-fetch, generated), expo-secure-store, expo-localization; FastAPI, SQLAlchemy 2 async,
  Alembic, Pydantic 2.
- Component library: **shortlist until the group A spike decides** (research R1): a Uniwind kit
  (React Native Reusables or HeroUI Native), Tamagui, or Unistyles with HDS's own components.
- Date picker: **shortlist until the spike decides** (R2): react-native-ui-datepicker or
  react-native-paper-dates, with HDS's own presets.
- Named by the prompt: IBM Plex Sans and IBM Plex Sans Hebrew (400, 500, 600); FormatJS Intl
  polyfills (`@formatjs/intl-*` for he and en, Asia/Jerusalem); `@hebcal/core` (`il: true`); a
  bidi-isolate helper of HDS's own (FSI/LRI … PDI); `@axe-core/playwright`.
- Needed by the spec and not named in the prompt (research R9 lists them for review):
  `expo-local-authentication` (biometric unlock, FR-010), `expo-calendar` (add to calendar on
  phones, FR-023), `@react-native-async-storage/async-storage` (availability draft and last-loaded
  shifts offline, FR-019), `expo-font` for the Plex families. No others are added without an ADR.

**Storage**: PostgreSQL 16 (unchanged), four Alembic migrations (025 to 028, data-model.md). On the
device: secure store for tokens (as today), async storage for drafts and cached lists.

**Testing**: Playwright on the web build under `apps/client/tests/e2e/ux/` (one spec per group, he
and en, desktop and a 390 × 844 phone viewport, axe-core on every visited screen); Jest + React
Native Testing Library for units, including the token contrast test; pytest unit tests with fake
repositories, integration tests on hds_p3_test, Schemathesis from the 001 OpenAPI; Hypothesis for the
scheduling package's new inputs (assignment times, not-submitted blocks).

**Target Platform**: iOS and Android phones (Expo), and modern browsers on desktop and phone (React
Native Web).

**Project Type**: mobile + web client with an existing web API.

**Performance Goals**: The roster week for one site (about 7 days × 3–6 templates × up to 10 people)
renders and scrolls at 60 fps on a mid-range Android phone; a cell tap on the availability grid
updates the cell in under 100 ms; the first screen after sign-in shows content or skeletons within
1 s on the shared environment.

**Constraints**: WCAG 2.2 AA in Hebrew and English (FR-064); RTL from the first token; money only
from the server's Decimal strings (Constitution VII, D10); every string in the locale files
(Constitution VIII); previews off in production (FR-069); additive API changes only, so no `/api/v2`
(Constitution V).

**Scale/Scope**: About 35 screens across four roles plus 12 preview stubs; 69 functional
requirements; tens of users per organisation, as today.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-checked after Phase 1 design (below).*

| Principle | How 007 meets it |
| --- | --- |
| I. Spec as source of truth | Every Proposed item is marked in spec.md (managers' roster edits, assignment times, submissions, WCAG 2.2 AA, previews, and the new endpoints). Tenders and L1–L18 are out. |
| II. Layered backend | New routers only validate and delegate; capability resolution lives in `hds.core.capabilities` (pure) and `services/capabilities.py`; new repositories for submissions, overrides, templates, saved views. The import-graph check covers them. |
| III. Pure scheduling package | Assignment times and "not submitted" reach `hds.scheduling` only as plain dataclass fields (an assignment's own span; a synthesised blocked entry with reason `not_submitted`). The package gains no imports. |
| IV. Test-first | Each group's Playwright spec lands failing before its [UI] tasks; services get fake-repository unit tests; the scheduler's new inputs get Hypothesis properties; Schemathesis covers every new endpoint. |
| V. API-first | Every endpoint change is made first in `specs/001-foundation-auth-org/contracts/openapi.yaml`, then the server, then `pnpm api-client:generate`; the drift checks stay. All changes are additive (new optional fields, new endpoints, new 403/409/412 codes on new behaviour). |
| VI. Security | The capability map is enforced in services, not only in the UI; `/me` capabilities are advisory for the UI. Longer phone sessions keep rotating refresh tokens and reuse detection (ADR 006); access tokens stay ≤ 15 min. |
| VII. Money and time | Assignment times are UTC instants; pay uses them through the existing Decimal pipeline; the client never computes money, it formats the server's Decimal strings. |
| VIII. i18n | All new copy in `en.json` and `he.json`, gender-neutral Hebrew; the ESLint literal-text rule stays on; RTL is covered in every group's spec. |
| IX. Quality gates | Scoped `make ci-local` per group, `make ci-local-db` once at PR-ready; new CI wiring for the `ux/` specs and the contrast test is a [CORE] task. |
| X. Migrations | Four new reversible migrations; no applied migration is edited. |
| XI. Documentation | ADRs for the library choice, the capability map, assignment times, availability submissions and phone sessions; docstrings and endpoint summaries throughout. |

No violations; Complexity Tracking is empty.

## Project Structure

### Documentation (this feature)

```text
specs/007-ux-redesign/
├── plan.md              # This file
├── research.md          # Phase 0: R1–R13
├── data-model.md        # Phase 1: migrations 025–028 and the client-side models
├── quickstart.md        # Phase 1: how to see each group working
├── contracts/
│   ├── api-changes.md   # every OpenAPI change, made first in the 001 contract
│   ├── capabilities.md  # the capability map: names, roles, scoping
│   └── ui-routes.md     # routes, shells, tab sets and preview stubs per role
├── checklists/requirements.md
└── tasks.md             # Phase 2 (/speckit-tasks)
```

### Source Code (repository root)

```text
apps/api/src/hds/
├── core/capabilities.py          # NEW pure map: role (+ managed projects) → capabilities
├── core/deps.py                  # require_* expressed through the map
├── models/{availability_submission,rule_override,week_template,publish_event,saved_view}.py  # NEW
├── models/{shift,organisation,refresh_token}.py                                 # changed
├── repositories/…                # one per new model
├── services/{capabilities,availability_submission,rule_override,week_template,saved_view}.py
├── services/{roster,schedule_inputs,pay,reports,auth}.py                        # changed
├── routers/{me,availability,shifts,roster_extras,saved_views,organisation}.py
├── scheduling/                   # assignment span + not_submitted reason (pure)
└── payroll/                      # prices the assignment's own span
apps/api/alembic/versions/025_…028_…py

packages/api-client/src/schema.d.ts   # regenerated, never hand-edited

apps/client/
├── app/                          # expo-router: (auth), (staff), (client), dev/gallery, previews
├── src/design/tokens/*.tokens.json   # DTCG 2025.10 source (base, semantic, component)
├── src/design/theme/             # generated for the chosen library
├── src/design/components/        # primitives with every state (X14)
├── src/capabilities/             # reads /me, gates routes, tabs, buttons
├── src/locale/                   # polyfills, bidi isolate, dates, money, holidays
├── src/screens/{my-work,roster,runs,admin,reports,client,account,previews}/
└── tests/{unit,e2e/ux}/          # Jest + RNTL; Playwright a–h specs
docs/adr/013-…017-….md
```

**Structure Decision**: Keep the existing two apps (`apps/api`, `apps/client`) and the generated
`packages/api-client`. Old `(employee)` routes are replaced group by group; the old e2e specs a group
replaces are updated or removed in the same group (working plan section 5).

## Delivery

| PR | Groups | [CORE] first | [UI] then |
| --- | --- | --- | --- |
| 1 | A | `a-design-system.spec.ts`; tokens JSON + contrast test; gallery route switch; CI for `ux/` | RTL spike + ADR, theme generation, primitives, date picker, locale helpers |
| 2 | B | `b-roles.spec.ts`; capability map + `/me`; phone sessions; organisation contact; previews switch | shells, homes, refusal page, account screen, contact card, preview stubs |
| 3 | C | `c-my-work.spec.ts`; availability submissions, not-submitted rule in scheduling | availability week, my shifts, my pay |
| 4 | D, E | `d-roster.spec.ts`, `e-schedule-runs.spec.ts`; assignment times through pay and rules; managers' edits; overrides; publish preview; copy week; templates; submission tracker; If-Match | roster web and phone, publish review, schedule runs |
| 5 | F, G, H | `f-admin.spec.ts`, `g-reports.spec.ts`, `h-a11y-sweep.spec.ts`; saved views; report key figures if missing | admin tables and forms, reports, accessibility pass, statement page |

Within a group: [CORE] specs and API → [UI] → close (scoped `make ci-local`, `/code-review high`, no
open ledger row, ≤ 3 risk mutants named up front, one commit). One group at a time on the branch.

## Post-design Constitution Check

Re-checked after data-model.md and contracts: the new tables sit behind repositories; the scheduling
package receives only dataclass fields; every API change in `contracts/api-changes.md` is additive;
money stays Decimal end to end. Still no violations.

## Complexity Tracking

None.
