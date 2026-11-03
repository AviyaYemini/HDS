# Implementation Plan: Availability (Part 1: leftovers from 002)

**Branch**: `003-availability` | **Date**: 2026-11-03 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/003-availability/spec.md`, Part 1 only. Part 2
(availability and manual shifts) is planned later and appended here.

## Summary

Finish four items left over from 002. Clients get an editable contract start date, with the date
order judged on the pair after the change. The five PATCH request models refuse unknown fields. The
Client project path reads through a repository method that cannot mean "every project". The two
unreachable promote service methods go. A new reversible migration, 019, indexes `expires_at` on
`refresh_tokens` and `invitation_tokens` and drops `users.failed_login_attempts` and
`users.locked_until`. The 001 OpenAPI contract follows the schema changes.

## Technical Context

**Language/Version**: Python 3.12+ (the project's `requires-python`)

**Primary Dependencies**: FastAPI, Pydantic v2, SQLAlchemy 2 (async), Alembic, as in 001 and 002

**Storage**: PostgreSQL 16

**Testing**: pytest on PostgreSQL for integration and concurrency, SQLite-free unit tests with fakes,
Hypothesis where useful, Schemathesis through `tests/contract` and `scripts/contract_cli.sh`

**Target Platform**: Linux server (Docker for local development)

**Project Type**: web service (`apps/api`)

**Performance Goals**: SC-003: the bulk clean-up's work grows with the expired records, not the total

**Constraints**: no behaviour change outside FR-001 to FR-005 (FR-010); migration 019 reversible

**Scale/Scope**: five PATCH models, two services, two repositories, one migration, one contract

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-checked after Phase 1 design.*

| Principle | Check | Result |
| --- | --- | --- |
| I. Spec as source of truth | Part 1 items are labelled Proposed; ORG-2 (clients) covers the start date | Pass |
| II. Layered backend | Date rule in the client service; the Client project read in a repository; routers unchanged in role | Pass |
| III. Pure scheduling package | Not touched | Pass |
| IV. Test-first | Each item gets a failing test first; removals keep their rules covered on the reachable path | Pass |
| V. API-first | Contract updated in the same change: `contract_start` on the client PATCH, `additionalProperties: false` on four PATCH schemas. Refusing unknown fields is stricter on `/api/v1`; the one-off exception recorded in 002's plan (no release, no client yet) covers it | Pass, noted |
| VI. Security | Unchanged; closes a "no filter" hazard on the Client path | Pass |
| VII. Money and time | Dates stay dates; no money change | Pass |
| VIII. Internationalisation | No new error codes; existing ones keep `en` and `he` text | Pass |
| IX. Quality gates | All gates required; `alembic check` covers 019 | Pass |
| X. Migrations only | 019 is new and reversible. Dropping two columns is destructive: 002 stopped using them and nothing is deployed, which is the two-step rule's purpose. The downgrade restores the columns empty | Pass, one noted limit |
| XI. Documentation | Docstrings, migration docstring, contract, `docs/hds-api.md` and the README configuration table where touched | Pass |

**Post-design re-check**: the same result.

## Project Structure

### Documentation (this feature)

```text
specs/003-availability/
├── plan.md              # This file
├── research.md          # Part 1 decisions E1 to E6
├── data-model.md        # Part 1 schema changes
├── quickstart.md        # Part 1 validation
├── contracts/
│   └── part1-api-changes.md
└── tasks.md             # /speckit-tasks
```

### Source Code (repository root)

```text
apps/api/
├── alembic/versions/019_expiry_index_retire_locks.py   # new
├── src/hds/
│   ├── core/types.py                 # a shared "refuse unknown fields" base for PATCH models
│   ├── models/{user,refresh_token,invitation_token}.py
│   ├── repositories/project.py       # list_by_client gains the status filter
│   ├── routers/{clients,employees,projects,sites,me}.py
│   └── services/{client,employee,project}.py
└── tests/
    ├── integration/test_client_rules.py, test_patch_unknown_fields.py, test_migration_019.py
    └── unit/...
specs/001-foundation-auth-org/contracts/openapi.yaml
```

**Structure Decision**: the existing `apps/api` layout; no new package.

## Phases

One phase, P7, in groups that each end with all gates green and a commit:

- **Group A**: contract start editing (FR-001 to FR-004).
- **Group B**: unknown fields refused on the PATCH models (FR-005), and the contract.
- **Group C**: the Client project path and the unreachable promote methods (FR-006, FR-007).
- **Group D**: migration 019 (FR-008, FR-009), its tests, and the code that still names the columns.
- **Checkpoint**: the full merge gate, as at the end of 002.

## Complexity Tracking

None: no principle is broken.

---

# Implementation Plan: Part 2, availability and manual shifts

**Input**: spec.md Part 2 (FR-101 to FR-125, SC-101 to SC-106) and the stored prompt
`.claude/hds/prompts/02-availability/plan.md`: "Extend the existing backend with availability, shift
template, shift and assignment modules in the same layering. Rule checks (overlap, minimum rest, weekly
hours) live in a pure validation function inside the scheduling package so the generator can reuse them
later. Add the Expo (React Native + React Native Web) TypeScript app in apps/mobile with the generated
API client, employee screens first: my availability and my shifts. i18n with English strings and an
RTL-ready layout."

**Owner decision (2026-11-03)**: the app lives in `apps/client`, as docs/hds-spec.md section 7 and the
README name it; the prompt's "apps/mobile" is read as this app. The existing placeholder (locale files,
skipped Playwright tests) is kept and grown.

## Summary (Part 2)

The API gains five modules in the existing layering (routers → services → repositories → models):
availability entries, submission windows, shift types (the prompt's "shift templates"), shifts and
assignments. Every warning rule lives in a new pure package, `hds.scheduling`, as one validation
function that the generator will call later. The Expo app in `apps/client` ships the employee side
first: sign-in, my availability and my shifts, through a client generated from the OpenAPI document,
with every string externalised and an RTL-safe layout. Manager and Admin screens for windows, shift
types, the roster and publishing are API-only in this part.

## Technical Context (Part 2)

**Language/Version**: Python 3.12+ (API); TypeScript 5, strict (client)

**Primary Dependencies**: API as in Part 1. Client: Expo (React Native + React Native Web) with Expo
Router, `openapi-typescript` and `openapi-fetch` for the generated client, `i18next` and `react-i18next`,
`expo-secure-store` (exact versions pinned at scaffold time, research R9)

**Storage**: PostgreSQL 16; migration 020; the `btree_gist` extension for the no-overlap rule on windows

**Testing**: API as in Part 1, plus Hypothesis property tests on the pure rules. Client: Jest with React
Native Testing Library (at least 70% line coverage on logic), Playwright on the web build with an RTL
smoke test (docs/hds-spec.md testing table)

**Target Platform**: Linux server (API); iOS, Android and web from one codebase (client)

**Project Type**: web service and cross-platform client in the existing monorepo

**Performance Goals**: SC-101 and SC-102 are user-time goals; a single assignment with its warnings
answers in well under a second for an employee with a year of shifts (warnings read only the shifts
near the one being changed, research R3)

**Constraints**: warnings never refuse (FR-116); every visibility rule in the service layer (FR-125);
times stored in UTC and computed in the site's time zone (Constitution VII); Node.js 22 LTS and pnpm
are needed for the client and are not installed on the development machine yet (research R10)

**Scale/Scope**: about 30 new operations, 6 tables, one pure package, one client app with 3 screens

## Constitution Check (Part 2)

| Principle | Check | Result |
| --- | --- | --- |
| I. Spec as source of truth | Every FR traces to AVL-1 to AVL-4, ORG-4/5, SCH-3/4, EMP-1, FIN-1; SCH-4 and ORG-5 are Proposed in the original list | Pass |
| II. Layered backend | New routers, services, repositories and models only; services load shifts, the pure package judges them | Pass |
| III. Pure scheduling package | `hds.scheduling` imports only the standard library (`zoneinfo` included); every warning rule lives there, not only overlap, rest and weekly hours, because "no rule exists outside it" | Pass |
| IV. Test-first | Each group starts with failing tests; property tests for the rules; mutation checks as in Part 1 | Pass |
| V. API-first | The contract is updated with each group; the client is generated from it into `packages/api-client`, and CI regenerates it and fails on a difference | Pass |
| VI. Security | Role and organisation checks in services; an allowed and a forbidden test per operation; the employee shift view has no co-worker fields at all (research R7) | Pass |
| VII. Money and time | Shift pay rate is `Decimal`, positive; shift times `timestamptz` in UTC, local times computed with the site's zone; DST handled by R4 | Pass |
| VIII. Internationalisation | API messages and warning texts in `en` and `he`, as today; client strings externalised from the start with an ESLint literal-string rule; layout RTL-safe with an RTL smoke test. The prompt asks for English strings: client Hebrew translations are a follow-up, which VIII allows because it requires externalising, not translating | Pass, noted |
| IX. Quality gates | API gates unchanged; CI gains client jobs: ESLint, `tsc --noEmit`, Jest with coverage, Playwright web with RTL, and the generated-client drift check | Pass |
| X. Migrations only | Migration 020, reversible; no edit to an applied migration | Pass |
| XI. Documentation | Docstrings, migration docstring, `docs/hds-api.md`, the README (client setup), and the contract | Pass |

**Post-design re-check**: the same result.

## Project Structure (Part 2)

```text
apps/api/
├── alembic/versions/020_availability_shifts.py
├── src/hds/
│   ├── scheduling/                 # new, pure: rules.py (validate), types.py (inputs, warnings)
│   ├── models/{availability,submission_window,shift_type,shift,assignment}.py
│   ├── repositories/{availability,submission_window,shift_type,shift,assignment}.py
│   ├── services/{availability,shift_type,roster}.py
│   ├── routers/{availability,shift_types,shifts,me_shifts}.py
│   └── locales/{en,he}.json        # new error and warning codes
└── tests/{unit,integration,concurrency,property}/...
packages/api-client/                # generated TypeScript types and client
apps/client/                        # Expo app: app/ (routes), src/{api,i18n,screens,components}, tests/
```

**Structure Decision**: the existing monorepo layout of docs/hds-spec.md section 7.

## Phases (Part 2)

Each group: failing tests first, then code, contract, mutation checks, gates, /hds-review and
/code-review high, log, commit.

- **P8, foundations**: the pure `hds.scheduling` rules with property tests; migration 020 and the models
  (organisation rest and weekly settings, shift types with the three defaults, project shift types,
  windows, entries, shifts, assignments).
- **P9, availability**: windows (FR-106), entries with the window rule and the absence exception
  (FR-101 to FR-105a), Admin edits (FR-107), visibility (FR-108, FR-109).
- **P10, roster**: shift types and project shift types (FR-110 to FR-112); shifts (FR-113, FR-114,
  FR-119); assignments with warnings from the pure rules (FR-115 to FR-118).
- **P11, publishing and visibility**: publish (FR-121), drafts hidden (FR-122), the employee's own
  shifts (FR-123), Clients refused (FR-124), the cross-role leak sweep (SC-104).
- **P12, client**: scaffold `apps/client`, the generated client and its CI drift check, i18n and RTL,
  sign-in, my availability, my shifts, Jest and Playwright with the RTL smoke test.
- **Checkpoint P13**: the full merge gate plus the client gates.

## Complexity Tracking (Part 2)

None.
