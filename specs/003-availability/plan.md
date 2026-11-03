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
├── alembic/versions/019_expiry_indexes_retire_lock_columns.py   # new
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
