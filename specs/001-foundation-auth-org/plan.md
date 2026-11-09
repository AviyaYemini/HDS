# Implementation Plan: Foundation — Authentication and Organisation

**Branch**: `001-foundation-auth-org` | **Date**: 2026-11-01 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/001-foundation-auth-org/spec.md`

## Summary

Build the authentication and organisation-management foundation of HDS 2.0. Users sign in
with email + Argon2id-hashed passwords and receive a short-lived JWT access token plus a
rotating refresh token. Every subsequent request carries the access token; the backend
re-checks the live role on each call. Four roles (Employee, Manager, Admin, Client) gate all
endpoints in the service layer. Managers maintain employees (with groups and qualifications),
clients, sites and projects. Employees self-serve their own profile. Clients read their own
projects. All responses follow RFC 9457 problem-details; all strings are externalised; all
layouts are RTL-safe.

## Technical Context

**Language/Version**: Python 3.12

**Primary Dependencies**:
- FastAPI (HTTP layer, OpenAPI generation)
- Pydantic v2 (request/response schemas, validation)
- SQLAlchemy 2.0 ORM + Alembic (models, migrations)
- argon2-cffi (Argon2id password hashing)
- PyJWT (JWT signing/verification; replaced python-jose, see ADR 004)
- PostgreSQL 16 (production); SQLite (unit tests only, via SQLAlchemy dialect switch)

**Monorepo tooling**: uv (Python dependency/venv management), pnpm workspaces

**Storage**: PostgreSQL 16 — UUID primary keys, `NUMERIC(12,4)` for money, `TIMESTAMPTZ`
for all timestamps (stored UTC, rendered in org timezone), native ENUMs for role/type/status.

**Testing**:
- pytest + pytest-asyncio — all layers
- Hypothesis — property tests for role-boundary logic (services layer)
- Schemathesis — API contract tests driven from the generated OpenAPI file
- Playwright — one RTL smoke test per user journey (LTR + RTL passes)
- Integration tests run against a real PostgreSQL container, not SQLite

**Target Platform**: Linux server inside Docker; Docker Compose for local development

**Performance Goals**: API p95 < 300 ms for reads (spec §9); sign-in round-trip < 3 s
(SC-001)

**Constraints**:
- JWT access token TTL ≤ 15 min; refresh token rotates on each use
- mypy --strict enforced; import-graph check blocks layer violations (Principle II)
- Every PR must pass ruff, mypy, tests, and `alembic check` (Principle IX)
- No float for money fields anywhere in the call stack (Principle VII)
- All user-facing strings externalised before merge; RTL layouts required (Principle VIII)

**Scale/Scope**: ~50–200 employees, single organisation per deployment for now; data model
carries `organisation_id` on every table for future multi-tenancy (spec §6)

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Status | Notes |
|-----------|--------|-------|
| I. Spec as Source of Truth | PASS | All 33 FRs trace to spec §2 / §4 AUTH-* / ORG-* rows. FR-006 (invitations) marked *Proposed*. |
| II. Layered Backend | PASS | routers/schemas → services → repositories → models enforced by the AST import-graph check (T107), which also forbids query constructs and `.execute(` in routers/services; no DB access in routers or services. |
| III. Pure Scheduling Package | N/A | Scheduling package not introduced in this feature. |
| IV. Test-First | PASS | Unit tests for services (fake repos); Schemathesis contract tests; for this backend-only feature, API-level integration tests serve as user-journey tests. Six Playwright smoke tests are scaffolded as `test.skip`; they are enabled when the `apps/client` feature delivers the frontend and MUST pass before that feature merges. |
| V. API-First | PASS | OpenAPI contract produced in Phase 1; TypeScript client generation deferred to `apps/client` feature. |
| VI. Security | PASS | Argon2id, JWT ≤ 15 min + rotating refresh, role check on every endpoint in service layer, account lockout after 5 failures (FR-006b). |
| VII. Money and Time Invariants | PASS | `billing_rate` and `default_hourly_rate` stored as `NUMERIC(12,4)`; all timestamps `TIMESTAMPTZ` UTC; positive constraint enforced. |
| VIII. i18n from Day One | PASS | FR-029 (externalised strings), FR-030 (RTL layouts), SC-007 (Playwright RTL smoke test) in spec. |
| IX. Quality Gates | PASS | ruff + mypy strict + tests + `alembic check` block merges. Conventional Commits. |
| X. Migrations Only | PASS | Alembic manages all schema changes; no manual DDL permitted. |

**Complexity Tracking** (accepted, time-boxed deviations):

| Principle | Deviation | Why accepted | Exit criterion |
|-----------|-----------|--------------|----------------|
| IV | The six Playwright smoke tests are `test.skip` scaffolds | This feature is API-only; there is no UI to drive | `apps/client` MUST enable all six (LTR and RTL passes) before that feature merges |
| V | No CI job regenerates the TypeScript client | No client exists yet (`packages/api-client` is a stub) | **Prerequisite for the client feature:** before `apps/client` merges, CI MUST regenerate the TypeScript client from the OpenAPI document and fail if the committed copy differs (Constitution V), and the contract's operation IDs MUST first match the generated ones (see the note at the end of `tasks.md`). Until then CI diffs the committed `contracts/openapi.yaml` against the app's generated OpenAPI (T124) |

## Project Structure

### Documentation (this feature)

```text
specs/001-foundation-auth-org/
├── plan.md              # This file
├── research.md          # Phase 0 — stack decisions and patterns
├── data-model.md        # Phase 1 — entity/table design
├── contracts/
│   └── openapi.yaml     # Phase 1 — API contract
├── quickstart.md        # Phase 1 — local validation guide
└── tasks.md             # Phase 2 — created by /speckit-tasks
```

### Source Code (repository root)

```text
apps/
├── api/                         # Python backend
│   ├── pyproject.toml           # uv-managed; ruff + mypy config here
│   ├── alembic.ini
│   ├── alembic/
│   │   └── versions/
│   ├── src/
│   │   └── hds/
│   │       ├── core/            # Config, security helpers, DB session, deps
│   │       ├── models/          # SQLAlchemy ORM models (Organisation layer)
│   │       ├── repositories/    # DB access — the ONLY layer touching the DB
│   │       ├── services/        # Business logic, role enforcement
│   │       ├── routers/         # FastAPI routers (HTTP + Pydantic schemas only)
│   │       ├── locales/         # he.json / en.json message catalogues
│   │       ├── scripts/         # seed_dev and OpenAPI export
│   │       ├── tools/           # check_imports (layering enforcement)
│   │       └── main.py          # App factory
│   └── tests/
│       ├── unit/                # Services with fake repositories
│       ├── integration/         # Real PostgreSQL container
│       └── contract/            # Schemathesis driven from openapi.yaml
└── client/                      # TypeScript / Expo — deferred to next feature

packages/
└── api-client/                  # Generated TS client — deferred

infra/
├── docker-compose.yml           # Local dev: API + PostgreSQL + (later) Redis
├── Dockerfile.api
└── helm/                        # Production Kubernetes (later)

docs/
├── hds-spec.md
├── hds-api.md
└── adr/                         # Architecture Decision Records
```

**Structure Decision**: Monorepo with `apps/api` as the backend root. `uv` manages the
Python venv and lock file (`uv.lock`). `pnpm` workspaces own `apps/client` and
`packages/api-client` (deferred). `infra/docker-compose.yml` wires the API container to
PostgreSQL 16.
