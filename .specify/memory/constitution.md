<!--
SYNC IMPACT REPORT — remove before committing
==============================================
Version change:    (none) → 1.0.0
Added sections:    Core Principles (10), Engineering Standards, Governance
Removed sections:  n/a (initial creation)
Modified principles: n/a
Deferred TODOs:    none — all placeholders resolved from user input and docs/hds-spec.md
-->

# HDS Constitution

## Core Principles

### I. Spec as Source of Truth

The original HDS product spec (`docs/hds-spec.md`) is the authoritative scope boundary.
Every requirement MUST trace to it. Anything beyond the original spec MUST be labelled
**Proposed** and can be cut without justification.

*Testable:* every feature ticket cites a spec reference or carries a `Proposed` label;
PR template requires a spec-reference field.

### II. Layered Backend

The backend MUST follow a strict four-layer dependency rule:
**routers/schemas → services → repositories → models**.

- Routers MUST contain no business logic; they validate input and delegate to services.
- Repositories are the only code that reads from or writes to the database.
- Services MUST NOT import from routers; repositories MUST NOT import from services.

*Testable:* mypy import-graph check; services are unit-tested with fake repositories.

### III. Pure Scheduling Package

The `scheduling` package is a pure function. Given employees, constraints, and shift slots,
it returns an assignment set and unfilled-slot reasons. It MUST:

- import nothing from any other application package,
- make no database calls and no HTTP calls,
- be the only place scheduling rules live; no rule exists outside it.

*Testable:* the package can be imported in a bare Python environment with no app
dependencies; property-based tests (Hypothesis) confirm no hard constraint is violated on
random valid input.

### IV. Test-First

No feature is merged without tests covering:

- **Services**: unit tests with fake repositories.
- **Scheduler**: property-based tests (Hypothesis) for every hard constraint.
- **API**: Schemathesis contract tests generated from the OpenAPI file.
- **User journeys**: one Playwright smoke test per journey.

CI MUST block merges on any test failure.

### V. API-First

The OpenAPI document is the single contract between backend and clients.

- The TypeScript client MUST be generated from the OpenAPI file; hand-written fetch code
  is forbidden.
- CI MUST regenerate the client and fail if the committed copy differs.
- Breaking changes ship under a new `/api/v2` prefix; additive changes do not.

### VI. Security Non-Negotiables

- Passwords MUST be hashed with Argon2id; no other algorithm is permitted.
- Auth MUST use short-lived JWT access tokens (≤ 15 min) plus rotating refresh tokens.
- Every endpoint MUST enforce the role matrix (Employee, Manager, Admin, Client) in the
  service layer, not only in the UI.
- Secrets MUST come from the environment or HashiCorp Vault; they MUST never be committed.

*Testable:* dependency scan blocks high-severity CVEs; auth tests verify every role boundary.

### VII. Money and Time Invariants

- Monetary values MUST be stored and computed as `Decimal`; `float` is forbidden for money.
- Timestamps MUST be stored in UTC and converted to the organisation's timezone for display.
- Times and rates MUST be positive; cash flow is the only value permitted to be negative.

*Testable:* mypy type alias prevents `float` in money fields; unit tests verify timezone
round-trips and sign-rule enforcement.

### VIII. Internationalisation from Day One

- All user-facing strings MUST be externalised into translation files before the PR that
  introduces them is merged; hardcoded UI text is forbidden.
- Layouts MUST be RTL-safe (Hebrew first, English second).

*Testable:* ESLint rule flags string literals in JSX; CI validates no new hardcoded strings;
Playwright includes an RTL smoke test.

### IX. Quality Gates

Every pull request MUST pass all of the following before merge:

- `ruff` (lint and format), `mypy --strict`, and the full test suite (Python and TypeScript).
- Migration check: `alembic check` detects no diff between models and migrations.
- PRs MUST be small and focused; commits MUST follow the Conventional Commits specification.

CI is the authority; local pre-commit hooks mirror CI but do not substitute for it.

### X. Migrations as the Only Schema Path

Every database schema change MUST be delivered as a versioned, reversible Alembic migration.

- Manual `ALTER TABLE` statements in any environment are forbidden.
- An applied migration is never edited; a destructive change ships in two releases
  (stop using the column, then drop it).

*Testable:* CI runs `alembic check` and fails if models and migrations diverge.

## Engineering Standards

Development workflow follows `docs/hds-spec.md` sections 9–10: trunk-based branching,
squash merge, `main` always deployable, short-lived feature branches, and pull requests for
every change. The definition of done requires tests added, types clean, strings externalised,
accessibility labels present, and migrations reversible.

## Governance

This constitution supersedes all other stated practices. An amendment requires:

1. A pull request to `.specify/memory/constitution.md` with a written rationale.
2. Agreement from at least one other contributor (or sole-maintainer sign-off when alone).
3. A version bump per the policy below and an updated `Last Amended` date.

**Versioning policy**
- MAJOR: backward-incompatible removal or redefinition of a principle.
- MINOR: new principle or materially expanded guidance.
- PATCH: clarifications, wording, or typo fixes.

**Compliance review**: each PR checklist MUST include a line confirming no principle is
violated by the change.

**Version**: 1.0.0 | **Ratified**: 2026-11-01 | **Last Amended**: 2026-11-01
