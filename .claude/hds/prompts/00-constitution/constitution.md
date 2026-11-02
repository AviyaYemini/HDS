Create principles for HDS 2.0, a shift scheduling and workforce management app for a security company (web first, native phone apps later). Source documents: docs/hds-spec.md (sections 9 and 10 are the basis) and docs/hds-api.md.

Principles to encode, each testable:
1. Original product spec is the source of truth for scope; anything beyond it is marked Proposed and can be cut.
2. Layered backend: routers/schemas -> services -> repositories -> models. Routers hold no business logic; repositories are the only code that touches the database.
3. The scheduling package is a pure function: no database, no HTTP, imports no other app package. It is the only place scheduling rules live.
4. Test-first: unit tests for services, property-based tests (Hypothesis) for the scheduler, API contract tests, and a Playwright smoke test per user journey. No feature is done without tests.
5. API-first: the OpenAPI document is the contract; the TypeScript client is generated from it, never hand-written.
6. Security: Argon2id password hashing, short-lived JWT plus rotating refresh tokens, role checks on every endpoint (Employee, Manager, Admin, Client), secrets only from environment or Vault, never committed.
7. Money is a decimal, never a float. Times are stored in UTC and shown in the organisation's timezone. Times and rates must be positive; only cash flow may be negative.
8. All user-facing strings are externalised (i18n from day one) with RTL-safe layouts.
9. Quality gates in CI: ruff, mypy strict, tests, and a migration check must pass before merge. Small PRs, Conventional Commits.
10. Every schema change is an Alembic migration; no manual database edits.
Keep the constitution under two pages. Use exactly these ten principles; do not add or invent others.
