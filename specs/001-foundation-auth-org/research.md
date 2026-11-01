# Research: Foundation — Authentication and Organisation

**Date**: 2026-11-01 | **Branch**: `001-foundation-auth-org`

All technology choices were supplied directly by the user. This file records the rationale
for each choice and the key patterns adopted so downstream tasks have a stable reference.

---

## Decision: Password Hashing — Argon2id

**Decision**: Use `argon2-cffi` with the `argon2id` variant.

**Rationale**: Argon2id is the winner of the Password Hashing Competition (2015), resistant
to both side-channel (Argon2i) and GPU (Argon2d) attacks. It is the recommendation of OWASP
and the current standard for new systems. The `argon2-cffi` Python binding wraps the reference
C implementation and exposes a clean `PasswordHasher` API.

**Default parameters** (2026 baseline):
- `time_cost=2`, `memory_cost=65536` (64 MiB), `parallelism=2`
- Verify on every request; upgrade hash transparently on next successful login if parameters change.

**Alternatives considered**: bcrypt (still acceptable but no memory hardness), scrypt (OWASP
acceptable but less ecosystem support in Python), PBKDF2 (rejected: not memory-hard).

---

## Decision: JWT Strategy — Short-lived Access + Rotating Refresh

**Decision**: Issue a signed JWT access token (HS256 or RS256) with TTL ≤ 15 minutes.
Pair it with a rotating refresh token stored server-side (opaque, hashed in the DB).

**Rationale**: A short-lived access token limits the window of a stolen token. The refresh
token is opaque (a random UUID stored as a SHA-256 hash in `refresh_tokens`), so even DB
access does not expose usable tokens. Rotation on every refresh detects replay attacks
(refresh token reuse → revoke all tokens for that user).

**Key implementation rules**:
- The access token payload carries `sub` (user UUID), `org` (organisation UUID), `role`, `iat`, `exp`.
- Role is embedded in the token but the service layer **re-reads the live role from the DB**
  on every call (FR-004 + Clarification Q1). The token role is only used for the initial
  fast-path check; a DB miss overrides it.
- `refresh_tokens` table: `token_hash` (SHA-256, UNIQUE), `user_id`, `expires_at`, `revoked_at`.
  "Sign out of all devices" = set `revoked_at = now()` for every non-revoked row for the user.
- Refresh token TTL: 7 days (configurable via env var).

**Alternatives considered**: Stateless refresh tokens (rejected: cannot revoke without
blocklist); opaque session IDs (rejected: incompatible with future mobile/native clients).

---

## Decision: Role Enforcement — Service Layer, Not Middleware

**Decision**: Role and data-scope checks live in service methods, not in FastAPI middleware
or router dependencies alone.

**Rationale**: Constitution Principle VI requires checks in the service layer. Middleware
and dependency injectors provide a convenient first line of defence (e.g., `Depends(require_manager)`)
but are not sufficient — they operate on URL patterns, not on data ownership. A service
method can enforce "Employee may only access their own record" by comparing `current_user.id`
to the requested resource's `user_id`.

**Pattern**:
```
router dep: verify JWT, load User from DB (live role read)
service:    check role → check ownership/org scope → execute
repository: plain SQL / ORM, no auth logic
```

---

## Decision: Soft Delete Pattern

**Decision**: Rows are never hard-deleted. `is_deleted = true` hides them from all queries.
A unique partial index enforces business-uniqueness constraints only on non-deleted rows.

**Rationale**: Historical data (past rosters, billing, audit trails) must remain intact after
an employee or project is "deleted" by an Admin (spec Assumption, FR-016, FR-027). Hard
delete would cascade and destroy that history.

**Implementation**:
- Add `is_deleted BOOLEAN NOT NULL DEFAULT false` to `users`, `clients`, `projects`.
- All repository list/get queries add `WHERE is_deleted = false`.
- Unique constraints (e.g., email per org) use `CREATE UNIQUE INDEX ... WHERE is_deleted = false`.
- A separate `deleted_at TIMESTAMPTZ` column records when the record was hidden (for audit).

---

## Decision: Money Fields — NUMERIC(12,4), Never FLOAT

**Decision**: All monetary values use PostgreSQL `NUMERIC(12,4)` (12 significant digits,
4 decimal places). Pydantic schemas use Python `Decimal`. SQLAlchemy maps the column
with `Numeric(precision=12, scale=4, asdecimal=True)`.

**Rationale**: Constitution Principle VII. IEEE 754 floating-point cannot represent many
decimal fractions exactly, leading to rounding errors in financial calculations.
`NUMERIC` is exact arithmetic; `Decimal` in Python is exact arithmetic.

**Alternatives considered**: Integer pence/agorot (acceptable, but requires explicit
conversion throughout the call stack; `NUMERIC` is clearer).

---

## Decision: Account Lockout (Brute-Force Protection)

**Decision**: Track `failed_login_attempts` (SMALLINT) and `locked_until` (TIMESTAMPTZ)
on the `users` row. On each failed attempt, increment the counter. When counter ≥ 5,
set `locked_until = now() + interval '15 minutes'`. On success, reset both to zero.

**Rationale**: FR-006b. Column-level storage avoids a separate cache dependency at this
stage. A Redis-based rate limiter can replace this later without a schema change.

**Edge**: Lockout check happens before password verification to avoid timing leaks.

---

## Decision: SQLite for Unit Tests

**Decision**: Unit tests (services layer with fake repositories) do not use any database.
Integration tests use a real PostgreSQL 16 container (via `pytest-docker` or a pre-started
Compose service). SQLite is available as a dialect fallback if a developer wants a
dependency-free local run, but the integration suite always runs against PostgreSQL.

**Rationale**: SQLite lacks `NUMERIC` exactness, native `UUID` type, and `TIMESTAMPTZ`
semantics. Using it only for truly unit-isolated tests (where repositories are mocked)
prevents false positives.

---

## Decision: Monorepo Layout

**Decision**: `apps/api` for the Python backend, `apps/client` (deferred) for the
TypeScript/Expo frontend. `packages/api-client` (deferred) for the generated TypeScript
client. `infra/` for Docker/Helm. `uv` manages Python with `uv.lock`; `pnpm` manages
JS/TS with `pnpm-lock.yaml`.

**Rationale**: spec §7 (repository layout). Single repo simplifies cross-layer PR reviews
and keeps the generated TS client in sync with the backend (Constitution Principle V).

---

## Patterns Adopted

| Pattern | Used for |
|---------|----------|
| Repository pattern | All DB access isolated in `repositories/`; services use injected repo objects (test-mockable) |
| Dependency injection (FastAPI `Depends`) | DB session, current user, role guards |
| Partial unique index | Soft-delete-safe uniqueness (email per org, group name per org, etc.) |
| RFC 9457 problem-details | All API error responses; stable machine-readable `code` field |
| `Idempotency-Key` header | POST endpoints where retries are safe (spec API conventions) |
| Cursor pagination | Employee and project list endpoints (spec API conventions) |
| `TIMESTAMPTZ` + `ZoneInfo` | All timestamps stored UTC; site/org timezone for display only |
