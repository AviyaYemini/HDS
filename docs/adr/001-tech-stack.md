# ADR 001: Technology Stack — Foundation

**Date**: 2026-11-01 | **Status**: Accepted | **Feature**: `001-foundation-auth-org`

## Context

HDS 2.0 needs a production-ready backend for shift scheduling and workforce management.
The stack must support async database access, strict typing, OpenAPI-first development,
and exact decimal arithmetic for billing.

## Decisions

### Language: Python 3.12
Modern Python with improved performance and full `asyncio` support.

### HTTP framework: FastAPI
Automatic OpenAPI generation, native Pydantic v2 integration, async-first. Chosen over
Django REST Framework (heavier) and Flask (no built-in OpenAPI).

### Validation: Pydantic v2
Significantly faster than v1; native `Decimal` and `UUID` support.

### ORM + Migrations: SQLAlchemy 2.0 + Alembic
SQLAlchemy 2.0 async style with `AsyncSession`; Alembic for versioned, reversible schema
changes (Constitution Principle X). No raw `ALTER TABLE` permitted.

### Database: PostgreSQL 16
`NUMERIC(12,4)` for money (Constitution Principle VII), `TIMESTAMPTZ` for timestamps,
native UUID and ENUM types, partial unique indexes for soft-delete uniqueness constraints.

### Password hashing: argon2-cffi (Argon2id)
Winner of the Password Hashing Competition; resistant to GPU and side-channel attacks.
Parameters: `time_cost=2`, `memory_cost=65536` (64 MiB), `parallelism=2`.

### Auth: python-jose (JWT HS256) + rotating opaque refresh tokens
Access token TTL ≤ 15 minutes; refresh token stored as SHA-256 hash, rotated on each use.
Replay detection: any reuse of a revoked refresh token revokes all tokens for that user.

### Monorepo tooling: uv (Python) + pnpm workspaces (TypeScript)
`uv.lock` and `pnpm-lock.yaml` committed for reproducible builds.

### Local development: Docker Compose
PostgreSQL 16 + API container wired via environment variables; no credentials committed.

## Alternatives Rejected

- **bcrypt**: Acceptable but lacks memory-hardness; Argon2id is OWASP-preferred.
- **Stateless refresh tokens**: Cannot revoke without a blocklist; server-side hash chosen.
- **float for money**: Rejected outright — IEEE 754 rounding errors in billing (Principle VII).
- **SQLite for integration tests**: Lacks `NUMERIC` exactness and `TIMESTAMPTZ`; used only
  in unit tests with mocked repositories.
