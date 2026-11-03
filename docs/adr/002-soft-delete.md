# ADR 002: Soft Delete Strategy

**Date**: 2026-11-01
**Status**: Accepted

## Context

HDS needs to hide users, clients, and projects from normal queries without destroying audit history. Hard deletes would break foreign-key chains and lose historical data.

## Decision

All mutable domain entities use soft delete: `is_deleted BOOL NOT NULL DEFAULT false` + `deleted_at TIMESTAMPTZ`. Queries always include `WHERE is_deleted = false`. Partial unique indexes (`WHERE is_deleted = false`) preserve the uniqueness constraint for active records only (e.g., `uq_users_email_ci`, unique on `lower(email)`).

## Consequences

- **Pro**: Audit trail preserved; cascades are handled at service layer (e.g., deactivating client users when a client is deactivated).
- **Pro**: Accidental deletion is reversible by a DBA.
- **Con**: All repository queries must filter on `is_deleted`; enforced by `BaseRepository` and individual repo methods.
- **Con**: Partial indexes are PostgreSQL-specific; SQLite tests use a dialect-ignored `postgresql_where` arg on `Index`.

## Alternatives Considered

- **Archive tables**: Additional complexity; rejected for initial version.
- **Hard delete + event sourcing**: Over-engineered for current scale.
