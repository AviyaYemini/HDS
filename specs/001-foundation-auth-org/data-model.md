# Data Model: Foundation — Authentication and Organisation

**Branch**: `001-foundation-auth-org` | **Date**: 2026-11-01

All tables carry `id UUID PRIMARY KEY DEFAULT gen_random_uuid()`, `created_at TIMESTAMPTZ
NOT NULL DEFAULT now()`, and `updated_at TIMESTAMPTZ NOT NULL DEFAULT now()`.
All tables (except `organisations`) carry `organization_id UUID NOT NULL REFERENCES
organisations(id)` for future multi-tenancy.

Soft-delete tables carry `is_deleted BOOLEAN NOT NULL DEFAULT false` and `deleted_at
TIMESTAMPTZ NULL`. Unique constraints on soft-deleted tables use **partial unique indexes**
(`WHERE is_deleted = false`) so the same email can be re-used after deletion.

Money columns use `NUMERIC(12,4)` — never `FLOAT` (Constitution Principle VII).
All timestamps are `TIMESTAMPTZ` stored in UTC.

---

## Table: `organisations`

Top-level container. Every other record belongs to one organisation.

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| `id` | `UUID` | PK, default `gen_random_uuid()` | |
| `name` | `VARCHAR(255)` | NOT NULL | |
| `timezone` | `VARCHAR(64)` | NOT NULL | IANA tz, e.g. `"Asia/Jerusalem"` |
| `created_at` | `TIMESTAMPTZ` | NOT NULL, default `now()` | |
| `updated_at` | `TIMESTAMPTZ` | NOT NULL, default `now()` | |

---

## Table: `users`

Single sign-in identity for all four roles. Employees (role ∈ `{employee, manager, admin}`)
use this record as their workforce profile. Client users (role = `client`) are linked to a
`clients` row.

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| `id` | `UUID` | PK | |
| `organization_id` | `UUID` | NOT NULL, FK → `organisations.id` | |
| `email` | `VARCHAR(320)` | NOT NULL | Unique across all organisations (partial index below) |
| `password_hash` | `VARCHAR(255)` | NOT NULL | Argon2id; never store plaintext |
| `full_name` | `VARCHAR(255)` | NOT NULL | |
| `phone` | `VARCHAR(32)` | NULL | |
| `role` | `user_role` ENUM | NOT NULL | `employee \| manager \| admin \| client` |
| `is_active` | `BOOLEAN` | NOT NULL, default `true` | Deactivated → cannot sign in |
| `is_deleted` | `BOOLEAN` | NOT NULL, default `false` | Soft delete (Admin only) |
| `deleted_at` | `TIMESTAMPTZ` | NULL | Set when `is_deleted` flips to true |
| `default_hourly_rate` | `NUMERIC(12,4)` | NULL, CHECK > 0 | Employees/managers only; NULL for client users |
| `client_id` | `UUID` | NULL, FK → `clients.id` | Non-null only when `role = 'client'` |
| `failed_login_attempts` | `SMALLINT` | NOT NULL, default `0` | Reset on successful login |
| `locked_until` | `TIMESTAMPTZ` | NULL | Null = not locked; past = lock expired |
| `created_at` | `TIMESTAMPTZ` | NOT NULL, default `now()` | |
| `updated_at` | `TIMESTAMPTZ` | NOT NULL, default `now()` | |

**Indexes**:
```sql
-- Email unique across ALL organisations (sign-in looks up by email), excluding soft-deleted rows
CREATE UNIQUE INDEX uq_users_email
    ON users (email)
    WHERE is_deleted = false;

-- Lookup by org (list queries)
CREATE INDEX idx_users_org ON users (organization_id);

-- Lookup client users by client
CREATE INDEX idx_users_client ON users (client_id) WHERE client_id IS NOT NULL;
```

**Invariants**:
- `role = 'client'` ↔ `client_id IS NOT NULL`; all other roles ↔ `client_id IS NULL`.
- `default_hourly_rate IS NULL` for client-role users.

---

## Table: `refresh_tokens`

Opaque rotating refresh tokens. The actual token value is never stored; only its SHA-256
hash is kept.

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| `id` | `UUID` | PK | |
| `user_id` | `UUID` | NOT NULL, FK → `users.id` | |
| `token_hash` | `VARCHAR(64)` | NOT NULL, UNIQUE | hex-encoded SHA-256 of the raw token |
| `expires_at` | `TIMESTAMPTZ` | NOT NULL | Default: `now() + interval '7 days'` |
| `revoked_at` | `TIMESTAMPTZ` | NULL | Non-null → token invalidated |
| `created_at` | `TIMESTAMPTZ` | NOT NULL, default `now()` | |

**Indexes**:
```sql
CREATE INDEX idx_refresh_tokens_user ON refresh_tokens (user_id);
```

**"Sign out of all devices"**: `UPDATE refresh_tokens SET revoked_at = now() WHERE user_id = $1 AND revoked_at IS NULL`.

---

## Table: `groups`

Named collections of employees for reporting and filtering. Organisation-scoped.

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| `id` | `UUID` | PK | |
| `organization_id` | `UUID` | NOT NULL, FK → `organisations.id` | |
| `name` | `VARCHAR(255)` | NOT NULL | Unique within org |
| `created_at` | `TIMESTAMPTZ` | NOT NULL, default `now()` | |
| `updated_at` | `TIMESTAMPTZ` | NOT NULL, default `now()` | |

```sql
CREATE UNIQUE INDEX uq_groups_org_name ON groups (organization_id, name);
```

---

## Table: `qualifications`

Named capabilities that can be assigned to employees. Organisation-scoped.

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| `id` | `UUID` | PK | |
| `organization_id` | `UUID` | NOT NULL, FK → `organisations.id` | |
| `name` | `VARCHAR(255)` | NOT NULL | Unique within org |
| `created_at` | `TIMESTAMPTZ` | NOT NULL, default `now()` | |
| `updated_at` | `TIMESTAMPTZ` | NOT NULL, default `now()` | |

```sql
CREATE UNIQUE INDEX uq_qualifications_org_name ON qualifications (organization_id, name);
```

---

## Join Table: `user_groups`

Many-to-many: users ↔ groups (employees only).

| Column | Type | Constraints |
|--------|------|-------------|
| `user_id` | `UUID` | NOT NULL, FK → `users.id` |
| `group_id` | `UUID` | NOT NULL, FK → `groups.id` |

`PRIMARY KEY (user_id, group_id)`

---

## Join Table: `user_qualifications`

Many-to-many: users ↔ qualifications (employees only).

| Column | Type | Constraints |
|--------|------|-------------|
| `user_id` | `UUID` | NOT NULL, FK → `users.id` |
| `qualification_id` | `UUID` | NOT NULL, FK → `qualifications.id` |

`PRIMARY KEY (user_id, qualification_id)`

---

## Table: `clients`

A company that commissions projects. Managed by Managers/Admins.

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| `id` | `UUID` | PK | |
| `organization_id` | `UUID` | NOT NULL, FK → `organisations.id` | |
| `name` | `VARCHAR(255)` | NOT NULL | |
| `contact_name` | `VARCHAR(255)` | NULL | |
| `contact_email` | `VARCHAR(320)` | NULL | |
| `contact_phone` | `VARCHAR(32)` | NULL | |
| `contract_start` | `DATE` | NOT NULL | |
| `contract_end` | `DATE` | NULL | Open-ended if NULL |
| `is_active` | `BOOLEAN` | NOT NULL, default `true` | |
| `is_deleted` | `BOOLEAN` | NOT NULL, default `false` | Soft delete (Admin only) |
| `deleted_at` | `TIMESTAMPTZ` | NULL | |
| `created_at` | `TIMESTAMPTZ` | NOT NULL, default `now()` | |
| `updated_at` | `TIMESTAMPTZ` | NOT NULL, default `now()` | |

```sql
CREATE INDEX idx_clients_org ON clients (organization_id);
```

**Cascade rule**: when `is_active` flips to `false` or `is_deleted` to `true`, the service
layer sets `is_active = false` on all `users` where `client_id = clients.id` (FR-024).

---

## Table: `sites`

Physical locations where projects take place.

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| `id` | `UUID` | PK | |
| `organization_id` | `UUID` | NOT NULL, FK → `organisations.id` | |
| `name` | `VARCHAR(255)` | NOT NULL | |
| `address` | `TEXT` | NOT NULL | |
| `timezone` | `VARCHAR(64)` | NOT NULL | IANA tz |
| `created_at` | `TIMESTAMPTZ` | NOT NULL, default `now()` | |
| `updated_at` | `TIMESTAMPTZ` | NOT NULL, default `now()` | |

```sql
CREATE INDEX idx_sites_org ON sites (organization_id);
```

---

## Table: `projects`

A unit of work for a client at a site. Billing rate stored as exact decimal.

| Column | Type | Constraints | Notes |
|--------|------|-------------|-------|
| `id` | `UUID` | PK | |
| `organization_id` | `UUID` | NOT NULL, FK → `organisations.id` | |
| `client_id` | `UUID` | NOT NULL, FK → `clients.id` | |
| `site_id` | `UUID` | NOT NULL, FK → `sites.id` | |
| `name` | `VARCHAR(255)` | NOT NULL | |
| `type` | `project_type` ENUM | NOT NULL | `security \| training` |
| `billing_rate` | `NUMERIC(12,4)` | NOT NULL, CHECK > 0 | Per hour; never float |
| `start_date` | `DATE` | NOT NULL | |
| `end_date` | `DATE` | NULL | Open-ended if NULL |
| `status` | `project_status` ENUM | NOT NULL, default `'active'` | `active \| ended` |
| `is_deleted` | `BOOLEAN` | NOT NULL, default `false` | Soft delete (Admin only) |
| `deleted_at` | `TIMESTAMPTZ` | NULL | |
| `created_at` | `TIMESTAMPTZ` | NOT NULL, default `now()` | |
| `updated_at` | `TIMESTAMPTZ` | NOT NULL, default `now()` | |

```sql
CREATE INDEX idx_projects_org ON projects (organization_id);
CREATE INDEX idx_projects_client ON projects (client_id);
```

---

## Custom PostgreSQL Types (ENUMs)

```sql
CREATE TYPE user_role AS ENUM ('employee', 'manager', 'admin', 'client');
CREATE TYPE project_type AS ENUM ('security', 'training');
CREATE TYPE project_status AS ENUM ('active', 'ended');
```

---

## Entity Relationship Summary

```
organisations
    │
    ├─< users (organization_id)
    │       │
    │       ├─< user_groups >─< groups (organization_id)
    │       ├─< user_qualifications >─< qualifications (organization_id)
    │       ├─< refresh_tokens
    │       └── client_id ──────────────────────────────┐
    │                                                    │
    ├─< clients (organization_id) <─────────────────────┘
    │       │
    │       └─< projects (client_id)
    │                │
    └─< sites ───────┘ (site_id)
```

---

## State Transition Diagrams

### User `is_active` / `is_deleted`

```
Active ──deactivate──> Inactive ──reactivate──> Active
Active ──delete──────> Deleted  (terminal; Admin only)
Inactive ──delete────> Deleted  (terminal; Admin only)
```

### Project `status` / `is_deleted`

```
active ──mark ended──> ended ──mark active──> active  (both directions)
active ──delete──────> deleted  (terminal; Admin only)
ended  ──delete──────> deleted  (terminal; Admin only)
```

### Refresh token

```
valid ──use──> rotated (old revoked, new issued)
valid ──logout──> revoked
valid ──logout-all──> revoked (all tokens for user)
valid ──expire──> expired (checked on use)
```

---

## Alembic Migration Notes

- First migration: create ENUMs, then all tables in dependency order.
- `organisations` first (no FK deps).
- `clients`, `sites` next (depend on `organisations`).
- `users` after `clients` (FK to `clients`).
- `groups`, `qualifications` after `organisations`.
- Join tables last.
- `refresh_tokens` after `users`.
- All migrations are reversible (`downgrade()` MUST be implemented).
- `updated_at` triggers can be added via a helper that each table calls (or handled in the
  SQLAlchemy `before_flush` event — decision deferred to tasks).
