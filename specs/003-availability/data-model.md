# Data Model: Availability, Part 1 (leftovers from 002)

## Migration 019 (`019_expiry_index_retire_locks`)

| Table | Change | Downgrade |
| --- | --- | --- |
| `refresh_tokens` | index `idx_refresh_tokens_expires_at` on `expires_at` | drop the index |
| `invitation_tokens` | index `idx_invitation_tokens_expires_at` on `expires_at` | drop the index |
| `users` | drop `failed_login_attempts` and `locked_until` | add both back as in 004, empty (count 0, no lock) |

No data moves. Every address's sign-in count has been in `login_throttle` since 002.

## Client (unchanged columns, new rule)

- `contract_start`: required date, now editable; never empty.
- `contract_end`: optional date; when set, not before `contract_start`, judged on the pair after a change
  that sets either date.

---

# Data Model: Part 2, availability and manual shifts

All tables carry `id` (UUID), `created_at`, `updated_at` and `organization_id`, as before.

## Migration 020 (`020_availability_shifts`)

| Table | Fields and rules |
| --- | --- |
| `organisations` (changed) | `min_rest_hours` SMALLINT NOT NULL DEFAULT 11, `max_weekly_hours` SMALLINT NOT NULL DEFAULT 48; both positive |
| `shift_types` | `name` VARCHAR(255) NOT NULL, `starts_at_local` TIME NOT NULL, `ends_at_local` TIME NOT NULL, `crosses_midnight` BOOL NOT NULL, `is_active` BOOL NOT NULL DEFAULT true; name unique per organisation among active; a non-crossing type ends after it starts. Each organisation gets morning 07:00 to 15:00, afternoon 15:00 to 23:00 and night 23:00 to 07:00 (crosses midnight) |
| `project_shift_types` | `project_id`, `shift_type_id`; primary key on the pair |
| `submission_windows` | `starts_on` DATE, `ends_on` DATE (≥ `starts_on`), `deadline` TIMESTAMPTZ NOT NULL, `created_by`; no two windows of one organisation overlap (exclusion constraint) |
| `availability_entries` | `user_id`, `kind` (`blocked`, `open`, `close`, `absence`), `absence_kind` (`leave`, `reserve_duty`, `sick`; set if and only if kind is absence), `starts_on`, `ends_on` (≥ `starts_on`), `shift_type_id` NULL, `weekday` SMALLINT NULL (0 Sunday to 6 Saturday); absences take no shift type or weekday; index on (`user_id`, `starts_on`) |
| `shifts` | `project_id`, `shift_type_id`, `starts_at`, `ends_at` TIMESTAMPTZ (end after start), `headcount` SMALLINT ≥ 1, `pay_rate` NUMERIC(12,4) NULL (> 0 when set), `status` (`draft`, `published`) DEFAULT draft, `published_at` NULL; index on (`organization_id`, `starts_at`) and (`project_id`, `starts_at`) |
| `assignments` | `shift_id` (cascade with the shift), `user_id`, `assigned_by`; unique (`shift_id`, `user_id`); index on (`user_id`) |
| `idempotency_keys` | `user_id`, `key` VARCHAR(255), `request_hash` CHAR(64), `status_code` SMALLINT, `response_body` JSONB, `created_at`; primary key (`user_id`, `key`); index on `created_at` for the purge; no `organization_id` beyond the user's |

Downgrade drops the seven tables and the two organisation columns, and the extension only if 020 created
it.

## State

- Shift: `draft` → `published` (by publish). Changes by an Admin keep the status. No return to draft in
  this part.
- Window: open while now < deadline; it has no other state.

## Pure inputs (`hds.scheduling`)

`ShiftSlot(id, project_id, starts_at, ends_at, headcount, assigned)`, `Entry(kind, starts_on, ends_on,
shift_type_id, weekday)`, `Settings(min_rest, max_weekly, week_zone)`, `Warning(code, employee_id,
shift_ids)`. Each assigned employee is passed with `is_active`, for `warnings.employee_inactive`.

**Alias**: the stored prompt's "shift templates" and docs/hds-spec.md's `ShiftTemplate` are the
`shift_types` table here; one name is used from here on. Plain dataclasses with standard-library types only.
