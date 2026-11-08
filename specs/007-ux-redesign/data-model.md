# Data model: UX foundation and redesign

Five migrations, numbered in shipping order, all additive and reversible (Constitution X). Times are UTC instants; money is
Decimal (Constitution VII). Every new table carries `organization_id` and is read only through its
repository (Constitution II).

## 025 — organisation contact and phone sessions (group B); 029 — saved views (group F)

**organisations** (changed): `contact_phone` (string 32, null), `contact_email` (string 254, null),
`opening_hours` (jsonb, null: up to seven `{weekday, opens, closes}` items in local time).

**saved_views** (new, migration 029): `id`, `organization_id`, `user_id`, `screen` (string 48, for example
`staff`, `work_log`), `name` (1–60 chars), `query` (string 2000: the web address's query string),
`created_at`. Unique (user_id, screen, name).

**refresh_tokens** (changed): `client_kind` (string 8, `web` or `native`, default `web`), so a
rotation keeps the token's lifetime class.

## 026 — availability submissions (group C)

**availability_submissions** (new)

| Column | Type | Rule |
| --- | --- | --- |
| `id` | uuid pk | |
| `organization_id` | uuid fk | |
| `window_id` | uuid fk → submission_windows, on delete cascade | |
| `user_id` | uuid fk → users | |
| `submitted_at` | timestamptz | the latest submit |
| `snapshot` | jsonb | the employee's blocked, open and close entries for the window's dates at `submitted_at`, normalised and sorted |
| `snapshot_hash` | char(64) | hash of the normalised snapshot, compared with the live entries to derive Draft |

- `uq_availability_submissions_window_user` (window_id, user_id): one row per employee and window;
  submitting again replaces the snapshot.
- **Derived status** (never stored): Locked if now ≥ window deadline; else Submitted if a row exists
  and its hash equals the hash of the live entries; else Draft if any live entry falls in the window
  or a row exists; else Not submitted.
- **After the deadline**: employees' blocked, open and close entries in the window are refused with
  409 `availability.window_closed`, as in 003; absences stay editable. An Admin's hand edit (before
  the deadline or after) changes the employee's existing snapshot on the days it touches, to the
  live entries there, so it reaches scheduling at once; the rest stays as submitted (clarification
  Q4). It never creates a snapshot: to count someone who did not submit, an Admin submits on their
  behalf (`POST /api/v1/employees/{id}/availability-submissions`), recorded in `submitted_by`.
- **`submission_windows.requires_submission`** (bool, default true): false for windows already
  closed when 026 ran, so the not-submitted rule never reaches back into weeks without a Submit.
- **Scheduling input**: inside a window whose deadline has passed, the snapshot replaces the live
  entries; with no row, one synthetic blocked entry per date with reason `not_submitted`.

## 030 — who changed a submission by hand (group C)

**availability_submissions** gains `changed_by` (uuid fk → users, null) and `changed_at`
(timestamptz, null): the Admin whose hand edit last changed the snapshot, and when. `submitted_at`
and `submitted_by` stay the employee's (or the on-behalf Admin's), so an on-time submission still
reads as on time; a fresh submit clears both. Numbered 030 because 027 to 029 (groups D and F)
revise 026 and are not merged yet; a merge revision joins the heads when they are.

## 027 — assignment times and shift concurrency (group D)

**assignments** (changed)

| Column | Type | Rule |
| --- | --- | --- |
| `starts_at` | timestamptz, null | null means the shift's start |
| `ends_at` | timestamptz, null | null means the shift's end |

- `ck_assignments_times_paired`: both null or both set.
- `ck_assignments_times_order`: `ends_at > starts_at` when set.
- Service rule (cross-table, so not a constraint): `starts_at ≥ shift.starts_at − 4 h` and
  `ends_at ≤ shift.ends_at + 4 h`, else 422 `assignment.times_out_of_range`.
- **Effective span** = the assignment's times when set, else its shift's. Pay, rule checks, reports,
  My shifts and the roster read only the effective span.

**shifts**: no new column; `updated_at` (already on `TimestampedBase`) becomes the ETag source.

## 028 — overrides, templates, publish events (group D)

**rule_overrides** (new): `id`, `organization_id`, `assignment_id` (fk, on delete cascade),
`warning_code` (string 64), `reason` (text, 1–500 chars), `created_by` (fk users), `created_at`.
Unique (assignment_id, warning_code).

**week_templates** (new): `id`, `organization_id`, `project_id` (fk, on delete cascade), `name`
(1–80 chars, unique per project), `body` (jsonb: a list of `{weekday, shift_type_id, headcount,
assignments: [{user_id, start_offset_min?, end_offset_min?}]}`), `created_by`, timestamps.

**publish_events** (new): `id`, `organization_id`, `project_id`, `user_id` (the affected person),
`published_by`, `first`, `last` (dates), `shift_count`, `notify` (bool), `created_at`,
`delivered_at` (null until feature 010 delivers it). One row per affected person per publish.

## Client-side models (no schema)

- **Capability set**: `GET /me` → `capabilities: string[]`, `managed_project_ids: uuid[]`;
  cached in memory for the session and refreshed after any 403.
- **Availability draft** (device): pending cell changes per window, kept in async storage until the
  server accepts them, with the last-synced time.
- **Cached lists** (device): the last-loaded My shifts, with "updated hh:mm".
- **Undo stack** (memory): per screen, the inverse of each unpublished roster change and each
  availability cell change.
- **Design tokens**: DTCG JSON in three tiers; four themes; density.

## State transitions

```text
Availability (per employee, per window)
  Not submitted ──edit──▶ Draft ──submit──▶ Submitted ──edit──▶ Draft
        │                   │                   │
        └──── deadline ─────┴───── deadline ────┴──▶ Locked
  Locked without a submission → counts as unavailable for the whole window.
  Locked after a submission   → the last snapshot stands; unsent edits are dropped.

Shift (unchanged): draft ──publish──▶ published; any edit after publishing marks it
"changed since publish" on the roster (derived: updated_at > published_at).
```
