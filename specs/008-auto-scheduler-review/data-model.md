# Data model: Auto-scheduler review

Migration **034_proposal_review**, reversible. Tables and columns below; everything else is as 004
and 007 left it.

## `schedule_jobs` (changed)

| Column | Type | Notes |
| --- | --- | --- |
| `controls` | JSON, not null, default `{}` | `{"changes": "keep" \| "free", "wishes": "strict" \| "gentle"}`; missing keys take the defaults `free` and `strict` (today's order) |
| `chips` | JSON, not null, default `[]` | list of chips (below) |
| `based_on_id` | UUID, null, FK `schedule_jobs.id` | the run a rerun started from; its working copy is the starting point and its chips are copied |
| `current` | JSON, null | the working copy (below); set with `result` when the run finishes |
| `version` | integer, not null, default 0 | grows by one with every edit, undo, lock and accept of the working copy; the `ETag` |
| `needed` | integer, null | places to fill in the run, set when the problem is built |
| `best_filled` | integer, null | places filled in the best result so far |
| `progress_at` | timestamptz, null | when `best_filled` last changed |
| `stop_requested_at` | timestamptz, null | Stop pressed; the worker reads it |
| `ended` | varchar(16), null | `finished`, `stopped`, `timed_out`, `cancelled`, `failed` |

`proposal_state` gains `partly_accepted` (the rest discarded or expired after part was accepted); its
check constraint is replaced. `status` gains `cancelled`.

### Chip (one element of `chips`)

```json
{ "id": "uuid", "kind": "max_shifts|max_nights|max_hours|keep_on|keep_off|prefer",
  "user_id": "uuid", "limit": 2,
  "site_id": "uuid|null", "project_id": "uuid|null",
  "weekday": 0, "shift_type_id": "uuid|null" }
```

`limit` only for the three caps (whole numbers; hours as whole hours). `keep_on` takes a site or a
project; `keep_off` and `prefer` take a weekday (0 Sunday to 6 Saturday) or a shift type. At most 20
chips per run.

### Working copy (`current`)

The stored proposal of 004 (`slots`, `unfilled`, `employees`, `no_availability`, `short_published`,
`changes`; each slot now with its site's `zone`), with each slot's `assigned` replaced by:

- `run_before`: the slot's candidates on the roster when the run was made (for the change marks);
- `stored`: the slot's candidates on the roster as far as the proposal knows, moving with each part
  accepted;
- `people`: who the proposal puts there now, each

```json
{ "user_id": "uuid", "by": "solver|person", "locked": false,
  "override": { "codes": ["warnings.rest_too_short"], "reason": "text" } | null }
```

A pair (slot, person) is open while `people` and `stored` disagree on it, and accepted once it
differed from `run_before` and they agree (`hds.services.proposal_copy`). A null `current` reads as
the result, untouched.

plus `chips_met` (`[{ "id", "met": bool, "cause": "code|null" }]`) and `ended` copied from the job.
`unfilled[].reasons[]` gain `people`: `[{ "user_id", "code", "have": minutes, "need": minutes,
"week_minutes": int|null, "slot_minutes": int|null, "limit": int|null }]` for rest and the weekly
maximum.

## `assignments` (changed)

| Column | Type | Notes |
| --- | --- | --- |
| `locked_by` | UUID, null, FK `users.id` | who locked it |
| `locked_at` | timestamptz, null | when; null means not locked |

Check: both null or both set.

## `schedule_job_events` (new, append-only)

| Column | Type | Notes |
| --- | --- | --- |
| `id` | UUID, PK | |
| `organisation_id` | UUID, FK, not null | |
| `job_id` | UUID, FK `schedule_jobs.id`, not null | |
| `kind` | varchar(24), not null | created, finished, stopped, cancelled, failed, edited, undone, locked, unlocked, overridden, accepted, discarded, expired |
| `actor_id` | UUID, FK `users.id`, null | null for the worker's own events |
| `at` | timestamptz, not null | |
| `body` | JSON, not null | the edit, the part accepted, the counts, the reason |
| `version` | integer, null | the working copy's version after the event |

Index `(job_id, at)`. A trigger `schedule_job_events_append_only` raises on UPDATE and DELETE; the
downgrade drops it with the table.

## State of a proposal

```text
open ──accept all──────────────► accepted
open ──accept part──► open (parts accepted) ──accept rest──► accepted
open (parts accepted) ──discard the rest / expire──► partly_accepted
open ──discard / expire──► discarded / expired
```

Edits, undo, locks and accepts need state `open` and the current `version`.

## Pure types (`hds.scheduling`)

- `Controls(changes: "keep" | "free", wishes: "strict" | "gentle")`.
- `Chip(id, kind, user_id, limit, site_id, project_id, weekday, shift_type_id)`.
- `Blocked(user_id, code, have, need, week_minutes, slot_minutes, limit)`.
- `Problem` gains `start`, `controls`, `chips`, and `site_of` (project to site); `Slot` gains
  `site_id`.
- `Result` gains `chips_met` and `stopped`.
- `cover.Option(user_id, code | None, numbers, minutes_after, nights_after, weekends_after, wish)`.
- `fairness.Row(user_id, minutes, nights, weekends, outlier_minutes, outlier_nights,
  outlier_weekends)` and `fairness.Tradeoffs(...)`.
