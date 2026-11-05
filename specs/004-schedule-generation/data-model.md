# Data model: Schedule generation

Migration `021_schedule_generation` adds two tables and one column. Every table carries
`organization_id`, as all tables do. Times are UTC; dates are local dates at the project's site.

## staffing_needs (new)

A project's staffing need for one shift type on one weekday (US1, FR-001 to FR-003).

| Column | Type | Rules |
| --- | --- | --- |
| id | uuid | primary key |
| organization_id | uuid | references organisations |
| project_id | uuid | references projects, on delete cascade |
| shift_type_id | uuid | references shift_types; must be in the project's set and active (checked by the service) |
| weekday | smallint | 0 Sunday to 6 Saturday (`ck_staffing_needs_weekday`) |
| headcount | smallint | at least 1 (`ck_staffing_needs_headcount_positive`) |
| created_at, updated_at | timestamptz | |

Unique (`project_id`, `shift_type_id`, `weekday`). Replaced as a whole per project, under the project's
row lock. Removing a shift type from a project, or retiring it, deletes its rows (FR-002).

## schedule_jobs (new)

A generation run and, once it has succeeded, its proposal (US2 to US4, FR-004 to FR-026).

| Column | Type | Rules |
| --- | --- | --- |
| id | uuid | primary key |
| organization_id | uuid | references organisations |
| requested_by | uuid | references users |
| first_day, last_day | date | `last_day >= first_day`, at most 31 days (`ck_schedule_jobs_range`) |
| project_ids | uuid[] | the run's projects; empty means every active project |
| solver | text | `greedy` or `cpsat` (`cpsat` by default) |
| status | text | `queued`, `running`, `succeeded`, `failed` |
| attempts | smallint | claims so far; at most 3 |
| lease_until | timestamptz | set while running; a running job past it may be claimed again |
| started_at, finished_at | timestamptz | |
| error_code | text | set when failed, for example `jobs.worker_lost`, `jobs.solver_failed` |
| settings | jsonb | the minimum rest and weekly maximum the run used |
| fingerprint | char(64) | SHA-256 of the solved problem (research G9) |
| result | jsonb | the proposal, below; set when succeeded |
| optimal | boolean | set when succeeded |
| proposal_state | text | `open`, `accepted` or `discarded`; set when succeeded |
| decided_by, decided_at | uuid, timestamptz | who accepted or discarded it, and when |
| created_at, updated_at | timestamptz | |

Index on (`status`, `created_at`) for the claim. A proposal reads as `expired` when `proposal_state` is
`open` and `finished_at` is more than 7 days ago (FR-025).

**States**

```text
queued ──claim──▶ running ──solved──▶ succeeded (proposal open) ──accept──▶ accepted
   ▲                 │                         │                └─discard──▶ discarded
   └──lease expired──┘ (attempts < 3)          └─7 days──▶ expired (read only)
running ──error, or lease expired at attempt 3──▶ failed
```

### The proposal (`result`)

- `slots`: per slot, `{key, shift_id (existing draft or null), project_id, shift_type_id, date,
  starts_at, ends_at, headcount, assigned: [user ids]}`.
- `unfilled`: per slot with free places, `{key, places, reasons: [{code, employees}]}`. Codes:
  `schedule.unavailable`, `schedule.weekly_maximum`, `schedule.minimum_rest`, `schedule.overlap`,
  `schedule.needed_elsewhere`, `schedule.no_active_employee`.
- `employees`: per candidate, `{user_id, hours, nights, open_met, close_not_met}`.
- `no_availability`: the candidates with no entry in the range (FR-019).
- `short_published`: published shifts with fewer people than their need (FR-006).
- `changes`: `{added: [{slot key, user id}], removed: [...]}` against the current drafts.

## assignments (changed)

| Column | Type | Rules |
| --- | --- | --- |
| source | text | `manual` or `generated`; `manual` by default and for every existing row |

## organisations (unchanged; now settable)

`min_rest_hours` (1 to 24) and `max_weekly_hours` (1 to 168) already exist (020, checked positive).
Migration 021 tightens the checks to these bounds. Every existing value is 11 and 48, so no row breaks
them.

## Pure package types (`hds.scheduling`)

- `Problem`: `slots: tuple[Slot, ...]`, `fixed: tuple[ShiftSlot, ...]`, `candidates: tuple[Employee, ...]`,
  `entries: tuple[Entry, ...]`, `current: Mapping[slot key, tuple[user id, ...]]`, `settings: Settings`,
  `nights: frozenset[shift type id]`.
- `Slot`: `key`, `shift_id | None`, `project_id`, `shift_type_id`, `day`, `starts_at`, `ends_at`, `zone`,
  `headcount`.
- `Result`: `assigned: Mapping[slot key, tuple[user id, ...]]`, `unfilled: tuple[Unfilled, ...]`,
  `summary: tuple[EmployeeSummary, ...]`, `changes: Changes`, `optimal: bool`.
- `Unfilled`: `key`, `places`, `reasons: tuple[Reason, ...]`; `Reason`: `code`, `employees`.
- `Employee`, `Entry`, `Settings`, `ShiftSlot` and `RuleWarning` are those of 003.
