# Data model: Pay rates, pay to date and reports

Migration `022_pay_reports` adds columns to `organisations` and `projects`, two tables and one index.
Every table carries `organisation_id`. Times are UTC; dates and weekdays are local at the site. Money
amounts are computed, never stored (research R1); percentages are `NUMERIC(5,2)`.

## organisations (columns added): premium defaults (FR-006)

| Column | Type | Default | Rules |
| --- | --- | --- | --- |
| pay_day_normal_minutes | smallint | 480 | 1 to 1440 |
| pay_night_normal_minutes | smallint | 420 | 1 to 1440 |
| pay_tier1_minutes | smallint | 120 | 1 to 1440 |
| pay_overtime1_pct | numeric(5,2) | 125.00 | 100 to 300 |
| pay_overtime2_pct | numeric(5,2) | 150.00 | 100 to 300 |
| pay_rest_start_weekday | smallint | 5 (Friday) | 0 Sunday to 6 Saturday |
| pay_rest_start_time | time | 18:00 | |
| pay_rest_end_weekday | smallint | 6 (Saturday) | 0 to 6 |
| pay_rest_end_time | time | 22:00 | start and end differ; the window is at most 7 days, read forward from the start |
| pay_rest_pct | numeric(5,2) | 150.00 | 100 to 300 |
| pay_increase1_points | numeric(5,2) | 25.00 | above 0, at most 200 |
| pay_increase2_points | numeric(5,2) | 50.00 | above 0, at most 200 |

Each bound is a check constraint (`ck_organisations_pay_*`) and is also refused by the service with 422
`pay.settings_invalid`.

## projects (columns added): switches and overrides (FR-005 to FR-007)

| Column | Type | Rules |
| --- | --- | --- |
| pay_enabled | boolean | not null, default true |
| managers_see_pay | boolean | not null, default false |
| the twelve `pay_*` columns above | same types | nullable; `NULL` follows the organisation; same checks when set |

Effective setting = the project's value if set, else the organisation's. When `status` becomes `ended`,
the service fills every `NULL` from the organisation under the project's row lock (research R4). Changes
to an ended project are refused (409 `pay.project_ended`).

## special_days (new) (FR-008, FR-008a)

| Column | Type | Rules |
| --- | --- | --- |
| id | uuid | primary key |
| organisation_id | uuid | references organisations |
| project_id | uuid | references projects, on delete cascade; `NULL` for an organisation day |
| day | date | local date |
| name | varchar(255) | not blank |
| rate_pct | numeric(5,2) | 100 to 300 (`ck_special_days_rate`), default 150.00 |
| created_at, updated_at | timestamptz | |

Unique (`organisation_id`, `day`) where `project_id IS NULL`; unique (`project_id`, `day`) where
`project_id IS NOT NULL` (409 `special_days.duplicate`). A project day overrides an organisation day on the
same date for that project. When a project ends, the organisation days it does not override are copied
into its own list.

## project_managers (new) (FR-021, FR-022)

| Column | Type | Rules |
| --- | --- | --- |
| organisation_id | uuid | references organisations |
| project_id | uuid | references projects, on delete cascade |
| user_id | uuid | references users; an active Manager or Admin of the organisation when added |
| created_at | timestamptz | |

Primary key (`project_id`, `user_id`); index on `user_id`. Replaced as a set under the project's row lock.

## Index

`idx_shifts_org_published_start` on `shifts (organisation_id, starts_at) WHERE status = 'published'`, for
the reports and pay to date (research R9).

## Unchanged, used as is

- `users.default_hourly_rate` `NUMERIC(12,4)`, positive when set (001); now Admin only (research R7).
- `shifts.pay_rate` `NUMERIC(12,4)`, positive when set, Admin only (003).
- `assignments`, `shifts`, `sites.timezone`, `groups` and `user_groups`.

## Computed, not stored

- **PayLine**: shift id, project, local date, start, end, hours, base rate or `rate_missing`, parts
  `[{rate_pct, hours}]`, pay (`Amount`, absent when hidden, missing or pay off), `pay_enabled`.
- **PayToDate**: month, lines, `hours`, `pay`, `shifts_missing_rate`.
- **WorkLogRow**: user, project, shifts, hours, first and last date.
- **HoursRow**: user, project, shifts, hours, pay where visible; totals per group and overall.
