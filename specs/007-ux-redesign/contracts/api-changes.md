# API changes (contract-first)

Every change below is made first in `specs/001-foundation-auth-org/contracts/openapi.yaml`, then in
the server, then `pnpm api-client:generate` regenerates `packages/api-client`. All are additive under
`/api/v1` (Constitution V): new optional fields, new endpoints, and new error codes only on new
behaviour. Each endpoint gets a `summary` and a `description` (Constitution XI) and is covered by
Schemathesis.

## Group B

| Method and path | Change | Who | Notes |
| --- | --- | --- | --- |
| `GET /me` | adds `capabilities: string[]` (sorted) and `managed_project_ids: uuid[]` | every role | the UI's only source for gating (contracts/capabilities.md) |
| `POST /auth/login` | adds optional `client_kind: "web" \| "native"` (default `web`) | public | native refresh tokens last `REFRESH_TOKEN_TTL_DAYS_NATIVE` (30) and roll on refresh |
| `POST /auth/refresh` | unchanged shape; keeps the token's `client_kind` | public | |
| `GET /me/sessions` | new: the caller's active sessions (created, last used, client kind, current) | every role | N12 |
| `DELETE /me/sessions/{id}` | new: revoke one of the caller's sessions | every role | "sign out of other devices" stays `POST /auth/logout-all` |
| `GET /organisation/contact` | new: phone, e-mail, opening hours | every role | C6 |
| `PATCH /organisation/contact` | new | admin | 403 otherwise |

## Group C

| Method and path | Change | Who | Notes |
| --- | --- | --- | --- |
| `GET /me/availability-submissions?window_id=` | new: status (`not_submitted`, `draft`, `submitted`, `locked`), `submitted_at`, `deadline`, `counted_unavailable` | staff (self) | status derived (data-model 026) |
| `POST /me/availability-submissions` | new: `{window_id}` submits the caller's entries for the window | staff (self) | 409 `availability.window_closed` after the deadline (the code 003 already uses) |
| `POST/PATCH/DELETE` constraint endpoints | unchanged: after a deadline employees already get 409 `availability.window_closed` (003); an Admin's change inside a closed window refreshes that employee's existing snapshot, and creates none | staff, admin | absences unaffected |
| `POST /api/v1/employees/{id}/availability-submissions` | new: `{window_id}`; an Admin submits the employee's current entries on their behalf, at any time; `submitted_by` records the Admin | admin | the explicit way to count someone who missed the deadline; a hand edit alone never creates a submission |
| `GET /me/shifts` | unchanged in C: `from`/`to` already reach back in time for Past; group D makes `starts_at`/`ends_at` the caller's own times | staff | Past by pay period |
| `GET /me/shifts/{id}` | new: one published shift with the site's address and the project's managers as contacts (standing in for a shift leader) | staff (self, assigned) | E11; 404 for a draft or someone else's |

## Group D

| Method and path | Change | Who | Notes |
| --- | --- | --- | --- |
| `POST /shifts`, `PATCH /shifts/{id}`, `DELETE /shifts/{id}` | gate widens from admin to `roster.edit:project` | manager of the project, admin | `pay_rate` stays admin-only: 403 `shift.pay_rate_admin_only` |
| `POST /shifts/{id}/assignments` | same widening; body adds optional `starts_at`, `ends_at` | as above | 422 `assignment.times_out_of_range`; warnings returned as today |
| `PATCH /assignments/{id}` | new: change `starts_at`/`ends_at` (null both to reset) | as above | |
| `DELETE /assignments/{id}` | same widening | as above | |
| shift and assignment writes | optional `If-Match` (the shift's `ETag`); stale → 412 `roster.changed` | as above | `GET /shifts` returns each shift's `etag` |
| `GET /shifts` | each assignment carries effective `starts_at`, `ends_at`, `own_times: bool`, its warnings and overrides | manager, admin | |
| `POST /assignments/{id}/overrides` | new: `{warning_code, reason}` | `roster.edit:project` | 422 if the warning is not on that assignment |
| `GET /projects/{id}/publish-preview?from=&to=` | new: shifts, people, coverage (filled/required), unfilled slots, open warnings | `roster.read` (any Manager or Admin; publishing itself is `roster.publish:project`) | S12 |
| `POST /projects/{id}/publish` | adds optional `notify: bool` (default true); records `publish_events` | as today | R12 |
| `POST /projects/{id}/copy-week` | new: `{from_week, to_week, on_conflict: "allow" \| "overwrite" \| "unfilled", dry_run}` | `roster.edit:project` | dry run returns the change list |
| `GET/POST/DELETE /projects/{id}/week-templates`, `POST /projects/{id}/week-templates/{tid}/apply` | new | `roster.edit:project` | apply takes `{week, on_conflict, dry_run}` |
| `POST /projects/{id}/roster/bulk` | new: one change set over a stated scope, with `dry_run` | `roster.edit:project` | S15 |
| `GET /availability-windows/{id}/submissions` | new: per employee status and `submitted_at`; after the deadline `counted_unavailable` | manager, admin | S21 |
| rule checks | new warning code `warnings.not_submitted` (the `warnings.*` family) | — | from `validate` |

## Group E

| Method and path | Change | Notes |
| --- | --- | --- |
| `GET /schedule-jobs/{id}` | adds `optimal: bool` and `elapsed_seconds` if not already exposed | the honesty label (AS9) |

## Group F and G

| Method and path | Change | Who |
| --- | --- | --- |
| `GET/POST/DELETE /me/saved-views?screen=` | new | staff |
| list endpoints used by admin tables | `q`, `sort`, `page`, `page_size` where missing, additive | as today |
| `GET /reports/hours`, `GET /reports/work-log` | add a `totals` object (hours, gross pay where allowed, overtime hours, billable, unfilled) if missing | as today |

## Error codes added

`assignment.times_out_of_range`, `shift.pay_rate_admin_only`, `roster.changed`,
`organisation.opening_hours_invalid`, `override.warning_not_found`; warning `warnings.not_submitted`.
Each has an `en` and `he` locale key in the client and the API's locales.
