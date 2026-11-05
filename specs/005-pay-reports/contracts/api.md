# API changes: Pay rates, pay to date and reports

All under `/api/v1`, added to `specs/001-foundation-auth-org/contracts/openapi.yaml` group by group.
Roles: E Employee, M Manager, A Admin, C Client. Every operation is scoped to the caller's organisation,
answers 404 for another organisation's record, and has an allowed and a forbidden test in the role matrix.
Amounts are `Amount` strings with two decimals (`"612.50"`); percentages and hours are strings with two
decimals (`"125.00"`, `"7.50"`); rates keep `Money` (four decimals).

| Operation | Roles | Notes |
| --- | --- | --- |
| `POST /employees`, `PATCH /employees/{id}` (changed) | M, A | `default_hourly_rate` from a Manager, any value, 403 `users.rate_admin_only`; other fields unchanged |
| `GET /organisation/pay-settings` | M, A | The premium defaults of data-model FR-006 |
| `PATCH /organisation/pay-settings` | A | Any of them; 422 `pay.settings_invalid`, `pay.rest_window_invalid`; unknown fields 422 |
| `POST /projects` (changed) | M, A | Optional `pay_settings` (switches and overrides); from a Manager 403 `projects.pay_settings_admin_only` |
| `GET /projects/{id}/pay-settings` | M, A | `{pay_enabled, managers_see_pay, settings: {name: {value, source: project or organisation}}}` |
| `PATCH /projects/{id}/pay-settings` | A | Switches and overrides; `null` returns a setting to the default; 409 `pay.project_ended`; 422 as above |
| `GET /special-days?year=` | M, A | The organisation's days |
| `POST /special-days` | A | `{day, name, rate_pct?}`; 409 `special_days.duplicate` |
| `PATCH /special-days/{id}`, `DELETE /special-days/{id}` | A | Organisation or project day; a project day of an ended project 409 `pay.project_ended` |
| `GET /projects/{id}/special-days?year=` | M, A | The days in effect for the project, each with `source` (`organisation` or `project`) |
| `POST /projects/{id}/special-days` | A | Adds a project day or overrides an organisation day's rate; 409 `special_days.duplicate`, `pay.project_ended` |
| `GET /projects/{id}/managers` | M, A | `{items: [{id, full_name}]}` |
| `PUT /projects/{id}/managers` | A | `{user_ids}` replaces the set; 422 `project_managers.not_staff` |
| `GET /me/pay?month=YYYY-MM` | E, M, A | The caller's pay to date (data-model PayToDate); 422 `pay.month_invalid`; C 403 |
| `GET /reports/work-log?project_id=|site_id=|group_id=&from=&to=` | M, A | Exactly one scope (422 `reports.scope_invalid`); `from` defaults to the project's start, required for a site or group (422 `reports.start_required`); `to` defaults to today; 422 `reports.range_invalid` (backwards or over 366 days); no pay |
| `GET /reports/hours?from=&to=&group_by=employee|project` | M, A | Rows, group totals and grand total; `pay` only where the reader may see it (research R10); 422 `reports.range_invalid` |

Every operation answers 403 to the roles not listed, with the role-gate code, and 401 without a token.
Every new error code has a text in `en` and `he`.

`/me/pay` replaces the `/me/pay-forecast` placeholder of docs/hds-api.md, because the figure is pay to
date (spec clarification); docs/hds-api.md is updated with it.
