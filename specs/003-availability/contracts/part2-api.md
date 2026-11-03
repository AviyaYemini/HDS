# API changes, Part 2

All under `/api/v1`, added to `specs/001-foundation-auth-org/contracts/openapi.yaml` group by group.
Roles: E Employee, M Manager, A Admin, C Client. Every operation is scoped to the caller's organisation
and has an allowed and a forbidden test.

| Operation | Roles | Notes |
| --- | --- | --- |
| `GET /availability-windows?from=&to=` | E, M, A | Employees need it to know what is open |
| `POST /availability-windows`, `PATCH /availability-windows/{id}`, `DELETE /availability-windows/{id}` | M, A | 409 `windows.overlap`; 422 dates out of order |
| `GET /employees/{id}/constraints?from=&to=` | self, M, A | Another employee's id from an Employee: 404 |
| `POST /employees/{id}/constraints`, `PATCH /constraints/{id}`, `DELETE /constraints/{id}` | self (window rule), A (always) | 409 `availability.window_closed`, `availability.no_window`, `availability.conflict`; M on another's: 403 `availability.admin_required` |
| `GET /shift-types`, `POST /shift-types`, `PATCH /shift-types/{id}` | M, A (read also E) | Retire with `is_active: false` |
| `PUT /projects/{id}/shift-types` | M, A | The complete set of ids |
| `GET /shifts?from=&to=&project=&status=` | M, A | Cursor pagination; drafts and published, with assignments |
| `POST /shifts`, `PATCH /shifts/{id}`, `DELETE /shifts/{id}` | A | Answers `{shift, warnings}`; `pay_rate` A only |
| `POST /shifts/publish` | M, A | Body: project, from, to; publishes drafts in the range; answers the count |
| `POST /shifts/{id}/assignments` | A | Answers `{assignment, warnings}`; 409 `assignments.duplicate`, `assignments.employee_inactive` |
| `DELETE /assignments/{id}` | A | Answers `{warnings}` (for example under headcount) |
| `GET /me/shifts?from=&to=` | E, M, A | Own published assignments only; schema has no co-worker fields |

`Warning`: `{code, employee_id, shift_ids, detail}`; codes `warnings.overlap`, `warnings.rest_too_short`,
`warnings.weekly_hours_exceeded`, `warnings.blocked`, `warnings.absence`, `warnings.close_preference`,
`warnings.over_headcount`, `warnings.under_headcount`.

Clients get 403 on every operation above.
