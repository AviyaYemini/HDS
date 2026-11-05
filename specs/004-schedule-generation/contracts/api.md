# API changes: Schedule generation

All under `/api/v1`, added to `specs/001-foundation-auth-org/contracts/openapi.yaml` group by group.
Roles: E Employee, M Manager, A Admin, C Client. Every operation is scoped to the caller's organisation,
answers 403 to Employees and Clients, 404 for another organisation's record, and has an allowed and a
forbidden test.

| Operation | Roles | Notes |
| --- | --- | --- |
| `GET /projects/{id}/requirements` | M, A | The project's staffing needs: `{items: [{shift_type_id, weekday, headcount}]}` |
| `PUT /projects/{id}/requirements` | M, A | Replaces the set. 422 `requirements.shift_type_not_in_project`, `requirements.shift_type_retired`, `requirements.duplicate`; headcount at least 1 and weekday 0 to 6 by schema |
| `GET /organisation/scheduling` | M, A | `{min_rest_hours, max_weekly_hours}` |
| `PATCH /organisation/scheduling` | A | Either field; 1 to 24 and 1 to 168 by schema; unknown fields 422 |
| `POST /schedule-jobs` | M, A | Body `{from, to, project_ids?, solver?}`. 202 with the job (status `queued`). 422 `schedule.range_invalid` (backwards or more than 31 days), `schedule.project_not_active`. Optional `Idempotency-Key`: a repeat answers the first job; another body 422 `idempotency.key_reused` |
| `GET /schedule-jobs?status=` | M, A | The organisation's jobs, newest first, cursor pagination; no `result` |
| `GET /schedule-jobs/{id}` | M, A | The job: status, range, projects, settings used, requested by, times, `error_code`; once succeeded also `optimal`, `proposal_state` (`open`, `accepted`, `discarded`, `expired`) and `proposal` (research G12; data-model `result`) |
| `POST /schedule-jobs/{id}/accept` | M, A | Stores the proposal as drafts; answers `{shifts_created, shifts_updated, assignments_added, assignments_removed}`. 409 `schedule.not_ready` (not succeeded), `schedule.proposal_closed` (accepted, discarded or expired), `schedule.proposal_out_of_date` |
| `POST /schedule-jobs/{id}/discard` | M, A | 409 `schedule.not_ready`, `schedule.proposal_closed` |

`proposal.unfilled[].reasons[].code`: `schedule.unavailable`, `schedule.weekly_maximum`,
`schedule.minimum_rest`, `schedule.overlap`, `schedule.needed_elsewhere`, `schedule.no_active_employee`;
each has a text in `en` and `he`.

The job and proposal schemas name employees by id only; no pay, email or other personal field.
