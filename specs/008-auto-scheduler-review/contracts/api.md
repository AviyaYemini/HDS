# API changes: Auto-scheduler review

Added to `specs/001-foundation-auth-org/contracts/openapi.yaml` first (Constitution V), all under
`/api/v1`, all additive. Every operation: Manager or Admin only (403 `users.manager_required`); a
Manager only on runs whose projects they all manage (403 `roster.project_not_managed`, as 007);
another organisation's job is 404. Errors are RFC 9457 problems with a `code`.

## Changed

| Operation | Change | PR |
| --- | --- | --- |
| `POST /schedule-jobs` | body gains `controls`, `chips` (max 20) and `based_on` (a job id; copies its chips when `chips` is absent) | 3 |
| `GET /schedule-jobs/{id}` | gains `progress: {filled, needed, at}`, `ended`, `stop_requested`; `status` may be `cancelled`; `proposal_state` may be `partly_accepted` | 1, 4 |
| `POST /schedule-jobs/{id}/accept` | optional body `{days?: [date], project_ids?: [uuid], assignments?: [{key, user_id}]}`; empty or absent accepts all still open; `If-Match` with the version (412 `schedule.version_stale`); 409 `schedule.part_out_of_date` names each day and cause; answer gains `version` and `state` | 1 |
| `POST /schedule-jobs/{id}/discard` | on a partly accepted proposal, closes it as `partly_accepted` | 1 |

## New

| Operation | Purpose | Answers | PR |
| --- | --- | --- | --- |
| `GET /schedule-jobs/{id}/proposal` | the working copy over the roster: each assignment marked `added`, `changed`, `removed` or `unchanged`, with `by`, `locked`, `override`, `accepted`; counts per mark; per day and project filled over needed; `ETag` = version | 200 | 1 |
| `POST /schedule-jobs/{id}/edits` | one edit: `{op: assign\|unassign\|move\|swap, key, user_id, to_key?, other_user_id?, override_reason?}`; `If-Match` | 200 with the new proposal and its `warnings`; 422 `schedule.override_reason_required` with the warnings; 422 `warnings.employee_inactive`; 412 `schedule.version_stale`; 409 `schedule.proposal_closed`. `lock` and `unlock` join `op` in PR 3 | 1 |
| `POST /schedule-jobs/{id}/edits/undo` | undo the last edit; `If-Match` | 200; 409 `schedule.nothing_to_undo` | 1 |
| `GET /schedule-jobs/{id}/slots/{key}/cover` | who else could cover that slot in the working copy, ranked (codes and numbers only) | 200 | 2 |
| `GET /schedule-jobs/{id}/fairness` | the fairness rows and averages, and the trade-off counts | 200 | 3 |
| `PUT /assignments/{id}/lock`, `DELETE /assignments/{id}/lock` | lock or unlock a roster assignment, within the projects the person may edit | 200 with the assignment; 404 | 3 |
| `POST /schedule-jobs/{id}/stop` | stop and use best so far, or cancel before any result | 202; 409 `schedule.not_running` | 4 |
| `GET /schedule-jobs/{id}/record` | the run record, oldest first, paged | 200 | 4 |
| `GET /shifts/{id}/cover` | "Find someone" for a roster shift's empty place | 200 | 4 |

## Shapes (sketch; the OpenAPI is the authority)

- `Cover`: `{key, options: [{user_id, full_name, free: bool, code?, have?, need?, week_minutes?,
  slot_minutes?, limit?, minutes_after, nights_after, weekends_after, wish: open|close|null}]}`.
- `Fairness`: `{rows: [{user_id, minutes, nights, weekends, outlier: [minutes|nights|weekends]}],
  average: {minutes, nights, weekends}, tradeoffs: {empty_places, open_not_given, close_given,
  minutes_low, minutes_high, nights_low, nights_high, chips_not_met}}`.
- `RecordEvent`: `{kind, actor_id?, at, version?, body}`.

Every new free-text field (`override_reason`) uses `hds.core.types.text` (R184). Each new rule that
spans fields or reads stored data goes into `tests/contract/unprovable_operations.json` in the same
commit (R110): the edits (warnings depend on stored shifts), the accept part (stored drafts), the
chips (a person or project in the organisation), the lock (the person's projects).
