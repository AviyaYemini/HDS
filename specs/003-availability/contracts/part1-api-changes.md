# API changes, Part 1

All in `specs/001-foundation-auth-org/contracts/openapi.yaml`, and in the generated document.

| Operation | Change |
| --- | --- |
| `PATCH /api/v1/clients/{id}` | `ClientUpdateRequest` gains `contract_start` (date, optional, not nullable). A pair with the end before the start answers 422 `clients.date_range_invalid` (already declared) |
| `PATCH /api/v1/employees/{id}`, `/api/v1/clients/{id}`, `/api/v1/projects/{id}`, `/api/v1/sites/{id}` | Their request schemas declare `additionalProperties: false`; an unknown field answers 422 `validation.request_invalid` |
| `PATCH /me` | Unchanged: an unknown field answers 403 `profile.field_not_allowed` |

No operation, success code or parameter is added or removed, so the drift check is unaffected.
