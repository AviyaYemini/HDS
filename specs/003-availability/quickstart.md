# Quickstart: validating Part 1

Prerequisites as for 002: PostgreSQL running, `uv sync --extra dev` in `apps/api`.

| Item | How to prove it | Expected |
| --- | --- | --- |
| FR-001 to FR-004 | PATCH a client's `contract_start`; then a start after the stored end; then start and end together; then a rename on a client whose stored pair is out of order; then `contract_start: null` | Stored; 422 `clients.date_range_invalid`, nothing stored; both stored; stored; 422 `validation.request_invalid` |
| FR-005 | PATCH an employee, client, project and site with one unknown field; PATCH `/me` with `role` | 422 `validation.request_invalid` and nothing stored; 403 `profile.field_not_allowed` |
| FR-006 | A Client account with no client lists projects with and without filters | Empty every time |
| FR-007 | The full suite and the OpenAPI comparison | Unchanged apart from the contract changes |
| FR-008 | `EXPLAIN` the clean-up's delete on a table with many unexpired rows | Uses the `expires_at` index |
| FR-009 | Migrate a seeded database 018 → 019 → 018 → 019 | Columns gone after 019, back and empty after the downgrade, users kept |
| Checkpoint | The 002 merge gate: all gates, full suite, concurrency three times, timing, `contract_cli.sh`, drift check, migration round trip | Green |
