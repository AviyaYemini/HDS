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

---

# Quickstart: validating Part 2

Prerequisites: as Part 1, plus Node.js 22 LTS and pnpm for the client (research R10).

| Item | How to prove it | Expected |
| --- | --- | --- |
| Rules (R1) | Property tests on `hds.scheduling.validate` with random shifts | Every overlap, short rest and weekly excess in the input is reported; nothing else is |
| US4, FR-101 to FR-105a | As an Employee with a window open: add blocked, open, close and absence entries; after the deadline repeat | Stored; then refused with `availability.window_closed`, except the absence |
| US5, FR-106 to FR-109 | Manager opens a window, lists all entries, tries to edit one; Admin edits after the deadline | Window stored; list complete; Manager 403; Admin stored |
| US6, FR-113 to FR-118 | Admin creates a week of shifts and assigns with short rest, overlap and a blocked date | Every change stored with the matching warnings |
| US7, FR-121 to FR-124 | Employee lists `/me/shifts` before and after publish; Client lists shifts | Empty, then own shifts without co-workers; Client 403 |
| SC-104 | The role sweep over every read | No leak |
| SC-106 | A 22:00 to 06:00 shift on a normal night and on both DST nights | 8, 7 and 9 hours |
| Client | `pnpm -C apps/client test`, `lint`, `typecheck`, Playwright web with RTL; the generated-client drift check | All pass; no literal strings; RTL layout mirrors |
| Checkpoint | The Part 1 merge gate plus the client gates | Green |
