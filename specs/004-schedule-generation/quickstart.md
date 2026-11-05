# Quickstart: validating schedule generation

Prerequisites: as 003, plus the API on Python 3.12 (`apps/api/.python-version`) so OR-Tools installs
(research G1). Tests run on a private `_test` database. The worker runs with
`uv run python -m hds.worker` from `apps/api`, on the same `DATABASE_URL` as the API.

| Item | How to prove it | Expected |
| --- | --- | --- |
| Purity (Constitution III) | `tests/unit/scheduling/test_purity.py` | `import scheduling` works in a bare interpreter; only `cpsat.py` imports `ortools`, nothing imports the app |
| Hard rules (FR-011 to FR-016, SC-002) | Hypothesis property tests on `generate` with both solvers, at least 1,000 examples in CI | `validate` finds no overlap, rest, weekly, blocked, absence or inactive warning in any result, also against fixed shifts |
| Legacy flaws (SC-005) | Example tests: night then morning, a close preference yielding, hours balanced, nights balanced, two projects at once | Each passes for CP-SAT; greedy passes the hard-rule ones |
| Coverage and reasons (SC-003, SC-004) | Built feasible inputs; built failures with one known cause each | All places filled; each unfilled place names the built cause with the right count |
| Fairness (SC-006) | Two to five interchangeable employees over a week | Hours spread at most one shift length, nights spread at most one (CP-SAT) |
| US1, FR-001 to FR-003 | `PUT` then `GET /projects/{id}/requirements`; a type the project does not use; retire a type | Stored and read back; 422; its needs are gone |
| US7, FR-026 | Admin `PATCH /organisation/scheduling`; Manager tries; out-of-range values | Stored, and 003 warnings use it; 403; 422 |
| US2, FR-004 to FR-010 | `POST /schedule-jobs` for a week, run the worker, `GET` the job; repeat the POST with the same key | 202 queued, then succeeded with `optimal` and a proposal; the roster is unchanged; one job |
| Queue (research G7) | Two workers and one job; kill a worker mid-run | One claim; the job is claimed again after the lease, failed after 3 attempts |
| US3, FR-023 to FR-025 | Accept; accept again; change an entry and accept another proposal; discard | Drafts created, nothing published; 409 `schedule.proposal_closed`; 409 `schedule.proposal_out_of_date`; closed |
| US6, SC-007 | Accept, generate again, compare | Zero changes; one absence changes only that day's slots |
| Access (FR-027, SC-104 of 003) | Role matrix, cross-organisation sweep, roster visibility sweep | 403 for E and C, 404 across organisations; no proposal data in employee views |
| Benchmark (SC-001) | `uv run pytest -m benchmark` and `uv run python benchmarks/compare_solvers.py` | 30 employees, one week, under 30 seconds; the comparison table printed |
| Checkpoint | The 003 merge gate plus the new tests, the generated client and the client gates | Green |
