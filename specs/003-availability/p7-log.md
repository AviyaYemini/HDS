# 003 Part 1, phase P7: decision log

Decisions, review findings, mutation results and deviations for Part 1 (the leftovers from 002).
Tests run on the private database `hds_p3_test`; `hds_test` is never used.

## Setup (T001)

- `hds_p3_test` exists and had no connections when the phase started (`pg_stat_activity`).
- Analysis before implementation, two findings acted on:
  - T022 originally forced `enable_seqscan off`, which proves the index can be used, not that the
    clean-up uses it. It now seeds SC-003's volumes and reads the real plan.
  - SC-004 says sign-in, lockout and reset tests pass unchanged, but
    `test_a_registered_address_is_counted_in_the_throttle_table_only` reads the two columns 019 drops.
    Its intent (the count lives only in the throttle table) stays; only the column check changes (T025).

## Group A: contract start editing (T002 to T008)

- Decision (E1): `contract_start` joins `ClientUpdateRequest` as `StrictDate = omittable()`, so it may be
  left out but not sent as null. `edit_client` builds the resulting pair (sent value, else stored) and
  judges it only when it differs from the stored pair. That one condition covers FR-002 and FR-003,
  including a form that sends a broken stored pair back unchanged.
- Failing first: 8 tests (6 integration, 2 unit) failed because the start was ignored (200, value
  unchanged) or never judged (`DID NOT RAISE`). Unit tests for the unchanged pair passed before and
  after, as they should: they pin FR-003, which 002 already met for the end alone.
- Mutations, all caught by a named test, files restored:
  - A1 end judged against the stored start only: `test_a_new_start_is_judged_against_the_stored_end`
  - A2 stored pair judged on every edit: `test_an_unchanged_pair_is_not_judged[no-dates]` and
    `test_a_stored_date_pair_that_breaks_the_rule_does_not_block_other_edits`. The first run was
    caught only by a mock comparison `TypeError` in an unrelated test, which does not count, so it
    was re-run against the intended tests.
  - A3 judged only when the end changes: `test_a_new_start_is_judged_against_the_stored_end`
  - A4 start clearable: `test_the_contract_start_cannot_be_cleared`
  - A5 start not accepted: `test_a_manager_or_admin_corrects_the_contract_start[manager]`
- Gates: ruff, format, mypy strict (203 files), interrogate 100%, import graph, `alembic check`,
  full suite 1470 passed. Drift check: contract and implementation agree.
- /hds-review: no violations.
- /code-review high, seven findings:
  1. **Race on the date check (fixed).** With both dates editable, one request moving the start
     later and another moving the end earlier each passed against the old row and together stored
     a broken pair. The failing test reproduced it in round 0. Fix: `ClientRepository.get_by_id`
     gained `for_update` (with `populate_existing`, so the locked read sees the committed row), and
     `edit_client` locks when either date is sent.
     - The lock created a new deadlock, also reproduced first (round 2): deactivation and delete took
       the portal users and then the client row, while a date edit that deactivates took the client
       row first. Fix: one order everywhere, client row first. `edit_client` also locks on
       deactivation, and `soft_delete` always locks.
     - Tests: `tests/concurrency/test_client_dates_race.py` (the pair race, deadlock races against a
       deactivation and against a delete at 100 rounds each, and the fresh read under the lock), plus
       deterministic unit tests of the lock and its order in `tests/unit/test_client_service.py`.
     - Mutations, all caught: A6 no lock on a date edit; A7 no lock on deactivation; A8 no lock on
       delete; A9 lock without `populate_existing`. The delete race alone caught A8 in only 1 of 3
       runs, so the unit test `test_the_client_row_is_locked_before_any_portal_user[delete]` is the
       reliable catch. A9's first "catch" was the new test failing on its own (it read attributes
       after a rollback), which does not count; it was fixed and re-run.
  2. **Stale route description and docstring (fixed):** they now say "contract dates" and state the
     pair rule.
  3. **Backwards test docstring (fixed).**
  4. **Spec wording against the code (deviation, spec wording clarified).** FR-002 said "sets". The
     code judges only a pair that differs from the stored one. That follows the owner's wording ("an
     edit that does not touch either date") and 002's existing test, which sends the stored end back
     with a rename and expects 200. FR-002 now says "to a value other than the stored one", and
     FR-003 adds "or sends both back as stored". The behaviour is unchanged, so this is a wording
     clarification, not a new requirement.
  5. **`_client()` mock had no date defaults (fixed):** real defaults, `_dated` removed.
  6. **Contract description incomplete (fixed):** it states the unchanged-pair case.
  7. **Commit hygiene (fixed):** the T022 change went in its own commit (`docs(spec): make T022 ...`),
     and T002 to T008 are marked done with this commit.
- Final gates after the fixes: ruff, format, mypy strict (204 files), interrogate 100%, import graph,
  `alembic check`, full suite 1479 passed, concurrency suite 34 passed three times, drift check
  agrees.
