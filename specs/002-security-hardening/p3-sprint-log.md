# P3 sprint log (Groups B2, C, D and E, then the P3 checkpoint)

Decision log for the sprint requested on 2026-11-03. For each step: decisions taken without asking
and why, review findings and what happened to them, every mutation result, and anything that differs
from the tasks. Tests ran in the private database `hds_p3_test`, after checking it had no other
connections.

## Step 0: Money takes plain decimal strings only

Decisions already made by the owner: use a `BeforeValidator` in the `_iso_string_only` style, accept
only ASCII digits in the schema's shape, let a leading minus and zero through.

Decisions taken here:
- The shape limits (8 integer digits, 4 decimals) are enforced by the same pattern, so `123456789`
  is refused by the validator rather than by the digit count. Both answer 422
  `validation.request_invalid`, so a caller sees no difference.
- `True` is in the refused list as well: it is a JSON boolean and is not a string.

Test changes, and why:
- `test_json_numbers_parse_into_exact_decimals` in `tests/unit/test_time_money_invariants.py` asserted
  the behaviour being removed. It is split into `test_decimal_strings_parse_into_exact_decimals`
  (same four values, now as strings, same exactness assertion) and
  `test_json_numbers_are_refused_for_money`. This replaces the rule it pinned, on the owner's decision.

Failing first: 30 cases failed before the change (numbers and malformed strings answered 201).

Mutation results, all caught, files restored:

| Id | Mutation | Caught by |
| --- | --- | --- |
| K1 | Numbers accepted again | `test_an_employee_rate_outside_the_schema_is_refused[75]` |
| K2 | Any string accepted | same test, `'1e3'` |
| K3 | `\d` instead of ASCII digits | same test, `'٣'` |
| K4 | `match` instead of `fullmatch` | same test, `'1e3'` |
| K5 | No leading minus | `test_zero_and_negative_reach_the_services_and_keep_their_own_codes[-5]` |

Review: not run on this commit alone. The instruction applies reviews to each group, so this diff is
reviewed with B2.

### Correction: the mutation harness used the shared database

The scratch script `mut.py` hard-coded `hds_test`, so every mutation run in this feature until now,
including K1 to K5 above, ran on the shared database and not on `hds_p3_test`. That broke the rule
for this sprint. It is corrected, and K1 to K5 were rerun on `hds_p3_test` with the same result.

## Step 0b: where the stray duplicate-key row came from (decision 3)

The Group B mutation J1 failed at setup with a duplicate-key error on `uq_users_email_ci`.

- No test leaks rows: after each of the four relevant test files, `hds_p3_test` holds no users or
  organisations.
- The run was on `hds_test` (see the correction above), not on a private database.
- `hds_test` is empty and has no other connections now, so the row was short-lived.

Conclusion: most likely another session's test run was using `hds_test` at that moment, which is the
collision F26 describes. The owner had noted an unexplained pytest process. This is the most likely
cause, not a proven one: the row is gone, so it cannot be inspected. The mechanism does explain the
symptom, and my runs no longer touch that database. F26 (P6) removes the possibility for good.

## Step 0c: HMAC keys in the throttle table (decision 2)

Decisions taken here:
- New optional setting `THROTTLE_SECRET_KEY`. Unset, a key is derived from `JWT_SECRET_KEY` by HMAC
  with a fixed label, so the signing secret has no second use. It is not required, because a new
  required setting would break the documented one-line quickstart and every existing deployment for
  a value that can be derived safely. Set, it must be at least 32 bytes outside development, the
  same bar as the signing secret.
- Consequence, recorded in the README: changing either secret restarts every count. A lock can
  therefore end early on a secret rotation. Accepted, because rotation is rare and deliberate.
- The compose file does not pass the setting, so the Docker stack uses the derived key.

Text corrected: `services/throttle.py`, `repositories/login_throttle.py`, `models/login_throttle.py`,
`research.md`, `data-model.md`, `plan.md`, `contracts/error-codes.md`. They now say a copy of the
table alone cannot confirm an address, and that the keys are protected, not anonymous.

Differs from the instruction: migration 018's docstring still says "Only hashes are stored". It is
committed, and the sprint rules forbid editing committed migrations. The statement stays true.

Failing first: all new tests failed (`hash_key` took one argument, `throttle_key` did not exist).

A process note: the first commit attempt of this step was chained after a script that had failed, so
it went in without this log section and with one lint error. It was amended before any further
work. Later steps do not chain a commit after a step that can fail.

Mutation results, all caught on `hds_p3_test`, files restored:

| Id | Mutation | Caught by |
| --- | --- | --- |
| L1 | Plain SHA-256 again | `test_the_key_is_not_a_plain_hash_of_the_value` |
| L2 | The signing secret used directly | `test_without_a_setting_the_key_is_derived_and_never_the_signing_secret` |
| L3 | The setting ignored | `test_the_configured_secret_is_used` |
| L4 | Short secret allowed | `test_a_short_secret_is_refused_outside_development` |
