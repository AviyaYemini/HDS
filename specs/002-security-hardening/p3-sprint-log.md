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
