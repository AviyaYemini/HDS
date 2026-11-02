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

## Group B2: limit reset requests per address (T417 to T419, F28, FR-247)

Decisions taken here:
- `RESET_REQUEST_LIMIT_PER_ADDRESS` is 0 in `tests/conftest.py` by assignment, the same as the network
  limit. `tests/integration/test_reset_request_limit.py` turns it on for itself.
- `purge_stale` runs on every request, including at a limit of 0, because T418 says every request.
- `lock_until` is passed to `register_failure` although the repository ignores it for windowed kinds.
  The parameter is required, and a separate signature was not worth adding.

Test changes, and why:
- `tests/unit/test_invitation_service.py`: `_svc()` passes the two new constructor arguments. No
  assertion changed.

Failing first: 8 of the 9 tests in `test_reset_request_limit.py` failed once the setting existed, on
behaviour (6 mails, no rows). The ninth, a limit of 0, passes before and after by design. The three
follow-up unit tests failed before their fixes.

Reviews, covering Step 0 (Money), Step 0c (HMAC) and this group:

`/hds-review`:

| Finding | Action |
| --- | --- |
| `mypy --strict` failed at `invitation.py:305` (`no-any-return`) | Fixed: `limit: int` |
| `test_invitation_service.py` did not cite FR-247 | Fixed |
| `spec.md:548` named a technology ("the Argon2 check") | Fixed: "the password check". This is wording in a known-limitation note, not a requirement |

`/code-review high` (read-only, ten findings):

| # | Finding | Action |
| --- | --- | --- |
| 1 | No commit after counting, so a deadlock with a failed sign-in | Scenario is wrong: `register_failed_login` commits on its own, so a failed sign-in does not hold the user's row while it upserts. The commit is still right, for findings 6 and for keeping the count if a later step fails, so it was added, test first. A race test with both limits on was added too |
| 2 | Under the limit, an attacker can still replace a link every 12 minutes | **Not fixed, owner to decide.** Left as designed: see below. The docstring claim is corrected |
| 3 | No network limit on this path, so random addresses add rows for 24 hours | **Not fixed, owner to decide.** See below. The false "cannot grow faster than it is cleaned" text is corrected in `research.md` and `data-model.md` |
| 4 | A registered address does more work, so timing can differ | Group C scope (T359 to T361), then measured by T360 |
| 5 | No re-read after `lock_user`, so a deactivation that wins the lock still gets a link | Fixed, test first, as the re-invitation path does |
| 6 | The upsert's lock is held for the whole request | Fixed by the commit in finding 1. Over-limit requests still write the row: skipped, it is a small bounded cost |
| 7 | Nothing tests the limit under parallel requests | Fixed: `tests/concurrency/test_reset_request_races.py` |
| 8 | `data-model.md` column rules were stale for reset rows | Fixed, including "restarts at 0", which was already wrong (the code restarts at 1) |
| 9 | Key folding and purge lines are repeated | Skipped as cosmetic. T418 places the code in `invitation.py` |
| 10 | Naming, the `count = 0` starting value | Skipped as cosmetic |

Findings 2 and 3, why this was not a stop:
- The stop rule covers a fix that changes a requirement. Their root fixes would (keeping earlier links
  contradicts "only the latest link works"; a network limit on this path is not in FR-247 and D16 says
  there is none). FR-247 as written, the 5-per-hour limit and "the 5th of 5 still sends" are the
  owner's decisions, and the code meets them, so there is no defect to fix inside the spec.
- These are residual risks, not failures of the requirement, so I continued. One sentence of FR-247
  does overstate the result: "a third party cannot keep cancelling a victim's links". Under the limit
  they can replace a link about every 12 minutes, though each replacement mails the owner a working
  link, and over the limit the last link stands. The bound on the table is the request rate times
  `LOGIN_THROTTLE_RETENTION_HOURS`, not a fixed size. **spec.md was not changed. Both are raised in the
  final report for the owner to decide.**

Differs from the tasks:
- T418 has the unit of work save the count. It is committed early instead, and the account is read
  again after the lock. Both come from the review.

Mutation results on the final code, all caught on `hds_p3_test`, files restored:

| Id | Mutation | Caught by |
| --- | --- | --- |
| R1 | Limit check removed | `test_the_fifth_of_five_still_sends_and_the_sixth_does_not` |
| R2 | Links cancelled before the check | `test_requests_over_the_limit_leave_the_last_link_working` |
| R3 | Raw address as the key | `test_a_registered_and_an_unregistered_address_answer_alike_at_every_step` |
| R4 | Count equal to the limit is over | `test_the_fifth_of_five_still_sends_and_the_sixth_does_not` |
| R5 | No purge on this path | `test_each_request_removes_stale_rows_and_never_a_live_lock` |
| R6 | Any `locked_until` counts as locked | same test |
| R7 | Letter case not folded | `test_a_reset_request_is_counted_under_the_folded_address` (not caught at first: the `Email` schema type already folds case over HTTP, so a service-level test was added) |
| R8 | A limit of 0 still counts | `test_a_limit_of_zero_turns_it_off` |
| R9 | Check after the lookup | `test_a_registered_and_an_unregistered_address_answer_alike_at_every_step` |
| R10 | No commit after counting | `test_a_reset_request_saves_its_count_before_it_touches_the_account` |
| R11 | No re-read after the lock | `test_a_reset_request_stops_if_the_account_changed_while_it_waited_for_the_lock` |
| R12 | Re-read ignores `is_active` | same test |
| R13 | Count never decides | `test_parallel_requests_send_no_more_mail_than_the_limit` |

Gates: ruff, format, mypy strict, interrogate, import graph and alembic check pass; OpenAPI drift check
passes in its current form.

## Group C: stopped at the reset timing test (T359 to T361, F06, FR-228, SC-212)

**Status: stopped under the rule "the opt-in timing test fails". Not committed. Work is in the tree.**

Done, uncommitted:
- T359 `tests/integration/test_delivery_after_response.py`. The ordering test failed first, because no
  response had been sent when delivery started. It passes after the change, through the real
  middleware, using the direct ASGI harness. The failed-commit and failed-delivery tests passed before
  and after: they pin behaviour that must not change.
- T360 `tests/integration/test_reset_request_timing.py`. **It fails.**
- T361 `get_unit_of_work` queues one background task after a successful commit. `unit_of_work` without
  a request still runs actions at once. `tests/unit/test_unit_of_work.py`: two calls pass the new
  argument, no assertion changed, three tests added.

Not done: T362 (ADR 008, which should record the outcome of this decision), T363, T364, reviews, and
Groups D and E.

The test keeps the count on, with a high limit, as production runs it.

| Measurement | Unknown | Registered | Ratio | Pass mark |
| --- | --- | --- | --- | --- |
| Test as written, three runs (a new connection per request) | 28 ms | 33.6 ms | 1.20 | 1.15 |
| Without the early commit from Group B2, two runs | 20.3 ms | 24.7 ms | 1.22 | 1.15 |
| Pooled engine, as in production, two runs (temporary test edit, restored) | 3.1 to 3.7 ms | 5.4 to 5.9 ms | 1.58 to 1.74 | 1.15 |
| Login timing test, for comparison | 63.6 ms | 69.1 ms | 1.086 | 1.15 |

Explained:
- The cause is not delivery, which now runs after the response, and not the early commit from B2:
  removing it changes the ratio by 0.02. The registered path does more database work: `lock_user`, the
  re-read, `cancel_for_user`, the token insert and the commit that saves them.
- The re-read added from the B2 review is a small part of this; the rest is what D17 calls the
  residual, plus the lock and the cancel.
- D17 expected the residual to be hidden as in sign-in. It is not: sign-in has about 55 ms of password
  check around the same gap, and this path has none. About 15 of the 20 ms baseline in the test as
  written is opening a connection, so the true ratio is the pooled one.
- The login test is unaffected. Its property holds at 1.086.

Not attempted, because each option needs the owner's decision: the plan rejected a dummy write and a
delay, and moving the account work out of the request is a design change from D17.

### Resumed: the owner's four decisions

Recorded in the spec before any code (T420): the request only counts and answers (FR-228), SC-212 is
measured on a pooled engine, a request no longer cancels links (FR-224), and reset requests get a
network limit (FR-247).

Decisions taken here, without asking:
- `RESET_JOB_CONCURRENCY` 2 and `RESET_JOB_QUEUE_LIMIT` 100. The owner asked for a cap on running and
  waiting jobs and set no values. Two matches the hashing limit and keeps most connections free. The
  queue is long because a dropped job is a lost mail, and a waiting job costs almost nothing.
- Both counts always run, even when the first is over. A request over one limit then does exactly the
  same work as any other. It costs nothing extra: a blocked network has already had its chance at the
  address limit.
- The network warning is logged once per window, at the first request over the limit, not on every
  request, so an attacker cannot fill the log. It still appears in the first over-limit request.
- The early commit from Group B2 is removed: nothing follows the count in the request any more, and
  the job is queued as an after-commit action, so it only runs if the count was saved.
- `pr4-fix-list.md` still says a new link cancels earlier ones. It is the historical input list, so it
  is left as it was; the spec is the current rule.
- Tasks: T420 to T427 are added to Group C; T359 and T360 are amended by T421.
