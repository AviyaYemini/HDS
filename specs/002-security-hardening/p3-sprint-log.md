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
- First version: both counts always ran, so a request over one limit did the same work as any other.
  **The code review showed this was wrong and it was reversed**: see finding 1 below.
- The network warning is logged once per window, at the first request over the limit, not on every
  request, so an attacker cannot fill the log.
- The early commit from Group B2 is removed: nothing follows the count in the request any more, and
  the job is queued as an after-commit action, so it only runs if the count was saved.
- `pr4-fix-list.md` still says a new link cancels earlier ones. It is the historical input list, so it
  is left as it was; the spec is the current rule.
- Tasks: T420 to T427 are added to Group C; T359 and T360 are amended by T421.

### Group C, finished (T359 to T364, T420 to T427)

Timing on the final code, pooled engine, as the permanent form of T360: **ratio 1.010** (2.85 ms
unknown, 2.82 ms registered), against 1.58 to 1.74 before and a pass mark of 1.15. The test also
asserts that all 300 mails went out, so the job really ran between samples.

Test changes, and why:
- `test_reset_cancels_links.py`: `test_a_new_request_cancels_the_earlier_reset_link` and
  `test_the_earlier_reset_link_stops_working_as_soon_as_a_new_one_is_requested` asserted the rule the
  owner removed from FR-224. Replaced by `test_a_completed_reset_ends_the_other_reset_link` (the same
  outcome, kept) and `test_a_new_request_takes_no_link_away`.
- `test_reset_request_limit.py`: `test_requests_over_the_limit_leave_the_last_link_working` became
  `test_requests_never_take_away_a_link_already_mailed`, which is stricter: it redeems the first link.
- `test_invitation_service.py`: `test_a_reset_request_cancels_earlier_reset_links_only` became
  `test_issuing_a_reset_link_cancels_nothing`. The five `request_password_reset` tests that looked at
  the account now call `issue_reset_link`, where that work moved, with the same assertions.
- `test_unit_of_work.py`: two calls pass the new argument, no assertion changed, three tests added.
- `cancel_for_user` lost its `purpose` argument: nothing else used it. No test exercised it directly.

Failing first: 22 of 74 tests failed on the new flow before the service change, and 7 before the
review fixes.

Reviews:

`/hds-review`: no principle violated. `plan.md:254` still described the removed cancel (fixed), and
this log needed its section. It did not flag the router decision that the code review found (finding
8 below), and it should have.

`/code-review high` (read-only, nine findings):

| # | Finding | Action |
| --- | --- | --- |
| 1 | A request over the network limit still counted the address, so one client could silently use up any address's allowance | **Fixed, test first. This was my own decision above, and it was wrong.** The network is decided first, and over it no address is counted |
| 2 | No timeout, so a stalled mail provider holds every job slot | Fixed, test first: `RESET_JOB_TIMEOUT_SECONDS` (30). The job is stopped and logged by type; a stored link stays valid and its mail is lost. A new setting, not in the owner's decisions |
| 3 | Over-limit requests queue on one locked row, which could fill the pool | Fixed, test first: over its limit a network only reads its row. Stale rows go before any count lock |
| 4 | Invitation mail also moved after the response | Kept: D17 and T361 ask for it. **ADR 008 now records that a crash after a 202 leaves an invitee with no mail**, which the owner has not explicitly accepted. Raised in the report |
| 5 | `docs/hds-api.md` still described the old rules | Fixed |
| 6 | A failed job logged as "action action" | Fixed, test first: the job is a named function |
| 7 | The limiter is copied from `core/passwords.py` | Skipped: about 20 lines, no defect. Sharing it would touch the hashing path for no change in behaviour |
| 8 | The router decided whether to queue, against Principle II, although T423 said it should | Fixed: `request_password_reset` decides and calls a `queue_job` it is given, so the rule has unit tests |
| 9 | Stale comments | Fixed |

Differs from the tasks and the first design:
- T418's "purge on every request" is now every request that adds a row. A network over its limit adds
  none, and the purge would only be load during a flood.
- A network over its limit answers a little faster than one within it. That shows only that network's
  own state, which the client knows from its own count, and nothing about any address.
- T423 said the router queues the job. The service decides it, as finding 8 requires.
- `RESET_JOB_TIMEOUT_SECONDS` is an addition from the review.

Mutation results on the final code, all caught on `hds_p3_test`, files restored. Every run names a
failing test, and the harness now has a timeout per mutation:

| Id | Mutation | Caught by |
| --- | --- | --- |
| X1 | After-commit actions before the response (T363) | `test_the_response_is_complete_before_delivery_starts` |
| X2 | Job started inside the request | `test_nothing_looks_at_the_address_before_the_response` |
| X3 | Job queued over a limit | `test_the_fifth_of_five_still_sends_and_the_sixth_does_not` |
| X4 | Job queued only for a registered address | `test_nothing_looks_at_the_address_before_the_response` |
| X5 | Cap not enforced | `test_past_the_running_and_waiting_cap_a_job_is_dropped_and_logged_by_type` (first run was invalid, see below) |
| X6 | No concurrency limit | `test_no_more_than_the_limit_run_at_once` |
| X7 | Address in the drop log | `test_past_the_cap_a_job_is_dropped_and_the_answer_stays_202` (not caught at first, see below) |
| X8 | Cancel restored | `test_a_new_request_takes_no_link_away` |
| X9 | No re-read after the lock | `test_a_reset_job_stops_if_the_account_changed_while_it_waited_for_the_lock` |
| X10 | Link before the user lock | `test_issuing_a_reset_link_takes_the_users_row_before_the_link` |
| X11 | Network key without its prefix | `test_it_counts_every_address_from_one_network_together` |
| X12 | A limit of 0 does not turn it off | `test_the_last_within_the_limit_sends_and_the_next_does_not` |
| X13 | Every limit off | `test_the_fifth_of_five_still_sends_and_the_sixth_does_not` |
| X14 | Warning on every request over | `test_parallel_requests_over_the_network_limit_log_one_warning` |
| X15 | No warning | `test_one_warning_per_window_with_no_address_in_it` |
| X16 | Job uses the request's session | `test_the_job_uses_its_own_session_from_the_factory` |
| X17 | Network limit not decided | `test_the_last_within_the_limit_sends_and_the_next_does_not` |
| X18 | Address limit not decided | `test_the_fifth_of_five_still_sends_and_the_sixth_does_not` |
| X19 | Network count equal to the limit is over | `test_the_last_within_the_limit_sends_and_the_next_does_not` |
| X20 | No purge on this path | `test_each_request_removes_stale_rows_and_never_a_live_lock` |
| X21 | Any `locked_until` counts as locked | same test |
| X22 | Raw address as the key | `test_a_registered_and_an_unregistered_address_answer_alike_at_every_step` |
| X23 | Letter case not folded | `test_a_reset_request_is_counted_under_the_folded_address` |
| X24 | Job skips its unit of work | `test_the_response_is_complete_before_delivery_starts` |
| X25 | Re-read ignores `is_active` | `test_a_reset_job_stops_if_the_account_changed_while_it_waited_for_the_lock` |
| X26 | Over-limit request writes | `test_over_the_limit_a_request_only_reads_so_a_flood_takes_no_lock` |
| X27 | Address counted over the network limit | `test_over_the_limit_no_address_is_counted_so_its_allowance_is_not_used_up` |
| X28 | Read ignores the end of the window | `test_the_count_restarts_after_the_window` |
| X29 | Purge after the count locks | `test_a_reset_request_removes_stale_rows_before_it_takes_a_count_lock` (not caught at first, see below) |
| X30 | No job timeout | `test_a_job_that_hangs_is_stopped_and_gives_its_place_back` |
| X31 | Address counted at the crossing request | `test_over_the_limit_no_address_is_counted_so_its_allowance_is_not_used_up` (not caught at first, see below) |

What the first runs showed:
- X5 hung and then counted as caught with no failing test. A hang is not a catch: the test now bounds
  its wait, and the harness reports a hang as a problem.
- X7 was not caught, because the log filter masks addresses (`n***@hds.test`), so the test's "address
  not in the message" passed. That filter is a useful second layer, but the test now asserts the exact
  message.
- X29 ran with the network limit off, so it could not see the network count. The test now has it on.
- X31 found that no test looked at the address of the exact request that crossed the limit.
- The new timeout test first passed for the wrong reason, because an outer `wait_for` raises the same
  error. It now bounds the elapsed time.
- `RESET_JOB_TIMEOUT_SECONDS` was first a `float`. The static money check refused it, which is a fair
  rule to keep, so it is an `int`.

Gates: ruff, format, mypy strict, interrogate, import graph and alembic check pass.

## Group D: bulk purge, settings and documents (T365 to T368, and T349's outstanding text)

Done: `purge_expired_records`, the 503 text in all six operations' descriptions, ADR 007, the
deployment note in `infra/README.md`, and `docs/hds-api.md`. T366 needed no new work: its settings and
the proxy note were already added.

Decisions taken here:
- The script works in batches of 1000 with `SKIP LOCKED`, each in its own transaction. T365 did not ask
  for this. A single bulk delete could deadlock with revoke-all, because both lock many token rows in
  no fixed order.
- The real behaviour is tested on PostgreSQL in `tests/integration/test_purge_expired_records.py`, not
  only in the unit file T365 named: what matters is what the SQL deletes.

Test changes, and why:
- `test_employee_manager.py` (2 tests) and `test_client_service.py` (1 test) mock `create_if_absent`
  instead of `create`, because the services call it now. No assertion changed.

Failing first: 6 cases for the 503 text, the purge tests at import, then 3 parallel creates and the
partial report.

Reviews:

`/hds-review`: interrogate failed on three nested functions (fixed), and ADR 007 cited the wrong
requirement for the network limit (fixed).

`/code-review high` (read-only, ten findings, none reproduced at runtime by the reviewer):

| # | Finding | Action |
| --- | --- | --- |
| 1 | An unknown address's count is removed after the retention while a registered one never decays, so "the same rule" and my new text "cannot tell them apart" are false | **Not fixed, owner to decide.** See below. The false sentence is corrected in `docs/hds-api.md`, and ADR 007 states the gap |
| 2 | ADR 007 said every hashing path locks and re-reads. The create paths do not | ADR corrected. The review's two examples: two creates of one address answered 422, not the documented 409: **fixed, test first**. A client deactivated during the hash: not fixed, because the spec accepts this window; the ADR says the queue widens it |
| 3 | A test fixed its clock at import, so it would fail after five minutes | Fixed: times are computed in each test |
| 4 | Cleanup changes a token's answer from `auth.token_expired` to `auth.invalid_token` | Already stated in D19; added to `docs/hds-api.md` |
| 5 | No index on `expires_at`, so each batch can scan the table | Not fixed. It would need a migration T339 did not plan, and the script is optional. Raised in the report |
| 6 | Counts printed only at the end, and failure not documented | Fixed, test first: each count prints when known |
| 7 | The 503 sentence is repeated in 12 places | Skipped as cosmetic. T349 asked for the description text |
| 8 | `RETURNING` only to count rows, on hot paths too | Fixed: `rowcount`, as `repositories/base.py` does |
| 9 | `delete_expired` is copied between two repositories | Skipped as cosmetic |
| 10 | The 503 behaviour was not marked Proposed | Fixed |

Finding 1, why this was not a stop:
- The stop rule covers a security property that regresses. This one did not regress here: the gap has
  existed since Group B, when sign-in began removing stale rows, and it was missed then. The script
  does the same thing in bulk. **So Group B's "equal answers" was not fully true, and I reported it as
  if it were.**
- Each way of closing it is the owner's: a time limit on the account's count needs a rule change and
  probably a migration, and keeping every row for ever gives up the bound on the table.
- It is slow: each probe waits over 24 hours and costs failures on a real account.

A rule was broken in this group: the code review ran `uv run pytest tests/integration` with no
`TEST_DATABASE_URL`, so it used the shared `hds_test`, against the sprint rule. It was the review
agent's own run, and my brief did not tell it the rule. Afterwards `hds_test` had no connections and
0 users, organisations and throttle rows. The Group C review may have done the same; it reported not
running tests.

Mutation results, all caught on `hds_p3_test`, files restored. D5 and D7 first "passed" only because a
bug in the new test failed on the unmutated code, so the whole set was rerun after the fix:

| Id | Mutation | Caught by |
| --- | --- | --- |
| D1 | Live refresh tokens deleted | `test_refresh_tokens_past_their_expiry_go_and_the_rest_stay` |
| D2 | Live links deleted | `test_links_go_only_by_their_expiry` |
| D3 | Spent links deleted inside their lifetime | same test |
| D4 | Spent links kept past their expiry | same test |
| D5 | One batch only | `test_it_works_through_more_rows_than_one_batch_and_prints_only_counts` |
| D6 | Cut-off ignores the retention | `test_the_throttle_cut_off_is_the_retention_before_now` |
| D7 | Live lock removed in bulk | `test_stale_unlocked_throttle_rows_go_and_a_live_lock_stays` |
| D8 | Lost address ignored, employee | `test_two_creates_of_one_address_answer_201_and_409` |
| D9 | Lost address ignored, client user | `test_a_client_user_that_loses_the_address_during_the_hash_is_a_duplicate` |
| D10 | Counts printed only at the end | `test_a_failure_part_way_still_reports_what_was_already_removed` |

Gates: ruff, format, mypy strict, interrogate, import graph and alembic check pass.

A process note: the first commit of this group went in with one unit test failing, because `tail` hid
pytest's exit status from the `&&` chain. It was amended before any further work. Later commits read
pytest's own status.

## Group E: the contract records error responses, and the drift check compares them (T409 to T412)

`surface` now compares every status code. `openapi.yaml` matches the generated document on all 46
operations: 73 codes were added and 2 of them taken back out (below). Nothing the contract already
declared was removed. The in-process contract suite passes 47 of 47 with the narrower sets.

Corrections to the application's declarations instead of copying them into the contract (T411):

| Code | Removed from | Why |
| --- | --- | --- |
| 409 | `POST /auth/logout`, `POST /auth/logout-all`, `PATCH /me`, `PUT /employees/{id}/groups`, `PUT /employees/{id}/qualifications`, `DELETE /groups/{id}`, `DELETE /qualifications/{id}`, `POST /clients`, `PATCH /clients/{id}`, `DELETE /clients/{id}/users/{user_id}`, `POST /sites`, `PATCH /sites/{id}`, `POST /projects`, `PATCH /projects/{id}`, `DELETE /projects/{id}` | Nothing behind them raises a conflict. Fifteen operations in all, and 409 is now a list, `_CONFLICTS`, of the 12 that can |
| 403 | `POST /auth/logout`, `POST /auth/logout-all`, `GET /me` | `get_current_user` only raises 401, and these are open to every role |
| 422 | `POST /auth/logout-all`, `GET /me`, `GET /groups`, `GET /qualifications`, `GET /sites` | They take no input to validate |

Also added: `Retry-After` on the generated 429 and 503 (the handlers send it, and the document did
not say so), and four shared response components in the contract, of which one, `Locked`, was unused
and was removed again.

Decisions taken here:
- `_CONFLICTS` is a list in `core/openapi.py`, where T349 keeps error declarations. The code review
  suggested declaring 409 per route, which is a larger change with the same result for now.

Reviews:

`/hds-review`: no violations.

`/code-review high` (read-only; its test runs used `hds_p3_test`, as it was told; nine findings):

| # | Finding | Action |
| --- | --- | --- |
| 1, 2 | `DELETE /projects/{id}` and `DELETE /clients/{id}/users/{user_id}` were listed as conflicts, and nothing raises one | **Fixed, and my audit was the cause.** It matched called methods by name across classes, so `soft_delete` on projects inherited the conflict from employees. Redone per class, which confirmed the other 12 |
| 3 | A client deactivated during the hash can still get a portal user | Not fixed: `spec.md:576` accepts this window. Second review to raise it, so it goes in the report |
| 4 | The test's list copied `_CONFLICTS`, so it could not catch a wrong entry | Fixed: the test now reads each service's own code and pairs every route with the method behind it |
| 5 | Nothing ties `_CONFLICTS` to the services for a new route | Partly met by finding 4's test. A new route must still be added by hand |
| 6 | No index on `expires_at` | Same as Group D finding 5: not fixed, needs an unplanned migration, in the report |
| 7 | `Locked` component unused | Fixed: removed |
| 8 | Generated 429 and 503 had no `Retry-After` | Fixed, test first |
| 9 | Duplicate `users.duplicate_email` blocks | Skipped as cosmetic |

Mutation results, all caught on `hds_p3_test`, files restored:

| Id | Mutation | Caught by |
| --- | --- | --- |
| E1 | Drift check compares 2xx only again | `test_an_error_code_only_the_contract_declares_is_reported` |
| E2 | Blanket 409 on every write | `test_nothing_behind_this_operation_raises_a_conflict` |
| E3 | A real conflict route dropped | `test_this_operation_can_answer_a_conflict` |
| E4 | 403 on every signed-in route | `test_any_signed_in_user_may_call_this_so_it_never_answers_403` |
| E5 | 422 on every operation | `test_an_operation_with_no_input_cannot_fail_validation` |
| E6 | No 422 from us | `test_every_operation_with_input_can_fail_validation` (not caught at first: FastAPI adds its own 422 in another format, so the test now checks the format) |
| E7 | 503 dropped | `test_the_response_is_declared_and_the_description_explains_it` |
| E8 | False conflict route listed | `test_nothing_behind_this_operation_raises_a_conflict` |
| E9 | No `Retry-After` declared | `test_a_retry_answer_declares_its_header` |

Gates: ruff, format, mypy strict, interrogate, import graph and alembic check pass.
