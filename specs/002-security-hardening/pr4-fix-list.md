# PR 4 fix list (002-security-hardening)

Input for the Spec Kit flow. It collects every open issue from the code review of branch
`002-security-hardening` at commit `704024b` and the CodeRabbit comments on
[PR 4](https://github.com/AviyaYemini/HDS/pull/4) that were checked and kept.

Every item was re-checked against `704024b`. All 26 are open.

- Every item below is **Proposed**: none comes from the original product spec.
- "Required behaviour" states what must be true. "Plan note" is implementation input for the plan
  stage only and is not a requirement.
- Evidence labels:
  - **Reproduced**: shown through the real API, or by running the code.
  - **Measured**: timed or probed directly.
  - **Code reading**: found by reading the code, not reproduced.
- Spec 002 currently ends at FR-220, SC-209 and T304, and every task in `tasks.md` is checked.

## Summary

| Group | Items | Meaning |
| --- | --- | --- |
| A | F01 to F05 | Fix before merging |
| B | F06 to F14 | Fix in this PR; lower risk or narrower |
| C | F15 to F16 | Documentation corrections in this PR |
| D | F17 to F26 | Older gaps not introduced by this PR; in scope only if chosen |

## Decisions needed at the clarify stage

| Id | Question | Recommendation | Affects |
| --- | --- | --- | --- |
| D1 | What happens when staff deactivate a pending account? | Refuse it, or keep the account pending with its link cancelled. Either way it must never become reactivatable | F03 |
| D2 | Should an expired rotated credential still count as theft? This changes FR-209 | No: refuse it and end nothing | F08 |
| D3 | Should a completed password reset clear an active lockout? | Yes: the user has proved control of the mailbox | F11 |
| D4 | Is rate limiting on sign-in in scope? Spec 002 excludes it, but `docs/hds-api.md` lists it | Keep it out of this PR, plan it as its own feature, and correct the document | F04 |
| D5 | May a Client portal user be converted to staff? | No: refuse any role change on a Client account | F23 |
| D6 | Must a client's contract end date be on or after its start date? | Yes | F24 |

## Group A: fix before merging

### F01. Older reset links still work after a completed reset

- **Severity:** High. **Evidence:** Reproduced. **Source:** code review.
- **Problem:** completing a password reset spends only the link that was used. In the test, the
  owner reset their password with the newer of two links, then the older link set a different
  password and sign-in worked with it. Someone holding an earlier link keeps control for up to 60
  minutes after the owner believes the account is secure.
- **Required behaviour:** when a password reset completes, every other outstanding reset or
  invitation link of that account stops working. Issuing a new reset link also cancels earlier
  unused reset links of that account.
- **Acceptance:** after one link completes a reset, any older link is refused with
  `auth.token_used` and the password does not change. A new test fails before the fix.
- **Plan note:** `reset_password` in `apps/api/src/hds/services/invitation.py` (line 263) does not
  call `cancel_for_user`. `request_password_reset` (line 222) does not cancel earlier links.

### F02. Portal sessions survive client deactivation

- **Severity:** High. **Evidence:** Reproduced. **Source:** code review.
- **Problem:** deactivating a client deactivates its portal users but leaves their refresh tokens
  live. In the test, a Manager reactivated the user while the client was still inactive, and the
  refresh token issued before deactivation worked again.
- **Required behaviour:** deactivating or deleting a client, or removing a portal user, ends that
  user's sessions, as deactivating an employee does. A portal user cannot be reactivated while
  their client is inactive or deleted.
- **Acceptance:** a pre-deactivation refresh token stays refused after any later reactivation.
  Reactivating a portal user of an inactive client is refused with a dedicated, translated error
  in English and Hebrew.
- **Plan note:** `apps/api/src/hds/services/client.py`: `_deactivate_portal_users` (line 45) and
  `remove_client_user`. `ClientService` has no refresh-token repository. Use revocation reason
  `account`. `remove_client_user` also skips link cancellation (FR-207).

### F03. Deactivate then reactivate activates a pending account

- **Severity:** High. **Evidence:** Reproduced. **Source:** code review. **Needs:** D1.
- **Problem:** reactivating a pending invitee is refused with 409, but deactivating them first
  stamps `activated_at`, and reactivation then succeeds. This breaks FR-220, which says only
  accepting an invitation activates a pending account. The record also shows an activation that
  never happened, and the invitee can no longer accept or be re-invited.
- **Required behaviour:** no sequence of staff actions activates an account that never accepted
  its invitation. `activated_at` stays empty until a real activation.
- **Acceptance:** deactivate followed by reactivate on a pending account never leaves it active.
  The same holds when the pending account is deactivated through its client.
- **Plan note:** `apps/api/src/hds/services/employee.py` line 360 and
  `apps/api/src/hds/repositories/client.py` (`deactivate_client_users`, the `coalesce`).

### F04. Password hashing blocks the event loop for anonymous callers

- **Severity:** High. **Evidence:** Measured. **Source:** code review. **Needs:** D4.
- **Problem:** Argon2 runs synchronously inside async handlers. The timing fix means every sign-in
  for an unknown address now pays that cost, and such addresses never lock. One check took about
  36 ms, and 20 concurrent unknown-address sign-ins stalled a test event loop for 724 ms. Anonymous
  callers can therefore slow every other request in a worker.
- **Required behaviour:** password hashing and verification do not block other requests while they
  run. Concurrent hashing work is bounded. Equal work for known and unknown addresses (FR-214) is
  preserved.
- **Acceptance:** a test shows other requests progress during a burst of unknown-address sign-ins.
  SC-207 still passes.
- **Plan note:** run `verify_password`, `verify_against_dummy` and `hash_password` off the event
  loop behind a small limit. Callers are in `services/auth.py`, `services/invitation.py`,
  `services/employee.py` and `services/client.py`.

### F05. Log redaction leaks the end of a value containing a quote

- **Severity:** Medium. **Evidence:** Reproduced. **Source:** CodeRabbit.
- **Problem:** the redaction pattern stops at an escaped quote.
  `{"password": "ab\"cd"}` is logged as `{"password": "[REDACTED]"cd"}`, so part of the secret
  stays in the log.
- **Required behaviour:** no part of a sensitive value survives redaction, whatever characters it
  contains.
- **Acceptance:** a test with values containing `"`, `'` and `\` finds no fragment of the secret in
  the output. Redacting twice changes nothing further.
- **Plan note:** `_SENSITIVE_PAIR` in `apps/api/src/hds/core/logging.py` (line 22). Make the value
  group consume backslash escapes and stop only at an unescaped delimiter.

## Group B: fix in this PR

### F06. Reset-request timing reveals registered addresses

- **Severity:** Medium. **Evidence:** Code reading. **Source:** code review.
- **Problem:** after-commit mail runs before the response, so a request for a registered active
  address does extra work (a token write, a commit and delivery) that an unknown address skips. The
  endpoint promises it cannot reveal who has an account. The gap is a file write today and becomes
  a network call once a real mail provider exists.
- **Required behaviour:** `/auth/password-reset/request` takes the same observable time for known
  and unknown addresses. A slow mail provider does not hold the request open.
- **Acceptance:** a test shows delivery does not run before the response. A timing check like
  SC-207 passes for this endpoint.
- **Plan note:** `apps/api/src/hds/core/database.py` line 121 and `get_unit_of_work`. Run delivery
  after the response, or through a queue.

### F07. Migration 014 can mark a deactivated account as pending

- **Severity:** Medium. **Evidence:** Code reading. **Source:** code review.
- **Problem:** the backfill treats an account as pending if it is inactive, has an invitation link
  and has never used one. A staff-created employee who was deactivated and re-invited under the old
  code matches, so their leftover link can reactivate them. A used reset link proves the account
  was active, but the rule ignores it.
- **Required behaviour:** an account with any evidence of past activity is never treated as pending
  after migration.
- **Acceptance:** a migration test covers an inactive account with an unused invitation and a used
  reset link, and it ends up deactivated, not pending.
- **Plan note:** migration 014 is committed and the project hook blocks editing its code, so this
  needs a new data migration (017). Rows that can't be told apart should be reported, not guessed.

### F08. Old rotated tokens can repeatedly sign a user out

- **Severity:** Medium. **Evidence:** Code reading. **Source:** code review. **Needs:** D2.
- **Problem:** the revoked check runs before the expiry check, and revoked rows are never removed.
  Anyone holding one rotated token, even months old, can replay it as often as they like, and each
  replay ends every session of that user.
- **Required behaviour:** a credential past its expiry is refused and ends no sessions. Expired
  credential records are cleaned up. FR-209 is updated to say so.
- **Acceptance:** replaying an expired rotated token returns `auth.token_expired` and leaves other
  sessions live. A replay of an unexpired rotated token still ends them.
- **Plan note:** `refresh` in `apps/api/src/hds/services/auth.py` line 171.

### F09. A refresh in flight can outlive "end every session"

- **Severity:** Medium. **Evidence:** Code reading. **Source:** code review.
- **Problem:** a refresh inserts its new token in an uncommitted transaction. A concurrent password
  reset, sign-out-everywhere or theft response cannot see that row, so the new token stays live for
  up to 7 days. The window is narrow.
- **Required behaviour:** once "end every session" completes, no credential issued before or during
  it remains usable.
- **Acceptance:** a concurrency test in the style of `tests/concurrency` runs a refresh against
  each revoke-all path and finds no live token afterwards.
- **Plan note:** take a lock on the user row in both refresh and revoke-all, so one waits for the
  other (`services/auth.py` line 202 and `repositories/refresh_token.py`).

### F10. The time-zone check accepts non-IANA names

- **Severity:** Medium. **Evidence:** Measured. **Source:** code review.
- **Problem:** the check loads a file instead of consulting the zone list. On macOS `posixrules`,
  `Factory`, `asia/jerusalem` and `ASIA/JERUSALEM` were all accepted. The wrong-case names work only
  because that filesystem ignores case, so a saved value can fail on the Linux API image.
- **Required behaviour:** only canonical IANA zone names are accepted, with the same result on
  every platform.
- **Acceptance:** those four names return 422 `sites.timezone_invalid`, and valid zones still pass.
  `check_site_timezones` reports stored values that fail the new rule.
- **Plan note:** `validate_timezone` in `apps/api/src/hds/services/site.py` line 33. Check against
  `zoneinfo.available_timezones()`, excluding `Factory` and `posixrules`.

### F11. A completed reset does not clear a lockout

- **Severity:** Low. **Evidence:** Reproduced. **Source:** code review. **Needs:** D3.
- **Problem:** after five failures, a successful password reset still leaves sign-in returning 423
  with the new password for up to 15 minutes.
- **Required behaviour:** a completed reset clears the lock and the failure count.
- **Acceptance:** sign-in with the new password succeeds straight after a reset on a locked account.
- **Plan note:** `reset_password` in `apps/api/src/hds/services/invitation.py` line 268.

### F12. A failed rollback hides the original error

- **Severity:** Low. **Evidence:** Code reading. **Source:** CodeRabbit.
- **Problem:** if the rollback itself fails, for example on a dropped connection, its error replaces
  the request's real error.
- **Required behaviour:** the original error always propagates, and a rollback failure is logged
  by exception type only.
- **Acceptance:** a test with a failing rollback sees the original exception.
- **Plan note:** `unit_of_work` in `apps/api/src/hds/core/database.py` line 84.

### F13. The duplicate-address test can pass for the wrong reason

- **Severity:** Low. **Evidence:** Code reading. **Source:** CodeRabbit.
- **Problem:** the second insert omits `role`, so it fails on the NOT NULL rule before the
  case-insensitive index is tested. The test would still pass if that index were removed.
- **Required behaviour:** the test proves that the unique address rule refused the insert.
- **Acceptance:** the test adds `role`, asserts `uq_users_email_ci` in the error, and fails when the
  index is absent.
- **Plan note:** `apps/api/tests/integration/test_email_case.py` line 176.

### F14. The development link tool and its test misread pending accounts

- **Severity:** Low. **Evidence:** Code reading. **Source:** CodeRabbit, plus the code review.
- **Problem:** the fixture's "pending" account has `activated_at` set, so it is actually
  deactivated, and the test covers a state the real flow refuses. The script checks only
  `is_active`, so it issues invitation tokens for deactivated accounts that can never be accepted.
- **Required behaviour:** the tool issues invitation tokens only for pending accounts, and the test
  uses a truly pending account.
- **Acceptance:** a deactivated account is refused with exit status 1, and a pending one gets a
  token that can be redeemed.
- **Plan note:** `apps/api/tests/unit/test_dev_issue_link.py` line 61 and
  `apps/api/src/hds/scripts/dev_issue_link.py` line 77.

## Group C: documentation corrections

### F15. Duplicate scenario number in User Story 3

- **Severity:** Low. **Source:** CodeRabbit.
- **Problem:** two scenarios are numbered `3.`, which makes scenario-to-test tracing ambiguous.
- **Required behaviour:** the last scenario at `specs/002-security-hardening/spec.md` line 129 is
  numbered `4.`, and any reference to it is updated.

### F16. The short-secret check does not state its environment

- **Severity:** Low. **Source:** CodeRabbit.
- **Problem:** `specs/002-security-hardening/quickstart.md` line 47 says start-up fails "with a
  20-byte secret" without naming the environment, but development and test allow a short secret.
- **Required behaviour:** the row says `ENVIRONMENT=production` for that case.

## Group D: older gaps, in scope only if chosen

None of these were introduced by this PR.

| Id | Severity | Problem | Required behaviour | Where |
| --- | --- | --- | --- | --- |
| F17 | Medium | `PATCH` drops `null`, so phone, hourly rate, project end date and contract end date can't be cleared, and the API still answers 200 | An explicit `null` clears an optional field; an omitted field is left alone | `exclude_none=True` in `routers/me.py`, `employees.py`, `clients.py`, `projects.py`, `sites.py` |
| F18 | Medium | A Client-role user with no `client_id` would see every project of the organisation. No API path creates such a user today | Such a user sees no projects | `services/project.py` line 182 |
| F19 | Low | `/auth/logout` doesn't check that the token belongs to the caller | A token owned by someone else is not revoked, and the answer stays 204 | `services/auth.py` line 206 |
| F20 | Low | Routers call repositories directly, skipping the service layer and its role check (Constitution II) | Those reads go through services | `routers/sites.py`, `clients.py`, `employees.py` |
| F21 | Low | `UserRead` is defined twice, and a comment asks people to keep the copies identical | One shared definition | `routers/me.py`, `routers/employees.py` |
| F22 | Low | Duplicate helpers: `get_by_id_for_token` and `get_for_auth` are the same query, the token hash line is repeated, and `_verify_password` exists only for test patching | One helper for each | `repositories/user.py`, `services/auth.py`, `services/invitation.py` |
| F23 | Low | A Client portal user can be converted to staff with `client_id` still set (needs D5) | Follows D5 | `services/employee.py`, `edit_employee` and `promote_to_*` |
| F24 | Low | `contract_end` can be earlier than `contract_start` (needs D6) | Follows D6 | `services/client.py`, `routers/clients.py` |
| F25 | Low | Simultaneous creation of the same address returns a generic 422, not `users.duplicate_email` | Both paths return 409 `users.duplicate_email` | `services/employee.py`, `services/client.py` |
| F26 | Low | The integration suite truncates a fixed database, so two runs at once fail each other at random | Each run uses its own database, or a run refuses to start while another is active | `apps/api/tests/conftest.py` |

## Added after the P1 review

| Id | Severity | Problem | Required behaviour | Where |
| --- | --- | --- | --- | --- |
| F27 | Medium | Redeeming a link and a staff action on the same account deadlock (52 of 100 rounds for reset against deactivate), and the caller gets a server error | A path that touches a user and that user's links or tokens takes the user's row first | `services/invitation.py`, `services/client.py` |
| F28 | Medium | Anyone can keep cancelling a victim's reset links and flood their inbox by resubmitting the address, because a new request cancels the previous link and the endpoint has no limit | Reset requests are limited per address; over the limit the answer is unchanged and nothing is cancelled or sent | `services/invitation.py` |

F02 also gained a case: a portal user cannot be created under an inactive client.

## How the checks were run

At the earlier review of `c378d04`, unit tests and 415 integration and concurrency tests passed
in a private database. The contract tests were not run. Neither suite has been run on `704024b`.
Reproductions and probes were throwaway scripts, so nothing in the repository covers F01 to F05
yet. F19 and F21 to F25 come from reading the code.
