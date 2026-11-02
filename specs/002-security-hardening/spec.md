# Feature Specification: Security Hardening of Sign-In, Tokens and Account Rules

**Feature Branch**: `002-security-hardening`

**Created**: 2026-11-02

**Status**: Draft

**Input**: User description: "Fix the ten findings of the 2026-11-02 code review of the
foundation feature (001): leaked one-time links, weak secret handling, invitation links that
reactivate accounts, session and link reuse, sign-in lockout off by one, account discovery by
response time, case-sensitive emails, a removable last Admin, changes that only partly apply, and
unvalidated site time zones."

**Scope note**: Everything here is *Proposed* (Constitution I). It corrects behaviour of feature
001; it adds no new product capability. Requirement ids use the 2xx block (FR-201 and up) so
they cannot be confused with the ids of feature 001, which tests already cite.

## Clarifications

### Session 2026-11-02

- Q: When a client resends a session-renewal request because the first reply was lost, should the
  second use of that credential end all of the user's sessions? → A: Grace window: a second use
  within a few seconds of the first is refused without ending sessions; after the window it ends
  all of that user's sessions.
- Q: When staff re-invite an address whose invited account is still pending, but with a different
  role, should the account take the new role or should the re-invitation be refused? → A: The
  pending account takes the new role, under the same permission rules as a first invitation (only
  an Admin may invite an Admin).

- Q: After a locked account's cooldown ends, should its failed-attempt count restart at zero or
  stay until the next successful sign-in? → A: Restart at zero when the lock expires (this
  supersedes the earlier choice to keep the count until success, changed at planning on
  2026-11-02). A successful sign-in still clears the count.

### Session 2026-11-03 (PR 4 fix list, `pr4-fix-list.md`)

- Q: Which of the older gaps F17 to F26, which PR 4 did not introduce, are fixed in PR 4 before
  merging? → A: All of them (F17 to F26). F23 and F24 need the role-change rule (D5) and the
  contract-end-date rule (D6) decided below.
- Q: What happens when staff deactivate an account that is still pending (invited, never
  accepted)? (D1, F03) → A: It is refused with a dedicated, translated 409 error; the account
  stays pending and its link stays valid. When a client is deactivated, its pending portal users
  are skipped, not deactivated. `activated_at` is only ever set by a real activation.
- Q: Should replaying a used refresh token that has also expired still end all of the user's
  sessions? (D2, F08) → A: No. A credential past its expiry is refused with `auth.token_expired`
  and ends no sessions; expired credential records are cleaned up. Only an unexpired rotated
  token replayed after the grace window ends all sessions. This amends FR-209.
- Q: Is rate limiting on sign-in part of PR 4? (D4, F04) → A: Yes. It is in scope, in addition to
  moving password hashing off the event loop behind a bound. This reverses the earlier
  "rate limiting is out of scope" assumption; its limits and storage are decided below.
- Q: How does the sign-in rate limit count attempts and where are the counts kept? (F04) → A:
  Failed sign-ins are counted per submitted address (lowercased, registered or not) and per
  network address, in a new database table, so the limit is shared by all workers, survives a
  restart and covers unknown addresses. Limits are settings with defaults. The answer over each
  limit is set by the follow-up below and FR-221: 423 for an address, 429 for a network.
- Q: (Follow-up, 2026-11-03) Which answer does the per-address rate limit give? → A: The lockout
  answer. The per-address limit equals `LOGIN_MAX_FAILURES` and unknown addresses get exactly the
  lockout answer (423), which closes the last enumeration gap and supersedes the accepted FR-005
  exception. Only the network limit answers 429.
- Q: (Follow-up, 2026-11-03) Are the three proposed defaults kept? → A: Yes: a completed reset
  clears the lockout (F11), a Client account's role cannot change (F23), a client's contract end
  may not precede its start (F24).

## User Scenarios & Testing *(mandatory)*

### User Story 1 — Secrets and one-time links never leak (Priority: P1)

An operator deploys HDS. If the deployment is unsafe (a published or weak signing secret, or no
real way to deliver email), the system refuses to start instead of running quietly. Invitation
and password-reset links, which grant account access, are never written to logs or other places
where operators or tools can read them.

**Why this priority**: Today a deployment that forgets a setting starts normally and lets anyone
who knows a user's identifier act as that user, and anyone who can read the logs can take over an
account, including an Admin's. Both are full account takeover.

**Independent Test**: Start the system in each unsafe configuration and confirm it refuses; send
an invitation and a reset in a normal configuration and confirm the link appears in no log
output.

**Acceptance Scenarios**:

1. **Given** a deployment where the environment is not explicitly marked as development or test,
   **When** the signing secret is the published placeholder or shorter than the minimum,
   **Then** the system refuses to start and names the problem.
2. **Given** a production deployment with no real email delivery configured, **When** the system
   starts, **Then** it refuses to start.
3. **Given** any configuration, **When** an invitation or reset email is produced, **Then** no log
   line and no development mail outbox contains the link or token in clear, and recipient
   addresses are masked in logs.
4. **Given** the shipped example configuration, **When** an operator copies it unchanged,
   **Then** it does not start a production-capable deployment with a known secret.

---

### User Story 2 — Invitations cannot bring back a deactivated account (Priority: P1)

A staff member invites a new colleague, or re-invites someone who never accepted. An invitation
link only ever completes a *pending* account. It cannot reactivate an account that staff
deactivated, replace the password of an active account, or bypass the rule that only an Admin may
act on an Admin.

**Why this priority**: A deactivated person, including a deactivated Admin or a portal user of an
inactive client, can currently return with no staff action by redeeming an unused link.

**Independent Test**: Deactivate an account that holds an unused invitation link, redeem the
link, and confirm the account stays deactivated and the password is unchanged.

**Acceptance Scenarios**:

1. **Given** an account that staff deactivated, **When** anyone redeems an invitation link issued
   earlier for it, **Then** the redemption is refused and the account stays deactivated.
2. **Given** a deactivated account, **When** a Manager or Admin tries to invite its email,
   **Then** the invitation is refused with a clear message (reactivation is a separate, explicit
   staff action under the existing rules).
3. **Given** an account that has already accepted an invitation, **When** a leftover link for it
   is redeemed, **Then** the redemption is refused and the password and sessions are unchanged.
4. **Given** an account is deactivated or deleted, **When** it still has outstanding invitation or
   reset links, **Then** those links stop working.
5. **Given** a pending account invited as an Employee, **When** a Manager re-invites the same
   address as a Manager, **Then** the pending account's role becomes Manager and the new link
   replaces the earlier one.
6. **Given** a pending account, **When** a Manager re-invites it as an Admin, **Then** the request
   is refused (only an Admin may invite an Admin) and the pending role is unchanged.
7. **Given** two staff members who invite the same new address at the same moment, **When** both
   requests finish, **Then** exactly one pending account exists, both requests succeed (the
   later one acts as a re-invitation) and neither receives an error.
8. **Given** a pending account (invited, never activated), **When** staff try to reactivate it,
   **Then** the request is refused with a clear message that only the invitation can activate it,
   and the account stays pending; it is never reported as a generic data-constraint error.

---

### User Story 3 — Sessions and one-time links are truly single-use (Priority: P1)

Every renewal credential and every invitation or reset link works exactly once, even when two
requests arrive at the same moment. If an already-used renewal credential is presented again, the
system treats it as theft and ends all of that user's sessions.

**Why this priority**: Two simultaneous renewals currently both succeed, and replaying a used
credential ends nothing, so a stolen session stays alive. The foundation's design decision (replay
revokes everything) is not implemented.

**Independent Test**: Present one renewal credential twice at once and confirm exactly one succeeds;
present it a third time and confirm every session of that user is ended.

**Acceptance Scenarios**:

1. **Given** one valid renewal credential, **When** two renewals using it arrive simultaneously,
   **Then** exactly one succeeds and the other is refused.
2. **Given** a renewal credential that was already used, **When** it is presented again after
   the grace window, **Then** the request is refused and all of that user's sessions are ended.
3. **Given** a renewal credential that was used a moment ago, **When** it is presented again
   within the grace window (an honest retry), **Then** the request is refused but the user's
   sessions, including the one just issued, stay valid.
3. **Given** one valid invitation or reset link, **When** two redemptions arrive simultaneously,
   **Then** exactly one succeeds.

---

### User Story 4 — A change either fully happens or does not happen (Priority: P2)

When one action needs several steps (renewing a session, resetting a password, deactivating a
client and its portal users), a failure part-way leaves the data as it was before the action.
Users are never logged out, left with a half-changed account, or locked out of an active client
because a later step failed.

**Why this priority**: It is the base for stories 3 and 5 and gets more costly to change once
scheduling adds actions that write many records.

**Independent Test**: Force a failure in the last step of each multi-step action and confirm no
earlier step took effect.

**Acceptance Scenarios**:

1. **Given** a session renewal whose last step fails, **When** the failure occurs, **Then** the old
   credential is still valid.
2. **Given** a password reset whose last step fails, **When** the failure occurs, **Then** the old
   password, the link and the sessions are all unchanged.
3. **Given** a client update that fails, **When** the failure occurs, **Then** the client's portal
   users keep their previous state.

---

### User Story 5 — The last Admin cannot be removed, even by simultaneous actions (Priority: P2)

The organisation always keeps at least one active Admin (feature 001, FR-011), including when two
Admins act on each other at the same moment.

**Why this priority**: Two Admins deactivating each other at once can leave no active Admin, and
recovery then needs direct access to the data store.

**Independent Test**: Run two simultaneous deactivations between the only two Admins and confirm
one is refused.

**Acceptance Scenarios**:

1. **Given** exactly two active Admins, **When** each tries to deactivate or demote the other at
   the same moment, **Then** exactly one action succeeds and an active Admin remains.
2. **Given** one active Admin, **When** anyone tries to remove, deactivate or demote them,
   **Then** the action is refused.

---

### User Story 6 — Sign-in lockout matches the rule (Priority: P2)

An account locks after exactly the configured number of consecutive wrong passwords (default 5),
not one earlier. The count of failures restarts at zero when the lock expires, and is also cleared
by a successful sign-in as feature 001 (FR-006b) requires, so after the cooldown the user gets a
full set of attempts again.

**Why this priority**: Users are locked out after four mistakes, which contradicts the stated rule
and is not caught by any automated test against a real database.

**Independent Test**: Make exactly the configured number of wrong attempts and confirm the next
attempt is refused as locked, while one fewer wrong attempt leaves sign-in possible.

**Acceptance Scenarios**:

1. **Given** a limit of N failures, **When** a user fails N-1 times, **Then** a correct password
   still signs them in.
2. **Given** a limit of N failures, **When** a user fails N times, **Then** the next attempt,
   even with the correct password, is refused as locked for the cooldown period.
3. **Given** a locked account whose cooldown has ended, **When** the user enters one wrong
   password, **Then** the count restarts and sign-in is still possible; the account locks again
   only after the full configured number of new consecutive failures.

---

### User Story 7 — Sign-in does not reveal which accounts exist (Priority: P2)

An attacker cannot learn which email addresses are registered by comparing how long sign-in takes
or by any other difference in the answer. (Feature 001 accepted a locked-account notice that only
registered addresses could produce; FR-221 removes that exception.)

**Why this priority**: Unknown emails are answered immediately while known ones take noticeably
longer, so the generic error text protects nothing.

**Independent Test**: Measure answer times for many unknown and known emails with a wrong password
and confirm the two distributions cannot be told apart.

**Acceptance Scenarios**:

1. **Given** a wrong password, **When** the email is unknown versus known, **Then** the response
   body, status and timing are indistinguishable within normal variation.

---

### User Story 8 — Email addresses ignore letter case (Priority: P2)

`Bob@corp.com` and `bob@corp.com` are the same address: one account, one sign-in, one reset.

**Why this priority**: Today they can be two accounts, typing a different case at sign-in fails,
and a reset for that spelling silently does nothing.

**Independent Test**: Create an account, then sign in, request a reset and attempt a duplicate
creation using different letter case, and confirm each treats it as the same account.

**Acceptance Scenarios**:

1. **Given** an account created as `bob@corp.com`, **When** the user signs in as `Bob@Corp.com`,
   **Then** sign-in succeeds.
2. **Given** that account, **When** another account is created as `BOB@corp.com`, **Then** it is
   refused as a duplicate.
3. **Given** existing accounts whose addresses differ only by case, **When** the change is
   introduced, **Then** they are reported and resolved before the rule takes effect.
   The rollout step stops and lists the clashing accounts; staff resolve them first, and nothing
   is changed automatically.

---

### User Story 9 — Site time zones are real time zones (Priority: P3)

A site's time zone must be a valid zone name. Invalid values are refused when saved.

**Why this priority**: An invalid zone is stored today and will break the first feature that
converts times for that site (scheduling), so it is cheapest to fix before then.

**Independent Test**: Create and edit a site with a valid and an invalid zone and confirm only the
valid one is accepted.

**Acceptance Scenarios**:

1. **Given** a site form, **When** a user saves a zone that does not exist, **Then** the request is
   refused with a clear validation error and nothing is stored.
2. **Given** sites already stored with an invalid zone, **When** the rule is introduced, **Then**
   they are listed so staff can correct them.

---

### Edge Cases

- A renewal credential is replayed after its user was deactivated: the request is refused, with no
  extra effect.
- An honest retry arrives just inside, or just outside, the grace window: inside keeps sessions,
  outside ends them; there is no third outcome.
- Two invitations for the same address race: one pending account results, not two.
- A password reset is requested for an address that differs only by case from a registered one:
  it is treated as that registered address.
- An action fails after sending an email: the email is not sent, or the action is not undone in a
  way that leaves a usable link behind.
- A locked account's cooldown ends while a sign-in is in progress: the attempt is judged by the
  state at the moment the lock is checked; the count restarts at that moment if the lock has
  expired.
- A site time zone is valid but unusual (for example one without daylight saving): accepted.

## Requirements *(mandatory)*

### Functional Requirements

**Secrets and links (Story 1)**

- **FR-201**: The system MUST refuse to start unless the environment is explicitly marked as
  development or test when the signing secret is the published placeholder or shorter than 32
  bytes.
- **FR-202**: The system MUST refuse to start in production unless a real email delivery
  mechanism is configured.
- **FR-203**: The system MUST NOT write invitation links, reset links or their tokens in clear to
  any log or to any mail outbox, and MUST mask recipient email addresses in logs. A person
  working in a development environment can still obtain a working link for manual testing
  through an explicit, development-only action that shows it once and stores it nowhere.
- **FR-204**: The shipped example and local-development settings MUST NOT contain a signing secret
  that a production-capable start would accept.

**Invitations (Story 2)**

- **FR-205**: Redeeming an invitation MUST only complete an account that is pending
  (never activated); it MUST be refused for deactivated, deleted or already-activated accounts.
- **FR-206**: Inviting an address that belongs to a deactivated account MUST be refused. Inviting
  an address that belongs to a pending account MUST set the pending account's role to the role in
  the new invitation, under the same permission rules as a first invitation, and MUST replace the
  earlier link. Inviting an address that belongs to an active account is refused as a duplicate.
- **FR-207**: Deactivating or deleting an account MUST cancel its outstanding invitation and reset
  links, and accepting an invitation MUST cancel the account's other outstanding links.
- **FR-220**: Reactivating a pending account MUST be refused with a dedicated, translated error
  (English and Hebrew), and the account MUST stay pending. Only accepting an invitation activates
  a pending account.

**Single use (Story 3)**

- **FR-208**: A renewal credential, invitation link or reset link MUST succeed at most once, even
  under simultaneous use.
- **FR-209**: Presenting an already-used renewal credential MUST always be refused. If it is
  presented after the grace window (default 10 seconds, configurable) and it has not expired, it
  MUST also end all of that user's sessions; within the window it MUST NOT. A credential past its
  expiry MUST be refused as expired and MUST NOT end any session; expired credential records MUST
  be removed.

**All-or-nothing (Story 4)**

- **FR-210**: An action that changes more than one record MUST apply completely or not at all.
  This includes session renewal, password reset, client update with portal-user deactivation,
  and role or activation changes together with session revocation.

**Admin guard (Story 5)**

- **FR-211**: The rule that an organisation keeps at least one active Admin (feature 001, FR-011)
  MUST hold under simultaneous actions.

**Sign-in (Stories 6 and 7)**

- **FR-212**: An account MUST lock after exactly the configured number of consecutive failures.
- **FR-213**: The count of failures MUST restart at zero when a lock expires, and MUST be cleared
  by a successful sign-in.
- **FR-214**: A sign-in with an unknown email MUST take the same observable time and give the same
  answer as one with a known email and a wrong password.

**Email and time zones (Stories 8 and 9)**

- **FR-215**: Email addresses MUST be compared without regard to letter case everywhere they
  identify an account: creation, sign-in, invitation, reset and uniqueness.
- **FR-216**: Existing accounts whose addresses differ only by case MUST be found and listed, and
  the rollout MUST stop until staff have resolved them; the system MUST NOT merge, rename or
  deactivate them automatically.
- **FR-217**: A site's time zone MUST be a valid zone name; invalid values MUST be refused with a
  validation error and never stored.
- **FR-218**: Sites already stored with an invalid time zone MUST be reported.

**Verification**

- **FR-219**: Each fix MUST come with an automated test that fails before the fix, including tests
  for simultaneous actions and for sign-in lockout against the real database.

### Key Entities

- **Renewal credential**: the long-lived token that obtains new short-lived access; single use;
  belongs to a user; may be revoked.
- **One-time link**: an invitation or password-reset token; single use; expires; belongs to one
  account.
- **Pending account**: an invited account that has not yet set a password and was never active.
- **Failure count / lock**: the consecutive wrong-password count and the time until which sign-in
  is refused.

**Sign-in rate limiting (PR 4 clarification, F04)**

- **FR-221**: Sign-in MUST treat every submitted address, registered or not, by the same lockout
  rule. After `LOGIN_MAX_FAILURES` consecutive failures for an address (compared without regard to
  letter case) that address is locked for `LOGIN_LOCKOUT_MINUTES`: sign-in answers 423
  `auth.account_locked`, in the same form and with the same lock-end time rule, whether or not an
  account exists. The count restarts when the lock ends and is cleared by a successful sign-in. A
  correct password is refused while the address is locked and is not verified, exactly as for the
  existing account lock; this is stated openly, not hidden. Failures per network address are counted
  too, in a **fixed window** of `LOGIN_LOCKOUT_MINUTES`: failures older than the window do not
  count, so ordinary typos over days never add up. After `LOGIN_RATE_LIMIT_PER_NETWORK` failures
  inside one window (0 turns this limit off) that network address is refused with 429
  `auth.rate_limited` and a `Retry-After` header until the window ends, before any lookup or
  password work and for every address alike. A successful sign-in MUST NOT clear the network count
  (otherwise a holder of one valid account could sign in every few guesses and never be limited).
  An IPv6 client is counted by its /64 prefix, and a malformed forwarded-header entry is ignored.
  Counts are kept in the database, so all workers share them and they survive a restart. Stale counts MUST be removed without a
  separately scheduled job being required.
- **FR-222**: Password hashing and verification MUST NOT hold a database connection while they
  run, on every path that hashes (sign-in, accepting an invitation, confirming a reset, creating an
  employee, creating a client user, and inviting, which hashes a placeholder password). They MUST
  NOT block other requests, and MUST be bounded: when the queue for hashing is full the request is
  refused at once with 503 `service.busy` and a `Retry-After` header. Hashes MUST be computed
  before any row lock is taken and before any write of the request.
- **FR-223**: A client address taken from a forwarded header MUST be trusted only as configured by
  `TRUSTED_PROXY_COUNT` (default 0: the header is ignored and the connecting address is used).

**PR 4 fixes (`pr4-fix-list.md`; Proposed)**

F04 is FR-221 to FR-223 and F08 is FR-209. F15 and F16 correct this spec and the quickstart and
change no behaviour, so they need no requirement.

- **FR-224** (F01): When a password reset completes, every other outstanding reset or invitation
  link of that account MUST stop working (`auth.token_used`), and issuing a new reset link MUST
  cancel that account's earlier unused reset links.
- **FR-225** (F02): Deactivating or deleting a client, or removing a portal user, MUST end that
  user's sessions and cancel their links, and a portal user MUST NOT be reactivated while the
  client is inactive or deleted (409 `users.client_inactive`). A portal user MUST NOT be created
  under an inactive client (the same 409).
- **FR-226** (F03): No sequence of staff actions may activate an account that never accepted its
  invitation. Only a real activation writes `activated_at`. Deactivating a pending account MUST be
  refused (409 `users.pending_cannot_deactivate`), and client deactivation MUST skip pending
  portal users.
- **FR-227** (F05): No part of a sensitive value may survive log redaction, whatever characters it
  contains, and redacting twice MUST change nothing further.
- **FR-228** (F06): `POST /auth/password-reset/request` MUST take the same observable time for
  registered and unregistered addresses, and delivery MUST run after the response.
- **FR-229** (F07): An account with evidence of past activity (any refresh token, or any reset
  link) MUST NOT remain pending after migration; invitation links are not evidence; undecidable
  accounts MUST be reported by id and left pending.
- **FR-230** (F09): Once "end every session" completes, no credential issued before or during it
  may remain usable, including from a refresh in flight or a sign-in that verified the old
  password.
- **FR-231** (F10): Only canonical IANA zone names are accepted, matched case-sensitively, with the
  same result on every platform; `check_site_timezones` reports stored values that fail.
- **FR-232** (F11): A completed password reset MUST clear that account's lock and failure count.
- **FR-233** (F12): If a rollback fails, the request's original error MUST propagate, and the
  rollback failure MUST be logged by exception type only.
- **FR-234** (F13): The duplicate-address test MUST prove that `uq_users_email_ci` refused the
  insert, and fail when that index is absent.
- **FR-235** (F14): `dev_issue_link` MUST issue invitation tokens only for pending accounts, and
  refuse a deactivated account with exit status 1.
- **FR-236** (F17): In a `PATCH`, an explicit `null` clears an optional field, an omitted field is
  left alone, and `null` for a required field is 422; the schema MUST say the same.
- **FR-237** (F18): A Client-role user with no `client_id` MUST see no projects.
- **FR-238** (F19): `/auth/logout` MUST NOT revoke a token owned by someone else; the answer stays
  204.
- **FR-239** (F20): Routers MUST read through services, with the services' role checks
  (Constitution II).
- **FR-240** (F21): `UserRead` MUST have one shared definition, keeping its OpenAPI component name.
- **FR-241** (F22): The token lookup, the token hash and password verification MUST each have one
  helper.
- **FR-242** (F23): Any role change on a Client account MUST be refused (409
  `users.client_role_locked`).
- **FR-243** (F24): A client's `contract_end` MUST NOT be earlier than `contract_start` (422
  `clients.date_range_invalid`).
- **FR-244** (F25): Simultaneous creation of one address MUST give exactly one success and 409
  `users.duplicate_email`.
- **FR-245** (F26): Two test runs at once MUST NOT interfere with each other.
- **FR-246** (F27): A path that touches a user and that user's links or tokens MUST take the user's
  row first, so redeeming a link and a staff action on the same account never surface as a server
  error.
- **FR-247** (F28): Password-reset requests MUST be limited per submitted address, registered or not,
  in a fixed window. Over the limit the answer MUST stay 202, and no link is cancelled and no mail
  is sent, so the limit reveals nothing and a third party cannot keep cancelling a victim's links.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-201**: In every unsafe start configuration listed in Story 1, the system refuses to start
  in 100% of attempts.
- **SC-202**: Across a full run of normal operations, 0 log lines contain an invitation or reset
  token or an unmasked recipient address.
- **SC-203**: In 100 simultaneous double uses of a single-use credential or link, exactly 1
  succeeds each time.
- **SC-204**: Redeeming a link against a deactivated, deleted or already-active account changes no
  data in 100% of attempts.
- **SC-205**: In 100 simultaneous attempts between the last two Admins, an active Admin remains
  each time.
- **SC-206**: A user is locked after exactly N failures and not after N-1, for N from 2 to 10,
  and after a lock expires the user again has N attempts.
- **SC-207**: A sign-in with a wrong password performs the same password-checking work and returns
  the identical answer whether the email is unknown, known, or belongs to an inactive account;
  this is verified by an automated check on every build. An optional timing comparison over 500
  samples each, which cannot tell the two apart, is run by hand before a release.
- **SC-208**: Every registered address can sign in with any letter case, and 0 pairs of accounts
  differ only by case after rollout.
- **SC-209**: 0 sites with an invalid time zone can be saved, and all existing ones are reported.
- **SC-210**: At every step of a run of failed sign-ins up to and past the lock, a registered and an
  unregistered address give the same status, body shape, headers and number of password checks.
- **SC-211**: With 50 concurrent requests that hash a password and a pool of 5 database connections,
  an unrelated request is answered within 2 seconds, and requests past the hashing queue are
  refused at once with 503.
- **SC-212**: `POST /auth/password-reset/request` has a median time ratio below 1.15 between
  registered and unregistered addresses, and delivery starts after the response is complete.

## Assumptions

- Terms: a *renewal credential* is the refresh token; a *link* is an invitation or password-reset
  link; a *token* is the secret value inside a link or renewal credential. Both words are used in
  the plan and tasks with these meanings.
- A *development environment* is one marked `development` or `test`; any other marking is treated
  like production for the start-up checks.
- This feature corrects feature 001's behaviour and changes none of its endpoints' purpose; any
  new error answers use the existing problem-details format and error-code translation rules.
- The mechanism for real email delivery (which provider) is a separate decision; this feature only
  guarantees the system does not run production without one.
- The grace window applies only to renewal credentials, not to invitation or reset links, which
  are refused on any second use without further effect.
- Case-insensitivity applies to the whole address, not only the domain.
- Invalid existing time zones are reported, not auto-corrected, because the right zone cannot be
  guessed.
- Reactivating a deactivated person stays an explicit staff action under feature 001 (FR-015), not
  something an invitation does.
- Smaller review issues (clearing optional fields, contract-date order, portal-user role changes,
  logout ownership, layering of some reads) are out of scope here and are tracked separately.
- Email uniqueness holds across the whole deployment (feature 001), so a request to use an address
  that another organisation already holds is answered 409 `users.duplicate_email`. That answer
  reveals that the address exists somewhere in the deployment; this is unavoidable while one
  address can belong to only one account, and it reveals nothing about which organisation holds it.
  Every request that names an id (rather than an address) from another organisation is answered
  404, the same as for an id that does not exist.
- **Unknown addresses are locked like known ones (FR-221, clarification of 2026-11-03).** This
  closes the last sign-in enumeration gap and supersedes the locked-account exception that feature
  001 (FR-005) accepted: a registered and an unregistered address now give the same answers, 401 and
  then 423.
- **Managers cannot withdraw a pending invitation (consequence of the 2026-11-03 clarification Q2).**
  Refusing to deactivate a pending account leaves staff without a way to cancel it; only an Admin's
  soft-delete removes one. Accepted.
- **Known limitation, equal work is not equal time to the last millisecond:** a known account's
  wrong password also writes and commits its failure count, about 3 ms on top of about 55 ms for
  the Argon2 check (a ratio of about 1.05 in the opt-in timing test, SC-207).
- Out of scope: real email provider integration and multi-factor sign-in. (Rate limiting on
  sign-in was out of scope until the 2026-11-03 clarification; it is now in scope, see FR-221.)
- A pending portal user under a deleted client is left as it is: no API path creates one, and F23
  keeps it that way.
- **Proxy deployment (open until the ingress exists):** `TRUSTED_PROXY_COUNT` stays 0 because
  nothing is deployed behind a proxy yet. When the deployment feature adds the ingress it must set
  the count and add a check to its deployment steps; a wrong value cannot be detected at start-up.
- **Confirmed defaults (2026-11-03):** a completed password reset clears that account's lockout
  and failure count (F11); a Client portal account's role cannot be changed (F23); a client's
  contract end date must not be earlier than its start date (F24).
