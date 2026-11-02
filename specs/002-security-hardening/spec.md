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

1. **Given** a deployment where the environment is not explicitly marked as development, **When**
   the signing secret is the published placeholder or shorter than the minimum, **Then** the
   system refuses to start and names the problem.
2. **Given** a production deployment with no real email delivery configured, **When** the system
   starts, **Then** it refuses to start.
3. **Given** any configuration, **When** an invitation or reset email is produced, **Then** no log
   line contains the link or token, and recipient addresses are masked.
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
5. **Given** a re-invitation names a role different from the pending account's role, **When** it
   is sent, **Then** the pending account's role follows the new invitation, or the request is
   refused; the role is never silently ignored.

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
2. **Given** a renewal credential that was already used, **When** it is presented again,
   **Then** the request is refused and all of that user's sessions are ended.
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
not one earlier. The count of failures is cleared only by a successful sign-in, as feature 001
(FR-006b) states; the end of the cooldown does not clear it, so one more wrong password after
the cooldown locks the account again.

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
   password, **Then** the account is locked again; **When** they enter the correct password
   instead, **Then** they sign in and the count is cleared.

---

### User Story 7 — Sign-in does not reveal which accounts exist (Priority: P2)

An attacker cannot learn which email addresses are registered by comparing how long sign-in takes
or by any other difference in the answer, apart from the locked-account notice that feature 001
accepts (FR-005).

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
- Two invitations for the same address race: one pending account results, not two.
- A password reset is requested for an address that differs only by case from a registered one:
  it is treated as that registered address.
- An action fails after sending an email: the email is not sent, or the action is not undone in a
  way that leaves a usable link behind.
- A locked account's cooldown ends while a sign-in is in progress: the attempt is judged by the
  state at the moment it is checked, and the count is unchanged until a success.
- A site time zone is valid but unusual (for example one without daylight saving): accepted.

## Requirements *(mandatory)*

### Functional Requirements

**Secrets and links (Story 1)**

- **FR-201**: The system MUST refuse to start unless the environment is explicitly marked as
  development when the signing secret is the published placeholder or shorter than 32 bytes.
- **FR-202**: The system MUST refuse to start in production unless a real email delivery
  mechanism is configured.
- **FR-203**: The system MUST NOT write invitation links, reset links or their tokens to any log,
  and MUST mask recipient email addresses in logs.
- **FR-204**: The shipped example and local-development settings MUST NOT contain a signing secret
  that a production-capable start would accept.

**Invitations (Story 2)**

- **FR-205**: Redeeming an invitation MUST only complete an account that is pending
  (never activated); it MUST be refused for deactivated, deleted or already-activated accounts.
- **FR-206**: Inviting an address that belongs to a deactivated account MUST be refused; an active
  or pending account MUST NOT have its role silently ignored by a re-invitation.
- **FR-207**: Deactivating or deleting an account MUST cancel its outstanding invitation and reset
  links, and accepting an invitation MUST cancel the account's other outstanding links.

**Single use (Story 3)**

- **FR-208**: A renewal credential, invitation link or reset link MUST succeed at most once, even
  under simultaneous use.
- **FR-209**: Presenting an already-used renewal credential MUST refuse the request and end all
  of that user's sessions.

**All-or-nothing (Story 4)**

- **FR-210**: An action that changes more than one record MUST apply completely or not at all.
  This includes session renewal, password reset, client update with portal-user deactivation,
  and role or activation changes together with session revocation.

**Admin guard (Story 5)**

- **FR-211**: The rule that an organisation keeps at least one active Admin (feature 001, FR-011)
  MUST hold under simultaneous actions.

**Sign-in (Stories 6 and 7)**

- **FR-212**: An account MUST lock after exactly the configured number of consecutive failures.
- **FR-213**: The count of failures MUST be cleared only by a successful sign-in; the end of a
  cooldown MUST NOT clear it.
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
- **SC-206**: A user is locked after exactly N failures and not after N-1, for N from 2 to 10.
- **SC-207**: Answer times for unknown and known emails differ by no more than normal variation
  (a statistical test cannot tell the two apart over 500 samples each).
- **SC-208**: Every registered address can sign in with any letter case, and 0 pairs of accounts
  differ only by case after rollout.
- **SC-209**: 0 sites with an invalid time zone can be saved, and all existing ones are reported.

## Assumptions

- This feature corrects feature 001's behaviour and changes none of its endpoints' purpose; any
  new error answers use the existing problem-details format and error-code translation rules.
- The mechanism for real email delivery (which provider) is a separate decision; this feature only
  guarantees the system does not run production without one.
- Case-insensitivity applies to the whole address, not only the domain.
- Invalid existing time zones are reported, not auto-corrected, because the right zone cannot be
  guessed.
- Reactivating a deactivated person stays an explicit staff action under feature 001 (FR-015), not
  something an invitation does.
- Smaller review issues (clearing optional fields, contract-date order, portal-user role changes,
  logout ownership, layering of some reads) are out of scope here and are tracked separately.
- Out of scope: real email provider integration, rate limiting by network address, and multi-factor
  sign-in.
