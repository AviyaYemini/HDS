# Feature Specification: Foundation — Authentication and Organisation

**Feature Branch**: `001-foundation-auth-org`

**Created**: 2026-11-01

**Status**: Draft

**Input**: User description: "Build the foundation of HDS 2.0 from docs/hds-spec.md sections 2,
4 (AUTH-* and ORG-* rows) and docs/hds-api.md (Auth and Organisation)."

## User Scenarios & Testing *(mandatory)*

### User Story 1 — Sign In and Role-Based Access (Priority: P1)

A user opens HDS, enters their credentials, and lands on a home screen whose content and
navigation reflect exactly what their role allows. An Employee sees their own profile and
nothing else. A Manager sees company-wide views and employee management tools. An Admin sees
everything a Manager sees plus destructive operations (delete). A Client sees only their own
projects.

**Why this priority**: Every other story depends on the identity and role of the signed-in user.
Nothing else in the system is accessible without this.

**Independent Test**: A set of four test accounts (one per role) can each sign in, land on the
correct home screen, and be denied access to screens outside their role — without any other
feature being built.

**Acceptance Scenarios**:

1. **Given** a user with valid credentials, **When** they submit the sign-in form, **Then** they
   are authenticated and redirected to their role-appropriate home screen.
2. **Given** a signed-in user, **When** they attempt to navigate to a screen their role cannot
   access, **Then** they receive an "access denied" response and are not shown any restricted
   data.
3. **Given** a signed-in user, **When** they sign out, **Then** their session is invalidated and
   they cannot access protected screens until they sign in again.
4. **Given** a user signed in on multiple devices, **When** they choose "sign out of all devices",
   **Then** all active sessions are invalidated immediately.
5. **Given** a user who enters wrong credentials, **When** they submit the sign-in form,
   **Then** they receive a generic failure message (no hint about which field is wrong).

---

### User Story 2 — Employee Views and Edits Own Profile (Priority: P2)

An Employee opens their profile, reviews their personal details (name, contact information,
default pay rate field visible but read-only for them), and updates the fields they are
permitted to change (name and contact details).

**Why this priority**: Employees are the most numerous users. Self-service profile access
removes the admin burden of updating basic contact details, and is needed before any
shift-related feature can be built.

**Independent Test**: An Employee account can open "My Profile", see only their own data, edit
their name and contact details, save, and verify the change persists — while being unable to
view any other employee's profile.

**Acceptance Scenarios**:

1. **Given** a signed-in Employee, **When** they open their profile, **Then** they see their own
   name, contact details, role, and active status — and no other employee's data.
2. **Given** a signed-in Employee, **When** they update their contact details and save,
   **Then** the new values are persisted and shown on the next load.
3. **Given** a signed-in Employee, **When** they attempt to view another employee's profile
   (e.g., by guessing a URL), **Then** they receive an access denied response.

---

### User Story 3 — Manager Manages Employees, Groups, and Qualifications (Priority: P3)

A Manager opens the employee list, finds a specific person using search or filters, edits their
details, assigns them to one or more groups and qualifications, and (if needed) promotes them to
Manager or deactivates them. An Admin can additionally delete an employee record.

**Why this priority**: Employee management is a prerequisite for the roster, availability, and
reporting features that follow. Groups and qualifications are referenced everywhere in the
scheduling domain.

**Independent Test**: A Manager account can create a new employee, assign them to a group,
assign a qualification, edit their details, promote them to Manager, and deactivate them — all
verifiable in the employee list — without any shift or scheduling feature existing.

**Acceptance Scenarios**:

1. **Given** a signed-in Manager, **When** they create a new employee with the required fields,
   **Then** a new employee record appears in the employee list with status Active.
2. **Given** a signed-in Manager, **When** they search or filter the employee list,
   **Then** only employees matching the criteria are shown.
3. **Given** a signed-in Manager, **When** they edit an employee's details and save,
   **Then** the updated values are persisted.
4. **Given** a signed-in Manager, **When** they promote an employee to Manager,
   **Then** that person's role changes and they gain Manager-level access on next request.
5. **Given** a signed-in Manager, **When** they deactivate an employee,
   **Then** that employee can no longer sign in, but their record and history are retained.
6. **Given** a signed-in Admin, **When** they delete an employee record,
   **Then** the record is hidden from all listings while its historical data (rosters, reports)
   remains intact.
7. **Given** a signed-in Employee (non-Manager), **When** they attempt to access the employee
   list, **Then** they receive an access denied response.

---

### User Story 4 — Manager Manages Clients, Sites, and Projects (Priority: P4)

A Manager creates and edits clients (company details and client users), sites (locations), and
projects (linked to a client and site, with type, billing rate, and date range). An Admin can
delete a client or project. A project's status (active or ended) is visible to Managers.

**Why this priority**: Projects are the context for all shifts and financial data. Clients need
to exist before the client portal (User Story 5) works.

**Independent Test**: A Manager account can create a client with a client user, create a site,
create a project linked to both, edit the project's billing rate and end date, and change its
status — all verifiable in the relevant list views — without any shifts existing.

**Acceptance Scenarios**:

1. **Given** a signed-in Manager, **When** they create a new client with required details,
   **Then** a client record appears in the client list.
2. **Given** a signed-in Manager, **When** they add a client user to a client,
   **Then** that person can sign in with the Client role and see only that client's projects.
3. **Given** a signed-in Manager, **When** they create a project with a client, site, type,
   billing rate, and start date, **Then** the project appears in the active project list.
4. **Given** a signed-in Manager, **When** they mark a project as ended,
   **Then** it moves to the ended projects view and is no longer treated as active.
5. **Given** a signed-in Admin, **When** they delete a project,
   **Then** it is hidden from all listings while its billing and shift history is retained.
6. **Given** a signed-in Employee or Client, **When** they attempt to access the project
   management screens, **Then** they receive an access denied response.

---

### User Story 5 — Client Views Own Projects (Priority: P5)

A Client signs in and sees a list of their own projects. They can open a project to view its
details (type, billing rate, status, location, date range). They cannot see any other client's
projects, any employee data, or any management tools.

**Why this priority**: The client portal is lower priority than the internal management
foundation, but the role must be established now so its access boundary is enforced from
the start.

**Independent Test**: A Client account can sign in, see only their own projects, open a project
detail view, and be denied access to employee lists or other clients' projects.

**Acceptance Scenarios**:

1. **Given** a signed-in Client, **When** they view the project list, **Then** only projects
   belonging to their own client record are shown.
2. **Given** a signed-in Client, **When** they open a project, **Then** they see its details
   but no employee names, pay rates, or other clients' data.
3. **Given** a signed-in Client, **When** they attempt to access employee management or another
   client's project, **Then** they receive an access denied response.

---

### User Story 6 — Password Reset and Invitation (*Proposed*) (Priority: P6)

A Manager invites a new user by email. The invited user follows a time-limited link to set their
password and activate their account. Existing users can request a password reset link by email.

**Why this priority**: Marked *Proposed* — goes beyond the original spec's sign-in
requirement. Useful for real-world use but not essential for the internal foundation. Can be cut
without affecting stories 1–5.

**Independent Test**: A new user can be invited, activate their account via the emailed link,
and sign in. An existing user can reset their password via an emailed link without contacting an
admin.

**Acceptance Scenarios**:

1. **Given** a Manager sends an invitation to an email address, **When** the recipient follows
   the link within the validity window, **Then** they can set a password and sign in.
2. **Given** an invitation link that has expired, **When** the recipient tries to use it,
   **Then** they see a clear message and are offered a way to request a new link.
3. **Given** a user who has forgotten their password, **When** they request a reset, **Then**
   a time-limited reset link is sent to their registered email.

---

### Edge Cases

- What happens when the last Admin is deactivated or deleted? (The system must prevent an
  organisation from having zero Admins.)
- What happens when a Manager tries to promote themselves to Admin? (Only another Admin can
  grant the Admin permission.)
- What happens when a Manager tries to edit, deactivate, delete, or demote an Admin? (Refused;
  only an Admin may, per FR-011a. Demoting the last Admin is refused per FR-011.)
- What happens when a group or qualification that is assigned to employees is deleted? (The
  memberships are removed; the employees themselves are unaffected.)
- What happens when an invitation link is used a second time? (It must be invalidated after
  first use.)
- What happens when a user's role is downgraded (e.g., Manager → Employee) while they have an
  active session? (Their permissions must update at next request.)
- What happens when a Manager tries to create an employee with an email already in use within
  the same organisation? (The system must return a clear duplicate-email error; it must not
  silently merge or partially create the record.)
- What happens when a Manager reactivates a previously deactivated employee? (Their account
  and history are fully restored; they can sign in again with their existing credentials.)
- What happens when a Manager reactivates an ended project? (It returns to the active project
  list and can be assigned shifts again in the scheduling feature.)

## Requirements *(mandatory)*

### Functional Requirements

**Authentication**

- **FR-001**: The system MUST allow a user with valid credentials to sign in with email and
  password and receive a session that identifies their role and organisation. The organisation
  is resolved from the account; the user is never asked to supply it.
- **FR-002**: The system MUST invalidate a user's session when they sign out.
- **FR-003**: The system MUST allow a user to sign out of all active sessions simultaneously.
- **FR-004**: The system MUST enforce a role check on every protected action; no protected data
  or operation is accessible without a valid, role-appropriate session.
- **FR-005**: The system MUST return a generic, non-revealing error when sign-in fails (wrong
  credentials or unknown account). Exception: a locked account (FR-006b) MAY be reported as
  temporarily locked, which discloses that the account exists; this is an accepted trade-off.
- **FR-006** *(Proposed)*: The system MUST support time-limited invitation links for account
  activation and time-limited password-reset links; both MUST be invalidated after first use or
  on expiry.
- **FR-006b**: The system MUST lock a user account for a temporary cooldown period after a
  configurable number of consecutive failed sign-in attempts (default: 5 attempts, 15-minute
  cooldown). The user MUST receive a clear message indicating they must wait or contact a
  manager. The lockout counter MUST reset on a successful sign-in.

**Role and Access Boundaries**

- **FR-007**: Employee role MUST NOT grant access to other employees' profiles, project reports,
  cash flow, or management screens.
- **FR-008**: Manager role MUST grant access to all employee management, client management, site
  management, and project management screens, plus Employee-level access to their own profile.
- **FR-009**: Admin role MUST extend Manager permissions with the ability to delete (hide)
  employee and project records.
- **FR-009a** *(Proposed)*: An Admin MUST be able to delete (hide) a client record. The
  deletion MUST be refused while the client still has at least one non-deleted project;
  the system MUST return a clear error in that case.
- **FR-010**: Client role MUST restrict access to the signed-in user's own client's projects;
  no employee data, other clients' data, or management tools are accessible.
- **FR-011**: The system MUST prevent an organisation from having zero active Admins. This
  applies to deactivation, deletion, and any role change away from Admin.
- **FR-011a**: Only an Admin MAY edit, deactivate, delete, or demote an Admin account.

**Employee Management**

- **FR-012**: A Manager MUST be able to create a new employee record with at minimum: full
  name, email address, contact details, role, active status, and an initial password (minimum
  12 characters *(Proposed)*). Email addresses MUST be unique across the deployment (an email
  identifies exactly one account, so sign-in needs no organisation); attempting to
  create a duplicate MUST return a clear error naming the conflict.
- **FR-013**: A Manager MUST be able to search and filter the employee list by name, group,
  qualification, role, and active status.
- **FR-014**: A Manager MUST be able to edit an employee's details, including promoting them
  to Manager.
- **FR-015**: A Manager MUST be able to deactivate an employee; a deactivated employee cannot
  sign in but their record and history are retained. A Manager MUST also be able to reactivate
  a deactivated employee, restoring their ability to sign in.
- **FR-016**: An Admin MUST be able to delete (hide) an employee; the record is removed from
  all listings while historical data is preserved.
- **FR-017**: An Employee MUST be able to view and edit their own profile details (name,
  contact); they MUST be able to view (but not edit) their own `default_hourly_rate`;
  they MUST NOT be able to change their own role or active status.

**Groups and Qualifications**

- **FR-018**: A Manager MUST be able to create, rename, and delete groups; groups have a name
  and can contain multiple employees.
- **FR-019**: A Manager MUST be able to create, rename, and delete qualifications; a
  qualification can be assigned to or removed from an employee.
- **FR-020**: An employee can belong to zero or more groups and hold zero or more
  qualifications.

**Client and Site Management**

- **FR-021**: A Manager MUST be able to create and edit a client record with at minimum:
  company name, contact details, contract start and end dates, and active status.
- **FR-022**: A Manager MUST be able to add client users to a client, supplying an initial
  password as in FR-012; a client user is a sign-in account linked to exactly one client.
- **FR-023**: A Manager MUST be able to create and edit a site with at minimum: name, address,
  and timezone.
- **FR-024**: Deactivating or deleting a client MUST automatically deactivate all associated
  client user accounts.

**Internationalisation and Accessibility**

- **FR-029**: Every user-facing string introduced by this feature MUST be externalised into a
  translation file; hardcoded UI text is forbidden. Hebrew is the primary language; English is
  the secondary language.
- **FR-030**: All screens delivered by this feature MUST use RTL-safe layouts (Hebrew renders
  right-to-left without visual breakage). The Playwright smoke-test suite MUST include an RTL
  pass for each user journey.

**Project Management**

- **FR-025**: A Manager MUST be able to create and edit a project with at minimum: linked
  client, linked site, project name, type (security or training), billing rate per hour,
  start date, and status (active or ended).
- **FR-026**: A Manager MUST be able to change a project's status between active and ended in
  either direction; an ended project can be set back to active (e.g., for contract renewals).
- **FR-027**: An Admin MUST be able to delete (hide) a project; billing and shift history is
  preserved.
- **FR-028**: Billing rates and employee default hourly rates MUST be greater than zero. A
  project end date, when present, MUST NOT precede its start date. The system MUST reject
  invalid values with a validation error (never a server error).

**Non-functional requirements *(Proposed)*** — beyond the original spec; added to give the
hardening work (T134–T142) a traceable source (Constitution I).

- **NFR-001 Organisation scoping**: Every read and write of organisation data MUST be limited to
  the caller's organisation. A resource of another organisation MUST be indistinguishable from a
  missing one (404, never 403). Every identifier received in a body, path or query (client, site,
  group, qualification, user) MUST belong to the caller's organisation. Repository lookups and
  mutations MUST take the organisation and filter by it, so they are safe on their own.
- **NFR-002 CORS allow-list**: Cross-origin requests MUST be accepted only from the origins listed
  in the `CORS_ORIGINS` setting (default `http://localhost:3000`, `http://localhost:8081`).
  A wildcard origin MUST NOT be configurable while credentials are allowed.
- **NFR-003 Production secrets**: With `ENVIRONMENT=production` the application MUST refuse to
  start if `JWT_SECRET_KEY` is the default placeholder or is shorter than 32 bytes (UTF-8).
- **NFR-004 Request tracing**: Every response MUST carry an `X-Request-Id` (a well-formed incoming
  value is honoured, otherwise a new one is generated). The id MUST appear in every log line and
  in the body of every problem-details error. Log lines MUST be valid JSON.
- **NFR-005 Session revocation**: Deactivating, deleting or changing the role of a user, and
  completing a password reset, MUST revoke all of that user's refresh tokens.
- **NFR-006 Token rejection**: An access token MUST be rejected if it is signed with another key,
  expired, unsigned (`alg=none`), tampered with, malformed, or lacks an expiry. The accepted
  algorithm is fixed by configuration, never taken from the token.
- **NFR-007 Strict input**: Request booleans MUST be JSON booleans (`0`, `1` and `"true"` are
  rejected with 422). Reserved example domains such as `hds.test` are accepted as email
  addresses only outside production.

### Key Entities

- **Organisation**: The top-level container; all other records belong to one organisation.
  Attributes: name, timezone.
- **User**: A sign-in identity with a role (Employee, Manager, Admin, Client), name, email
  address, contact details, active status. Email is the unique identifier across the
  deployment. A Client-role user is linked to one Client record.
- **Employee**: Not a separate record; an employee is a User with role Employee, Manager or
  Admin. Employee attributes (contact details, default pay rate, active status) live on the
  User. The default pay rate is set by managers and read-only for the employee.
- **Group**: A named collection of employees. Used for reporting and filtering.
- **Qualification**: A named capability that can be assigned to employees.
- **Client**: A company that commissions projects. Attributes: name, contact details, contract
  dates, active status.
- **Site**: A physical location. Attributes: name, address, timezone.
- **Project**: A unit of work for a client at a site. Attributes: client, site, name, type
  (security / training), billing rate per hour, start date, end date (optional), status
  (active / ended).

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A user with valid credentials can complete sign-in and land on their home screen
  in under 3 seconds (p95) on a connection of at least 10 Mbps.
- **SC-002**: Each of the four roles (Employee, Manager, Admin, Client) can be verified as
  correctly scoped — able to access what they should, denied what they should not — by a
  functional test suite that covers every access boundary defined in FR-007 through FR-010.
- **SC-003**: In a usability test, at least 8 of 10 first-time Managers create a complete
  employee record (with group and qualification assignments) in under 2 minutes unaided.
- **SC-004**: Zero data leaks between roles: automated boundary tests confirm that no role can
  read or write data outside its permitted scope (100% of FR-007 to FR-011 verified).
- **SC-005**: All six user stories (P1–P6; P6 is *Proposed*) can be demonstrated end-to-end in
  a live session using a freshly seeded database with no manual database intervention.
- **SC-006**: An Employee can update their own profile in under 1 minute.
- **SC-007**: Every screen delivered by this feature renders correctly in both LTR (English) and
  RTL (Hebrew) orientations, verified by the Playwright RTL smoke test with zero layout
  breakages.

## Assumptions

- Each deployment of HDS serves exactly one organisation. Multi-tenancy isolation is a data
  model concern, not a separate application concern for this feature.
- The "delete" operation for employees and projects means "hidden from UI and excluded from
  active queries" — the record and its history are preserved in the database. (Aligned with
  the spec's recommendation and FR-016/FR-027.)
- Invitation and password-reset emails (FR-006) require an outbound email capability to exist
  in the environment. This is a deployment dependency, not a feature requirement.
- The employee's default pay rate is set and managed by Managers/Admins; the employee can see
  it on their profile but cannot edit it. Pay-rate reporting is out of scope for this feature.
- Client users see project details (name, type, billing rate, dates, status) but not employee
  names, shift data, or pay figures.
- Groups and qualifications are organisation-wide and not scoped per project in this feature;
  project-level staffing needs (which reference qualifications and shift types) are deferred to
  the scheduling feature.
- A user can hold only one role at a time. Role changes take effect on the user's **next
  request** — the system re-checks the live role on every protected call, so a promotion or
  demotion is effective immediately without requiring a sign-out.
- Availability, shifts, scheduling, financial reports, and mobile apps are explicitly out of
  scope for this feature.
- This feature delivers the API only. The screens described in the user stories are delivered
  by the `apps/client` feature; acceptance scenarios here are verified at the API level.
  FR-030, SC-003, SC-006 and SC-007 are verified when that feature lands.
- Until the invitation flow (FR-006, *Proposed*) is delivered, a Manager supplies each new
  user's initial password at creation (FR-012, FR-022).

## Clarifications

### Session 2026-11-01

- Q: When a role changes for an active user, at what point do new permissions take effect? → A: On the next request — the system re-checks the user's current role on every protected call, so a demotion or promotion is immediate without requiring a sign-out.
- Q: Should sign-in protect against brute-force attempts? → A: Yes, account-level cooldown — lock the account for 15 minutes after 5 consecutive failures; show a clear message; reset the counter on success (FR-006b).
- Q: What field is unique per user, and what happens on a duplicate? → A: Email is unique across the deployment; creating a duplicate returns a clear conflict error (FR-012, User entity).
- Q: Can a deactivated employee be reactivated, and can an ended project be set back to active? → A: Yes to both — a Manager can reverse either state; lifecycle is not terminal (FR-015, FR-026).
- Q: How is the organisation resolved at sign-in? → A: Each deployment has one organisation; users sign in with email and password only and the organisation is resolved from the account (FR-001, FR-012).
- Q: How does a new user get a first password before the invitation flow exists? → A: The Manager supplies an initial password (minimum 12 characters, *Proposed*) at creation (FR-012, FR-022).
- Q: May the lockout response reveal that an account exists? → A: Yes, accepted trade-off; a locked account is reported as temporarily locked (FR-005, FR-006b).
- Q: Does this feature need RTL-safe layouts and externalised strings from the start? → A: Yes — all screens must use externalised strings and RTL-safe layouts per constitution Principle VIII; Playwright smoke tests include an RTL pass (FR-029, FR-030, SC-007).
