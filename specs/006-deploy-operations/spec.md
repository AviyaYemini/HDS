# Feature Specification: Deployment and operations

**Feature Branch**: `006-deploy-operations`

**Created**: 2026-11-06

**Status**: Draft

**Input**: User description (the stored prompt `.claude/hds/prompts/05-deploy/specify.md`): "Operators can
deploy HDS 2.0 repeatably, see its health, and recover from failure. Use docs/hds-spec.md sections 9 and
10. Requirements: one-command local environment, automated deploys, health and metrics endpoints,
backups with a tested restore, zero-downtime updates, no secrets in the repository."

Sections 9 (non-functional requirements) and 10 (engineering standards) continue the product spec in
`docs/hds-api.md`. Anything here that the original does not state is marked **Proposed**.

## Clarifications

### Session 2026-11-06

- Q: Where does the shared environment that main deploys to run? → A: On a cluster on the operator's
  own machine first (open decision 8's default). The pipeline builds and publishes each version; the
  environment notices the new version and installs it itself, so the pipeline needs no way in.
- Q: How much recent data may be lost if the database is lost? → A: Up to one day: a daily backup,
  kept 30 days.
- Q: Where are backups kept? → A: On the machine and, encrypted, in storage off the machine; the
  restore test restores from the off-machine copy.
- Q: How are operators told that a backup, a restore test or a deploy failed? → A: A message is sent
  to the operators for each failure, and it is also shown with the health status; no alert rules on
  metrics.
- Q: How does returning to the previous version work when the database changed since? → A: Code only,
  in one action; the database is left as it is, which is safe because every database change keeps the
  previous version working (FR-016). Undoing a database change is a separate, deliberate step.

## User Scenarios & Testing *(mandatory)*

The actors here are not application roles. A **developer** works on HDS from a copy of the repository;
an **operator** runs the shared environments. When working alone, one person is both. Employees,
Managers, Admins and Clients appear only as the people who must not notice an update.

### User Story 1 - A developer runs the whole system with one command (Priority: P1)

A developer with a fresh copy of the repository and only the documented prerequisites starts every part
of HDS (database, API, background worker and web client) with one command, following the quickstart.
Within minutes they can sign in with the documented sample accounts. One command stops everything; the
data is kept until they choose to wipe it.

**Why this priority**: Every other story is built and checked in this environment; the original asks for
a single command (§9 Portability) and a quickstart that works from a clean copy (§10 Documentation).

**Independent Test**: On a machine with only the prerequisites, follow the quickstart from a fresh copy,
sign in as each sample role, stop, start again and find the data still there.

**Acceptance Scenarios**:

1. **Given** a fresh copy and the prerequisites, **When** the developer runs the documented start
   command, **Then** every part starts, the database is brought to the current version, the sample data
   is loaded once, and the client and API answer.
2. **Given** a running environment, **When** the developer runs the start command again, **Then**
   nothing is duplicated and the existing data is kept.
3. **Given** a required setting is missing, **When** the developer starts, **Then** the start stops at
   once with a message naming the missing setting, and nothing starts with a made-up value.
4. **Given** a running environment, **When** the developer runs the stop command, **Then** everything
   stops and the data is still there on the next start; a separate, clearly named command wipes it.

---

### User Story 2 - An operator sees whether HDS is healthy (Priority: P1)

An operator, or the platform that runs HDS, asks each running part whether it is alive and whether it
is ready to serve. "Alive" means the process works; "ready" means it can also reach what it depends on.
The operator also reads metrics: for each endpoint the rate of requests, the rate of errors and how long
requests take, and for schedule jobs how many are waiting, running, succeeded and failed.

**Why this priority**: The other operational stories rely on it: an update only proceeds when the new
copy reports ready, and recovery starts with knowing something is wrong. The original asks for rate,
errors and duration per endpoint (§9 Observability).

**Independent Test**: With the system running, read the health checks and metrics; stop the database
and see the ready check fail while the alive check still passes; start it again and see ready return.

**Acceptance Scenarios**:

1. **Given** a running part with its dependencies up, **When** its alive and ready checks are read,
   **Then** both report healthy.
2. **Given** the database is unreachable, **When** the API's ready check is read, **Then** it reports
   not ready and names the failing dependency, and the alive check still reports alive.
3. **Given** requests have been served, **When** the metrics are read, **Then** they show, per endpoint,
   the request count, the error count and the distribution of durations.
4. **Given** someone outside the operators' network, **When** they ask for the metrics, **Then** they
   are refused or cannot reach them.
5. **Given** any health or metrics answer, **When** it is inspected, **Then** it contains no secrets and
   no personal data.

---

### User Story 3 - An operator recovers the data from a backup (Priority: P1)

Backups of the database are taken automatically on a schedule. An operator can restore any kept backup
into an empty environment by following a written procedure, and the result matches the data at the
time of the backup. A restore is tested regularly and automatically, not only when it is needed.

**Why this priority**: The database holds pay rates, worked hours and rosters that cannot be rebuilt;
the original asks for backups with a tested restore (§9 Reliability).

**Independent Test**: Take a backup of an environment with known data, restore it into an empty one,
and compare: every record is present and the application works on the restored copy.

**Acceptance Scenarios**:

1. **Given** the shared environment is running, **When** the backup schedule comes round, **Then** a
   backup is taken and kept, and its success or failure is recorded where the operator sees it.
2. **Given** a backup, **When** the operator follows the restore procedure into an empty environment,
   **Then** the data matches the backup and users can sign in and see their shifts and pay.
3. **Given** the automated restore test runs, **When** the restored copy does not match or the restore
   fails, **Then** the operators get a message and the test is marked failed.
4. **Given** backups older than the retention period, **When** the schedule runs, **Then** they are
   removed, and the newest successful backup is never removed.
5. **Given** a backup is stored, **Then** it is encrypted and readable only by operators, because it
   holds personal data (§9 Privacy).

---

### User Story 4 - A merge to main is deployed automatically (Priority: P2)

When a change is merged to the main line and every check passes, it is built once and deployed to the
shared environment without manual steps. The same built artefact that was checked is the one deployed.
Each deploy is recorded with the version it installed, and the operator can return to the previous
version with one action.

**Why this priority**: The original keeps main always deployable and runs deploy as the last stage of the
pipeline (§7, §10 Branching); it depends on Stories 2 and 5 to deploy safely.

**Independent Test**: Merge a visible change, watch it reach the shared environment without anyone
acting, then return to the previous version and see the change gone.

**Acceptance Scenarios**:

1. **Given** a merge to main whose checks all pass, **When** the pipeline finishes, **Then** the new
   version is running in the shared environment, and the version it reports matches the merge.
2. **Given** a merge whose checks fail, or whose build has a high-severity vulnerability, **Then**
   nothing is deployed and the running version is unchanged.
3. **Given** a deploy whose new version never reports ready, **Then** the deploy stops, the previous
   version keeps serving, and the operators get a message.
4. **Given** a deployed version, **When** the operator returns to the previous one, **Then** it runs
   again on the database as it is, without data loss (FR-016).

---

### User Story 5 - Users do not notice an update (Priority: P2)

While a new version replaces the old one, people using HDS keep working: pages load, requests succeed,
and a sign-in in progress stays signed in. A schedule job running during the update finishes or is
picked up again, and is never done twice.

**Why this priority**: The original asks for zero-downtime deploys by rolling updates, and for schedule
jobs that are retried and idempotent (§9 Reliability).

**Independent Test**: Send a steady stream of requests while a deploy runs and count failures; start a
schedule job just before the deploy and check it ends once.

**Acceptance Scenarios**:

1. **Given** a steady stream of requests, **When** a deploy runs, **Then** no request fails because of
   the deploy.
2. **Given** a signed-in user, **When** a deploy runs, **Then** they stay signed in afterwards.
3. **Given** a schedule job running when its worker is replaced, **Then** the job either finishes before
   the worker stops or is retried on another, and its result is recorded once.
4. **Given** a change that adds to the database, **When** it is deployed, **Then** the old and new
   versions both work while they run side by side.

---

### User Story 6 - No secrets live in the repository (Priority: P1)

Passwords, signing keys and other secrets are never stored in the repository or in built artefacts.
Each environment gets them from a secret store or its own settings at start-up. A check stops any change
that would add a secret, and the documentation tells a developer how to create their own local ones.

**Why this priority**: A leaked signing key lets anyone act as any user. The original requires secrets
only from a secret store or the environment (§9 Security), and the constitution forbids secrets in code.

**Independent Test**: Try to commit a change containing a sample secret and see it refused; search the
repository and the built artefacts and find none; start an environment with its secrets supplied from
outside.

**Acceptance Scenarios**:

1. **Given** a change that contains something shaped like a secret, **When** it is checked locally or
   in the pipeline, **Then** it is refused with the file and line named.
2. **Given** a built artefact, **When** it is inspected, **Then** it contains no secrets.
3. **Given** a shared environment, **When** it starts, **Then** it reads its secrets from outside the
   repository, and refuses to start if one is missing or is a known development value.
4. **Given** a secret must change, **When** the operator replaces it, **Then** the system uses the new
   one after a restart, without a code change. **Proposed.**

---

### Edge Cases

- The start command runs on a machine where a needed port is already in use: it stops with a message
  naming the port, not with a part half started.
- The database is at an older version than the code: the version change is applied before the API
  serves; if it fails, the API does not start and says why.
- Two deploys are triggered close together: they run one after the other, never at the same time
  (FR-023). **Proposed.**
- A backup runs while users are writing: the backup is consistent as of one moment.
- The backup storage is full or unreachable: the backup fails loudly and the previous backups are kept.
- The off-machine copy fails while the local backup succeeds: the backup counts as failed and is
  reported, and the copy is tried again on the next run.
- A restore is aimed at an environment that already holds data: it refuses unless the operator
  explicitly confirms replacing it.
- The ready check is slow because a dependency hangs: it answers within a short limit and reports not
  ready rather than hanging.
- A deploy includes a database change that removes something the previous version uses: it is not
  allowed in one release (FR-016).
- The metrics would show a user's name or e-mail address in a label: such values are never used.

## Requirements *(mandatory)*

### Functional Requirements

**Local environment**

- **FR-001**: A developer MUST be able to start every part of HDS (database, API, background worker and
  web client) with one documented command from a fresh copy of the repository, given only the
  documented prerequisites (§9 Portability, §10 Documentation).
- **FR-002**: Starting MUST bring the database to the current version before the API serves, and MUST
  load the sample data only if it is not already there.
- **FR-003**: Starting MUST stop with a clear message when a required setting is missing; it MUST NOT
  invent a value for a secret.
- **FR-004**: One command MUST stop everything and keep the data; a separate, clearly named command
  MUST wipe it.
- **FR-005**: The quickstart MUST be checked automatically from a clean copy on every change to the
  main line (§10 Documentation).

**Health and metrics**

- **FR-006**: Every long-running part (API and worker) MUST answer an alive check that depends only on
  the process itself, and a ready check that also checks what it needs (for the API: the database and
  the job queue, which is held in the database). The ready check MUST answer within 2 seconds. **The split into two checks is
  Proposed.**
- **FR-007**: The API MUST expose, per endpoint, the rate of requests, the rate of errors and the
  distribution of durations (§9 Observability).
- **FR-008**: The metrics MUST include schedule jobs by state (queued, running, succeeded, failed) and
  how long they take (§10 Background jobs). **Proposed.**
- **FR-008a**: Each schedule job MUST be followable as one trace from the request that queued it
  through the worker that ran it, and every log line MUST carry the identifiers that join it to its
  trace (§9 Observability: "a trace across API and worker for every schedule job"). A place to store
  and browse traces is **Proposed** for a later feature; until then a trace is followed through the
  logs.
- **FR-009**: Health checks and metrics MUST contain no secrets and no personal data; metrics MUST NOT be
  reachable from outside the operators' network.
- **FR-010**: Every running part MUST report the version it runs, so an operator can tell what is
  deployed. **Proposed.**
- **FR-010a**: A failed backup, off-machine copy, restore test or deploy MUST send a message to the
  operators naming what failed and when, and MUST also be shown with the health status. The message
  MUST contain no secrets and no personal data. **Proposed.**

**Backups and restore**

- **FR-011**: The shared environment's database MUST be backed up automatically at least once a day,
  and backups MUST be kept for at least 30 days, so at most one day of data is lost. **The frequency
  and retention are Proposed.**
- **FR-012**: A backup MUST be consistent as of one moment, encrypted, and readable only by operators
  (§9 Privacy). Each backup MUST also be copied to storage off the machine the environment runs on, so
  losing that machine loses no more than one day of data. **The off-machine copy is Proposed.**
- **FR-013**: A written procedure MUST let an operator restore any kept backup into an empty
  environment; a restore into an environment with data MUST require explicit confirmation.
- **FR-014**: A restore MUST be tested automatically at least once a week by restoring the newest
  backup into a separate, empty environment and checking it; a failure MUST be reported to the
  operator (§9 Reliability: "a tested restore"). The test MUST restore from the off-machine copy.
  **The weekly interval is Proposed.**

**Deploys and updates**

- **FR-015**: A merge to the main line whose checks all pass MUST be built once and published as a
  release version; the shared environment MUST notice the new version and install it without manual
  steps, with no way into the environment from the pipeline. A failed check or a high-severity
  vulnerability MUST stop the release (§7 CI/CD, §10 Dependencies).
- **FR-016**: Database changes MUST keep the version before them working, so old and new versions can
  run side by side during an update; a change that removes something ships in two releases, first
  stop using it, then remove it (§10 Migrations).
- **FR-017**: An update MUST replace running copies one at a time, sending traffic to a new copy only
  once it reports ready, and letting an old copy finish its requests before it stops; no request may
  fail because of the update (§9 Reliability: rolling updates).
- **FR-018**: A deploy whose new version does not become ready within a set time MUST stop and leave the
  previous version serving (§9 Reliability: "zero-downtime deploys via rolling updates").
- **FR-019**: Sign-ins MUST survive an update (§9 Reliability: "zero-downtime deploys").
- **FR-020**: A schedule job interrupted by an update MUST be retried and recorded once (§9
  Reliability: "retried and idempotent").
- **FR-021**: The operator MUST be able to return to the previous deployed version with one action. It
  changes only the running version and leaves the database as it is (FR-016 keeps that safe); undoing
  a database change is a separate, deliberate step and is never part of it. **Proposed.**
- **FR-022**: Each deploy MUST be recorded with its version, time, and outcome. **Proposed.**
- **FR-023**: Deploys MUST NOT run at the same time; a second waits for the first. **Proposed.**
- **FR-028**: Each published version MUST carry proof that the project's own release process made it
  from the main line, and the shared environment MUST install only a version with that proof, never
  one without it or with proof from anyone else. **Proposed** (the owner's request, 2026-11-07).

**Secrets**

- **FR-024**: No secret MAY be stored in the repository or in a built artefact; environments MUST get
  them from a secret store or their own settings at start-up (§9 Security, constitution).
- **FR-025**: A check MUST refuse a change that adds something shaped like a secret, both locally and in
  the pipeline, naming the file and line.
- **FR-026**: A shared environment MUST refuse to start when a secret is missing or is a known
  development value.
- **FR-027**: Changing a secret MUST need only the new value and a restart, not a code change.
  **Proposed.**

### Key Entities

- **Environment**: A place HDS runs: a developer's local one, or the shared one. Has its own settings
  and secrets, and runs one version at a time.
- **Release version**: An identified build of every part, made once and deployed unchanged.
- **Deploy record**: Which version was installed where, when, and whether it succeeded.
- **Backup**: A consistent copy of the database at one moment, with when it was taken, its size,
  whether it succeeded, and when it expires.
- **Restore test result**: When the newest backup was last restored and checked, and the outcome.
- **Health status**: Per running part, whether it is alive and ready, and which dependency fails.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A developer goes from a fresh copy to signed in, using only the quickstart, in under 15
  minutes the first time and under 2 minutes afterwards.
- **SC-002**: A merge to the main line reaches the shared environment, with no one acting, within 30
  minutes while the environment is on.
- **SC-003**: During a deploy under a steady stream of requests, no request fails because of the deploy.
- **SC-004**: An operator can tell whether each part is alive and ready, and which version it runs,
  within 1 minute.
- **SC-005**: At most one day of data is lost if the database is lost, and an operator restores the
  newest backup into an empty environment within 1 hour by following the procedure.
- **SC-006**: Every weekly restore test of the last month succeeded, or its failure was reported.
- **SC-007**: A search of the repository and the built artefacts finds no secrets.
- **SC-008**: Returning to the previous version takes under 10 minutes.

## Assumptions

- There is one shared environment for now: a cluster on the operator's own machine (open decision 8's
  default). It may be off at times; it catches up with the newest release when it is on. Moving to a
  cloud cluster is only for a public demo, later.
- Operators are trusted people outside the application's roles; this feature adds no screens for
  Employees, Managers, Admins or Clients.
- Daily backups and 30-day retention are settings and can change.
- Phone builds and app store releases are out of scope: they are a later v1 feature (push and native
  builds). This feature deploys the API, the worker and the web client.
- Alert rules on metrics (error rates, slow requests) and dashboards are out of scope; only failures of
  backups, restore tests and deploys send a message (FR-010a).
- The audit log is out of scope here; it is a later v1 feature. Traces are in scope (FR-008a): an
  earlier draft left them out, and planning brought them back, as the original asks for them.
- The existing local setup (a start command for the database and API) is extended, not replaced.
