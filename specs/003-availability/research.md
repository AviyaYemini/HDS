# Research: Availability, Part 1 (leftovers from 002)

## E1. Editing the contract start (FR-001 to FR-004)

**Decision**: `contract_start` joins `ClientUpdateRequest` as an omittable, non-nullable date, like
the other required PATCH fields since 002 (F17). In `ClientService.edit_client` the date rule runs
when the change sets either date and the resulting pair differs from the stored one: the new start
(or stored), the new end (or stored). An unchanged pair is not judged, so a stored pair that already
breaks the rule does not block an unrelated edit (002 Group D kept the same rule for the end alone).

**Rationale**: one rule, one place; the stored value fills the side not sent.

**Alternatives**: a separate "change dates" operation (more surface for no gain).

## E2. Refusing unknown fields (FR-005)

**Decision**: a base model with `extra="forbid"` for the PATCH request models of employees, clients,
projects and sites. `/me` keeps `extra="allow"`, because its service already answers unknown fields with
403 `profile.field_not_allowed` and that stays. Pydantic answers 422, which the error handler maps to
`validation.request_invalid`; the generated schema gains `additionalProperties: false`.

**Rationale**: refusal happens before any service runs, so nothing can be half-applied.

**Alternatives**: a check in each service (four copies, and the request would reach the service).

## E3. The Client project path (FR-006)

**Decision**: `ProjectRepository.list_by_client` gains the status filter and serves every listing of
one client: a Client account's, and a staff listing filtered by client. `list_by_org` loses its optional
client, so it always means the whole organisation (staff only), and no read can mean "every client"
because an empty client was passed by mistake. The service still refuses an account with no client
first (002). If that guard were lost, `list_by_client` with no client would still match nothing, but only
because `projects.client_id` is NOT NULL.

**Alternatives**: a guard inside `list_by_org`, or only routing the Client caller elsewhere (both keep the ambiguous parameter for the next caller).

## E4. The unreachable promote methods (FR-007)

**Decision**: remove `EmployeeService.promote_to_manager` and `promote_to_admin`. Role changes go
through `edit_employee`, which already enforces the Admin-only rules, the last-Admin guard, session
revocation and the Client role lock. Before removing their unit tests, each rule they pin is checked to
have a test on `edit_employee`; any missing one is added there first.

## E5. Indexes for the clean-up (FR-008)

**Decision**: migration 019 creates `idx_refresh_tokens_expires_at` and
`idx_invitation_tokens_expires_at`, matching the existing `idx_*_user` names. The models declare them
too, so `alembic check` stays clean.

The index alone was not enough (found in implementation): `DELETE ... WHERE id IN (subquery)` joined
the batch against every row. Each batch now selects its rows' physical addresses through the index,
locked with `SKIP LOCKED`, then deletes exactly those addresses (a TID scan), so one batch reads about
its own rows whatever the size of the table or the expired backlog. The purge's drain stops only when a
batch removes nothing, because skipped rows make a batch short.

## E6. Dropping the retired columns (FR-009)

**Decision**: migration 019 drops `users.failed_login_attempts` and `users.locked_until`. Downgrade adds
them back with their 004 definitions (`failed_login_attempts SMALLINT NOT NULL DEFAULT 0`,
`locked_until TIMESTAMPTZ NULL`), so values are not restored; they were unused. The `User` model drops
the attributes, and every test or document that still names them changes with it.

**Rationale**: Constitution X's two steps are "stop using, then drop". 002 stopped using them; nothing
is deployed, so no running code can still read them.

---

# Research: Part 2, availability and manual shifts

## R1. Where the rules live

**Decision**: a new package `apps/api/src/hds/scheduling/` with one entry point,
`validate(change, context) -> list[Warning]`. It takes plain dataclasses: the shifts involved (id,
project, start and end in UTC, headcount), the employee's other shifts nearby, their availability entries
for those dates, and the organisation's settings (minimum rest, weekly maximum, time zone for weeks). It
returns warnings with a code, the employee and the shift ids. It covers overlap, rest, weekly hours,
blocked, absence, close, headcount and an assigned employee who is no longer active.

**Rationale**: the prompt asks for overlap, rest and weekly hours there; Constitution III says no rule
exists outside the package, so the availability and headcount checks go there too. The future generator
calls the same function. The hook and the import-graph check already treat a directory named
`scheduling` as pure.

**Alternatives**: rules in the services (breaks III); a separate distribution under `packages/` (that
tree is the TypeScript workspace).

## R2. Weeks and days

**Decision**: a "calendar week" for the weekly maximum is Sunday to Saturday in the organisation's time
zone, the Israeli working week; availability dates are dates in the site's time zone of the shift they
are compared with, so a night shift belongs to the date it starts.

**Alternatives**: ISO weeks (Monday start), which do not match the original's users.

## R3. What the warnings read

**Decision**: for an assignment the service loads that employee's shifts starting from the new shift's
start minus (longest shift plus minimum rest) to its end plus the same margin, and the whole calendar
week or weeks it touches, across every project, drafts included (FR-117). One query, indexed by employee
and start.

**Rationale**: bounded reads whatever the employee's history.

## R4. Local times to UTC

**Decision**: a shift's start and end are the shift type's local times on the chosen date in the site's
zone, converted with `zoneinfo`. A type that crosses midnight ends on the next date. A local time that
does not exist (spring forward) moves forward by the gap; an ambiguous one (fall back) takes the first
occurrence. Lengths and rest are differences of UTC instants (FR-118, SC-106).

## R5. Submission windows

**Decision**: a window has a first and a last date (inclusive) and a deadline (UTC instant). Windows of
one organisation cannot overlap: an exclusion constraint on `(organization_id, daterange)` with
`btree_gist`. An employee change is allowed when every date it touches lies in one window whose deadline
has not passed, judged in the service at the time of the request. Absences skip the window rule
(FR-105a).

**Alternatives**: a service check only (racy between two Managers).

## R6. One entry per date and shift type

**Decision**: entries are ranges, so uniqueness cannot be a plain unique key. The service takes the
employee's row lock (`lock_user_row`, as in 002) and refuses an entry that overlaps an existing one of
the same employee with the same or no shift type on any shared date and weekday (FR-104, which the
owner confirmed: an entry without a shift type covers every shift that date). `availability.conflict`
(409).

## R7. Employee view of shifts

**Decision**: a separate operation, `GET /me/shifts?from=&to=`, with its own response schema that has
no assignments, headcount or names, and carries the site's IANA time zone so the screens can show
local times (FR-118, FR-123). Published only, assigned to the caller only. The staff operation
`GET /shifts` refuses Employees and Clients.

**Rationale**: a schema without the fields cannot leak them; filtering fields from a shared schema can.

## R8. Warnings in answers

**Decision**: creating or changing a shift or an assignment answers 200 or 201 with the record and a
`warnings` list; each warning has a stable `code` (for example `warnings.rest_too_short`), the employee
id, the shift ids and a localised `detail`. Codes and texts sit in the API locale files with the error
codes.

**Idempotency (owner decision 2026-11-03)**: `POST /shifts/{id}/assignments` accepts an
`Idempotency-Key` header, as docs/hds-api.md says for retried writes (FR-115a). The key, the caller, a
hash of the request (method, path, body) and the stored answer (status and body) are kept in
`idempotency_keys` for 24 hours. A repeat with the same key and hash returns the stored answer and
writes nothing; the same key with another hash answers 422 `idempotency.key_reused`; two requests with
one key at once are serialised by the key's primary key, so exactly one runs. Without the header, the
unique (shift, employee) key still refuses a second assignment with 409 `assignments.duplicate`. Keys
are per caller, so one user cannot replay another's answer. Expired keys are removed by the existing
purge script.

## R9. The client stack

**Decision**: Expo with Expo Router, TypeScript strict, React Native Web for the browser build.
`openapi-typescript` generates types from the OpenAPI document into `packages/api-client`, and
`openapi-fetch` is the typed client over them, so no request is written by hand (Constitution V).
`i18next` and `react-i18next` with `en.json`; RTL through `I18nManager` and start/end styles only (no
left/right). Tokens: `expo-secure-store` on phones; on the web the access and refresh tokens stay in
memory only, so a reload signs in again, rather than putting a refresh token where scripts can read it.

**Alternatives**: Orval or a heavier generated SDK (more generated code to review); storing the web
refresh token in local storage (readable by any injected script).

## R10. Tooling prerequisites

**Decision**: Node.js 22 LTS and pnpm (the repository already has a pnpm workspace). They are not on the
development machine; P12 starts by installing them, or runs the client gates in CI only until then.
