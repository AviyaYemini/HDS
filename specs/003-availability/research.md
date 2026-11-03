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
