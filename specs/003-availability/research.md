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

**Decision**: the Client branch of `ProjectService.list_projects` calls `ProjectRepository.list_by_client`,
which gains the status filter; `list_by_org` keeps `client_id=None` meaning "every client" for staff
only. The service still refuses an account with no client first (002), so `list_by_client` is never
called with an empty id; it also returns nothing for one, by construction, because its filter is
equality.

**Alternatives**: a guard inside `list_by_org` (keeps the ambiguous parameter).

## E4. The unreachable promote methods (FR-007)

**Decision**: remove `EmployeeService.promote_to_manager` and `promote_to_admin`. Role changes go
through `edit_employee`, which already enforces the Admin-only rules, the last-Admin guard, session
revocation and the Client role lock. Before removing their unit tests, each rule they pin is checked to
have a test on `edit_employee`; any missing one is added there first.

## E5. Indexes for the clean-up (FR-008)

**Decision**: migration 019 creates `idx_refresh_tokens_expires_at` and
`idx_invitation_tokens_expires_at`, matching the existing `idx_*_user` names. The models declare them
too, so `alembic check` stays clean.

## E6. Dropping the retired columns (FR-009)

**Decision**: migration 019 drops `users.failed_login_attempts` and `users.locked_until`. Downgrade adds
them back with their 004 definitions (`failed_login_attempts SMALLINT NOT NULL DEFAULT 0`,
`locked_until TIMESTAMPTZ NULL`), so values are not restored; they were unused. The `User` model drops
the attributes, and every test or document that still names them changes with it.

**Rationale**: Constitution X's two steps are "stop using, then drop". 002 stopped using them; nothing
is deployed, so no running code can still read them.
