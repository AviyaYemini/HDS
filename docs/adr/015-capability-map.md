# ADR 015: One capability map serves the API's role checks and the client's navigation

**Date**: 2026-11-07
**Status**: Accepted.

## Context

The owner's note on feature 007: clients "see only what applies to them, never 'You are not allowed
to do this.'" The cause was in the client: its navigation started from the staff view and hid two
reports, so a client was offered My availability, My shifts and My pay and the API refused them
(research report N1). The API's own role checks were scattered as role sets in `hds.core.deps` and
`hds.core.authz`. Nothing tied what the client offered to what the API allowed (N2).

## Decision

**One table, `hds.core.capabilities`.** A pure module maps each role to named capabilities
(`my_work.read`, `roster.edit:project`, `staff.delete`, …), following `docs/hds-spec.md` section 2: a
Manager inherits every Employee capability, and only an Admin deletes, edits pay on a shift or changes
admin-only settings. `specs/007-ux-redesign/contracts/capabilities.md` is its contract, and a unit
test reads that table and compares it with the module.

**The API asks it.** The route gates `require_staff`, `require_manager`, `require_manager_or_client`
and `require_admin` each ask for one capability whose role set is exactly the set the gate always
admitted, so no route changes who it lets in; a test pins that. Project-scoped capabilities, named
with a `:project` suffix, are checked in services: an Admin holds them on every project, a Manager only
on the projects they manage (`project_managers`).

**The client reads it.** `GET /me` returns `capabilities` (sorted) and `managed_project_ids`. The
client builds its tabs, routes, buttons and menus from that list only, and refreshes it after any
403. The list is advisory for the UI; every endpoint still enforces the matrix in its service
(Constitution VI).

## Consequences

- A new screen or action is gated by adding a capability here, to the contract table and to the
  client, never by testing a role in the client.
- Changing who may do something is one change in one table, visible in review next to its contract.
- The gates' error codes (`users.manager_required` and so on) are unchanged, so existing clients and
  tests keep working.

## Alternatives considered

- **Roles in the client**, as before: the cause of N1, and it drifts from the API.
- **Probing endpoints from the client** to see what answers 403: slow, and the user still meets
  refusals.
- **Permissions stored per user in the database**: more than four fixed profiles need; it can come
  later behind the same `GET /me` field.
