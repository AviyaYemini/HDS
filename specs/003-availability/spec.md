# Feature Specification: Availability (Part 1: leftovers from 002 first)

**Feature Branch**: `003-availability`

**Created**: 2026-11-03

**Status**: Draft

**Input**: User description: "Part 1 of feature 003 (availability), done first: four leftovers from 002-security-hardening, which the owner chose on 2026-11-03. (1) A Manager or Admin can change a client's contract start date; the rule that the contract end is not before the start is checked against the pair as it will be after the change, and a pair that already breaks the rule does not block an edit that does not touch either date. (2) Small corrections with no new capability: remove service operations that no request can reach (promoting to Manager or Admin, which role editing already covers); a Client account's project list never treats a missing client as "no filter"; a change request that names a field the operation does not accept is refused instead of silently ignored. (3) The bulk clean-up of expired sign-in tokens and one-time links finds expired records without reading every record. (4) The two account fields that held the sign-in failure count and lock, which nothing has used since 002 moved that count to one shared store, are removed. No behaviour visible to users changes except (1) and the refusal of unknown fields in (2). Availability itself is Part 2, specified later from the stored prompt. No technology details."

**Scope note**: This feature is built in two parts. **Part 1** (this text) finishes four items left over
from feature 002 and is implemented first. **Part 2**, availability and manual shifts (docs/hds-spec.md
AVL-*, EMP-1 and the manual SCH rows), is specified later from the stored prompt and appended here.
Part 1 items are *Proposed* (Constitution I): they correct existing behaviour or finish work 002 began,
and add no capability beyond editing a client's start date.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Correct a client's contract start date (Priority: P1)

A Manager or Admin finds that a client was saved with the wrong contract start date and corrects it,
without deleting and re-creating the client, which is often impossible because projects belong to it.

**Why this priority**: Today a wrong start date cannot be fixed at all, and it can stop a correct end
date from being saved, because an end before the start is refused. It is the only change in Part 1
that users can see.

**Independent Test**: Create a client with a start date, change only the start date, and confirm it is
stored; then try a start date later than the stored end date and confirm it is refused and nothing
changes.

**Acceptance Scenarios**:

1. **Given** a client with start 2026-01-01 and no end date, **When** a Manager changes the start to
   2026-02-01, **Then** the start is stored as 2026-02-01.
2. **Given** a client with start 2026-01-01 and end 2026-06-30, **When** a Manager changes the start
   to 2026-07-01, **Then** the change is refused with the client date-order error and nothing changes.
3. **Given** a client with start 2062-01-01 (a typing error) and no end date, **When** a Manager sends
   start 2026-01-01 and end 2026-12-31 in one change, **Then** both are stored, because the rule is
   judged on the pair as it will be after the change.
4. **Given** a client whose stored pair already breaks the rule, **When** a Manager renames it without
   sending either date, **Then** the rename is stored.
5. **Given** any client, **When** an Employee or a Client account tries to change its start date,
   **Then** it is refused as for any other client change.

---

### User Story 2 - Edits never silently ignore a field (Priority: P2)

Someone who sends a change with a field the operation does not accept, for example a misspelt field
name or a field that cannot be edited there, is told so, instead of receiving a success that did not
do what they meant.

**Why this priority**: A silent success hides mistakes. Feature 002 found that a cleared value sent
under a name the operation does not accept was answered as a success and changed nothing.

**Independent Test**: Send a change to a client, an employee, a project and a site that includes one
unknown field, and confirm each is refused and nothing is stored.

**Acceptance Scenarios**:

1. **Given** any record that can be edited, **When** a change names a field the operation does not
   accept, **Then** the change is refused with the request validation error and nothing is stored,
   including the fields that were valid.
2. **Given** a user editing their own profile, **When** the change names a field they may not edit,
   **Then** the existing refusal for a field not allowed on one's own profile still applies.

---

### User Story 3 - Housekeeping that changes nothing users see (Priority: P3)

The organisation's data stays correct and tidy as it grows: unused account fields are gone, the
clean-up of expired credentials stays quick however many records are kept, internal operations that
no request can reach are removed, and a Client account without a client can never be shown everyone's
projects by any route.

**Why this priority**: None of these changes what a user sees today, but each removes a way for a
later change to go wrong, and the clean-up would otherwise slow down as records accumulate.

**Independent Test**: The whole existing test suite passes unchanged, and the published API document
changes only where Stories 1 and 2 say it does.

**Acceptance Scenarios**:

1. **Given** a large number of unexpired credentials and a few expired ones, **When** the clean-up
   runs, **Then** it removes exactly the expired ones, and its work grows with the number of expired
   records, not with the total.
2. **Given** the retired account fields, **When** this feature is installed, **Then** they no longer
   exist, and every sign-in, lock and reset behaves exactly as before.
3. **Given** a Client account that has no client, **When** any route lists or reads projects for it,
   **Then** it gets nothing, whichever internal path serves the request.

### Edge Cases

- A change that sends the start date unchanged together with a new end date is judged on the new pair.
- Clearing the end date is always allowed; an open-ended contract has no end to compare.
- The start date cannot be cleared: a client always has one.
- A change that sends only unknown fields is refused, not treated as an empty change.
- A field name that differs only in letter case from an accepted one is unknown.
- The removed account fields may hold values left from before feature 002; they are discarded, since
  nothing has read them since 002.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: A Manager or Admin MUST be able to change a client's contract start date in the same
  change operation as the client's other fields.
- **FR-002**: When a change sets the start date, the end date or both to a value other than the
  stored one, the contract end MUST NOT be earlier than the start, judged on the pair as it will be
  after the change (using the stored value of any date not sent); otherwise the change is refused with
  `clients.date_range_invalid` and nothing is stored.
- **FR-003**: A change that sets neither date, or sends both back as stored, MUST NOT be refused
  because of the stored pair, even if that pair already breaks the rule.
- **FR-004**: The contract start date MUST NOT be clearable; a request to clear it is refused with the
  request validation error.
- **FR-005**: A change that names a field the operation does not accept MUST be refused with the
  request validation error and MUST change nothing. Editing one's own profile keeps its existing
  refusal, `profile.field_not_allowed`.
- **FR-006**: Any listing or read of projects for a Client account MUST return nothing when that
  account has no client, through every internal path, not only the one used today.
- **FR-007**: Internal operations that no request can reach MUST be removed, with no change in what
  any request does.
- **FR-008**: The clean-up of expired sign-in credentials and one-time links MUST find expired records
  without examining every stored record, so that its work grows with the number of expired records.
- **FR-009**: The two account fields that held the sign-in failure count and lock MUST be removed.
  Their values are discarded; nothing has read them since feature 002 moved the count to one store.
- **FR-010**: Apart from FR-001 to FR-005, nothing a user or a client of the API can observe MUST
  change.

### Key Entities

- **Client**: an organisation the company works for. Its contract has a start date, which is
  required and now editable, and an optional end date, which may not precede the start.
- **Account**: a user. It loses two fields that no longer hold anything.
- **Sign-in credential and one-time link**: records that expire and are removed by the clean-up.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A Manager can correct a client's wrong start date in one change, including when the
  stored end date is earlier than the new start would allow on its own and the end is changed in the
  same request.
- **SC-002**: Of change requests that name an unknown field, 100% are refused and none changes any
  stored value.
- **SC-003**: With 100,000 unexpired credentials and 1,000 expired ones, the clean-up removes exactly
  the 1,000 and examines no more than a few times that many records.
- **SC-004**: After installation, no stored account has the retired fields, and every existing test
  of sign-in, lockout and reset passes unchanged.
- **SC-005**: The published API document changes only to add the start date to the client change and
  to declare that unknown fields are refused on change operations.

## Assumptions

- Nothing is deployed yet, so removing the retired account fields in the release after the one that
  stopped using them meets the two-step rule for destructive changes (Constitution X).
- "Change operations" are the partial edits of a profile, an employee, a client, a project and a site.
  Requests that create records keep their current handling of unknown fields; they are not in scope.
- The client date-order error and the request validation error already exist and keep their meaning.
- Removing unreachable internal operations is judged by the existing tests and the published API
  document staying the same.

## Out of scope (Part 1)

- Availability, shift templates, shifts and assignments: Part 2.
- Changing which roles may edit a client.
- Any other leftover from 002, including the occasional false alarm in the contract fuzzing script.
