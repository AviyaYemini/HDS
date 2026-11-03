
## 8. API surface

A JSON REST API under `/api/v1`, documented by the OpenAPI schema FastAPI generates. The web and mobile clients are the only consumers; the generated TypeScript client keeps both in sync with it.

| Resource group | Example endpoints | Notes |
| --- | --- | --- |
| Auth | `POST /auth/login`, `/auth/refresh`, `/auth/logout` | Access token 15 minutes; refresh token rotates on every use and works exactly once. Addresses are matched without regard to letter case. Any address, registered or not, locks after exactly the configured number of consecutive failures, and the count restarts when the lock ends. Failures from one network address are limited too (429 `auth.rate_limited`). *(Proposed)* Sign-in can answer 503 `service.busy` when the password-hashing queue is full |
| Invitations and password reset *(Proposed)* | `POST /auth/invite`, `/auth/invite/accept`, `/auth/password-reset/request`, `/auth/password-reset/confirm` | One-time links. An invitation completes only a pending account (never activated); a deactivated account cannot be invited back; links are cancelled when an account is deactivated, deleted or activated |
| People | `GET/POST /employees`, `PATCH /employees/{id}`, `GET/POST /groups` | Managers create and edit; only admins delete |
| Clients | `GET/POST /clients`, `GET /clients/{id}/projects` | Client users are accounts linked to one client |
| Sites and projects | `GET/POST /projects`, `PUT /projects/{id}/requirements` | Type, billing rate and staffing needs live here. A site's time zone must be a real IANA name (`sites.timezone_invalid`, 422) |
| Availability | `GET/POST /availability-windows`, `PATCH/DELETE /availability-windows/{id}`, `GET/POST /employees/{id}/constraints`, `PATCH/DELETE /constraints/{id}` | Managers and Admins open submission windows. Employees edit their own blocked, open and close entries inside an open window and their absences any time; Admins edit anyone's at any time; Managers read everyone's *(003)* |
| Shift types | `GET/POST /shift-types`, `PATCH /shift-types/{id}`, `GET/PUT /projects/{id}/shift-types` | Every organisation starts with morning, afternoon and night; Managers and Admins add, change and retire types and choose each project's set; everyone who works shifts reads them *(003)* |
| Shifts | `GET /shifts?from=&to=&project=`, `PATCH /shifts/{id}` | Cursor pagination; admins edit the pay field |
| Assignments | `POST /shifts/{id}/assignments`, `DELETE /assignments/{id}` | Returns constraint warnings, not just errors |
| Schedule jobs | `POST /schedule-jobs`, `GET /schedule-jobs/{id}` | Asynchronous; covers all active projects by default; result is a draft roster to review |
| Finance | `GET /me/pay-forecast`, `GET /finance/cash-flow`, `GET /finance/billing` | Pay rates and billing rates; cash flow may be negative |
| Overview | `GET /overview` | Managers and admins only |
| Reports | `GET /reports/work-log`, `/reports/hours`, `?format=xlsx` | Date range required |
| Client portal | `GET /portal/projects`, `/portal/projects/{id}/billing`, `POST /portal/requests` | Scoped to the client's own projects, no employee fields |
| Calendar | `GET /calendar/{token}.ics` | Unguessable token per employee, revocable |
| Devices | `POST /devices` | Registers a push token |

The full endpoint catalog, with roles and release per endpoint, is on the API reference tab.

**Conventions**

- Errors use RFC 9457 problem-details JSON with a stable machine-readable `code`.
- An account is *pending* (invited, never activated), *active*, *deactivated* or *deleted*. Only accepting an invitation leaves pending; staff reactivation applies to deactivated accounts, and reactivating a pending one returns 409 `users.pending_cannot_reactivate` *(Proposed)*.
- An email address is one address whatever its letter case: it is trimmed and lowercased on the way in, sign-in, reset and invitation find the same account, and a second account with the same address in another case is refused with 409 `users.duplicate_email`. Because addresses are unique across the whole deployment, that 409 reveals that the address exists somewhere in the deployment (not in which organisation).
- A refresh token works once. Presenting a used one is refused with 401 `auth.token_revoked`; if it was rotated more than `REFRESH_REUSE_GRACE_SECONDS` (default 10) ago, every session of that user also ends, because it may be a stolen copy. A token past its expiry is refused with `auth.token_expired` and ends no session, even if it was already used; once cleanup has deleted it, the same token answers `auth.invalid_token`, and a deleted link answers `auth.token_expired`. No session can outlive "end every session": refresh, sign-in and every revocation of all sessions run one after the other for a user. Invitation and reset links are single use too (`auth.token_used`). A completed reset cancels every other link of the account and clears its sign-in lock. A new reset request cancels nothing, because anyone can make one, so an account can hold several live reset links.
- Deactivating or deleting a client, or removing a portal user, ends that user's sessions and cancels their links. A portal user cannot be reactivated while the client is inactive or deleted, or created under an inactive client.
- A pending account cannot be deactivated (409 `users.pending_cannot_deactivate`); only accepting its invitation changes it, and only an Admin's delete removes it, so a Manager cannot withdraw an invitation.
- A refused sign-in (unknown address, wrong password, inactive account) returns the same 401 `auth.invalid_credentials` after the same amount of work. An address with no account locks by the same rule as one with an account, with the same 423 `auth.account_locked`. The count restarts after `LOGIN_THROTTLE_RETENTION_HOURS` (24) without a failure, for every address. Failures from one network address inside a window of `LOGIN_LOCKOUT_MINUTES` answer 429 `auth.rate_limited` with `Retry-After`, for every address (`LOGIN_RATE_LIMIT_PER_NETWORK`, default 30; a successful sign-in does not clear this count).
- *(Proposed)* Operations that hash a password (sign-in, invitation, accepting one, confirming a reset, creating an employee or a client user) answer 503 `service.busy` with `Retry-After` when the hashing queue is full, and change nothing.
- Every request is one transaction: a change either fully happens or does not, and invitation and reset email is sent only after the change is saved and the response has gone out, so a 202 does not mean the mail was handed over.
- A reset request only counts and answers 202, for every address. The account is looked at, the link stored and the mail sent after the response; a failure there is logged and never reported, and a crash loses that one mail. Requests are limited per address and per network in a 60-minute window (defaults 5 and 30); over a limit nothing is issued or sent and the answer is still 202.
- In a `PATCH`, an omitted field is left alone, an explicit `null` clears a field that allows it (a phone, an hourly rate, a contract or project end date, a client's contact details), and `null` on a required field is refused with 422. In the `PATCH` of an employee, client, project, site, group or qualification, a field the operation does not accept is refused with 422 `validation.request_invalid` *(Proposed)*; editing one's own profile keeps its 403 `profile.field_not_allowed`. Create requests still ignore unknown fields.
- Timestamps travel as ISO 8601 in UTC; clients render in the site's timezone.
- Writes that can be retried (job creation, assignment) accept an `Idempotency-Key` header.
- Breaking changes ship under a new `/api/v2` prefix; additive changes do not.

**Error codes added or changed by feature 002**

| Code | HTTP | When |
| --- | --- | --- |
| `invitations.account_deactivated` | 409 | Inviting an address that belongs to a deactivated account |
| `users.pending_cannot_reactivate` | 409 | Reactivating an account that was invited and never activated |
| `users.pending_cannot_deactivate` | 409 | Deactivating an account that was invited and never activated |
| `users.client_inactive` | 409 | Reactivating a portal user whose client is inactive or deleted, or creating one under an inactive client |
| `users.client_role_locked` | 409 | A role change on a Client account *(Proposed)* |
| `clients.date_range_invalid` | 422 | A client's contract end is before its start, judged on the pair after a create or change; a Manager or Admin may correct either date *(Proposed)* |
| `windows.overlap` | 409 | A submission window shares a date with another *(003)* |
| `windows.date_range_invalid` | 422 | A submission window ends before it starts *(003)* |
| `availability.no_window` | 409 | An employee's blocked, open or close entry has a date outside every submission window *(003)* |
| `availability.window_closed` | 409 | The deadline of the window for those dates has passed *(003)* |
| `availability.conflict` | 409 | Another entry of the employee already applies to one of those shifts and dates *(003)* |
| `availability.admin_required` | 403 | A Manager changing another employee's availability *(003)* |
| `availability.absence_kind_invalid` | 422 | An absence without its kind, or a kind on another entry *(003)* |
| `availability.absence_whole_days` | 422 | An absence limited to a shift type or weekday *(003)* |
| `availability.date_range_invalid` | 422 | An entry ends before it starts *(003)* |
| `availability.no_matching_day` | 422 | An entry's weekday never falls between its first and last date *(003)* |
| `availability.shift_type_retired` | 422 | An entry limited to a retired shift type *(003)* |
| `shift_types.duplicate_name` | 409 | Another active shift type has the name, in any letter case *(003)* |
| `shift_types.retired` | 422 | A retired shift type added to a project *(003)* |
| `shift_types.times_equal` | 422 | A shift type that starts and ends at the same time *(003)* |
| `sites.timezone_invalid` | 422 | A site is saved with a time zone that is not an exact name on the IANA list, letter case included |
| `auth.token_used` | 401 | A link is spent, cancelled, or belongs to an account that is not in the right state; the same answer in every case |
| `auth.token_revoked` | 401 | A refresh token was already used (see above) |
| `auth.token_expired` | 401 | A refresh token or link is past its expiry; a refresh token ends no session |
| `auth.account_locked` | 423 | After exactly N consecutive failed sign-ins, until the cooldown ends |
| `users.duplicate_email` | 409 | The address exists in any letter case, in any organisation |
| `users.admin_required` | 403 | A Manager re-inviting a pending account as Admin |
| `users.last_admin` | 409 | Removing, deactivating or demoting the last active Admin; safe against two Admins acting at once |

Asking for another organisation's id (path, query or body) is always 404, the same as for an id that does not exist; the employee list refuses a `group_id` or `qualification_id` from another organisation the same way.

**Offline and real time**

- Mobile caches the employee's upcoming shifts locally and refreshes when online (MOB-3).
- Roster changes reach phones by push notification first. A live WebSocket channel is deferred until a feature needs it.

## 9. Non-functional requirements

Targets are starting points to validate in Phase 4, not commitments.

| Area | Requirement |
| --- | --- |
| Security | Argon2id password hashing; secrets only from Vault or environment; rate limiting on login; role and data-scope checks enforced in the service layer, not only the UI (employees never see other employees' rosters, project reports or cash flow; clients see only their own projects and no employee fields); dependency and container scanning in CI |
| Privacy | Employee contact data and hours are personal data: encrypted in transit and at rest, access logged in the audit trail, exportable and deletable on request |
| Performance | API p95 under 300 ms for reads; roster generation for 30 employees and 31 days finishes within the 20-second solve cap |
| Reliability | Database backups with a tested restore; zero-downtime deploys via rolling updates; schedule jobs are retried and idempotent |
| Internationalisation | Hebrew first with full right-to-left layout, English second; all strings externalised; dates and numbers formatted by locale; Sunday-first week |
| Accessibility | Screen-reader labels on all controls, minimum touch target of 44 points, contrast meeting WCAG 2.2 AA |
| Observability | Structured JSON logs with a request id, RED metrics (rate, errors, duration) per endpoint, a trace across API and worker for every schedule job |
| Quality | Typed code (mypy strict, TypeScript strict); at least 85% test coverage on the scheduling package; CI blocks merges on lint, type or test failures |
| Portability | Runs from a single `docker compose up` for development; no cloud-vendor-specific services in the core design |

## 10. Engineering standards

Sections 7 to 9 already set layering, typing, API conventions, security and observability. This section adds the standards they leave out, and names what enforces each one so none depend on memory. Numeric targets are starting points.

| Area | Standard | Enforced by |
| --- | --- | --- |
| Branching | Trunk-based: short-lived feature branches, a pull request for every change even when working alone, squash merge, `main` always deployable | Branch protection on `main` |
| Commits | Conventional Commits (`feat:`, `fix:`, `docs:`, `refactor:`) so the changelog can be generated | commitlint in CI |
| Pull requests | Template with what and why, test evidence, and a screenshot for any UI change | PR template, required review checklist |
| Definition of done | Tests added, types and lint clean, strings externalised, accessibility labels present, migration reversible, decision record written if a design choice was made | PR checklist |
| Formatting and static checks | Python: Ruff (lint and format) and mypy strict. TypeScript: ESLint, Prettier and `tsc` strict. One shared config per language | pre-commit hooks locally; CI is the authority |
| Backend tests | pytest. Services tested with fake repositories; integration tests run against a real PostgreSQL container, not SQLite | CI |
| Client tests | Jest and React Native Testing Library for components and state (at least 70% line coverage on logic, not styling). Playwright for the web build; Maestro for one end-to-end phone flow per release gate | CI |
| API contract | Schema-based API tests generated from the OpenAPI file (Schemathesis). CI regenerates the TypeScript client and fails if the committed copy differs | CI |
| Migrations | Every migration is reversible and reviewed. An applied migration is never edited. A destructive change ships in two releases: stop using, then drop | Review checklist, migration test in CI |
| Dependencies | Lockfiles committed (`uv.lock`, `pnpm-lock.yaml`). Automated update pull requests weekly (Renovate or Dependabot). Container base images pinned by version. High-severity vulnerabilities block the build | CI scan |
| Error handling | Services raise typed domain errors; one handler maps them to problem-details codes. Routers never catch broad exceptions; stack traces never reach clients | Code review, API tests |
| Logging | Structured JSON with request id and user id. No secrets and no personal data (phone, email) in logs. `info` for business events, `warning` for rejected input and rule conflicts, `error` for the unexpected | Log-format test, review |
| Background jobs | Each job has a status (queued, running, succeeded, failed) and a stored failure reason. Retries with backoff for transient errors only. An infeasible roster is a result, not a failure | Worker tests |
| Documentation | `README` quickstart works from a clean clone with `docker compose up`. One decision record in `docs/adr` per major choice. The OpenAPI file is the API reference | CI smoke test, review |

## 11. Roadmap

The work is sequenced so each phase delivers something runnable and teaches one new skill area. No calendar dates are set yet; sizing each phase is one of the open decisions.

&#91;embedded content: roadmap · 6 phases, 6 gates\]

Phase 4 can start once the Phase 1 gate passes, using mock roster data until Phases 2 and 3 finish. Phases 0 to 3 stay on familiar Python ground; Phase 4 is the largest learning step. Within Phase 3 the pay forecast, an MVP item, comes first; billing, cash flow and the client API are v1 items.

## 12. Open decisions

Nine choices to settle before Phase 0 starts. Each has a recommended default, so silence means the default stands. The original already settled four earlier open items: the system is for the company's real use, there is no site-manager role, clients are a core user, and pay estimates are in scope.

| # | Decision | Options | Recommendation |
| --- | --- | --- | --- |
| 1 | Rates | The original has a rate on the project and an hourly rate on the shift. Is the project rate what the client is billed, and the shift rate what the employee earns? | Yes: billing rate on the project; pay on the shift, defaulting to the employee's own rate |
| 2 | Tenders | The original speaks of clients' projects and tenders. Is a tender a separate stage before a project, or just a project? | A project with a status for now; add a tender stage if you need to track bids |
| 3 | Deleting | Admins delete employees and projects. Remove all history, or hide the record and keep its history? | Hide and keep history, so past reports and billing stay correct; it looks deleted in the screens |
| 4 | Payslip | The original asks for a pay report (תלוש). A summary of gross pay, or a legally valid payslip? | Summary of gross pay; legal payslips stay out of scope |
| 5 | Rest and hour limits | Confirm H2 (11 h rest) and H3 (48 h weekly) against applicable labour law and any collective agreement | Keep as configurable settings; verify the legal values before real use |
| 6 | Client framework | Expo (React Native), Flutter, Kotlin Multiplatform | Expo: TypeScript, one codebase for web and phones, shares types with the generated API client |
| 7 | Repository | New `hds-v2` repo, or rewrite inside the existing HDS repo | New repo; tag HDS 1.0 and leave it as the "before" |
| 8 | Hosting target for Phase 5 | Local kind or k3s, a home server, a managed cloud cluster | Local kind or k3s first; move to a cloud cluster only for a public demo |
| 9 | Weekly time budget | Hours per week you can commit | Needed to size each phase and set dates |
