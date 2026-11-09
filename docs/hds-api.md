
## 8. API surface

A JSON REST API under `/api/v1`, documented by the OpenAPI schema FastAPI generates. The web and mobile clients are the only consumers; the generated TypeScript client keeps both in sync with it: `packages/api-client` is generated from this document (`pnpm api-client:generate`), and CI fails when the committed copy differs (`pnpm api-client:check`).

| Resource group | Example endpoints | Notes |
| --- | --- | --- |
| Auth | `POST /auth/login`, `/auth/refresh`, `/auth/logout` | Access token 15 minutes; refresh token rotates on every use and works exactly once. Addresses are matched without regard to letter case. Any address, registered or not, locks after exactly the configured number of consecutive failures, and the count restarts when the lock ends. Failures from one network address are limited too (429 `auth.rate_limited`). *(Proposed)* Sign-in can answer 503 `service.busy` when the password-hashing queue is full |
| Invitations and password reset *(Proposed)* | `POST /auth/invite`, `/auth/invite/accept`, `/auth/password-reset/request`, `/auth/password-reset/confirm` | One-time links. An invitation completes only a pending account (never activated); a deactivated account cannot be invited back; links are cancelled when an account is deactivated, deleted or activated |
| People | `GET/POST /employees`, `PATCH /employees/{id}`, `GET/POST /groups` | Managers create and edit; only admins delete |
| Clients | `GET/POST /clients`, `GET /clients/{id}/projects` | Client users are accounts linked to one client |
| Sites and projects | `GET/POST /projects`, `GET/PUT /projects/{id}/requirements` | Type, billing rate and staffing needs live here. A site's time zone must be a real IANA name (`sites.timezone_invalid`, 422). Staffing needs: Managers and Admins read and replace a project's needs as a whole, as people per shift type and weekday (0 Sunday to 6 Saturday); a type the project does not use, a retired one or a repeated shift type and weekday is refused with 422 *(004)* |
| Scheduling limits | `GET/PATCH /organisation/scheduling` | The minimum rest (1 to 24 hours, default 11) and weekly maximum (1 to 168 hours, default 48) that generation and the warnings of manual rostering use; Managers read them, Admins change them *(004)* |
| Pay settings | `GET/PATCH /organisation/pay-settings`, `GET/PATCH /projects/{id}/pay-settings`, `GET/POST /special-days`, `PATCH/DELETE /special-days/{id}`, `GET/POST /projects/{id}/special-days`, `GET/PUT /projects/{id}/managers` | The premium defaults (overtime after 8 hours, 7 on a night shift, at 125% then 150%; the weekly rest from Friday 18:00 to Saturday 22:00 at 150%; increases of 25 and 50 points on a rest or special day), which projects follow unless they set their own; pay switched on or off per project and opened to its assigned Managers; special days for the organisation or one project (a project's day wins); Managers and Admins read, Admins change. An ended project's settings and days are frozen (409 `pay.project_ended`), and deleting a project that has not ended freezes them the same way. Only Admins set an employee's default rate (403 `users.rate_admin_only`) or a new project's pay settings *(005, Proposed)* |
| Availability | `GET/POST /availability-windows`, `PATCH/DELETE /availability-windows/{id}`, `GET/POST /employees/{id}/constraints`, `PATCH/DELETE /constraints/{id}` | Managers and Admins open submission windows. Employees edit their own blocked, open and close entries inside an open window and their absences any time; Admins edit anyone's at any time; Managers read everyone's *(003)* |
| Shift types | `GET/POST /shift-types`, `PATCH /shift-types/{id}`, `GET/PUT /projects/{id}/shift-types` | Every organisation starts with morning, afternoon and night; Managers and Admins add, change and retire types and choose each project's set; everyone who works shifts reads them *(003)* |
| Shifts | `GET /shifts?from=&to=&project_id=&status=`, `POST /shifts`, `PATCH/DELETE /shifts/{id}` | Cursor pagination; Managers and Admins read the roster, drafts included; only Admins create, change and delete shifts, including the pay field. A Manager is given a shift's own `pay_rate` only for projects whose pay they may see in the hours report, and `null` elsewhere; `GET /employees` gives a Manager no one's `default_hourly_rate` but their own *(005, Proposed)*. Every change answers with its rule warnings and is never refused for one *(003)* |
| Assignments | `POST /shifts/{id}/assignments`, `DELETE /assignments/{id}` | Admin only. Returns rule warnings (rest, overlap, weekly hours, availability, headcount, inactive employee), never refuses for them. `Idempotency-Key` on the create *(003)* |
| Availability submissions *(007, Proposed)* | `GET/POST /me/availability-submissions`, `POST /employees/{id}/availability-submissions` | An employee submits their availability for a window; the status is derived: Not submitted, Draft (changed since submitting), Submitted, or Locked once the deadline passes. After the deadline scheduling reads the submitted snapshot, and someone who did not submit counts as unavailable for the whole window: the auto-scheduler leaves them out and a manual assignment warns (`warnings.not_submitted`). An Admin may submit on an employee's behalf, and an Admin's hand edit of an employee's entries counts at once: the snapshot takes the live entries on the days the edit touched, and records who changed it beside who submitted it (ADR 017) |
| My shift *(007, Proposed)* | `GET /me/shifts?from=&to=` (also past dates), `GET /me/shifts/{id}` | The caller's own published shift with its site and the project's managers as contacts; never anyone else's |
| Publishing and my shifts | `POST /projects/{id}/publish`, `GET /me/shifts` | Managers and Admins publish a project's drafts for a date range at the project's site; drafts are seen by Managers and Admins only. Each employee sees only their own published shifts, with the project, the site and its time zone, and never who else is on them *(003)* |
| Schedule jobs | `POST /schedule-jobs`, `GET /schedule-jobs`, `GET /schedule-jobs/{id}`, `POST /schedule-jobs/{id}/accept`, `POST /schedule-jobs/{id}/discard` | Managers and Admins ask for a roster for 1 to 31 days over every active project, or chosen ones (`Idempotency-Key` accepted); a worker solves it within a time limit (20 s by default) and stores a proposal: who works each slot, every empty place with the rule that blocked it, the share of each employee, and whether it is proven optimal. Nothing changes in the roster until the proposal is accepted: accepting stores it as drafts, all or nothing (an existing draft keeps its id), and is refused if anything it was made from has changed since; nothing is published. A proposal open for 7 days expires. Employees and clients never see a proposal *(004)* |
| Finance | `GET /me/pay?month=`, `GET /finance/cash-flow`, `GET /finance/billing` | `/me/pay` *(005)*: the caller's gross pay so far in a month (`YYYY-MM`, the current one by default), before tax: each published shift that has ended, its hours at each rate (overtime, weekly rest, special days) with each rate's share of the pay (to the agora, adding up to the line's pay exactly) and its pay, and the totals; draft and future shifts are not counted; a project with pay off shows hours only, a missing rate is shown as missing, never zero. It replaces the `/me/pay-forecast` placeholder. Cash flow and billing are v1; cash flow may be negative |
| Overview | `GET /overview` | Managers and admins only |
| Reports | `GET /reports/work-log`, `/reports/hours`, `?format=xlsx` | *(005)* Managers and Admins. The work-log lists who worked at exactly one of a project, a site or a group, once per person and project, with shifts, hours and first and last date; for a project the range starts on its start date by default and then has no length limit, so it covers the whole project, and `to` is today; no pay. The hours report groups hours and shifts by employee or by project; an Admin sees pay for projects with pay on, a Manager only for projects they are assigned to whose pay is open to Managers, and elsewhere there is no pay figure at all. `shifts_missing_rate` counts the shifts shown with pay that have a line missing a rate; a row, group or total whose shown lines all lack a rate has that count and no pay figure, never 0.00. A demotion to Employee removes a Manager from their projects. Ranges given explicitly cover at most 366 days, and only published shifts that have ended count. Excel export (`?format=xlsx`) is v1 |
| Client portal | `GET /portal/projects`, `/portal/projects/{id}/billing`, `POST /portal/requests` | Scoped to the client's own projects, no employee fields |
| Roster editing *(007, Proposed)* | `POST/PATCH/DELETE /shifts`, `POST /shifts/{id}/assignments`, `PATCH/DELETE /assignments/{id}`, `POST /projects/{id}/publish` | A Manager edits and publishes only the rosters of the projects they manage, an Admin every one (403 `roster.project_not_managed`; decisions O9 and T098); a shift's own pay rate stays the Admin's. Each assignment may carry the person's own times within 4 hours of the shift, and every rule and pay figure reads them (ADR 018). Changes send the shift's `etag` as `If-Match`; a stale one is 412 `roster.changed`. The roster read names each assignment's person, deactivated ones included |
| Roster tools *(007, Proposed)* | `POST /assignments/{id}/overrides`, `GET /projects/{id}/publish-preview?from=&to=`, `POST /projects/{id}/copy-week`, `GET/POST/DELETE /projects/{id}/week-templates`, `POST /projects/{id}/week-templates/{id}/apply`, `POST /projects/{id}/roster/bulk`, `GET /availability-windows/{id}/submissions` | Overriding a warning with a reason; the publish review (any Manager reads it, as the roster); copying a week and saving or applying it as a template, with a dry run; bulk headcount and removal over drafts; who has submitted availability for a window. Each change takes the project's management as roster editing does |
| Schedule runs, scoped *(007, Proposed)* | `POST /schedule-jobs`, `POST /schedule-jobs/{id}/accept`, `/discard` | A Manager asks for, accepts and discards runs only over projects they manage (403 `roster.project_not_managed`); with none named, a run covers the Manager's own active projects (none: 422 `schedule.no_projects`) or an Admin's every one, and the job keeps the list. Accepting and discarding check again at the time, the whole run or nothing. `elapsed_seconds` is the time a run has searched by the server's clock, for the progress bar. Runs stay readable by any Manager |
| Proposal review *(008, Proposed)* | `GET /schedule-jobs/{id}/proposal`, `POST /schedule-jobs/{id}/edits`, `POST /schedule-jobs/{id}/edits/undo`, `GET /schedule-jobs/{id}/slots/{key}/cover`; `POST /schedule-jobs/{id}/accept` with a part | An Admin, or a Manager of every project in the run, reviews a proposal as a change to the roster when it was made (each assignment added, changed, removed or unchanged, with counts) and edits it before accepting: assign, unassign, move, swap. Each edit checks the rules over the person's shifts in every project; a break the roster allows overriding needs `override_reason` (422 `schedule.override_reason_required`, the breaks in `errors`), an inactive employee is refused. Accepting takes a part (`days`, `project_ids` or `assignments`; nothing named: all still open), all or nothing within one accept; a part whose drafts changed, or that breaks a rule not overridden, is refused (409 `schedule.part_out_of_date`) while the rest stays acceptable; the whole of an untouched proposal (no edit, nothing accepted, nothing named) is refused instead with 409 `schedule.proposal_out_of_date` once anything it was made from has changed, as before 008. Discarding a partly accepted proposal closes it as `partly_accepted`. The working copy has a `version` (the `ETag`); writes send it in `If-Match` (412 `schedule.version_stale`). An empty place's reasons keep their counts and name each person kept off by rest or the weekly maximum, with the numbers in minutes; "who else could cover" a slot lists every active staff member not on it, of any project or kind, judged over all their shifts and the working copy: free people first (no "would rather not", then the fewest nights, minutes and weekend shifts after it, then name), then those who would break a rule, with the rule and numbers, never pay, a site or another project's shift. Every edit, undo, accept and discard goes into the run's record, which only grows (ADR 020); the run's own steps and the record's reading come with 008's PR 4 |
| Saved views *(007, Proposed)* | `GET/POST /me/saved-views?screen=`, `DELETE /me/saved-views/{id}` | One person's named search and sort on an admin or report table, unique per screen (409 `views.duplicate_name`) |
| Me and sessions *(007, Proposed)* | `GET /me`, `GET /me/sessions`, `DELETE /me/sessions/{id}`, `POST /auth/logout-others` | `GET /me` also answers `capabilities` (what the person may do, from one map the API's gates share; specs/007-ux-redesign/contracts/capabilities.md) and `managed_project_ids` (where a Manager's `:project` capabilities apply). Sign-in takes `client_kind` (`web` by default, or `native`): a phone's refresh token lasts 30 days and renews on use, the web's 7 (ADR 016). `/me/sessions` lists the caller's signed-in devices with `current` on the one asking; ending another user's session answers 404. `/auth/logout-others` signs every other device out, and answers 409 `session.current_unknown`, ending nothing, when the asking session is not known |
| Contact card *(007, Proposed)* | `GET/PATCH /organisation/contact` | The phone, e-mail and opening hours every role reads (a client's contact card); only an Admin changes them |
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
| `shifts.times_invalid` | 422 | A shift that does not end after it starts, or only one of its two times given *(003)* |
| `shifts.type_not_in_project` | 422 | A shift of a type the project does not use, or a retired one *(003)* |
| `shifts.cursor_invalid` | 422 | A roster cursor the API did not return *(003)* |
| `shifts.pay_rate_invalid` | 422 | A shift pay rate that is not greater than zero *(003)* |
| `shifts.date_range_invalid` | 422 | A publish range that ends before it starts *(003)* |
| `assignments.duplicate` | 409 | The employee is already on the shift *(003)* |
| `assignments.employee_inactive` | 409 | Assigning an inactive employee *(003)* |
| `requirements.shift_type_not_in_project` | 422 | A staffing need on a shift type the project does not use, or another organisation's *(004)* |
| `requirements.shift_type_retired` | 422 | A staffing need on a retired shift type *(004)* |
| `requirements.duplicate` | 422 | A shift type and weekday given twice in one set of needs *(004)* |
| `schedule.range_invalid` | 422 | A generation range that ends before it starts or covers more than 31 days *(004)* |
| `schedule.project_not_active` | 422 | A generation project that is not an active project of the organisation, another organisation's included *(004)* |
| `schedule.cursor_invalid` | 422 | A job-list cursor the API did not return *(004)* |
| `schedule.not_ready` | 409 | Accepting or discarding a job that has not succeeded *(004)* |
| `schedule.proposal_closed` | 409 | Accepting or discarding a proposal already accepted, discarded or expired *(004)* |
| `schedule.proposal_out_of_date` | 409 | Accepting a proposal after something it was made from changed (availability, shifts nearby, employees, needs, shift types, limits) *(004)* |
| `idempotency.key_reused` | 422 | An `Idempotency-Key` sent again with a different request *(003)* |
| `sites.timezone_invalid` | 422 | A site is saved with a time zone that is not an exact name on the IANA list, letter case included |
| `auth.token_used` | 401 | A link is spent, cancelled, or belongs to an account that is not in the right state; the same answer in every case |
| `auth.token_revoked` | 401 | A refresh token was already used (see above) |
| `auth.token_expired` | 401 | A refresh token or link is past its expiry; a refresh token ends no session |
| `auth.account_locked` | 423 | After exactly N consecutive failed sign-ins, until the cooldown ends |
| `users.duplicate_email` | 409 | The address exists in any letter case, in any organisation |
| `users.admin_required` | 403 | A Manager re-inviting a pending account as Admin |
| `users.last_admin` | 409 | Removing, deactivating or demoting the last active Admin; safe against two Admins acting at once |
| `users.rate_admin_only` | 403 | A Manager sets, changes or clears an employee's default pay rate *(005)* |
| `projects.pay_settings_admin_only` | 403 | A Manager sends pay settings with a new project *(005)* |
| `pay.settings_invalid` | 422 | A premium setting or special-day rate outside its bounds; the published schema refuses most first, with `validation.request_invalid`, so this code is the service's own guard *(005)* |
| `pay.rest_window_invalid` | 422 | A weekly rest whose start and end are the same moment of the week *(005)* |
| `pay.project_ended` | 409 | Changing the pay settings or special days of an ended project *(005)* |
| `special_days.duplicate` | 409 | A second special day on one date, for the organisation or for one project *(005)* |
| `special_days.name_required` | 422 | A special day with a blank name *(005)* |
| `project_managers.not_staff` | 422 | Assigning to a project someone who is not an active Manager or Admin of the organisation *(005)* |
| `pay.month_invalid` | 422 | A month for `/me/pay` not written `YYYY-MM`; the published pattern refuses most first, with `validation.request_invalid`, so this code is the service's own guard *(005)* |
| `reports.scope_invalid` | 422 | A work-log without exactly one of `project_id`, `site_id` and `group_id` *(005)* |
| `reports.start_required` | 422 | A work-log of a site or a group without `from` *(005)* |
| `reports.range_invalid` | 422 | A report range that ends before it starts, or one given explicitly that covers more than 366 days *(005)* |
| `session.current_unknown` | 409 | Signing out of other devices with a token that names no live session of the caller, such as one issued before sessions were marked; nothing is ended *(007)* |
| `organisation.opening_hours_invalid` | 422 | Opening hours that name a weekday twice or close at or before they open *(007)* |

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
