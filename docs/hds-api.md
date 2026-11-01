
## 8. API surface

A JSON REST API under `/api/v1`, documented by the OpenAPI schema FastAPI generates. The web and mobile clients are the only consumers; the generated TypeScript client keeps both in sync with it.

| Resource group | Example endpoints | Notes |
| --- | --- | --- |
| Auth | `POST /auth/login`, `/auth/refresh`, `/auth/logout` | Access token 15 minutes; refresh token rotates on every use |
| People | `GET/POST /employees`, `PATCH /employees/{id}`, `GET/POST /groups` | Managers create and edit; only admins delete |
| Clients | `GET/POST /clients`, `GET /clients/{id}/projects` | Client users are accounts linked to one client |
| Sites and projects | `GET/POST /projects`, `PUT /projects/{id}/requirements` | Type, billing rate and staffing needs live here |
| Availability | `GET/POST /employees/{id}/constraints` | Employees edit their own; admins edit any |
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
- Timestamps travel as ISO 8601 in UTC; clients render in the site's timezone.
- Writes that can be retried (job creation, assignment) accept an `Idempotency-Key` header.
- Breaking changes ship under a new `/api/v2` prefix; additive changes do not.

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
