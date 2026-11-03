# HDS 2.0 — Product & Technical Spec

Nov 1, 2026 · @Aviya

## 1. Vision and goals

HDS gives a security and training company one clear, live picture of its projects, employees and money, and shows each person the slice of it they are allowed to see. It starts as a web application; native iOS and Android apps follow from the same codebase. This spec is built from the original HDS product spec, which stays the source of truth. Anything added beyond it is marked as proposed.

**The problem**

- Tracking projects (security and training) and employees is chaotic.
- It is hard to give a clear picture of the business at any moment, from a single shift up to the whole company.
- Money is hard to follow, and data is hard to reflect back to employees and clients.

**Users**

1. Company owners, who get a snapshot of the business whenever they need one.
2. Clients, who see what is happening in their own projects and tenders.
3. Employees, who receive and submit a work roster, report shifts, reserve duty and similar, and see a pay forecast.

Managers and admins run the system; the profiles are in section 2.

**Core scenarios**

- **A.** A manager wants to know who worked at a given site or group, in a date range or since the project was created.
- **B.** A manager wants to build a roster from the constraints employees submitted, covering all active projects.
- **C.** A manager wants the up-to-date cash flow from existing data, or a future forecast.
- **D.** A client wants a breakdown of what they were billed: number of hours, number of employees and shift types.
- **E.** An employee wants to submit availability or see their personal roster.
- **F.** An employee wants to know the gross pay they can expect at the end of the month.

**Non-functional goals**

1. A modular project that can be extended when needed and adapted to additional customers (other companies).
2. Information security.
3. Sound programming principles.

**Success**

The original definition: the system runs satisfactorily over the network and signs users in under several different profiles, which grant access to data and the ability to create or change values and types.

Proposed engineering targets: a roster for 30 employees across several projects in under 30 seconds; no generated roster breaks a hard rule, proven by automated tests; an employee reaches their roster in three taps.

**Out of scope for now**

- Net pay, tax and social-insurance calculations. The original asks for gross pay only.
- Legally valid payslips. The system produces a pay report with gross figures.
- Payment processing and accounting-system integration.
- Billing other companies for using HDS. The data model is ready for several companies, but there is no subscription billing.

**Idea for later, from the original:** a bulletin board for casual jobs.

## 2. Users and roles

Four profiles, as in the original: employee, manager, admin and client. A manager inherits everything an employee can do, because a manager is sometimes also an employee who works shifts. Admin is the set of extra permissions that some managers hold. Company owners sign in as managers and use the overview for their snapshot. An employee can be promoted to manager, and a manager can resign.

| Capability | Employee | Manager | Admin | Client |
| --- | --- | --- | --- | --- |
| See own roster, hours, pay forecast and pay report | Yes | Yes | Yes | No |
| Submit own availability and constraints | Yes | Yes | Yes | No |
| Report own worked shifts, reserve duty and similar | Yes | Yes | Yes | No |
| See other employees' rosters and availability | No | Yes | Yes | No |
| Company overview: cash flow, available workforce, project status, employee availability | No | Yes | Yes | No |
| Project reports and cash flow | No | Yes | Yes | Own projects' billing only |
| Generate a roster across all active projects | No | Yes | Yes | No |
| Edit the roster and employee availability by hand | No | No | Yes | No |
| Edit the pay field on a shift | No | No | Yes | No |
| Delete employees or projects | No | No | Yes | No |
| Manage employees, clients and projects (create and edit) | No | Yes | Yes | No |
| See billing detail: hours billed, when, in which roles | No | Yes | Yes | Own projects only |
| Send a request or inquiry to the manager or customer service | No | No | No | Yes |
| Answer client requests | No | Yes | Yes | No |

**Access rules from the original**

- Employees cannot view project reports, cash flow or other employees' rosters.
- Clients cannot access employee fields or other clients' projects.
- The original lists roster generation as a manager scenario and roster editing under admin permissions, so the table keeps that split.

## 3. Legacy audit: what HDS 1.0 teaches us

HDS 1.0 (FastAPI, SQLite, Jinja2; about 2,300 lines of Python, 2 passing tests) proves the roster part of the domain works, but it covers only a slice of the original spec. Its useful parts become requirements; its flaws become explicit rules for 2.0.

**Carry over as requirements**

- Projects with an hourly rate and per-shift staffing needs (morning, afternoon, night).
- Default shift templates: morning 06:00-14:00, afternoon 14:00-22:00, night 22:00-06:00 (overnight shifts roll into the next day).
- Per-employee constraints: allowed or blocked shifts, allowed or blocked dates, shifts marked open or close (called preferred and disliked in 1.0).
- Shift assignment as many-to-many (several employees per shift).
- Manual shift reporting by employees, cost reports per project and employee, Excel export, and an `.ics` calendar feed.
- A business overview page (`/admin/overview`), which is the seed of the original's manager overview.
- Hebrew RTL interface.

**Drop or redo**

| Problem in 1.0 | Rule for 2.0 |
| --- | --- |
| `venv/`, `.pycache/`, `.DS_Store` and `database.db` (with password hashes) committed to git | Strict `.gitignore`, secrets and data never in the repo |
| Hardcoded session secret `supersecretkey` | All secrets from environment or Vault |
| Unsalted SHA-256 passwords | Argon2id hashing |
| No CSRF protection on form posts | Token-based auth; no cookie-authenticated state changes without CSRF defence |
| `admin.py` at 1,241 lines mixing HTTP, SQL, scheduling and reports | Layered architecture; scheduler is a pure, separately tested module |
| Hand-rolled `ALTER TABLE` migrations | Versioned migrations (Alembic) |
| Unused dependencies (`sqlalchemy`, `PyJWT`) | Every dependency justified and used |
| One `hourly_rate` on the project used for both cost and billing, and no client, pay or training concept | Two rates as in the original: a billing rate on the project, and a pay rate on the employee and on the shift (section 6) |
| Only an `is_admin` flag, no client login | Four profiles: employee, manager, admin, client (section 2) |

**Scheduler flaws that become test cases**

- Rest gaps are not enforced: the one-shift-per-day check compares dates only, so an employee can finish a night shift at 06:00 and start a morning shift at 06:00 the same day.
- A "disliked" shift acts as a hard block, so shifts stay unfilled when a soft preference should have yielded.
- Fairness balances shift counts, not hours worked, and does not balance nights.
- Generation runs one project at a time, while the original asks for a roster across all active projects.

## 4. Functional requirements

Every requirement below traces to the original spec (scenarios A to F, its entities, its rules) or is marked **Proposed**, so you can strike any proposed row. Releases: **MVP** (first usable release), **v1**, **Later**. The original gives no priorities, so the releases are my proposal: MVP covers scenarios A, B, E and F, because they depend only on employees, projects, rates and the roster; v1 adds C and D, which need worked hours and client accounts.

| ID | Module | Requirement | Source | Release |
| --- | --- | --- | --- | --- |
| AUTH-1 | Accounts | Sign in, refresh, sign out, and sign out of all devices | Original success criteria | MVP |
| AUTH-2 | Accounts | Invitations and password reset by email link | Proposed | v1 |
| AUTH-3 | Accounts | Four profiles (employee, manager, admin, client) with access limited as in section 2; a manager inherits employee actions | Original entities 1 to 3, rule 3 | MVP |
| ORG-1 | Organisation | Manage employees: create, edit, promote to manager, deactivate; admin can delete | Original entity 2, changes over time | MVP |
| ORG-2 | Organisation | Manage clients: company details, client users, contract end | Original entity 3, changes over time | MVP |
| ORG-3 | Organisation | Manage projects: client, type (security or training), billing rate, location, dates, status; admin can delete | Original entities 6 and 7 | MVP |
| ORG-4 | Organisation | Staffing needs per project by shift type and weekday | Original entity 7 | MVP |
| ORG-5 | Organisation | Custom shift templates beyond morning, afternoon and night | Proposed | v1 |
| ORG-6 | Organisation | Employee groups, so reports can be run per group | Original scenario A | v1 |
| AVL-1 | Availability | Employees submit blocked dates and blocked shift types | Original scenario E, rule 2 | MVP |
| AVL-2 | Availability | Employees mark shifts as open (would like to work them) or close (would rather not); both are preferences the scheduler weighs, not blocks | Original rule 2 | MVP |
| AVL-3 | Availability | Weekly cycle: a new roster each week, with a submission deadline | Original changes over time | v1 |
| AVL-4 | Availability | Report absences by type: leave, reserve duty, sick | Original users, employee | v1 |
| AVL-5 | Availability | Recurring weekly availability pattern | Proposed | v1 |
| SCH-1 | Scheduling | Generate a roster from submitted constraints across all active projects for a date range, with a preview before saving | Original scenario B, rule 2 | MVP |
| SCH-2 | Scheduling | List slots that could not be filled, with the reason each failed | Proposed | MVP |
| SCH-3 | Scheduling | Admin adds, removes, edits and extends the roster by hand | Original entity 4, admin permissions | MVP |
| SCH-4 | Scheduling | Live rule warnings while editing a roster | Proposed | v1 |
| SCH-5 | Scheduling | Lock selected assignments so regeneration keeps them | Proposed | v1 |
| SCH-6 | Scheduling | Employees swap shifts with approval | Proposed | Later |
| EMP-1 | Employee | See the personal roster (published shifts only) | Original scenario E | MVP |
| EMP-2 | Employee | Report a worked shift for review | Original entity 1 | v1 |
| EMP-3 | Employee | Monthly hours and pay report with gross figures | Original entity 1 | v1 |
| FIN-1 | Finance | Pay rate per employee, and an hourly pay field on each shift that an admin can edit | Original entities 2 and 7 | MVP |
| FIN-2 | Finance | Billing rate per project | Original entity 6 | MVP |
| FIN-3 | Finance | Expected gross pay for the month, per employee | Original scenario F | MVP |
| FIN-4 | Finance | Cash flow: current from recorded hours and forecast from the roster; billing income minus pay; may be negative | Original scenario C, rule 1 | v1 |
| FIN-5 | Finance | Billing breakdown per client and project: hours, number of employees, shift types, amounts | Original scenario D | v1 |
| FIN-6 | Finance | Record real invoices and payments to make cash flow exact | Proposed | Later |
| OVR-1 | Overview | Manager overview: cash flow, available workforce, project status, employee availability | Original entity 2 | v1 |
| RPT-1 | Reports | Who worked at a site or group in a date range, or since the project was created | Original scenario A | MVP |
| RPT-2 | Reports | Hours and pay reports per employee and per project | Original entity 8 | MVP |
| RPT-3 | Reports | Employee availability report: who is free when | Original entity 8 | v1 |
| RPT-4 | Reports | Excel and PDF export | Proposed, from HDS 1.0 | v1 |
| RPT-5 | Reports | Calendar feed (`.ics`) per employee and per project | Original entity 9 (marked with a question mark) | v1 |
| CLI-1 | Client portal | Clients sign in and see only their own projects, never employee fields | Original entity 3, rule 3 | v1 |
| CLI-2 | Client portal | Client sees the billing breakdown (FIN-5) for their projects | Original scenario D | v1 |
| CLI-3 | Client portal | Clients send requests or inquiries; managers answer them | Original entity 3 | v1 |
| MOB-1 | Phone apps | Publish the same app as native iOS and Android builds | Added by you after the original | v1 |
| MOB-2 | Phone apps | Push notification when a roster is published or changed | Proposed | v1 |
| MOB-3 | Phone apps | Offline read access to the upcoming roster | Proposed | v1 |
| MOB-4 | Phone apps | Clock in and out at the site, with an optional location check | Proposed | Later |
| ADM-1 | Admin | Audit log of who changed what and when | Proposed | v1 |
| ADM-2 | Validation | Times and rates must be positive; only cash flow values may be negative | Original rule 1 | MVP |
| IDEA-1 | Later | Bulletin board for casual jobs, where open shifts can be claimed | Original idea for later | Later |

## 5. Scheduling engine

The engine is a pure function: given employees, constraints, shift slots from every project being scheduled, and existing assignments, it returns an assignment set, a list of unfilled slots with reasons, and a score breakdown. It touches no database and no HTTP, so it is fully unit- and property-testable. It is built as a constraint-programming model (OR-Tools CP-SAT), with HDS 1.0's greedy algorithm kept as a baseline to benchmark against. By default a run covers all active projects at once (original scenario B), so one employee is balanced across projects and rule H1 holds between them.

**Hard constraints (never violated)**

| ID | Rule | Default |
| --- | --- | --- |
| H1 | An employee is assigned to at most one shift at any moment | fixed |
| H2 | Minimum rest between the end of one shift and the start of the next | 11 hours, configurable |
| H3 | Maximum hours per employee per calendar week | 48 hours, configurable |
| H4 | Blocked dates and blocked shift types are respected | fixed |
| H5 | Only active employees with the required qualification are assigned | fixed |
| H6 | Locked (manually pinned) assignments are kept | fixed |

The defaults for H2 and H3 are placeholders to confirm against Israeli labour rules before launch (see open decisions). H4 implements the original's rule that the roster follows the constraints employees submit; H5 and H6 are proposed additions.

**Soft goals (traded off in the objective)**

- Cover every required slot (highest weight).
- Honour shifts marked open; avoid shifts marked close.
- Balance total hours across employees.
- Balance the number of night shifts across employees.
- Limit consecutive working days.
- Minimise changes against an existing roster when regenerating.

**Objective**

```latex
\min \; w_1 U + w_2 P + w_3 \left(H_{\max} - H_{\min}\right) + w_4 \left(N_{\max} - N_{\min}\right) + w_5 C
```

U is the count of unfilled slots, P the preference penalty, H the hours per employee, N the night shifts per employee, and C the changes versus the previous roster. The weights w are configurable per organisation.

**Behaviour**

- If no feasible full roster exists, the solver returns the best partial roster and names the blocking constraint for each unfilled slot.
- Solve time is capped (default 20 seconds) and returns the best solution found so far, flagged as not proven optimal.
- Generation runs as a background job; the client polls or receives a notification.

**Test strategy**

- Example-based tests for each hard constraint, including every bug listed in the legacy audit.
- Property-based tests (Hypothesis): for any random valid input, the output violates no hard constraint.
- Benchmark suite comparing greedy and CP-SAT on coverage, fairness spread and runtime at 10, 30 and 100 employees.

## 6. Domain model

The model grows HDS 1.0's six tables to fourteen entities. Every table carries `id` (UUID), `created_at`, `updated_at` and an `organization_id`, so a second company can use the same system, as the original asks ("adaptable to additional customers").

| Entity | Key fields | Relationships |
| --- | --- | --- |
| Organization | name, timezone, scheduling settings (rest hours, weekly cap, objective weights) | has many users, clients, sites |
| User | name, email, phone, password hash, role (employee, manager, admin, client), active, locale, default hourly pay rate | belongs to organization; managers and admins are also employees; a client user belongs to one client |
| Client | name, contact details, contract start and end, active | has many projects and client users |
| Site | name, address, timezone | has many projects |
| Project | client, site, name, type (security or training), billing rate per hour, status (active, ended), start and end date | belongs to a client and a site |
| Group | name | many-to-many with users |
| ShiftTemplate | name, start time, end time, crosses midnight | belongs to organization |
| ShiftRequirement | weekday, template, headcount, required qualification | belongs to project |
| Shift | project, start and end (UTC), location, hourly pay rate (admin-editable), status (draft, published, locked) | has many assignments |
| Assignment | shift, user, source (generated, manual, reported), locked, approved by | joins shift and user |
| Constraint | user, type (blocked, open, close, absence), absence kind (leave, reserve duty, sick), scope (date, shift type, weekday), valid from and to | belongs to user |
| Qualification | name | many-to-many with users |
| ClientRequest | client, subject, message, status, answered by | belongs to a client |
| AuditEvent | actor, action, entity, before, after, timestamp | append-only |

Invoices, payments and the casual-jobs bulletin board are later additions and have no tables yet.

**Money rules**

- **Pay:** the hourly pay for an assignment is the shift's pay rate if set, otherwise the employee's default rate. An admin can edit the shift's pay field, as in the original.
- **Billing:** hours worked on a project times the project's billing rate.
- **Cash flow:** billing income minus pay. It is the only value that may be negative.
- **Validity:** times and rates must be positive (original rule 1).

**Access rules in the model.** Employees cannot read other employees' rosters, project reports or cash flow, and clients cannot read employee fields or other clients' projects (original rule 3). These are enforced in the service layer, not only in the screens.

**Changes from HDS 1.0**

- Constraints become typed rows instead of free-form `value_json` that the parser had to guess at.
- Shift times are stored as UTC timestamps with a site timezone, so overnight shifts and daylight-saving changes compute correctly.
- Clients, groups, project types, pay rates and absences are new, because the original needs them.
- Admin "delete" hides an employee or project and keeps its history, so past rosters, reports and billing stay correct. This is a design choice to confirm (section 12).

## 7. Architecture and tech stack

A Python API and solver behind a versioned REST interface, and one TypeScript codebase that ships to web, iOS and Android. The backend extends your existing Python strength; the client is the new skill to learn.

&#91;embedded content: architecture · 8 components\]

Clients call the API over HTTPS. A solve request is queued in Redis, run by a worker, saved to PostgreSQL, and pushed to phones.

| Layer | Choice | Why |
| --- | --- | --- |
| API | Python 3.12, FastAPI, Pydantic v2 | Continues HDS 1.0; typed models; OpenAPI generated for free |
| Data access | SQLAlchemy 2.0, Alembic migrations | Replaces hand-rolled `ALTER TABLE` |
| Database | PostgreSQL 16 | Real constraints, time zones, concurrent writes; SQLite stays for unit tests only |
| Solver | OR-Tools CP-SAT | Industry-standard for rostering; Python-native |
| Background jobs | Redis with a Celery worker | Solves can take seconds; keep the API responsive |
| Auth | Argon2id, short-lived JWT access token, rotating refresh token | Works for web and native clients |
| Client | TypeScript, Expo (React Native) with React Native Web | One codebase for iOS, Android and web, RTL support built in |
| API client | Generated from OpenAPI | Types shared end to end, no hand-written fetch code |
| Push | Expo notifications over APNs and FCM | Covers both phone platforms |
| Packaging | Docker images, Docker Compose locally | Same artefacts in dev and production |
| Deployment | Kubernetes with Helm, secrets in HashiCorp Vault | Builds on your existing Kubernetes and Vault experience |
| CI/CD | GitHub Actions: lint, type-check, test, build, scan, deploy | Gate every merge |
| Observability | Structured JSON logs, Prometheus metrics, OpenTelemetry traces | Debug a slow solve in production |

Repository layout: one monorepo with `apps/api`, `apps/client`, `packages/api-client`, `infra/` (Docker, Helm, CI) and `docs/` (this spec and decision records).

**Backend layering.** Routers handle HTTP only; services hold use-case logic; repositories own SQL; the `scheduling` package is pure domain code with no imports from the layers above it.

&#91;embedded content: UML package view · 4 components, 7 packages\]

Dashed arrows are dependencies. A request enters through the routers and moves down one layer at a time. Long solves reach the worker through the job queue; the worker reuses the repositories and is the only caller of the scheduling package, which imports nothing. infra/ (Docker, Helm, CI) builds and deploys both components.