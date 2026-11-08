# Documentation index

## Product and scope

| Document | What it is |
| --- | --- |
| [`hds-spec.md`](hds-spec.md) | The product and technical spec. The source of truth for scope |
| [`hds-api.md`](hds-api.md) | Written API overview: resource groups and conventions |
| [`architecture-guide.md`](architecture-guide.md) | How the code is organised and why, with a request traced end to end |

## Plans and research

| Document | What it is |
| --- | --- |
| [`ux-working-plan.md`](plans/ux-working-plan.md) | The UX working plan: decisions applied, the Option B design language, features 007 to 017, preview stubs and Playwright tests per group (**Proposed**) |
| [`ux-improvements-2026-11.md`](research/ux-improvements-2026-11.md) | The UX research report behind it: 155 suggestions, owner decisions, and findings held for legal review |

## Runbooks

| Runbook | When |
| --- | --- |
| [`restore.md`](runbooks/restore.md) | Restoring a backup, recovering a lost machine, the Drive set-up, moving to S3 |
| [`operations.md`](runbooks/operations.md) | Deploys (following one, a failed one, rolling back and resuming, holding a version, signed releases, the release's dry run) Vault (set-up, changing a secret, the database password, unsealing, never uninstalling hds-secrets), and alerts and `make ops-status` |

## Architecture decisions

| ADR | Decision |
| --- | --- |
| [001](adr/001-tech-stack.md) | Technology stack for the foundation |
| [002](adr/002-soft-delete.md) | Soft delete strategy |
| [003](adr/003-jwt-refresh.md) | Short-lived JWT access tokens with rotating refresh tokens |
| [004](adr/004-pyjwt.md) | Replace python-jose with PyJWT |
| [005](adr/005-request-scoped-transactions.md) | One transaction per request, mail after commit |
| [006](adr/006-refresh-token-reuse-detection.md) | Refresh-token reuse detection with a grace window |
| [007](adr/007-sign-in-abuse-controls.md) | Sign-in abuse controls: bounded hashing, one lockout rule, network limits |
| [008](adr/008-delivery-after-the-response.md) | Work on a reset request's address runs after the response |
| [009](adr/009-rule-checks-and-warnings.md) | Roster rules live in one pure function, and manual changes only warn |
| [010](adr/010-client-stack.md) | One Expo app with a generated API client; web tokens stay in memory (amended by 019) |
| [011](adr/011-solvers-and-job-queue.md) | Two solvers behind one function, and a table that is also the job queue |
| [012](adr/012-pay-calculation.md) | Pay worked out in one pure package, from settings that projects follow or override |
| [015](adr/015-capability-map.md) | One capability map serves the API's role checks and the client's navigation |
| [016](adr/016-phone-sessions.md) | Phone sessions last 30 days and renew on use; the client declares its kind |
| [017](adr/017-availability-submissions.md) | Availability is submitted as a snapshot, and a missed deadline counts as unavailable |
| [018](adr/018-assignment-times.md) | Each assignment may carry its own times, and every rule reads the person's span |
| [019](adr/019-web-sessions-per-tab.md) | The web keeps its session per tab (amends 010) |

Every architectural decision gets an ADR (Constitution XI). Number new ones in sequence.

## Features

Each feature has a folder under [`specs`](../specs/001-foundation-auth-org/spec.md) holding the
spec, plan, tasks, data model, quickstart and OpenAPI contract.

| Feature | Start here |
| --- | --- |
| 001 Foundation: authentication and organisation | [`spec.md`](../specs/001-foundation-auth-org/spec.md), [`tasks.md`](../specs/001-foundation-auth-org/tasks.md), [`openapi.yaml`](../specs/001-foundation-auth-org/contracts/openapi.yaml) |

## Rules and process

- [Constitution](../.specify/memory/constitution.md): the binding principles and quality gates.
- [API README](../apps/api/README.md): layout, configuration, tests and CI gates for the backend.
- [Infrastructure README](../infra/README.md): running PostgreSQL and the API locally.
- [Pull request template](../.github/pull_request_template.md): the checklist every change
  fills in.
