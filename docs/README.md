# Documentation index

## Product and scope

| Document | What it is |
| --- | --- |
| [`hds-spec.md`](hds-spec.md) | The product and technical spec. The source of truth for scope |
| [`hds-api.md`](hds-api.md) | Written API overview: resource groups and conventions |
| [`architecture-guide.md`](architecture-guide.md) | How the code is organised and why, with a request traced end to end |

## Architecture decisions

| ADR | Decision |
| --- | --- |
| [001](adr/001-tech-stack.md) | Technology stack for the foundation |
| [002](adr/002-soft-delete.md) | Soft delete strategy |
| [003](adr/003-jwt-refresh.md) | Short-lived JWT access tokens with rotating refresh tokens |
| [004](adr/004-pyjwt.md) | Replace python-jose with PyJWT |

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
