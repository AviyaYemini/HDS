# Implementation Plan: Deployment and operations

**Branch**: `006-deploy-operations` | **Date**: 2026-11-06 | **Spec**: [spec.md](spec.md)

**Input**:
- **The spec**: FR-001 to FR-027 (with FR-008a and FR-010a) and SC-001 to SC-008.
- **The stored prompt** `.claude/hds/prompts/05-deploy/plan.md`: "Docker images, Kubernetes with
  Helm, HashiCorp Vault for secrets, GitHub Actions for CI/CD, Prometheus metrics and OpenTelemetry
  traces, PostgreSQL backups with a restore drill."
- **The owner's three answers during planning**: an SMTP mail sender, Flux to pull releases, and
  Google Drive for the off-machine copy, with S3 expected later (research R4, R9, R11).

## Summary

HDS gains a shared environment: a single-node k3s cluster that k3d runs in Docker on the operator's
Mac.

- **Release**: a merge to `main` that passes CI makes the release workflow build three images,
  scan them, and publish them with a Helm chart to GHCR. Flux in the cluster sees the new chart and
  upgrades. A rolling update, with migrations that keep the previous release working, lets no
  request fail.
- **Health and observability**: the API and the worker answer `/livez` and `/readyz`, serve RED and
  job metrics to Prometheus on a private port, and carry one OpenTelemetry trace per schedule job
  from the API through the worker.
- **Backups**: a daily `pg_dump` is encrypted by rclone and kept both on the machine and on Google
  Drive. A weekly restore test restores the Drive copy into an empty database and checks it.
- **Secrets**: they live in Vault and reach the pods through the Vault Secrets Operator. gitleaks
  and Trivy refuse any secret in a change or an image.
- **Alerts**: Alertmanager e-mails the operators about each failed backup, restore test or deploy
  through an SMTP relay. The same relay gives the API its first real mail backend, which a
  production environment needs before it can start.
- **Local environment**: `make up` now starts everything, including migrations, the seed and the
  web client.

## Technical Context

**Language/Version**: Python 3.12 (the API, worker and scripts), TypeScript 5 with Expo (the web
build, unchanged). Shell (POSIX `sh`, checked with ShellCheck) for the backup scripts. YAML for the
Helm chart, Flux and Actions.

**Primary Dependencies**:
- **New Python runtime packages**: `prometheus-client`, `opentelemetry-sdk`,
  `opentelemetry-exporter-otlp-proto-http`, `opentelemetry-instrumentation-fastapi`,
  `opentelemetry-instrumentation-sqlalchemy`.
- **New Python dev package**: `aiosmtpd`, a fake SMTP server for the tests.
- **New cluster parts**: k3d (k3s), Helm 3, Flux 2 (source, helm and notification controllers),
  HashiCorp Vault and the Vault Secrets Operator, kube-prometheus-stack (with Grafana off), rclone
  and an unprivileged nginx image.
- **New CI tools**: Trivy, gitleaks, ShellCheck and k3d.
- **Pinning**: every version is pinned in Group A.

**Storage**:
- PostgreSQL 16, unchanged. Migration 024 adds `schedule_jobs.trace_context`.
- Backups go to a local-path persistent volume and a Google Drive folder, through rclone crypt
  (research R11).
- Vault keeps its own integrated storage (raft) on a persistent volume.

**Testing**:
- **Kept**: pytest, Hypothesis, Schemathesis, Jest and Playwright.
- **New in the API**: unit tests for the health checks, the metrics middleware (labels from route
  templates only), the SMTP sender against `aiosmtpd`, the stricter start-up checks, trace context
  across a queued job, and the worker's shutdown.
- **New CI jobs**:
  - `quickstart`: a clean checkout, then `make env up`, then sign-ins (FR-005)
  - `chart`: `helm lint` and `helm template` with kubeconform
  - `backup-restore`: the backup and restore scripts against compose, with a local rclone crypt
    remote
  - `rolling-update`: k3d, an upgrade under load, zero failures (SC-003)
  - `compatibility`: the previous release's API tests against the new migration head (FR-016)
  - `secrets`: gitleaks
  - Trivy, in the release workflow

**Target Platform**:
- **Shared**: macOS on Apple silicon running Docker Desktop, with k3d inside it. Images are built
  for `linux/arm64` and `linux/amd64`.
- **CI**: GitHub-hosted Ubuntu runners.

**Project Type**: the operations layer of the existing monorepo (`infra/`, `.github/`), with small
changes to the API.

**Performance Goals**:
- `/readyz` answers within 2 s.
- A deploy reaches the cluster within 30 min of the merge, the machine being on (SC-002).
- A rollback takes under 10 min (SC-008).
- A restore takes under 1 h (SC-005).
- No failed request during a rolling update (SC-003).

**Constraints**:
- No inbound access to the operator's machine (Clarifications).
- No secret in the repository, the images or the values files (FR-024).
- Metrics never public, and no personal data in labels (FR-009).
- Each database change keeps the previous release working (FR-016, Constitution X).
- At most one day of data lost (SC-005).

**Scale/Scope**:
- 1 migration, 2 new API endpoints (`/livez`, `/readyz`) and 1 kept (`/health`), 1 new mail
  backend, 2 new scripts (`create_admin`, `check_restore`)
- 3 images, 1 Helm chart, 1 Flux setup, 2 workflows (CI extended and a new release workflow)
- 6 alert rules, 3 CronJobs, 2 runbooks and 1 ADR

## Constitution Check

| Principle | Check | Result |
| --- | --- | --- |
| I. Spec as source of truth | §7 (Docker, Helm, Vault, Actions, observability), §9 (Portability, Reliability, Observability, Security) and §10 (Branching, Dependencies, Migrations, Documentation) trace to the original. The owner's additions are marked Proposed in the spec. The trace store and Vault auto-unseal are Proposed for later | Pass |
| II. Layered backend | Health checks are in `hds.core.health`, and the database ping goes through a repository (`repositories/health.py`). The metrics middleware and tracing are in `core`. The SMTP sender is in `core/mail.py`. `create_admin` and `check_restore` go through services and repositories | Pass |
| III. Pure scheduling package | Untouched. Tracing spans wrap the worker's call to it, never its inside | Pass |
| IV. Test-first | Failing tests first in every group, including the API unit tests, the quickstart, backup-restore and rolling-update CI jobs, and the chart tests. Schemathesis is unchanged: the new paths are outside the OpenAPI document. The Playwright journeys run against the cluster in the rolling-update job | Pass |
| V. API-first | The OpenAPI document is unchanged, because the health and metrics paths are deliberately outside it, as `/health` is today. The client is not regenerated | Pass |
| VI. Security | Secrets come from Vault through the Vault Secrets Operator, or from the environment; gitleaks and the Trivy secret scan back this up. Known development values are refused outside development. Trivy blocks high and critical findings. Metrics stay private behind a NetworkPolicy. The role matrix is unchanged | Pass |
| VII. Money and time | Untouched. Backup names and manifests use UTC | Pass |
| VIII. Internationalisation | No new user-facing string. The operator e-mails and the CLI output are operator text in English, not part of the product UI | Pass, noted |
| IX. Quality gates | Every existing gate is kept. The new CI jobs are listed above, and ShellCheck runs on the scripts | Pass |
| X. Migrations only | Migration 024 is additive and reversible. The migrate Job runs `alembic upgrade head` only. A manual restore uses `pg_restore` of a whole backup, which is not a schema change | Pass |
| XI. Documentation | `infra/README.md` is rewritten (local and shared). New: `docs/runbooks/{restore,operations}.md` and ADR 013 (deployment and operations). The root README quickstart becomes `make env up`. `apps/api/README.md` gains the new settings. Docstrings and module docstrings are at 100%. The migration states what and why | Pass |

**Post-design re-check**: the same result.

## Project Structure

### Documentation (this feature)

```text
specs/006-deploy-operations/
├── spec.md, plan.md, research.md, data-model.md, quickstart.md
├── contracts/ops.md
├── checklists/requirements.md
└── tasks.md            # /speckit-tasks
```

### Source Code

```text
apps/api/
├── alembic/versions/024_schedule_job_trace_context.py
├── src/hds/
│   ├── core/
│   │   ├── config.py           # HDS_VERSION, METRICS_PORT, SMTP_*, known development values refused
│   │   ├── health.py           # live and ready checks, 2 s limit
│   │   ├── metrics.py          # RED middleware, route-template labels, build info
│   │   ├── tracing.py          # OpenTelemetry set-up; traceparent in and out
│   │   ├── logging.py          # trace_id, span_id in each line
│   │   └── mail.py             # SmtpMailSender, registered in REAL_MAIL_BACKENDS
│   ├── repositories/health.py  # SELECT 1, alembic revision
│   ├── models/schedule_job.py  # trace_context
│   ├── services/schedule_job.py    # keep the traceparent on queue; continue it in the worker
│   ├── main.py                 # /livez, /readyz, /health, metrics server on METRICS_PORT
│   ├── worker.py               # ops server on 9100, SIGTERM drain, lease give-back
│   └── scripts/{create_admin,check_restore}.py
└── tests/{unit,integration}/...    # health, metrics labels, smtp, settings, trace, worker drain
infra/
├── Dockerfile.api              # non-root, HDS_VERSION build arg, pinned base
├── Dockerfile.web              # Expo web export, served by nginx-unprivileged
├── Dockerfile.backup           # postgres:16 client tools and rclone
├── docker-compose.yml          # migrate, web; ops ports; healthchecks on /readyz
├── backup/{backup,restore,restore-test,prune,vault-snapshot}.sh
├── helm/hds/                   # Chart.yaml, values.yaml, values-{local,ci}.yaml, templates/...
├── flux/clusters/local/        # OCIRepository, HelmRelease, Alertmanager provider and alerts
├── cluster/                    # k3d config, Vault and VSO values, kube-prometheus-stack values
└── README.md
scripts/ops/                    # status, rollback, restore, unseal helpers called by make
.github/workflows/
├── ci.yml                      # + quickstart, chart, backup-restore, rolling-update, compatibility, secrets
└── release.yml                 # build, scan, push images and chart on CI success on main
.pre-commit-config.yaml         # + gitleaks
Makefile                        # env, up, down, wipe, ops-*
docs/
├── adr/013-deployment-and-operations.md
└── runbooks/{restore,operations}.md
```

**Structure Decision**: everything for running HDS lives in `infra/`, and the operator commands are
`make` targets backed by `scripts/ops/`. The API keeps its layers; the operations code goes in
`core`, and its one database read goes in a repository.

## Phases

Each group follows the same steps:
1. failing tests first, then code;
2. the gates, then `/hds-review` and `/code-review high`;
3. the log, then a commit.

- **A. Foundations and pins**: pin every tool and image version. Add the gitleaks pre-commit hook and
  the CI `secrets` job (FR-025). Refuse known development values outside development (FR-026).
  Harden `Dockerfile.api` (non-root, `HDS_VERSION`).
- **B. One-command local environment**: add the compose `migrate` and `web` services and
  `Dockerfile.web`; add `make env` and `make wipe`; rewrite the quickstart; add the CI `quickstart`
  job (FR-001 to FR-005).
- **C. Health, version and metrics**: add `core/health.py` with its repository, `/livez`, `/readyz`
  and the `/health` alias; add the metrics middleware and the private port; add the worker's ops
  server and job metrics (FR-006 to FR-010).
- **D. Traces**: add migration 024, `core/tracing.py`, the traceparent kept on queue and continued
  in the worker, and the trace fields in the logs (FR-008a).
- **E. SMTP mail**: add `SmtpMailSender` (STARTTLS required, redaction kept), the settings, tests
  against `aiosmtpd`, and the `create_admin` script (research R9, R13).
- **F. Worker drain**: on SIGTERM, stop claiming, finish or give back the lease, and exit within the
  grace period (FR-020).
- **G. Helm chart**: add every template, the values, the probes, the rolling-update settings, the
  PodDisruptionBudgets, the NetworkPolicy, the migrate hook with its advisory lock, and the Ingress.
  Add the CI `chart` job (FR-016 to FR-019).
- **H. Backups**: add `Dockerfile.backup`, the scripts, the CronJobs, the manifest, retention,
  `check_restore` and the Vault snapshot. Add the CI `backup-restore` job, which restores to a fresh
  database and compares (FR-011 to FR-014).
- **I. Release and Flux**: add `release.yml` (build, Trivy, push, chart OCI) and the Flux files for
  the cluster; add `make ops-rollback` and `make ops-resume` (FR-015, FR-018, FR-021 to FR-023).
- **J. Vault and the Vault Secrets Operator**: add the Vault values, the KV paths, the
  VaultStaticSecrets with rollout restart, and `make ops-unseal` (FR-024, FR-027).
- **K. Monitoring and alerts**: add the kube-prometheus-stack values, the ServiceMonitors, the six
  alert rules, the Alertmanager SMTP receiver, the Flux Alertmanager provider and `make ops-status`
  (FR-010a).
- **L. Rolling-update proof**: add the CI `rolling-update` job (k3d, upgrade under load, Playwright
  smoke on the upgraded cluster) and the `compatibility` job (SC-003, FR-016).
- **M. The shared environment, end to end**: run `make ops-cluster` on the operator's machine, the
  first Admin, the first deploy from `main`, a backup, a restore test, a rollback and a failure
  e-mail. Then write the runbooks and ADR 013, and add the operations section to `docs/hds-api.md`
  §9 (quickstart scenarios 3 and 4).

Group M needs the owner's machine and accounts: an SMTP relay, a Google account, and the GHCR
packages' visibility. Each step it needs is in `docs/runbooks/operations.md`.

## Complexity Tracking

| Choice | Why needed | Simpler alternative rejected because |
| --- | --- | --- |
| Flux in the cluster | The pipeline may not reach the machine (Clarifications) | A self-hosted runner runs pull-request code on the machine in a public repository |
| Vault plus the Vault Secrets Operator, with manual unseal | The original names Vault. Pods keep reading environment variables | Plain Kubernetes Secrets fail Constitution VI's "environment or Vault" intent for a shared environment; auto-unseal needs a cloud key service |
| A separate metrics port in the API process | Metrics must not be public (FR-009) | Filtering `/metrics` at the ingress fails open if the rule is wrong |
| A trace column on `schedule_jobs` | One trace across the API and the worker (§9) needs the context to travel with the job | There is no broker message to carry a header |
| rclone for the off-machine copy | Drive now and S3 later, with encryption, behind one interface | Separate Drive and S3 code paths, plus a separate encryption step |
