# Research: Deployment and operations

**Feature**: [spec.md](spec.md) | **Plan**: [plan.md](plan.md) | **Date**: 2026-11-06

The stored plan prompt names the stack: "Docker images, Kubernetes with Helm, HashiCorp Vault for
secrets, GitHub Actions for CI/CD, Prometheus metrics and OpenTelemetry traces, PostgreSQL backups with
a restore drill." The owner settled three gaps during planning (2026-11-06): an SMTP mail sender,
Flux to pull releases into the cluster, and Google Drive for the off-machine backup copy, with S3
expected later. Every other decision below follows the constitution, `docs/hds-spec.md` §7 and
`docs/hds-api.md` §9 and §10. Exact versions are pinned in Group A of `tasks.md` (§10 Dependencies:
"Container base images pinned by version").

## R1. The cluster: k3s in Docker on the operator's machine (k3d)

- **Decision**: One single-node k3s cluster, run inside Docker with k3d, on the operator's Mac. It
  uses k3s's bundled Traefik ingress and local-path storage. The cluster maps ports 80 and 443 to
  local ports, and the app answers at `http://hds.localhost:8080`.
- **Rationale**: Open decision 8 defaults to "local kind or k3s first", and the clarification chose a
  cluster on the operator's machine. k3s does not run natively on macOS; k3d runs it in the Docker
  that the local environment already needs, so no virtual-machine tool is added. Traefik and
  local-path come with k3s, so no ingress or storage controller is added.
- **Alternatives**: kind (no ingress or default storage class, so two more components); k3s in a
  virtual machine (one more tool, and a second network to route); minikube (heavier, and the
  original does not name it).

## R2. Images: one per part, built once, tagged by commit

- **Decision**: Three images, built by the release workflow and pushed to GitHub Container Registry:
  `hds-api` (API, worker and migrations: the existing `infra/Dockerfile.api`, hardened to run as a
  non-root user without a shell entry point), `hds-web` (the Expo web export served by an
  unprivileged nginx), and `hds-backup` (PostgreSQL 16 client tools and rclone, for backups and the
  restore test). Each is tagged with the full commit SHA, and the version is baked in as
  `HDS_VERSION` (FR-010). Base images are pinned by version and digest.
- **Rationale**: "Built once and deployed unchanged" (FR-015, US4): the cluster runs the very digest
  that CI scanned. The worker and migrations share the API image so their code cannot drift.
- **Web build**: The client reads `EXPO_PUBLIC_API_URL ?? "http://localhost:8000"`. The image builds
  with an empty value, so the browser calls the API on its own origin, and the ingress sends the
  API's prefixes (`/api`, `/auth`, `/me` today) to the API. A test checks that every route in the
  API's route table starts with a prefix the ingress sends. One artefact then works in every
  environment.
- **Alternatives**: nginx serving the API too (no); a Node server for the web build (it is static
  files); Caddy (adds nothing over nginx here).

## R3. Packaging: one Helm chart, `infra/helm/hds`

- **Decision**: One chart with templates for the API Deployment, the worker Deployment, the web
  Deployment, PostgreSQL (a StatefulSet on the official `postgres:16` image), a migration Job, the
  backup and restore-test CronJobs, the Ingress, PodDisruptionBudgets, ServiceMonitors, alert rules
  and the Vault Secrets Operator resources. The values have an `environment` block (`local` and
  `ci`), with no secrets in any values file.
- **Rationale**: The original names Helm (§7 Deployment). One chart keeps the parts on one version.
- **Alternatives**: the Bitnami PostgreSQL chart (its free images moved behind a subscription in
  2025, and it brings far more than one instance needs); CloudNativePG (an operator for one
  database; a good later step, not needed for a daily dump).

## R4. Deploy flow: CI publishes, Flux pulls (the owner's choice)

- **Decision**:
  1. **Release** (`.github/workflows/release.yml`). It runs on `workflow_run` when CI succeeds on
     `main`, with `concurrency: release` (FR-023). It builds the three images, scans them with Trivy
     and fails on a high or critical finding (FR-015, §9 Security, §10 Dependencies). Then it pushes
     the images, packages the chart with `appVersion` set to the SHA and `version: 0.1.<run
     number>`, and pushes the chart to GHCR as an OCI artefact.
  2. **Flux** (source-controller, helm-controller and notification-controller only) runs in the
     cluster. An `OCIRepository` watches the chart with `semver: ">=0.1.0"` and polls every 5
     minutes. A `HelmRelease` installs it. This is all outbound: nothing reaches into the machine.
  3. **Records** (FR-022): Helm's release history keeps each revision with its version, time and
     status, and Flux records an event for each one.
- **Why the chart is published, not a commit to `main`**: CI never writes to the protected branch,
  and the release is one immutable artefact.
- **When the machine is off**: Flux catches up with the newest chart when the machine comes back.
- **Failed deploy** (FR-018): `HelmRelease.spec.upgrade` waits up to 10 minutes for every pod to be
  ready, with `remediation.retries: 0` and `strategy: rollback`. The rolling update never removes an
  old pod before a new one is ready (R5), so the previous version serves throughout. A failure sends
  a message through Flux's Alertmanager provider (R9).
- **Rollback** (FR-021): `make ops-rollback` suspends the `HelmRelease`, so Flux does not reinstall
  the newest version, and runs `helm rollback hds` to the previous revision. It changes only the
  pods, never the database. `make ops-resume` hands control back to Flux. Pinning the `semver`
  range in the cluster's Flux files is the lasting form.
- **Private images**: The release workflow pushes to the repository's packages. If the packages are
  private, the cluster pulls with a read-only token kept in Vault and synced as a pull secret.
- **Alternatives**: a self-hosted Actions runner (unsafe for a public repository); Flux image
  automation (it writes commits to git); Argo CD (heavier, with its own UI; Flux matches "pull from
  git or a registry" with fewer parts).

## R5. Zero-downtime updates

**Decision**:

- **API**: `replicas: 2` with `RollingUpdate` (`maxUnavailable: 0`, `maxSurge: 1`). Traffic moves to
  a new pod only once `/readyz` passes. A `preStop` pause of 5 seconds lets the ingress drop the old
  pod first, then uvicorn finishes its open requests within `terminationGracePeriodSeconds: 30`. A
  `PodDisruptionBudget` keeps at least one pod (FR-017).
- **Worker**: `replicas: 1`, and `RollingUpdate` starts the new worker first. On `SIGTERM` the
  worker stops claiming. A running solve has up to 60 seconds left (`SCHEDULE_TIME_LIMIT_SECONDS` is
  at most 60), so it finishes and stores its result inside `terminationGracePeriodSeconds: 90`. If
  the worker is killed anyway, the job's lease expires and another worker claims it. The existing
  claim compares the attempt before it stores, so the result is recorded once (FR-020). New: on
  `SIGTERM` the worker gives back the lease of a job it has not started solving, so the job does
  not wait the 5-minute lease.
- **Migrations**: a Helm `pre-upgrade` Job runs `alembic upgrade head` before any new pod starts.
  It holds a PostgreSQL advisory lock so two can never run together. Every migration must keep the
  previous release working (expand, then contract; §10 Migrations, FR-016). A CI check, the
  **compatibility test**, runs the previous release's API tests against the new head. If a
  migration fails, the upgrade stops before any pod changes.
- **Sign-ins** (FR-019): access tokens are signed with the one key from Vault, and refresh tokens
  live in the database, so both survive a pod change. A test signs in, deploys, and refreshes.
- **Proof** (SC-003): a CI job, **rolling-update**, creates a k3d cluster and installs the chart at
  the base commit. It then upgrades to the change's images while a load script sends a steady
  stream of signed-in reads and writes, and it fails on any error response or connection error.

**Rationale**: these are the standard Kubernetes controls for a rolling update, and each maps to one
FR.

## R6. Health and version

**Decision**:

- **API**:
  - `GET /livez` checks nothing outside the process.
  - `GET /readyz` runs `SELECT 1` and checks the migration version equals the code's head, with a
    2-second total limit (FR-006). It answers 503 with the failing check's name, and never a
    message from the database.
  - `GET /health` stays as an alias of `/livez` for the existing compose file and probes.
  - None of the three is in the OpenAPI document; all are open, as `/health` is today.
- **The job queue**: it is the `schedule_jobs` table (ADR 011), so "the database" covers it. There
  is no broker to check.
- **Worker**: it serves `/livez`, `/readyz` and `/metrics` on port 9100 with a small ASGI app in
  its own event loop (uvicorn and Starlette are already dependencies). It is alive while its loop
  turns, and ready while it can reach the database.
- **Version** (FR-010): both answer `HDS_VERSION` in the `/livez` body, and as the metric
  `hds_build_info{version}`.
- **Health of the backup jobs**: the backup and restore-test CronJobs report through Kubernetes job
  status, which kube-state-metrics exposes.

**Alternatives**: a single `/health` that checks the database (an outage would then restart every
pod, which cannot help); exec probes for the worker (they cannot serve metrics).

## R7. Metrics: Prometheus

**Decision**:

- **API**: `prometheus-client` serves the API's metrics on a separate port (9100), not the public
  port, so the ingress never routes them (FR-009). A middleware records:
  - `hds_http_requests_total{method, route, status}`
  - `hds_http_request_duration_seconds{method, route}`, a histogram
  - The `route` label is the route template (`/api/v1/shifts/{id}`), never the path, so no id or
    personal data becomes a label. An unmatched path is `route="unmatched"`.
- **Worker**:
  - `hds_schedule_jobs{state}`, read from the table on each scrape (queued, running, succeeded,
    failed) (FR-008)
  - `hds_schedule_job_duration_seconds{outcome}`
- **Collection**: kube-prometheus-stack (Prometheus Operator, Prometheus, Alertmanager,
  kube-state-metrics) runs in a `monitoring` namespace, with Grafana switched off (dashboards are
  out of scope). Prometheus finds the targets through ServiceMonitors, and a NetworkPolicy lets only
  the `monitoring` namespace reach port 9100.
- **Local environment**: no metrics stack. The ports are open on localhost.

**Rationale**: these are the RED metrics per endpoint (§9 Observability).

**Alternatives**: OpenTelemetry metrics (Prometheus is named, and is simpler to scrape);
`prometheus-fastapi-instrumentator` (it labels by path by default, and the middleware is about 40
lines).

## R8. Traces: OpenTelemetry, joined through the job row

- **Decision**: The API and the worker use the OpenTelemetry SDK, with FastAPI and SQLAlchemy
  instrumentation:
  - **Joining the trace**: when the API queues a schedule job, it stores the current
    `traceparent` on the job row (migration 024, one nullable column), and the worker starts its
    spans as children of it. Each schedule job is then one trace across the API and the worker (§9
    Observability).
  - **Logs**: every log line gains `trace_id` and `span_id` next to `request_id`.
  - **Export**: spans go over OTLP when `OTEL_EXPORTER_OTLP_ENDPOINT` is set, and are dropped
    otherwise. The chart can point at any OTLP collector. No trace store is deployed: one would add
    a component that neither the original nor the plan prompt names, so it is **Proposed** for a
    later feature. Until then a trace is read by its id across the JSON logs.
- **Spec impact**: the spec put traces out of scope as an assumption. The original (§9) and the
  plan prompt both include them, so the spec is amended: FR-008a brings them back, and the
  assumption is removed.
- **Alternatives**: a Jaeger or Tempo instance (a new store to run and back up; Proposed later); a
  trace header on a broker message (there is no broker).

## R9. Failure messages: Alertmanager by e-mail, SMTP (the owner's choice)

**Decision**:

- **One SMTP relay for everything**: the relay's host, port, user and password come from Vault, and
  any relay works (for example a Gmail app password, or a provider's free tier). It sends both kinds
  of message:
  - **Application mail**: `SmtpMailSender` in `hds.core.mail` is added to `REAL_MAIL_BACKENDS`, so a
    production environment can start. It uses the standard library's `smtplib` in a thread, with
    STARTTLS required. It delivers the password-reset and invitation mail that 002 built, keeping
    their redaction rules.
  - **Operator mail** (FR-010a): Alertmanager e-mails the operators through the same relay.
- **Alert rules, on failure signals only** (the clarification excludes rules on error rates):
  - the backup CronJob's last Job failed
  - no successful backup in 26 hours, so a missed run counts as a failure
  - the restore test failed, or did not succeed in 8 days
  - the off-machine copy failed
  - Flux reports a failed `HelmRelease` (Flux's notification-controller forwards it to Alertmanager)
- **Message content**: the alert names what failed and when, carries no secret and no personal data,
  and links nowhere outside the cluster.
- **Shown with the health status**: `make ops-status` prints the parts' readiness and versions, the
  last backup, the last restore test and the last deploy, with any alert that is firing.

**Alternatives**: a chat webhook (a second channel, and application mail still needs SMTP);
`aiosmtplib` (a new dependency for a few messages a day).

## R10. Secrets: Vault and the Vault Secrets Operator

**Decision**:

- **Vault**: one Vault server runs in a `vault` namespace from HashiCorp's chart, as a single node
  with integrated storage (raft) on a persistent volume. Raft is chosen over file storage because it
  can take snapshots (`vault operator raft snapshot save`).
- **Unsealing**: it starts sealed after each restart, and the operator unseals it with
  `make ops-unseal`, which reads the key from the operator's password manager. The unseal keys and
  root token never enter the repository or the cluster.
- **Syncing**: the Vault Secrets Operator syncs KV v2 paths (`hds/api`, `hds/smtp`, `hds/backup`,
  `hds/registry`) into Kubernetes Secrets that the pods read as environment variables (FR-024).
- **Rotation** (FR-027): the operator writes a new value to Vault. The operator refreshes the
  Secret and restarts the dependent Deployments with `rolloutRestartTargets`, so there is no code
  change and no outage.
- **Refusing known values** (FR-026): the settings already refuse the published JWT placeholder and
  short keys. They now also refuse every value written in `.env.example`, the compose file or the
  test settings, such as `hds_dev` or `change-me`, in a password or key, when `ENVIRONMENT` is not a
  development one.
- **Secret scanning** (FR-025): gitleaks runs as a pre-commit hook and as a CI job over the change.
  A second CI job runs Trivy's secret scanner over each built image.
- **Rationale**: the original names Vault (§7, Constitution VI). The operator keeps the app reading
  plain environment variables, which the settings already do, so the app needs no Vault client.
- **Alternatives**:
  - the Vault Agent injector (it writes files, so the app would need a file reader)
  - auto-unseal (it needs a cloud key service; Proposed with the cloud move)
  - External Secrets Operator (a third-party operator for the same job)

## R11. Backups: a daily `pg_dump`, encrypted by rclone, to the machine and Google Drive

**Backup**: a CronJob runs at 02:30 in the organisation's zone, with `startingDeadlineSeconds:
86400`, so a run missed while the machine slept happens when it wakes. It runs in the `hds-backup`
image:

1. `pg_dump --format=custom`, which is one consistent snapshot of the database (FR-012).
2. A manifest: the migration version, the time, the row count of every table, the size and a
   SHA-256.
3. Two copies through rclone `crypt` remotes:
   - `local-crypt:`, over a persistent volume on the machine
   - `offsite-crypt:`, over a Google Drive folder
   - Encryption is NaCl secretbox under a password from Vault, so Drive holds only ciphertext and
     encrypted names (FR-012).
4. If the off-machine copy fails, the Job fails, which raises the alert (R9). The local copy is
   kept.
5. Retention: backups older than 30 days are deleted on both sides (FR-011). The newest backup whose
   manifest verifies is never deleted.

**Restore test** (FR-014): a CronJob runs weekly on Sundays at 04:00, under the same deadline rule.
Its pod has a native sidecar running an empty `postgres:16`: a separate, empty environment that
dies with the pod. The test:

1. Downloads the newest backup from `offsite-crypt:`, never the local copy, and checks its SHA-256.
2. Runs `pg_restore`.
3. Compares the migration version and every table's row count with the manifest.
4. Runs `python -m hds.scripts.check_restore` from the API image against the restored database.
   This read-only script checks that the API's `/readyz` checks pass and that it can load the
   organisation, its users and the last month's shifts through the repositories. No check account
   is needed, so no fake user ever enters the real data.
5. Fails, and so alerts, on any difference.

**Restore procedure** (FR-013, SC-005): `make ops-restore BACKUP=<name>` restores a backup into the
cluster's database, and refuses unless that database is empty or `CONFIRM=replace` is given. It
stops the API and the worker first and starts them afterwards. `docs/runbooks/restore.md` is the
written procedure, and it covers a lost machine too: a new cluster, Vault restored from its own
backup, then the database from Drive.

**Vault's own data**: Vault holds the backup password, so it is backed up as well. A weekly
encrypted raft snapshot of Vault goes to the same two places under a separate password.
The operator keeps that password, with the unseal keys, outside the machine.

**Google Drive**:

- **Access**: rclone uses a Drive OAuth token authorised once with `rclone authorize drive`, with
  scope `drive.file`, so it can only see the files it created. The token lives in Vault (`hds/backup`).
- **The 7-day expiry trap**: rclone's own client ID, or the owner's own client ID once published,
  is used. A Google OAuth app left in testing status expires its refresh tokens after 7 days and
  would silently stop backups. The runbook says this.
- **Moving to S3 later**: only the `offsite` remote's configuration changes (type `s3`, a bucket and
  keys in Vault). The crypt layer, the scripts, the manifest and the tests stay as they are.

**Why**: `pg_dump` meets "at most one day lost" (SC-005) with the least to run. rclone speaks both
Drive and S3, and its crypt remote does the encryption, so the move to S3 is a configuration change.

**Alternatives**:

- WAL archiving with pgBackRest (point-in-time recovery, which the clarification did not ask for)
- Velero (it backs up volumes, not a consistent database dump)
- age or GPG encryption (rclone crypt already encrypts, and also hides the names)

## R12. The one-command local environment

`make up` starts the whole local environment. It is the existing target, extended:

- **Compose services**:
  - a `migrate` one-shot service (`alembic upgrade head`, then the idempotent `seed_dev`), which the
    API and the worker wait for with `service_completed_successfully` (FR-002)
  - a `web` service serving the web export built at image build time, at `http://localhost:8081`
- **Prerequisites**: only Docker and make.
- **Stopping and wiping** (FR-004): `make down` keeps the data, and `make wipe` asks before it
  deletes `infra/volumes/postgres`.
- **Secrets** (FR-003): `make env` writes `.env` with a fresh random `JWT_SECRET_KEY`, and never
  overwrites an existing file. Compose's `:?` already refuses a missing key.
- **Ports in use**: compose's own error names the port.
- **Proof** (FR-005): a CI job, **quickstart**, runs `make env up` in a clean checkout. It waits for
  `/readyz` and the web page, signs in as each sample role through the API, runs `make down up`, and
  checks the data is still there.

Expo's development server with hot reload stays a separate `pnpm client:web`, for client work.

## R13. The development seed and the shared environment

`seed_dev` is for development only: it creates `@hds.test` accounts with published passwords. The
shared environment runs as `ENVIRONMENT=production`, so the settings refuse `@hds.test` addresses
and development secrets there. It gets its first Admin from a new one-shot command,
`hds.scripts.create_admin`. That command reads the address from its arguments and the password from
standard input, refuses if any user exists, and is run once by the operator. There is no seed in the
cluster.

## R14. What stays as it is

- Logs stay structured JSON on stdout. Only the trace fields are added (R8).
- `TRUSTED_PROXY_COUNT` is 1 in the cluster, for Traefik. k3d's load balancer works at the TCP
  level, so it adds no forwarded header. An integration check in the rolling-update job asserts
  that the sign-in throttle sees the client's address and not Traefik's. This is the check that
  `infra/README.md` demands (ADR 007).
- Phone builds, alert rules on error rates, dashboards and a trace store stay out of scope.
