# Operations contract: Deployment and operations

**Feature**: [../spec.md](../spec.md) | **Research**: [../research.md](../research.md)

What operators, probes and the pipeline rely on. Nothing here is in the OpenAPI document: health and
metrics are for probes and operators, not for the client, and the generated client is unchanged.

## 1. Health

### API, on its public port (8000)

| Method and path | 200 when | Failure | Auth |
| --- | --- | --- | --- |
| `GET /livez` | the process runs | none: no answer at all means not alive | none |
| `GET /readyz` | the database answers and is not behind the code's migrations, all within 2 s | 503 | none |
| `GET /health` | as `/livez`, kept for existing probes and the compose file | as `/livez` | none |

The body is always JSON:

```json
{"status": "ok", "version": "3f2c1e0…"}
```

```json
{"status": "not_ready", "version": "3f2c1e0…", "failing": ["database"]}
```

The values `failing` can hold are `database` (unreachable, or it took longer than 2 s) and
`migrations` (the database is behind the code: `alembic_version` is empty or missing, or holds a
revision in the code's own history that is not its head). A revision the code does not know is
newer and counts as ready, because each migration keeps the previous release working (FR-016); this
lets old pods keep serving during an upgrade and after a rollback. The body never carries an error message, a
host name or a connection string (FR-009). The ingress routes none of these paths, so they are
reachable only inside the cluster. Locally they answer on `localhost:8000`.

### Worker, on its operations port (9100)

| Method and path | 200 when |
| --- | --- |
| `GET /livez` | the job loop has turned within the last 30 s, or the worker holds a job still inside its lease (a solve runs up to 60 s) |
| `GET /readyz` | as `/livez`, and the database answers within 2 s |
| `GET /metrics` | always |

The worker's bodies have the same shape as the API's. When its loop is stuck, both answer 503 with
`"failing": ["worker_loop"]`. Its readiness checks the database only, not the migrations: the
migrate Job has already run before new pods start.

The API's ready check uses connections of its own (no pool), never the request pool, so a busy
pool cannot make every pod report the database down together.

### Probes in the chart

| Part | Liveness | Readiness | Startup |
| --- | --- | --- | --- |
| API | `/livez`, every 10 s, 3 failures | `/readyz`, every 5 s, 2 failures | `/livez`, up to 60 s |
| Worker | `:9100/livez`, every 10 s, 3 failures | `:9100/readyz`, every 5 s | `:9100/livez`, up to 60 s |
| Web | `GET /`, every 10 s | `GET /`, every 5 s | none |
| PostgreSQL | `pg_isready` | `pg_isready` | none |

## 2. Metrics (Prometheus text format)

The API and the worker each serve their metrics on port 9100, which no ingress routes. Locally,
compose maps the API's to `localhost:9100` and the worker's to `localhost:9101`. In the cluster
only the `monitoring` namespace may connect to it (FR-009).

### API

| Metric | Type | Labels |
| --- | --- | --- |
| `hds_http_requests_total` | counter | `method`, `route`, `status` |
| `hds_http_request_duration_seconds` | histogram, buckets 0.01 to 10 s | `method`, `route` |
| `hds_build_info` | gauge, always 1 | `version` |

- **`route`** is the route template, for example `/api/v1/shifts/{id}`, or `unmatched`. **`status`**
  is the status code.
- **Errors** are counted from `status`: 5xx are errors, and 4xx are the client's.
- **Labels never carry** a path, a query string, an id, a name or an e-mail address. A test checks
  every label value against the route table.

### Worker

| Metric | Type | Labels |
| --- | --- | --- |
| `hds_schedule_jobs` | gauge, read from the table on each scrape | `state` = `queued`, `running`, `succeeded`, `failed` |
| `hds_schedule_job_duration_seconds` | histogram, buckets 1 to 120 s | `outcome` = `succeeded`, `failed` |
| `hds_build_info` | gauge | `version` |

### Kubernetes, through kube-state-metrics

`kube_cronjob_status_last_successful_time` and `kube_job_status_failed` for `hds-backup`,
`hds-restore-test` and `vault-snapshot`.

## 3. Alerts (Alertmanager, by e-mail to `OPERATOR_EMAILS`)

| Alert | Fires when |
| --- | --- |
| `HdsBackupFailed` | the newest `hds-backup` Job failed, including when only the off-machine copy failed |
| `HdsBackupMissing` | no successful backup for 26 h (a missed or stuck run), for 1 h (`for: 1h`), so waking the Mac after a day off does not e-mail before the catch-up run ends |
| `HdsRestoreTestFailed` | the newest `hds-restore-test` Job failed |
| `HdsRestoreTestMissing` | no successful restore test for 8 days, for 1 h (`for: 1h`) |
| `HdsVaultSnapshotFailed` | the newest `vault-snapshot` Job failed; while Vault is sealed after a restart this fires, as a reminder to unseal |
| `HdsDeployFailed` | Flux reports the `hds` HelmRelease failed (forwarded by Flux's Alertmanager provider) |

Each message gives the alert's name, the object, the time and a one-line reason (FR-010a). It never
quotes a log line, a secret or any personal data. There are no other rules: rules on error rates or
latency are out of scope (spec, Clarifications).

## 4. Settings (environment variables)

New or changed settings of the API and the worker. The existing ones are in `apps/api/README.md`.

| Variable | Default | Rule |
| --- | --- | --- |
| `HDS_VERSION` | `dev` | baked into the image by the build |
| `METRICS_PORT` | `0` (off) | the operations port; compose and the chart set `9100`, so a process started by hand never fights over it; a taken port stops start-up naming `METRICS_PORT` |
| `MAIL_BACKEND` | `outbox` in development | `smtp` is now a real backend |
| `SMTP_HOST`, `SMTP_PORT` | none, `587` | required when `MAIL_BACKEND=smtp` |
| `SMTP_USERNAME`, `SMTP_PASSWORD` | none | the password is a secret, never logged |
| `MAIL_FROM` | none | required when `MAIL_BACKEND=smtp` |
| `SMTP_STARTTLS` | `true` | must be `true` outside development |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | unset | unset, spans are dropped (research R8) |
| `OTEL_SERVICE_NAME` | `hds-api` / `hds-worker` | set by the chart |

Outside development, start-up now also refuses:

- a password or key equal to any value published in the repository (`.env.example`, the compose
  file, the CI files);
- `MAIL_BACKEND=smtp` without its host and sender;
- STARTTLS switched off.

As before, the error names the setting and never the value (FR-026).

## 5. Make targets

### Local environment, run from the repository root

| Target | Does | Data |
| --- | --- | --- |
| `make env` | writes `.env` with a random `JWT_SECRET_KEY`; refuses if `.env` exists | none |
| `make up` | builds and starts PostgreSQL, the migrate step and seed, the API, the worker and the web client | kept |
| `make down` | stops everything | kept |
| `make wipe` | stops everything and deletes the local database, after asking | deleted |
| `make ps`, `make logs` | as today | none |

### Shared environment (`ops-*`), needing `kubectl` access to the cluster

| Target | Does |
| --- | --- |
| `make ops-cluster` | creates the k3d cluster and installs Flux, Vault, the Vault Secrets Operator and the monitoring stack (once per machine) |
| `make ops-unseal` | unseals Vault after a restart |
| `make ops-status` | for each part, readiness and version; the last backup, restore test and deploy; any firing alert |
| `make ops-backup` | runs a backup now |
| `make ops-restore-test` | runs the restore test now |
| `make ops-backups` | lists the backups on Drive by name, whose names are encrypted there (**Proposed**, review R76) |
| `make ops-restore BACKUP=<name> [CONFIRM=replace]` | checks the backup first, then, with Flux and the `hds-backup` CronJob suspended, stops the API and worker, restores the backup into the cluster's database, runs `migrate`, starts them again with their earlier replica counts and sets both back; refuses a database with data unless `CONFIRM=replace`. A failure after the database was emptied leaves the API and worker stopped (docs/runbooks/restore.md §1) |
| `make ops-rollback` | suspends Flux for `hds` and rolls back to the previous Helm revision; code only (FR-021) |
| `make ops-resume` | hands `hds` back to Flux |
| `make ops-create-admin EMAIL=<address>` | creates the first Admin; reads the password from the terminal |

Every target prints what it did, and each exits non-zero on failure.

## 6. Release artefacts (GHCR)

| Artefact | Name | Tag |
| --- | --- | --- |
| API, worker and migrations image | `ghcr.io/<owner>/hds-api` | full commit SHA |
| Web image | `ghcr.io/<owner>/hds-web` | full commit SHA |
| Backup image | `ghcr.io/<owner>/hds-backup` | full commit SHA |
| Chart | `oci://ghcr.io/<owner>/charts/hds` | `0.1.<run number>`, `appVersion` = commit SHA |

The chart refers to images by digest, so what runs is what was scanned. The release workflow fails,
and publishes nothing, if any image has a high or critical finding or a secret (FR-015, FR-025).
