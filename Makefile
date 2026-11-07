# The local environment, from the repository root (specs/006-deploy-operations US1).
#
#   make env     write .env with a fresh random signing key (once; it refuses to overwrite)
#   make up      start everything: PostgreSQL, migrations and seed, the API, the worker, the web client
#   make build   only rebuild the images (make up also builds)
#   make down    stop everything; the database is KEPT
#   make wipe    stop everything and DELETE the local database, after asking
#   make ps, make logs [S=service]
#   make hooks   install the git hooks (ruff, mypy, gitleaks, commitlint), once per clone
#
# The shared environment, with kubectl access to the cluster (contracts/ops.md section 5):
#   make ops-backup        a backup now; make ops-restore-test, the restore test now
#   make ops-backups       the backups on Drive, by name (Proposed)
#   make ops-restore BACKUP=<name> [CONFIRM=replace]   restore a backup (docs/runbooks/restore.md)
#   make ops-rollback      suspend Flux for hds and roll back to the previous revision; ops-resume
#                          hands hds back to Flux (docs/runbooks/operations.md)
#
# The compose file lives in infra/ and Compose reads a `.env` file from THE COMPOSE FILE'S folder
# (infra/), not from here, so every target passes `--env-file .env` to use the one in the repository
# root. Do not use `--project-directory .`: the compose file's relative paths (build context,
# volumes) assume infra/.
#
# Host ports default to 5432, 8000, 8090, 9100 and 9101; set POSTGRES_PORT, API_PORT, WEB_PORT,
# API_METRICS_PORT or WORKER_METRICS_PORT to move one.

# No default provenance attestation on any build. Some Compose versions (CI's, which builds through
# bake) ignore the compose file's `build.provenance: false`, and the attestation gives every build a
# new image id, so `make up --build` recreated the running stack (CI run 42201726100). This buildx
# switch turns it off whichever way Compose builds; the compose file keeps `provenance: false` too.
export BUILDX_NO_DEFAULT_ATTESTATIONS := 1

COMPOSE := docker compose -f infra/docker-compose.yml --env-file .env
# Commands that only read the stack's state still parse the compose file, which insists on
# JWT_SECRET_KEY; a stand-in fills in when none is set. It is never used to start anything.
COMPOSE_READ := JWT_SECRET_KEY="$${JWT_SECRET_KEY:-unused-by-down}" $(COMPOSE)
POSTGRES_IMAGE := postgres:16.15-alpine@sha256:aa6cb33cf4c19c765cefcf48d96176258800870a9a1e930ad126200f9d825139

.PHONY: env up build down wipe ps logs hooks ci-local ci-local-db
.PHONY: ops-backup ops-restore-test ops-backups ops-restore ops-rollback ops-resume ops-unseal
.PHONY: ops-vault-configure

# CI's checks that need no database, here and in CI's order (scripts/ops/ci_local.sh): ruff, format,
# interrogate, mypy, the import graph, pip-audit, the unit and property tests, the API image,
# ShellCheck, the Helm charts, the Markdown links, gitleaks and commitlint over origin/main..HEAD,
# and the client.
ci-local:
	scripts/ops/ci_local.sh

# Opt in, slow (about 25 minutes): the migration check on a throwaway database, then the
# integration, contract and concurrency suites on hds_p3_test (scripts/ops/ci_local_db.sh).
ci-local-db:
	scripts/ops/ci_local_db.sh

# Install the git hooks from .pre-commit-config.yaml: on each commit ruff, ruff format, mypy and the
# gitleaks scan of the staged change; on each message commitlint. Run once per clone. CI stays the
# authority; `git commit --no-verify` skips the hooks when you mean to.
# pre-commit, pinned (infra/VERSIONS.md), is installed as a uv tool: the git hook it writes runs
# that tool's Python, which stays put. A uvx run would leave the hook pointing into uv's cache, and
# `uv cache clean` would then break every commit.
hooks:
	uv tool install pre-commit==4.7.0
	"$$(uv tool dir --bin)/pre-commit" install --install-hooks --hook-type pre-commit \
		--hook-type commit-msg

# Write .env from .env.example with a fresh JWT_SECRET_KEY, readable by you alone (umask 077, so
# mode 0600). Refuses when .env exists, so it can never replace a key you already use. Deletes
# nothing.
env:
	@if [ -e .env ]; then echo "make env: .env exists; it was left as it is." >&2; exit 1; fi
	@umask 077 && key="$$(openssl rand -hex 32)" && \
		sed "s/^JWT_SECRET_KEY=.*/JWT_SECRET_KEY=$$key/" .env.example > .env && \
		echo "make env: wrote .env with a new JWT_SECRET_KEY."

# Build and start everything, and wait until the API and the worker are ready. Keeps the data. The
# build picks up a pulled change to the client, a dependency or a Dockerfile; when nothing changed it
# yields the same image, so `make up` on a running stack recreates nothing. A taken port stops it
# before anything starts; anything else that fails shows each service's state and recent log.
up: .env
	@RUNNING="$$($(COMPOSE_READ) ps --status running --services 2>/dev/null)" scripts/ops/ports_free.sh \
		postgres=$${POSTGRES_PORT:-5432} api=$${API_PORT:-8000} web=$${WEB_PORT:-8090} \
		api=$${API_METRICS_PORT:-9100} worker=$${WORKER_METRICS_PORT:-9101}
	@$(COMPOSE) up -d --build --wait || { \
		echo "make up: the stack did not become ready. The services and their recent logs:" >&2; \
		$(COMPOSE_READ) ps -a >&2 || true; \
		$(COMPOSE_READ) logs --tail=40 migrate api worker web >&2 || true; \
		exit 1; }
	@echo "make up: ready. API http://localhost:$${API_PORT:-8000}, web http://localhost:$${WEB_PORT:-8090}"

# Rebuild the images. Keeps the data; the next `make up` starts the new images.
build: .env
	$(COMPOSE_READ) build

# Stop and remove the containers. Keeps the database files in infra/volumes.
down: .env
	$(COMPOSE_READ) down

# Stop everything and DELETE the local database, after asking. The files belong to the database's
# own user, so a throwaway container removes them.
wipe: .env
	@printf 'Delete the local database? [y/N] ' && read answer && [ "$$answer" = "y" ] || \
		{ echo "make wipe: nothing deleted."; exit 1; }
	$(COMPOSE_READ) down -v
	docker run --rm -v "$(CURDIR)/infra/volumes:/volumes" --entrypoint rm $(POSTGRES_IMAGE) -rf /volumes/postgres
	@echo "make wipe: the local database is deleted."

# Show the state of the containers.
ps: .env
	$(COMPOSE_READ) ps

# Follow the logs of all services; add a service name with `make logs S=api`.
logs: .env
	$(COMPOSE_READ) logs -f --tail=100 $(S)

.env:
	@echo "No .env in the repository root. Run 'make env' first." >&2
	@exit 1

# The backups in the cluster (specs/006 T045b): each makes a Job from its CronJob, waits for it and
# prints its logs; it refuses while one is still running.
ops-backup:
	scripts/ops/backup_jobs.sh run backup

ops-restore-test:
	scripts/ops/backup_jobs.sh run restore-test

ops-backups:
	scripts/ops/backup_jobs.sh list

# A restore in the cluster (specs/006 T049b), with Flux suspended throughout. BACKUP and CONFIRM
# pass through as they are.
ops-restore:
	BACKUP="$(BACKUP)" CONFIRM="$(CONFIRM)" scripts/ops/restore.sh

# Vault (specs/006 T052, T053; docs/runbooks/operations.md, section Vault): unseal after a restart;
# configure once after init, with the root token in VAULT_TOKEN (never an argument).
ops-unseal:
	scripts/ops/unseal.sh

ops-vault-configure:
	scripts/ops/vault_configure.sh

# Deploys (specs/006 T049, FR-021): code only, never a migration downgrade.
ops-rollback:
	REVISION="$(REVISION)" scripts/ops/rollback.sh

ops-resume:
	scripts/ops/resume.sh
