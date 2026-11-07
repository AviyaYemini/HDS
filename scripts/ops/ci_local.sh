#!/bin/sh
# `make ci-local`: CI's jobs that need no database, on this machine, in CI's order of cost
# (.github/workflows/ci.yml). It stops at the first failure. The database suites, the migration
# check, the quickstart and the backup-restore test are not here; `make ci-local-db` runs the first
# two on hds_p3_test, and infra/backup/tests/run.sh is the last (it builds its own images).
#
# Needs uv and Docker. The client's checks run in the pinned Node image, so no Node is needed here.
# The secret scan and commitlint cover the commits from CI_LOCAL_BASE (origin/main by default) to
# HEAD, as CI does for a pull request. Versions as infra/VERSIONS.md.
set -eu

cd "$(dirname "$0")/../.."
ROOT=$(pwd)
BASE="${CI_LOCAL_BASE:-origin/main}"

NODE_IMAGE="node:22.23.4-bookworm-slim@sha256:5ec23b7c622b00547753bc6ffd78085dd868a68aeb41c8e0a1418f033e13d96d"
SHELLCHECK_IMAGE="koalaman/shellcheck:v0.11.0@sha256:61862eba1fcf09a484ebcc6feea46f1782532571a34ed51fedf90dd25f925a8d"
GITLEAKS_IMAGE="ghcr.io/gitleaks/gitleaks:v8.30.1@sha256:c00b6bd0aeb3071cbcb79009cb16a60dd9e0a7c60e2be9ab65d25e6bc8abbb7f"
LYCHEE_IMAGE="lycheeverse/lychee:0.24.2@sha256:e2d19e57cf6ab037026f20b8e449a1f30d9d7f81eef4194763aab2eab20bd28d"

# CI's settings for the API's checks: an explicit environment and a published, refused-elsewhere key.
export ENVIRONMENT=test
export JWT_SECRET_KEY=ci-only-not-a-real-secret-0123456789

scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT

step() { printf '\n== %s\n' "$*"; }
api() { (cd apps/api && "$@"); }

step "API: dependencies"
api uv sync --extra dev --quiet

step "lint: ruff check and format"
api uv run ruff check src/ tests/
api uv run ruff format --check src/ tests/

step "docstrings: interrogate"
api uv run interrogate src/

step "typecheck: mypy --strict"
api uv run mypy --strict src/ tests/

step "import graph"
api uv run python -m hds.tools.check_imports

step "dependency audit: pip-audit"
api uv run pip-audit

step "unit and property tests (CI's 1000 examples)"
HDS_GENERATE_EXAMPLES=1000 api uv run pytest tests/unit/ tests/property/ -q -p no:cacheprovider

step "the API image"
infra/tests/check_api_image.sh

step "the backup make targets (a fake kubectl)"
infra/tests/check_backup_jobs.sh

step "ShellCheck"
docker run --rm -v "$ROOT:/mnt:ro" -w /mnt "$SHELLCHECK_IMAGE" -x scripts/ops/*.sh infra/tests/*.sh \
    infra/helm/tests/*.sh infra/backup/*.sh infra/backup/tests/*.sh

step "Helm charts: lint, kubeconform and the rendered-manifest checks"
infra/helm/tests/assert.sh

step "Markdown links (offline, as CI)"
# Only tracked Markdown, as a clean checkout has it: node_modules and caches hold their own.
git ls-files -z '*.md' > "$scratch/markdown"
docker run --rm -v "$ROOT:/input:ro" -v "$scratch:/list:ro" -w /input --entrypoint sh "$LYCHEE_IMAGE" \
    -c 'xargs -0 lychee --offline --no-progress --include-fragments < /list/markdown'

step "secret scan: gitleaks over $BASE..HEAD (redacted)"
docker run --rm --user "$(id -u):$(id -g)" -v "$ROOT:/repo:ro" -w /repo "$GITLEAKS_IMAGE" \
    git --no-banner --redact --log-opts="$BASE..HEAD" .

step "commitlint over $BASE..HEAD"
# Each message is linted from standard input in the Node image, with the repository's rules
# (commitlint.config.mjs). Not through pre-commit: its hook reads the last commit's message file
# rather than the one given, and pre-commit stashes unstaged changes while it runs.
mkdir "$scratch/messages"
for commit in $(git rev-list "$BASE..HEAD"); do
    git log -1 --format=%B "$commit" > "$scratch/messages/$(git log -1 --format=%h "$commit")"
done
docker run --rm -v "$ROOT/commitlint.config.mjs:/work/commitlint.config.mjs:ro" \
    -v "$scratch/messages:/messages:ro" -v hds-npm-cache:/root/.npm -w /work "$NODE_IMAGE" sh -c '
        set -e
        npm init -y >/dev/null
        npm install --silent --no-audit --no-fund @commitlint/cli@21.2.2 \
            @commitlint/config-conventional@21.2.4 >/dev/null
        for message in /messages/*; do
            [ -e "$message" ] || continue
            printf "%s " "$(basename "$message")"
            npx commitlint < "$message"
            echo ok
        done'

step "client: ESLint, tsc, Jest and the generated client's drift"
# On a copy of the tracked files, never on this checkout: `pnpm install` in the Linux image would
# replace this machine's node_modules with Linux builds and a store path inside the container.
mkdir "$scratch/repo" "$scratch/tools"
git ls-files -co --exclude-standard -z | rsync -a --from0 --files-from=- ./ "$scratch/repo/"
# The generator asks uv for the OpenAPI document; the Node image has no uv, so the document is
# exported here first and a stand-in uv in the container prints it.
api uv run python -m hds.scripts.export_openapi > "$scratch/tools/openapi.json"
printf '#!/bin/sh\ncat /tools/openapi.json\n' > "$scratch/tools/uv"
chmod +x "$scratch/tools/uv"
docker run --rm -v "$scratch/repo:/repo" -v "$scratch/tools:/tools:ro" \
    -v hds-pnpm-store:/pnpm-store -w /repo -e CI=1 \
    -e HOST_UID="$(id -u)" -e HOST_GID="$(id -g)" \
    -e PATH="/tools:/usr/local/bin:/usr/bin:/bin" "$NODE_IMAGE" sh -c '
        set -e
        # The container runs as root; on Linux what it writes would be root-owned, and the rm -rf
        # of $scratch could not delete it. Hand the files back, whether the checks pass or fail.
        trap "chown -R \"\$HOST_UID:\$HOST_GID\" /repo || true" EXIT
        corepack enable
        pnpm config set store-dir /pnpm-store >/dev/null
        pnpm install --frozen-lockfile --reporter=silent
        pnpm client:lint
        pnpm client:typecheck
        pnpm client:test --silent
        pnpm api-client:check'

printf '\nci-local: every check passed\n'
