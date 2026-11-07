#!/bin/sh
# `make ci-local`: CI's jobs that need no database, on this machine, for what the branch changed.
# `make ci-local-full` (`ci_local.sh --full`): all of them, in CI's order of cost
# (.github/workflows/ci.yml), once, when the pull request is ready to push. Both stop at the first
# failure, and both end by listing which steps ran and which were skipped, and why.
# `ci_local.sh --plan` lists what the scoped run would run, and why, and runs nothing;
# infra/tests/check_ci_local.sh tests the choice with it, pointing CI_LOCAL_ROOT at a throwaway
# repository (the checkout this script is in, by default).
#
# The scoped run compares the working tree (commits, staged and unstaged changes, untracked files)
# with its merge base with CI_LOCAL_BASE (origin/main by default), and runs a step only when a path
# it checks changed. The API's steps follow CI's `changes` job (apps/api/ or the 001 contract); the
# client's follow apps/client/, packages/ and the workspace files, and an API change alone runs the
# generated client's drift check; each infra check follows its own files. The cheap repo-wide checks
# (the Markdown links, gitleaks and commitlint) always run. A change to this script or to ci.yml, or
# a base git cannot find, runs everything. The infra checks run with GITHUB_ACTIONS=true and
# CI=true, as in CI.
#
# The database suites, the migration check, the quickstart and the backup-restore test are not
# here; `make ci-local-db` runs the first two on hds_p3_test, once per pull request when it is ready
# to push, and infra/backup/tests/run.sh is the last (it builds its own images). The full suite
# runs in CI.
#
# Needs uv and Docker. The client's checks run in the pinned Node image, so no Node is needed here.
# The secret scan and commitlint cover the commits from CI_LOCAL_BASE to HEAD, as CI does for a pull
# request. Versions as infra/VERSIONS.md.
set -eu

cd "${CI_LOCAL_ROOT:-$(dirname "$0")/../..}"
ROOT=$(pwd)
BASE="${CI_LOCAL_BASE:-origin/main}"

NODE_IMAGE="node:22.23.4-bookworm-slim@sha256:5ec23b7c622b00547753bc6ffd78085dd868a68aeb41c8e0a1418f033e13d96d"
SHELLCHECK_IMAGE="koalaman/shellcheck:v0.11.0@sha256:61862eba1fcf09a484ebcc6feea46f1782532571a34ed51fedf90dd25f925a8d"
GITLEAKS_IMAGE="ghcr.io/gitleaks/gitleaks:v8.30.1@sha256:c00b6bd0aeb3071cbcb79009cb16a60dd9e0a7c60e2be9ab65d25e6bc8abbb7f"
ACTIONLINT_IMAGE="rhysd/actionlint:1.7.12@sha256:b1934ee5f1c509618f2508e6eb47ee0d3520686341fec936f3b79331f9315667"
LYCHEE_IMAGE="lycheeverse/lychee:0.24.2@sha256:e2d19e57cf6ab037026f20b8e449a1f30d9d7f81eef4194763aab2eab20bd28d"

# The paths each step checks, as extended regular expressions over repository-relative paths.
API_PATHS='^apps/api/|^specs/001-foundation-auth-org/contracts/openapi\.yaml$'
# Unit tests that read files outside apps/api: the harness's own tests, and the tests that read the
# repository's configuration (test_settings_known_values.py's list: workflows, compose, scripts,
# ini and toml files, Makefiles, Dockerfiles, JavaScript tool configs and the env example).
HARNESS_PATHS='^\.claude/(hooks/|settings\.json$|hds/review-ledger-template\.md$)|^\.specify/templates/'
CONFIG_PATHS='\.(ya?ml|sh|ini|toml)$|(^|/)\.env\.example$|(^|/)Makefile$|(^|/)Dockerfile[^/]*$|\.config\.(ts|js|mjs)$'
CLIENT_PATHS='^apps/client/|^packages/|^package\.json$|^pnpm-(lock|workspace)\.yaml$'
IMAGE_PATHS='^apps/api/|^infra/Dockerfile\.api$|^infra/tests/check_api_image\.sh$|^\.dockerignore$'
BACKUP_JOBS_PATHS='^scripts/ops/backup_jobs\.sh$|^infra/tests/check_backup_jobs\.sh$|^Makefile$'
DEPLOY_OPS_PATHS='^scripts/ops/(rollback|resume|restore|unseal)\.sh$|^infra/tests/(check_deploy_ops|rolling_update)\.sh$|^infra/cluster/k3d\.yaml$|^infra/backup/|^Makefile$'
RELEASE_PATHS='^infra/release/|^infra/helm/|^infra/tests/check_release\.sh$|^infra/tests/fixtures/release/|^\.github/workflows/release\.yml$'
STATUS_PATHS='^scripts/ops/status\.sh$|^infra/tests/check_status\.sh$|^infra/cluster/backup-pvc\.yaml$'
SHELL_PATHS='^(scripts/ops|infra/tests|infra/helm/tests|infra/backup|infra/backup/tests|infra/release)/[^/]+\.sh$'
WORKFLOW_PATHS='^\.github/workflows/'
CHART_PATHS='^infra/(helm|cluster|flux)/|^infra/VERSIONS\.md$|^\.github/workflows/release\.yml$'
FIXED_CHART_PATHS='^infra/helm/'
VAULT_PATHS='^scripts/ops/vault_configure\.sh$|^infra/cluster/vault|^infra/tests/check_vault_policy\.sh$|^infra/VERSIONS\.md$'
ALERT_PATHS='^infra/(helm|cluster)/|^infra/tests/check_alert(s|manager)\.sh$|^infra/VERSIONS\.md$'
CI_LOCAL_PATHS='^scripts/ops/ci_local\.sh$|^infra/tests/check_ci_local\.sh$'
# A change to the mirror itself, or to what it mirrors, runs everything.
SELF_PATHS='^scripts/ops/ci_local\.sh$|^\.github/workflows/ci\.yml$'

# CI's settings for the API's checks: an explicit environment and a published, refused-elsewhere key.
export ENVIRONMENT=test
export JWT_SECRET_KEY=ci-only-not-a-real-secret-0123456789

scratch=$(mktemp -d)
summary="$scratch/summary"
changed="$scratch/changed"
: > "$summary"
: > "$changed"
current=""
mode=scoped
plan=false
last_wanted=false

finish() {
    status=$?
    if [ -s "$summary" ]; then
        if $plan; then printf '\n== ci-local (plan): what would run\n'
        else printf '\n== ci-local (%s): what ran\n' "$mode"; fi
        cat "$summary"
        [ "$status" -eq 0 ] || printf 'stopped at the first failure, in: %s\n' "$current"
    fi
    rm -rf "$scratch"
    exit "$status"
}
trap finish EXIT

step() { current="$*"; printf '\n== %s\n' "$*"; }
api() { (cd apps/api && "$@"); }
# The infra checks see what they see in CI.
infra() { GITHUB_ACTIONS=true CI=true "$@"; }
note() { printf '%-8s %s (%s)\n' "$1" "$2" "$3" >> "$summary"; }

# want LABEL PATHS: whether to run a step, noted either way; when it runs, its heading is printed.
# last_wanted says whether the step was chosen, so that an `elif want` after it is skipped even
# under --plan, where nothing runs.
want() {
    if [ "$mode" = full ]; then
        reason=$why_full
    elif hits=$(grep -Ec "$2" "$changed"); then
        reason="$hits changed, such as $(grep -E -m 1 "$2" "$changed")"
    else
        last_wanted=false
        note skipped "$1" "nothing it checks changed"
        return 1
    fi
    last_wanted=true
    if $plan; then note "to run" "$1" "$reason"; return 1; fi
    note ran "$1" "$reason"
    step "$1"
}
# always LABEL: a repo-wide step, which runs whatever changed.
always() {
    if $plan; then note "to run" "$1" "always"; return 1; fi
    note ran "$1" "always"
    step "$1"
}

case "${1:-}" in
    --full) mode=full why_full="make ci-local-full" ;;
    --plan) plan=true ;;
    "") ;;
    *) echo "usage: $0 [--full | --plan]" >&2; exit 2 ;;
esac
if [ "$mode" = scoped ]; then
    if ! merge_base=$(git merge-base "$BASE" HEAD 2>/dev/null); then
        mode=full why_full="no merge base with $BASE"
    # --no-renames: a moved file counts under its old path and its new one.
    elif ! { git diff --name-only --no-renames "$merge_base" > "$changed" \
                && git ls-files --others --exclude-standard >> "$changed"; }; then
        mode=full why_full="git could not list the changes"
    elif grep -Eq "$SELF_PATHS" "$changed"; then
        mode=full why_full="$(grep -E -m 1 "$SELF_PATHS" "$changed") changed"
    else
        sort -u -o "$changed" "$changed"
        printf '== ci-local: %s paths changed since %s (merge base with %s)\n' \
            "$(wc -l < "$changed" | tr -d ' ')" "$(git rev-parse --short "$merge_base")" "$BASE"
        sed 's/^/   /' "$changed"
        echo "   (make ci-local-full runs every step)"
    fi
fi

if want "API: dependencies" "$API_PATHS|$HARNESS_PATHS|$CONFIG_PATHS|$CLIENT_PATHS"; then
    api uv sync --extra dev --quiet
fi

if want "lint: ruff check and format" "$API_PATHS"; then
    api uv run ruff check src/ tests/
    api uv run ruff format --check src/ tests/
fi

if want "docstrings: interrogate" "$API_PATHS"; then
    api uv run interrogate src/
fi

if want "typecheck: mypy --strict" "$API_PATHS"; then
    api uv run mypy --strict src/ tests/
fi

if want "import graph" "$API_PATHS"; then
    api uv run python -m hds.tools.check_imports
fi

if want "dependency audit: pip-audit" "$API_PATHS"; then
    api uv run pip-audit
fi

if want "unit and property tests (CI's 1000 examples)" "$API_PATHS"; then
    HDS_GENERATE_EXAMPLES=1000 api uv run pytest tests/unit/ tests/property/ -q -p no:cacheprovider
elif ! $last_wanted && want "the unit tests of the harness and of the repository's configuration" \
        "$HARNESS_PATHS|$CONFIG_PATHS"; then
    (cd apps/api && uv run pytest -q -p no:cacheprovider tests/unit/test_harness_*.py \
        tests/unit/test_settings_known_values.py tests/unit/test_deployment_files.py \
        tests/unit/test_ingress_prefixes.py)
fi

if want "the API image" "$IMAGE_PATHS"; then
    infra infra/tests/check_api_image.sh
fi

if want "the backup make targets (a fake kubectl)" "$BACKUP_JOBS_PATHS"; then
    infra infra/tests/check_backup_jobs.sh
fi

if want "the rollback and resume targets (fake flux, helm and kubectl)" "$DEPLOY_OPS_PATHS"; then
    infra infra/tests/check_deploy_ops.sh
fi

if want "the release's scripts and workflow" "$RELEASE_PATHS"; then
    infra infra/tests/check_release.sh
fi

if want "make ops-status (a fake kubectl and helm)" "$STATUS_PATHS"; then
    infra infra/tests/check_status.sh
fi

if want "ShellCheck" "$SHELL_PATHS"; then
    docker run --rm -v "$ROOT:/mnt:ro" -w /mnt "$SHELLCHECK_IMAGE" -x scripts/ops/*.sh \
        infra/tests/*.sh infra/helm/tests/*.sh infra/backup/*.sh infra/backup/tests/*.sh \
        infra/release/*.sh
fi

if want "actionlint" "$WORKFLOW_PATHS"; then
    docker run --rm -v "$ROOT:/repo:ro" -w /repo "$ACTIONLINT_IMAGE" -no-color -oneline
fi

if want "Helm charts: lint, kubeconform and the rendered-manifest checks" "$CHART_PATHS"; then
    infra infra/helm/tests/assert.sh
fi

if want "the fixed-version charts against $BASE (as CI on a pull request)" "$FIXED_CHART_PATHS"
then
    infra infra/helm/tests/chart_version.sh "$BASE"
fi

if want "the scoped run's choice of steps (--plan in throwaway repositories)" "$CI_LOCAL_PATHS"
then
    infra infra/tests/check_ci_local.sh
fi

if want "Vault's policies and roles (a dev Vault)" "$VAULT_PATHS"; then
    infra infra/tests/check_vault_policy.sh
fi

if want "the alert rules (promtool) and Alertmanager's configuration (amtool)" "$ALERT_PATHS"; then
    infra infra/tests/check_alerts.sh
    infra infra/tests/check_alertmanager.sh
fi

if always "Markdown links (offline, as CI)"; then
    # Only tracked Markdown, as a clean checkout has it: node_modules and caches hold their own.
    git ls-files -z '*.md' > "$scratch/markdown"
    docker run --rm -v "$ROOT:/input:ro" -v "$scratch:/list:ro" -w /input --entrypoint sh \
        "$LYCHEE_IMAGE" -c 'xargs -0 lychee --offline --no-progress --include-fragments < /list/markdown'
fi

if always "secret scan: gitleaks over $BASE..HEAD (redacted)"; then
    docker run --rm --user "$(id -u):$(id -g)" -v "$ROOT:/repo:ro" -w /repo "$GITLEAKS_IMAGE" \
        git --no-banner --redact --log-opts="$BASE..HEAD" .
fi

if always "commitlint over $BASE..HEAD"; then
    # Each message is linted from standard input in the Node image, with the repository's rules
    # (commitlint.config.mjs). Not through pre-commit: its hook reads the last commit's message
    # file rather than the one given, and pre-commit stashes unstaged changes while it runs.
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
fi

# The client's checks: all of them when the client changed; only the generated client's drift when
# the API alone changed, since a new or changed endpoint changes the client (Constitution V).
client=""
if want "client: ESLint, tsc, Jest and the generated client's drift" "$CLIENT_PATHS"; then
    client=all
elif ! $last_wanted && want "client: the generated client's drift (the API changed)" "$API_PATHS"
then
    client=drift
fi
if [ -n "$client" ]; then
    # On a copy of the tracked files, never on this checkout: `pnpm install` in the Linux image
    # would replace this machine's node_modules with Linux builds and a store path inside it.
    mkdir "$scratch/repo" "$scratch/tools"
    git ls-files -co --exclude-standard -z | rsync -a --from0 --files-from=- ./ "$scratch/repo/"
    # The generator asks uv for the OpenAPI document; the Node image has no uv, so the document is
    # exported here first and a stand-in uv in the container prints it.
    api uv run python -m hds.scripts.export_openapi > "$scratch/tools/openapi.json"
    printf '#!/bin/sh\ncat /tools/openapi.json\n' > "$scratch/tools/uv"
    chmod +x "$scratch/tools/uv"
    docker run --rm -v "$scratch/repo:/repo" -v "$scratch/tools:/tools:ro" \
        -v hds-pnpm-store:/pnpm-store -w /repo -e CI=1 -e CLIENT_CHECKS="$client" \
        -e HOST_UID="$(id -u)" -e HOST_GID="$(id -g)" \
        -e PATH="/tools:/usr/local/bin:/usr/bin:/bin" "$NODE_IMAGE" sh -c '
            set -e
            # The container runs as root; on Linux what it writes would be root-owned, and the
            # rm -rf of $scratch could not delete it. Hand the files back, pass or fail.
            trap "chown -R \"\$HOST_UID:\$HOST_GID\" /repo || true" EXIT
            corepack enable
            pnpm config set store-dir /pnpm-store >/dev/null
            pnpm install --frozen-lockfile --reporter=silent
            if [ "$CLIENT_CHECKS" = all ]; then
                pnpm client:lint
                pnpm client:typecheck
                pnpm client:test --silent
            fi
            pnpm api-client:check'
fi

if $plan; then
    printf '\nci-local (plan): nothing was run\n'
else
    printf '\nci-local (%s): every check that ran passed\n' "$mode"
fi
