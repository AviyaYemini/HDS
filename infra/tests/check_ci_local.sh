#!/bin/sh
# Checks the scoped `make ci-local`'s choice of steps (scripts/ops/ci_local.sh --plan): for each
# kind of change, the steps it would run are exactly the expected ones, so a step is neither skipped
# for a change it checks nor run for one it does not. Each case is a throwaway git repository that
# holds only the changed paths, which is all --plan reads: it runs no step. Run by the CI lint job,
# and by `make ci-local` when the script or this check changes: infra/tests/check_ci_local.sh
set -eu

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
SCRIPT="$ROOT/scripts/ops/ci_local.sh"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
failures=0
cases=0

# The steps' labels, as ci_local.sh prints them; @BASE@ stands for the case's base commit.
DEPS="API: dependencies"
API="$DEPS
lint: ruff check and format
docstrings: interrogate
typecheck: mypy --strict
import graph
dependency audit: pip-audit
unit and property tests (CI's 1000 examples)"
CONFIG_TESTS="the unit tests of the harness and of the repository's configuration"
IMAGE="the API image"
BACKUP_JOBS="the backup make targets (a fake kubectl)"
DEPLOY_OPS="the rollback and resume targets (fake flux, helm and kubectl)"
RELEASE="the release's scripts and workflow"
STATUS="make ops-status (a fake kubectl and helm)"
SHELLCHECK="ShellCheck"
ACTIONLINT="actionlint"
CHARTS="Helm charts: lint, kubeconform and the rendered-manifest checks"
FIXED_CHARTS="the fixed-version charts against @BASE@ (as CI on a pull request)"
SELF_CHECK="the scoped run's choice of steps (--plan in throwaway repositories)"
VAULT="Vault's policies and roles (a dev Vault)"
ALERTS="the alert rules (promtool) and Alertmanager's configuration (amtool)"
ALWAYS="Markdown links (offline, as CI)
secret scan: gitleaks over @BASE@..HEAD (redacted)
commitlint over @BASE@..HEAD"
CLIENT="client: ESLint, tsc, Jest and the generated client's drift"
DRIFT="client: the generated client's drift (the API changed)"
EVERY_STEP="$API
$IMAGE
$BACKUP_JOBS
$DEPLOY_OPS
$RELEASE
$STATUS
$SHELLCHECK
$ACTIONLINT
$CHARTS
$FIXED_CHARTS
$SELF_CHECK
$VAULT
$ALERTS
$ALWAYS
$CLIENT"

git_() { git -C "$repo" -c user.name=check -c user.email=check@example.invalid "$@"; }

# expect NAME HOW EXPECTED PATH...: a repository whose base commit holds only README.md, then each
# PATH written and left committed, staged or untracked (HOW); --plan must choose exactly EXPECTED,
# one label per line, in any order.
expect() {
    name=$1 how=$2 expected=$3
    shift 3
    cases=$((cases + 1))
    repo="$tmp/case$cases"
    git init -q "$repo"
    echo base > "$repo/README.md"
    git_ add README.md
    git_ commit -q -m "chore: base"
    base=$(git_ rev-parse HEAD)
    for path in "$@"; do
        mkdir -p "$repo/$(dirname "$path")"
        echo change >> "$repo/$path"
    done
    case $how in
        committed) git_ add -A && git_ commit -q -m "chore: change" ;;
        staged) git_ add -A ;;
        untracked) ;;
        *) echo "unknown HOW: $how" >&2; exit 2 ;;
    esac
    # A label ends with its reason in parentheses: "to run   LABEL (REASON)".
    CI_LOCAL_ROOT="$repo" CI_LOCAL_BASE="$base" sh "$SCRIPT" --plan > "$tmp/plan" 2>&1 \
        || { echo "FAIL  $name: --plan exited non-zero"; cat "$tmp/plan"; failures=$((failures + 1)); return 0; }
    sed -n 's/^to run   //p' "$tmp/plan" | sed 's/ ([^()]*)$//' | sort > "$tmp/actual"
    printf '%s\n' "$expected" | sed "s/@BASE@/$base/g" | sort > "$tmp/wanted"
    if cmp -s "$tmp/wanted" "$tmp/actual"; then
        printf 'ok    %s\n' "$name"
    else
        printf 'FAIL  %s (- wanted, + chosen):\n' "$name"
        diff "$tmp/wanted" "$tmp/actual" | sed -n 's/^</      -/p; s/^>/      +/p'
        failures=$((failures + 1))
    fi
}

expect "a docs-only change runs only the repo-wide checks" committed "$ALWAYS" \
    docs/guide.md specs/006-deploy-operations/tasks.md .claude/hds/CLAUDE.hds.md
expect "an API change runs the API's steps, its image and the client's drift" committed \
    "$API
$IMAGE
$DRIFT
$ALWAYS" apps/api/src/hds/main.py
expect "the 001 contract runs the API's steps and the client's drift (CI's changes job)" staged \
    "$API
$DRIFT
$ALWAYS" specs/001-foundation-auth-org/contracts/openapi.yaml
expect "a client change runs every client check, an untracked file too" untracked \
    "$DEPS
$CLIENT
$ALWAYS" apps/client/src/screen.tsx
expect "a chart change runs the chart, release and alert checks and the configuration tests" \
    committed "$DEPS
$CONFIG_TESTS
$RELEASE
$CHARTS
$FIXED_CHARTS
$ALERTS
$ALWAYS" infra/helm/hds/values.yaml
expect "an ops script runs its own check, ShellCheck and the configuration tests" staged \
    "$DEPS
$CONFIG_TESTS
$STATUS
$SHELLCHECK
$ALWAYS" scripts/ops/status.sh
expect "the release workflow runs actionlint, the release and chart checks" committed \
    "$DEPS
$CONFIG_TESTS
$RELEASE
$ACTIONLINT
$CHARTS
$ALWAYS" .github/workflows/release.yml
expect "the Makefile runs the make-target checks and the configuration tests" untracked \
    "$DEPS
$CONFIG_TESTS
$BACKUP_JOBS
$DEPLOY_OPS
$ALWAYS" Makefile
expect "the hooks run the harness's unit tests" committed \
    "$DEPS
$CONFIG_TESTS
$ALWAYS" .claude/hooks/hds_hooks.py
expect "a Vault policy runs the Vault, chart and alert checks" staged \
    "$CHARTS
$VAULT
$ALERTS
$ALWAYS" infra/cluster/vault-policy.hcl
expect "this check runs itself, ShellCheck and the configuration tests" committed \
    "$DEPS
$CONFIG_TESTS
$SHELLCHECK
$SELF_CHECK
$ALWAYS" infra/tests/check_ci_local.sh
expect "ci.yml runs every step, as make ci-local-full does" committed "$EVERY_STEP" \
    .github/workflows/ci.yml

if [ "$failures" -gt 0 ]; then
    echo "check_ci_local: $failures of $cases failed" >&2
    exit 1
fi
echo "check_ci_local: all $cases passed"
