#!/usr/bin/env bash
# Run feature 007's Playwright specs (apps/client/tests/e2e/ux/) on this machine, which has no Node.
#
#   scripts/ops/e2e_ux.sh                 every ux spec, both viewports
#   scripts/ops/e2e_ux.sh a-design-system  one group's spec (any Playwright filter works)
#   PREVIEWS=0 scripts/ops/e2e_ux.sh b-roles   against a build with the previews switched off
#
# The API runs here (uv) on port 8100 against hds_p3_test, migrated and seeded first; the web build
# and Playwright run in the pinned Playwright image on a copy of the tracked files, never on this
# checkout, so node_modules here is never replaced with Linux builds. The browser in the container
# reaches the API through host.docker.internal. One run at a time on hds_p3_test (the test-database
# rule): the script refuses to start while another session is connected to it.
set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"

# Pinned like CI's (apps/client/package.json pins @playwright/test 1.64.1).
PLAYWRIGHT_IMAGE="mcr.microsoft.com/playwright:v1.64.1-noble"
DB_NAME="hds_p3_test"
DATABASE_URL="postgresql+asyncpg://hds:hds_dev@localhost:5432/$DB_NAME"
PORT=8100
PREVIEWS="${PREVIEWS:-1}"

busy=$(docker exec infra-postgres-1 psql -U hds -d postgres -tAc \
    "SELECT count(*) FROM pg_stat_activity WHERE datname = '$DB_NAME' AND pid <> pg_backend_pid()")
if [ "$busy" != "0" ]; then
    echo "e2e_ux: $DB_NAME has $busy other connection(s); another run may be using it" >&2
    exit 1
fi

scratch="$(mktemp -d)"
api_pid=""
# Called by the EXIT trap below, which ShellCheck does not follow.
# shellcheck disable=SC2329
cleanup() {
    if [ -n "$api_pid" ]; then kill "$api_pid" 2>/dev/null || true; fi
    rm -rf "$scratch"
}
trap cleanup EXIT

api_env=(ENVIRONMENT=test "DATABASE_URL=$DATABASE_URL"
    JWT_SECRET_KEY=e2e-only-not-a-secret-0123456789-abcdefgh LOGIN_RATE_LIMIT_PER_NETWORK=0
    'CORS_ORIGINS=["http://localhost:3000"]')

(cd apps/api && env "${api_env[@]}" uv run alembic upgrade head >/dev/null \
    && env "${api_env[@]}" uv run python -m hds.scripts.seed_dev >/dev/null)
(cd apps/api && exec env "${api_env[@]}" uv run uvicorn hds.main:app --host 0.0.0.0 --port "$PORT" \
    --log-level warning) &
api_pid=$!
curl --retry 30 --retry-connrefused --retry-delay 1 -sf "http://localhost:$PORT/health" -o /dev/null

mkdir "$scratch/repo"
git ls-files -co --exclude-standard -z | rsync -a --from0 --files-from=- ./ "$scratch/repo/"

status=0
docker run --rm --ipc=host --add-host=host.docker.internal:host-gateway \
    -v "$scratch/repo:/repo" -v hds-pnpm-store:/pnpm-store -v hds-npm-cache:/root/.npm -w /repo \
    -e CI=1 -e E2E_EXTERNAL_API=1 -e E2E_API_URL=http://host.docker.internal:8100 -e PREVIEWS="$PREVIEWS" -e HOST_UID="$(id -u)" -e HOST_GID="$(id -g)" \
    "$PLAYWRIGHT_IMAGE" bash -c '
        set -e
        trap "chown -R \"\$HOST_UID:\$HOST_GID\" /repo || true" EXIT
        # The Playwright image has Node but no corepack: install the pnpm the root package.json pins.
        npm install -g --silent "pnpm@$(node -p "require(\"./package.json\").packageManager.split(\"@\")[1].split(\"+\")[0]")"
        pnpm config set store-dir /pnpm-store >/dev/null
        pnpm install --frozen-lockfile --reporter=silent
        cd apps/client
        previews=""; [ "$PREVIEWS" = 1 ] && previews=1
        EXPO_PUBLIC_API_URL=http://host.docker.internal:8100 EXPO_PUBLIC_PREVIEWS=$previews \
            pnpm exec expo export --platform web --output-dir dist >/dev/null
        pnpm exec playwright test --project=ux-desktop --project=ux-phone "$@"
    ' _ "$@" || status=$?

rm -rf apps/client/playwright-report
if [ -d "$scratch/repo/apps/client/playwright-report" ]; then
    cp -R "$scratch/repo/apps/client/playwright-report" apps/client/playwright-report
fi
exit "$status"
