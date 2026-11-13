#!/usr/bin/env bash
# The CEO demo on this Mac: a local database with the roster workbook's data, the API, the
# schedule worker and the web app. Nothing leaves the machine; the data stays in demo-data/.
#
#   scripts/demo/run_demo.sh                 # reuse demo-data/roster.json and the database
#   scripts/demo/run_demo.sh WORKBOOK.xlsx   # parse the workbook again first
#   DEMO_FRESH=1 scripts/demo/run_demo.sh    # load the data into a new, empty database
#
# Then open http://localhost:8081 (or DEMO_WEB_PORT) and sign in as admin@hds.test with the
# development password.
# Needs Docker running, uv, and Node 22 with pnpm. Ctrl+C stops the API, worker and web app;
# the database container keeps running (docker stop hds-ceo-demo-pg to stop it).
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
export PATH="/opt/homebrew/opt/node@22/bin:$HOME/.local/bin:$PATH"
DATA="$ROOT/demo-data"
CONTAINER=hds-ceo-demo-pg
PG_PORT=${DEMO_PG_PORT:-55434}
API_PORT=${DEMO_API_PORT:-8000}
WEB_PORT=${DEMO_WEB_PORT:-8081}
IMAGE="postgres:16.15-alpine@sha256:aa6cb33cf4c19c765cefcf48d96176258800870a9a1e930ad126200f9d825139"

mkdir -p "$DATA"
docker info >/dev/null 2>&1 || { echo "run_demo: start Docker Desktop first." >&2; exit 1; }

if [[ $# -ge 1 ]]; then
  python3 -E "$ROOT/scripts/demo/parse_roster.py" "$1" --out "$DATA/roster.json" \
    --overrides "$DATA/overrides.json"
fi
[[ -f "$DATA/roster.json" ]] || { echo "run_demo: no demo-data/roster.json; pass the workbook." >&2; exit 1; }

# The database: local only, so no password (trust on 127.0.0.1).
if [[ -n "${DEMO_FRESH:-}" ]] && docker ps -a --format '{{.Names}}' | grep -qx "$CONTAINER"; then
  docker rm -f "$CONTAINER" >/dev/null
  docker volume rm "$CONTAINER-data" >/dev/null 2>&1 || true
fi
if ! docker ps --format '{{.Names}}' | grep -qx "$CONTAINER"; then
  if docker ps -a --format '{{.Names}}' | grep -qx "$CONTAINER"; then
    docker start "$CONTAINER" >/dev/null
  else
    docker run -d --name "$CONTAINER" -e POSTGRES_USER=hds -e POSTGRES_DB=hds_demo \
      -e POSTGRES_HOST_AUTH_METHOD=trust -p "127.0.0.1:$PG_PORT:5432" \
      -v "$CONTAINER-data:/var/lib/postgresql/data" "$IMAGE" >/dev/null
  fi
fi
until docker exec "$CONTAINER" pg_isready -U hds -d hds_demo >/dev/null 2>&1; do sleep 1; done

export ENVIRONMENT=development
export DATABASE_URL="postgresql+asyncpg://hds@127.0.0.1:$PG_PORT/hds_demo"
[[ -f "$DATA/.signing-key" ]] || openssl rand -hex 32 > "$DATA/.signing-key"
JWT_SECRET_KEY=$(cat "$DATA/.signing-key")
export JWT_SECRET_KEY
export CORS_ORIGINS="[\"http://localhost:$WEB_PORT\",\"http://localhost:8090\",\"http://localhost:19006\"]"

cd "$ROOT/apps/api"
# The project's own environment; an existing one is left as it is (dev tools included).
[[ -x .venv/bin/python ]] || uv sync --frozen --quiet
.venv/bin/alembic upgrade head
ORGS=$(docker exec "$CONTAINER" psql -U hds -d hds_demo -tAc "select count(*) from organisations")
if [[ "$ORGS" == "0" ]]; then
  .venv/bin/python -m hds.scripts.seed_demo "$DATA/roster.json"
fi

.venv/bin/uvicorn hds.main:app --host 127.0.0.1 --port "$API_PORT" &
API=$!
.venv/bin/python -m hds.worker &
WORKER=$!
trap 'kill $API $WORKER 2>/dev/null || true' EXIT

cd "$ROOT"
[[ -d node_modules ]] || pnpm install --frozen-lockfile
cd "$ROOT/apps/client"
EXPO_PUBLIC_API_URL="http://localhost:$API_PORT" pnpm exec expo start --web --port "$WEB_PORT"
