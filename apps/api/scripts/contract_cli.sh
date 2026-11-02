#!/usr/bin/env bash
# T113: run the Schemathesis CLI against a RUNNING, SEEDED server (CI "contract" job and locally).
#
#   DATABASE_URL=<scratch db url> scripts/contract_cli.sh [BASE_URL] [MAX_EXAMPLES]
#
# Pass A runs every built-in check on every operation except those listed in
# tests/contract/unprovable_operations.json (suite "cli").
# Pass B runs the same checks on those, minus "positive_data_acceptance", because a valid request
# is correctly refused there. The file gives the reason for each (a date order and a time zone,
# which an OpenAPI 3.0 schema cannot state, and the sign-in lockout). The pytest contract suite
# reads the same file, so the two lists cannot drift apart.
set -euo pipefail

# Safety guard: this run creates, edits, locks and deletes data. Run it only against a database
# whose name ends in _test or _scratch. DATABASE_URL must be the URL the server under test uses.
db_name="$(python3 - <<'PY'
import os
from urllib.parse import urlparse

print(urlparse(os.environ.get("DATABASE_URL", "")).path.lstrip("/"))
PY
)"
case "$db_name" in
  *_test | *_scratch) ;;
  *)
    cat >&2 <<MSG
REFUSING to run: DATABASE_URL points at '${db_name:-<unset>}', not a *_test or *_scratch database.
This script fuzzes the API with write access (it creates, edits and deletes data).
Point it at a scratch database, start the server with the same URL, then rerun:

  docker run -d --name hds-scratch -e POSTGRES_USER=hds -e POSTGRES_PASSWORD=hds \
    -e POSTGRES_DB=hds_scratch -p 5433:5432 postgres:16-alpine
  export DATABASE_URL=postgresql+asyncpg://hds:hds@localhost:5433/hds_scratch
  uv run alembic upgrade head && uv run python -m hds.scripts.seed_dev
  uv run uvicorn hds.main:app --port 8000 &
  scripts/contract_cli.sh http://localhost:8000
MSG
    exit 3
    ;;
esac

BASE_URL="${1:-http://localhost:8000}"
EXAMPLES="${2:-25}"
SPEC="$BASE_URL/api/v1/openapi.json"
LIST="$(dirname "$0")/../tests/contract/unprovable_operations.json"
# One operation id per line from the shared list (bash 3 on macOS has no mapfile).
SPECIAL=()
while IFS= read -r op; do SPECIAL+=("$op"); done < <(python3 - "$LIST" <<'PY'
import json
import sys

for op in json.load(open(sys.argv[1]))["operations"]:
    if "cli" in op["suites"]:
        print(op["operation_id"])
PY
)
[ "${#SPECIAL[@]}" -gt 0 ] || { echo "no operations read from $LIST" >&2; exit 4; }

TOKEN=$(curl -sf -X POST "$BASE_URL/auth/login" -H 'Content-Type: application/json' \
  -d '{"email":"admin@hds.test","password":"Dev1234!"}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["access_token"])')
AUTH="Authorization: Bearer $TOKEN"

# Keep the seeded accounts alive during the run (see scripts/contract_hooks.py): without this the
# fuzzer deactivates its own admin and later operations are only ever answered 401 or 403.
export HDS_CONTRACT_BASE_URL="$BASE_URL"
export SCHEMATHESIS_HOOKS=contract_hooks
export PYTHONPATH="$(cd "$(dirname "$0")" && pwd)${PYTHONPATH:+:$PYTHONPATH}"

exclude=(); include=()
for op in "${SPECIAL[@]}"; do exclude+=(--exclude-operation-id "$op"); include+=(--include-operation-id "$op"); done

echo "== Pass A: all checks, all other operations"
uv run schemathesis run "$SPEC" --checks all -H "$AUTH" --max-examples "$EXAMPLES" "${exclude[@]}"
echo "== Pass B: cross-field and lockout operations, without positive_data_acceptance"
uv run schemathesis run "$SPEC" --checks all --exclude-checks positive_data_acceptance \
  -H "$AUTH" --max-examples "$EXAMPLES" "${include[@]}"
