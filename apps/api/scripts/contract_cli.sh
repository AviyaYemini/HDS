#!/usr/bin/env bash
# T113: run the Schemathesis CLI against a RUNNING, SEEDED server (CI "contract" job and locally).
#
#   scripts/contract_cli.sh [BASE_URL] [MAX_EXAMPLES]
#
# Pass A runs every built-in check on every operation except the three below.
# Pass B runs the same checks on those three, minus "positive_data_acceptance", because a
# valid request is correctly refused there:
#   * POST /projects, PATCH /projects/{id}: end_date >= start_date spans two fields, which an
#     OpenAPI 3.0 schema cannot state (FR-028, answered 422 projects.date_range_invalid);
#   * POST /auth/login: the run itself creates users and then guesses their passwords, so the
#     account lockout (423) is the designed answer.
set -euo pipefail

BASE_URL="${1:-http://localhost:8000}"
EXAMPLES="${2:-25}"
SPEC="$BASE_URL/api/v1/openapi.json"
SPECIAL=(login_auth_login_post create_project_api_v1_projects_post edit_project_api_v1_projects__id__patch)

TOKEN=$(curl -sf -X POST "$BASE_URL/auth/login" -H 'Content-Type: application/json' \
  -d '{"email":"admin@hds.test","password":"Dev1234!"}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["access_token"])')
AUTH="Authorization: Bearer $TOKEN"

exclude=(); include=()
for op in "${SPECIAL[@]}"; do exclude+=(--exclude-operation-id "$op"); include+=(--include-operation-id "$op"); done

echo "== Pass A: all checks, all other operations"
uv run schemathesis run "$SPEC" --checks all -H "$AUTH" --max-examples "$EXAMPLES" "${exclude[@]}"
echo "== Pass B: cross-field and lockout operations, without positive_data_acceptance"
uv run schemathesis run "$SPEC" --checks all --exclude-checks positive_data_acceptance \
  -H "$AUTH" --max-examples "$EXAMPLES" "${include[@]}"
