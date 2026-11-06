#!/bin/sh
# Check the one-command local environment from a clean checkout (specs/006-deploy-operations T007,
# FR-001 to FR-005, SC-001). CI's `quickstart` job runs it; it stops at the first unmet step.
#
# It WIPES the local database (step 10 and 12), so it refuses to run unless QUICKSTART_THROWAWAY=1
# says this checkout is a throwaway one. Never run it in a checkout whose database you want.
#
# Usage, from the repository root of a fresh clone:
#   QUICKSTART_THROWAWAY=1 scripts/ops/quickstart_check.sh
# The ports follow the same variables as the compose file (POSTGRES_PORT, API_PORT, WEB_PORT,
# API_METRICS_PORT, WORKER_METRICS_PORT), so a second stack can run beside another; set
# COMPOSE_PROJECT_NAME too, so the two never share containers.
set -eu

if [ "${QUICKSTART_THROWAWAY:-}" != "1" ]; then
    echo "Refusing: this wipes the local database. Set QUICKSTART_THROWAWAY=1 in a throwaway clone." >&2
    exit 2
fi

API="http://localhost:${API_PORT:-8000}"
WEB="http://localhost:${WEB_PORT:-8090}"
COMPOSE="docker compose -f infra/docker-compose.yml --env-file .env"
PASSWORD="Dev1234!"  # the seed's published development password (seed_dev.py)

step() { printf '\n== %s\n' "$*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

# Wait up to $2 seconds for $1 to answer 200.
wait_for() {
    deadline=$(( $(date +%s) + $2 ))
    until curl -sf -o /dev/null "$1"; do
        [ "$(date +%s)" -lt "$deadline" ] || return 1
        sleep 2
    done
}

# Sign in as $1 and print the access token.
token_for() {
    curl -sf -X POST "$API/auth/login" -H 'Content-Type: application/json' \
        -d "{\"email\": \"$1\", \"password\": \"$PASSWORD\"}" |
        python3 -c 'import json, sys; print(json.load(sys.stdin)["access_token"])'
}

# The number of qualifications named $1, read as the Admin.
qualifications_named() {
    curl -sf "$API/api/v1/qualifications" -H "Authorization: Bearer $(token_for admin@hds.test)" |
        python3 -c 'import json, sys; print(sum(q["name"] == sys.argv[1] for q in json.load(sys.stdin)["items"]))' "$1"
}

# Run make with a time limit (macOS has no `timeout`, so a background watchdog).
make_within() {
    limit=$1; shift
    make "$@" &
    pid=$!
    ( sleep "$limit"; kill "$pid" 2>/dev/null ) &
    watchdog=$!
    status=0
    wait "$pid" || status=$?
    kill "$watchdog" 2>/dev/null || true
    return "$status"
}

step "1. make env writes .env with a fresh 64-hex-character key"
rm -f .env
make env
grep -Eq '^JWT_SECRET_KEY=[0-9a-f]{64}$' .env || fail ".env has no 64-hex-character JWT_SECRET_KEY"

step "2. a second make env refuses and leaves .env unchanged"
before=$(cat .env)
if make env 2>/dev/null; then fail "a second make env succeeded"; fi
[ "$(cat .env)" = "$before" ] || fail "a second make env changed .env"

step "3. make up from nothing (within 600 s, under SC-001's 15 minutes)"
started=$(date +%s)
make_within 600 up || fail "make up failed or took over 600 s"
echo "make up took $(( $(date +%s) - started )) s (cold)"

step "4. /readyz answers 200 within 120 s"
wait_for "$API/readyz" 120 || fail "$API/readyz never answered 200"

step "5. the web client serves the app"
curl -sf "$WEB/" | grep -q '<div id="root"' || fail "$WEB/ does not serve the app's root element"

step "6. every sample account signs in"
for who in admin manager employee extra client; do
    token_for "$who@hds.test" >/dev/null || fail "$who@hds.test cannot sign in"
done

step "7. data survives make down and make up (within 120 s)"
name="Quickstart check $(date +%s)"
curl -sf -X POST "$API/api/v1/qualifications" -H 'Content-Type: application/json' \
    -H "Authorization: Bearer $(token_for admin@hds.test)" -d "{\"name\": \"$name\"}" >/dev/null ||
    fail "could not create a qualification"
make down
started=$(date +%s)
make_within 120 up || fail "a warm make up failed or took over 120 s"
echo "make up took $(( $(date +%s) - started )) s (warm)"
wait_for "$API/readyz" 60 || fail "$API/readyz never answered after a restart"
[ "$(qualifications_named "$name")" = "1" ] || fail "the qualification did not survive a restart"

step "8. make up on the running stack exits 0, seeds nothing twice and restarts nothing"
api_started=$($COMPOSE ps -q api | xargs docker inspect --format '{{.State.StartedAt}}')
make up || fail "make up on a running stack failed"
[ "$($COMPOSE ps -q api | xargs docker inspect --format '{{.State.StartedAt}}')" = "$api_started" ] ||
    fail "make up restarted the running API"
organisations=$($COMPOSE exec -T postgres psql -U "${POSTGRES_USER:-hds}" -d "${POSTGRES_DB:-hds}" -Atc "select count(*) from organisations")
[ "$organisations" = "1" ] || fail "there are $organisations organisations after a second make up"

step "9. a missing JWT_SECRET_KEY stops make up, naming it"
make down
cp .env .env.keep
sed 's/^JWT_SECRET_KEY=.*/JWT_SECRET_KEY=/' .env.keep > .env
if out=$(make up 2>&1); then mv .env.keep .env; fail "make up started without JWT_SECRET_KEY"; fi
mv .env.keep .env
echo "$out" | grep -q JWT_SECRET_KEY || fail "the refusal does not name JWT_SECRET_KEY"

step "10. make wipe deletes the data, and the next make up has only the seed"
make up
echo y | make wipe
make_within 600 up || fail "make up after a wipe failed"
wait_for "$API/readyz" 120 || fail "$API/readyz never answered after a wipe"
[ "$(qualifications_named "$name")" = "0" ] || fail "data survived make wipe"

step "11. a taken port stops make up, naming it, and starts nothing"
make down
# Docker releases the stopped stack's ports a moment late, so the holder retries its bind; it
# creates the ready file once it listens.
ready_file=$(mktemp -u)
python3 -c 'import socket, sys, time
s = socket.socket()
for _ in range(20):
    try:
        s.bind(("0.0.0.0", int(sys.argv[1]))); break
    except OSError:
        time.sleep(0.5)
s.listen(); open(sys.argv[2], "w").close(); time.sleep(120)' "${API_PORT:-8000}" "$ready_file" &
holder=$!
for _ in $(seq 30); do [ -e "$ready_file" ] && break; sleep 0.5; done
[ -e "$ready_file" ] || { kill "$holder" 2>/dev/null; fail "could not hold port ${API_PORT:-8000}"; }
rm -f "$ready_file"
if out=$(make up 2>&1); then kill "$holder"; fail "make up started with port ${API_PORT:-8000} taken"; fi
kill "$holder" 2>/dev/null || true
echo "$out" | grep -q "${API_PORT:-8000}" || fail "the refusal does not name port ${API_PORT:-8000}"
[ -z "$($COMPOSE ps -q)" ] || fail "make up left containers running after refusing"

step "12. a failing migration stops make up, and the API does not start"
echo y | make wipe
$COMPOSE up -d --wait postgres
$COMPOSE exec -T postgres psql -U "${POSTGRES_USER:-hds}" -d "${POSTGRES_DB:-hds}" -c "create table organisations (id int)"
if out=$(make up 2>&1); then fail "make up succeeded although migration 001 cannot run"; fi
[ -z "$($COMPOSE ps -q --status running api)" ] || fail "the API started although migrations failed"
echo "$out" | grep -qi "organisations" || fail "the output does not show the migration error"

echo y | make wipe
echo
echo "quickstart: every step passed"
