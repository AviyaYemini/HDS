#!/bin/sh
# `make ci-local-db`, opt in: CI's database jobs on this machine. Slow (about 25 minutes), so it is
# never part of `make ci-local` or of any per-turn check.
#
#   1. The migration check: `alembic upgrade head` then `alembic check` on a fresh, throwaway
#      PostgreSQL container, as CI's migration-check job does on its own fresh database. A
#      long-lived database can hold leftovers that no migration defines.
#   2. The integration, contract and concurrency suites on hds_p3_test, the one named database a
#      developer's runs may use. The tests' own guard refuses any other, and their advisory lock
#      stops a second run on it at once.
#
# Needs uv, Docker, and the local PostgreSQL that `make up` starts (port 5432 by default).
set -eu

cd "$(dirname "$0")/../.."
POSTGRES_IMAGE="postgres:16.15-alpine@sha256:aa6cb33cf4c19c765cefcf48d96176258800870a9a1e930ad126200f9d825139"
CHECK_PORT="${CI_LOCAL_CHECK_PORT:-55439}"
TEST_URL="${TEST_DATABASE_URL:-postgresql+asyncpg://hds:hds_dev@localhost:5432/hds_p3_test}"

export ENVIRONMENT=test
export JWT_SECRET_KEY=ci-only-not-a-real-secret-0123456789

step() { printf '\n== %s\n' "$*"; }

step "migration check on a fresh database"
container="hds-ci-local-migrations-$$"
docker run -d --rm --name "$container" -e POSTGRES_USER=hds -e POSTGRES_PASSWORD=check \
    -e POSTGRES_DB=hds_check -p "127.0.0.1:$CHECK_PORT:5432" "$POSTGRES_IMAGE" >/dev/null
trap 'docker stop "$container" >/dev/null 2>&1 || true' EXIT
tries=0
until docker exec "$container" pg_isready -U hds -q; do
    tries=$((tries + 1))
    [ "$tries" -lt 30 ] || { echo "the throwaway database never became ready" >&2; exit 1; }
    sleep 1
done
check_url="postgresql+asyncpg://hds:check@localhost:$CHECK_PORT/hds_check"
(cd apps/api && DATABASE_URL="$check_url" uv run alembic upgrade head >/dev/null)
(cd apps/api && DATABASE_URL="$check_url" uv run alembic check)

step "integration, contract and concurrency suites on $(echo "$TEST_URL" | sed 's#.*/##')"
(cd apps/api && TEST_DATABASE_URL="$TEST_URL" uv run pytest tests/integration/ tests/contract/ \
    tests/concurrency/ -q -p no:cacheprovider)

printf '\nci-local-db: every check passed\n'
