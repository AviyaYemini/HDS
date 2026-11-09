#!/bin/sh
# Old code on the new schema (specs/006-deploy-operations T061, FR-016): the base's code and its
# integration tests, on a database migrated by the head's migrations. A change that drops or
# renames a column the base still uses fails here, before it can break the old pods during a
# rolling update (Group L; research R5). Run by the CI compatibility job:
#
#   infra/tests/compat.sh [BASE]   BASE: the commit to check against (the pull request's base, or
#                                  the merge base with origin/main)
#
#   1. A temporary worktree at BASE, with its own dependencies (uv sync).
#   2. The head's apps/api/alembic/versions/ in place of the base's (the base's env.py stays): files
#      are copied, since the base has no switch for this until this feature merges.
#   3. The base's integration tests, without its own test_migration_* tests, which pin the base's
#      head revision, on a throwaway PostgreSQL of the cluster's version, never a developer's
#      database: its one database is named hds_p3_test, the name the tests' guard allows, but it is
#      this container's, not the shared server's. The tests migrate it to the head's revision first.
#   4. After them, alembic_version must hold the head's revision.
#
# A base older than a revision in compat-exemptions.txt is not checked (ADR 021).
#
# Needs git, uv and Docker.
set -eu

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
POSTGRES_IMAGE="postgres:16.15-alpine@sha256:aa6cb33cf4c19c765cefcf48d96176258800870a9a1e930ad126200f9d825139"
PORT="${HDS_COMPAT_PORT:-55440}"
cd "$ROOT"
base=${1:-$(git merge-base origin/main HEAD)}
base=$(git rev-parse --verify "$base^{commit}")
fail() { printf 'FAILED: %s\n' "$*" >&2; exit 1; }
say() { printf '\n== %s\n' "$*"; }

work=$(mktemp -d "${TMPDIR:-/tmp}/hds-compat.XXXXXX")
container="hds-compat-$$"
cleanup() {
    docker stop "$container" > /dev/null 2>&1 || true
    git -C "$ROOT" worktree remove --force "$work/base" > /dev/null 2>&1 || true
    rm -rf -- "${work:?}"
}
trap cleanup EXIT

say "the base, $(git log -1 --format='%h %s' "$base")"
git worktree add --detach --quiet "$work/base" "$base"
head_revision=$(cd apps/api && uv run --quiet alembic heads | sed -n 's/^\([^ ]*\) (head)$/\1/p')
[ -n "$head_revision" ] || fail "the head's migration revision could not be read"
base_revision=$(cd "$work/base/apps/api" && uv run --quiet alembic heads | sed -n 's/^\([^ ]*\) (head)$/\1/p')
echo "base revision $base_revision, head revision $head_revision"

# A rename recorded as exempt (ADR 021): the base's code cannot run on the new schema, and no
# release of it exists. Only the exact step onto that migration; anything else is checked.
if note=$(infra/tests/compat_exempt.sh "$base"); then
    printf '\ncompatibility: not checked; %s\n' "$note"
    exit 0
fi

say "the head's migrations in the base's place"
versions="$work/base/apps/api/alembic/versions"
find "$versions" -mindepth 1 -maxdepth 1 -name '*.py' -exec rm -f -- {} +
cp apps/api/alembic/versions/*.py "$versions/"

say "a throwaway PostgreSQL"
password=$(openssl rand -hex 16)
docker run -d --rm --name "$container" -e POSTGRES_USER=hds -e POSTGRES_PASSWORD="$password" \
    -e POSTGRES_DB=hds_p3_test -p "127.0.0.1:$PORT:5432" "$POSTGRES_IMAGE" > /dev/null
tries=0
until docker exec "$container" pg_isready -h 127.0.0.1 -U hds -q; do
    tries=$((tries + 1))
    [ "$tries" -lt 30 ] || fail "the throwaway database never became ready"
    sleep 1
done
url="postgresql+asyncpg://hds:$password@localhost:$PORT/hds_p3_test"

say "the base's integration tests on the head's schema"
status=0
(cd "$work/base/apps/api" && uv sync --quiet --extra dev \
    && ENVIRONMENT=test JWT_SECRET_KEY="ci-only-not-a-real-secret-0123456789" TEST_DATABASE_URL="$url" \
        uv run pytest tests/integration/ -q -p no:cacheprovider \
            --ignore-glob='tests/integration/test_migration_*.py') || status=$?
[ "$status" -eq 0 ] || fail "the base's code does not work on the head's schema (pytest exited $status)"

say "the schema the base ran on"
ran=$(docker exec "$container" psql -U hds -d hds_p3_test -Atc 'SELECT version_num FROM alembic_version')
[ "$ran" = "$head_revision" ] || fail "the tests ran on revision '$ran', not the head's $head_revision"
printf '\ncompatibility: the base (%s) passes its integration tests on the head'"'"'s schema (%s)\n' \
    "$(git rev-parse --short "$base")" "$head_revision"
