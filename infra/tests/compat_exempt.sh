#!/bin/sh
# Whether the step from BASE to the working tree is a migration exempt from the rolling-update
# rules (ADR 021): the head's newest migration is listed in infra/tests/compat-exemptions.txt, and
# BASE's newest is the one it revises. Anything else, a later migration on top or a base further
# back, is checked as usual. Used by compat.sh and rolling_update.sh.
#
#   infra/tests/compat_exempt.sh BASE   exit 0 (and a note) when exempt; 1 when not
set -eu

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
cd "$ROOT"
base=$1
versions=apps/api/alembic/versions
newest() { sed -n 's#^\(.*/\)\{0,1\}\([0-9][0-9][0-9]_[a-z0-9_]*\)\.py$#\2#p' | sort | tail -n 1; }
base_head=$(git ls-tree --name-only "$base" "$versions/" | newest)
head_head=$(find "$versions" -maxdepth 1 -name "*.py" | newest)

while read -r revision record; do
    case "$revision" in '' | '#'*) continue ;; esac
    [ "$revision" = "$head_head" ] || continue
    revises=$(sed -n 's/^down_revision: str | None = "\(.*\)"$/\1/p' "$versions/$revision.py")
    if [ -n "$revises" ] && [ "$base_head" = "$revises" ]; then
        printf 'exempt: %s to %s, by %s\n' "$base_head" "$revision" "$record"
        exit 0
    fi
done < infra/tests/compat-exemptions.txt
exit 1
