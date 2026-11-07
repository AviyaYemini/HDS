#!/bin/sh
# A pull request that changes infra/helm/hds-postgres must raise its Chart.yaml version (specs/006
# T047, review R98): the release pushes that chart only at a version the registry does not have, so
# a change at an old version would never reach the cluster. hds itself is versioned by the release.
#
#   chart_version.sh BASE [CHART_DIR]   BASE is the target branch (origin/main in CI)
#
# Run by the CI chart job on pull requests only, with the full history (fetch-depth: 0).
set -eu

base=${1:?usage: $0 BASE [CHART_DIR]}
chart=${2:-infra/helm/hds-postgres}
fail() { printf 'FAILED: %s\n' "$*" >&2; exit 1; }

merge_base=$(git merge-base "$base" HEAD) || fail "no merge base with $base; fetch the full history"
if git diff --quiet "$merge_base" HEAD -- "$chart"; then
    echo "chart version: $chart is unchanged"
    exit 0
fi
version_at() { git show "$1:$chart/Chart.yaml" 2> /dev/null | sed -n 's/^version: *//p'; }
old=$(version_at "$merge_base")
new=$(version_at HEAD)
[ -n "$new" ] || fail "$chart/Chart.yaml has no version"
if [ -z "$old" ]; then
    echo "chart version: $chart is new, at $new"
    exit 0
fi
highest=$(printf '%s\n%s\n' "$old" "$new" | sort -V | tail -n 1)
if [ "$new" = "$old" ] || [ "$highest" != "$new" ]; then
    fail "$chart changed, but its version $new is not above $old; raise it in $chart/Chart.yaml"
fi
echo "chart version: $chart changed and went from $old to $new"
