#!/bin/sh
# make ops-rollback (specs/006-deploy-operations T049, FR-021, contracts/ops.md section 5), from the
# operator's machine with flux, helm, kubectl and jq: suspend Flux for the hds HelmRelease, so it
# cannot reinstall the newest version, then roll the hds release back and wait for it.
#
# The target is the last deploy before the version running now (review R115): the newest earlier
# revision Helm marks superseded with another app version, counted from the revision a rollback
# copied when the one running is itself a rollback ("Rollback to <n>", by this script or by Flux's
# remediation). So a second rollback goes further back, never to the version just rolled back
# from, and a failed revision is never a target. REVISION=<n> picks one instead. Code only: the migration Job is a pre-install and pre-upgrade hook, not a
# pre-rollback one, so no migration runs and the schema stays as it is (FR-016 keeps the older code
# working with it). Flux stays suspended until `make ops-resume`.
#
# Environment: REVISION, HDS_NAMESPACE (hds), HDS_FLUX_NAMESPACE (flux-system), HDS_ROLLBACK_TIMEOUT
# (10m).
set -eu
ns=${HDS_NAMESPACE:-hds}
flux_ns=${HDS_FLUX_NAMESPACE:-flux-system}
timeout=${HDS_ROLLBACK_TIMEOUT:-10m}

say() { printf '%s\n' "$*"; }
fail() { printf 'FAILED: %s\n' "$*" >&2; exit 1; }

history=$(helm -n "$ns" history hds -o json) || fail "the history of hds could not be read; nothing was changed"
target=${REVISION:-$(printf '%s' "$history" | jq -r '
    (map(select(.status == "deployed")) | last) as $cur
    | ((($cur.description // "") | capture("^Rollback to (?<n>[0-9]+)") | .n | tonumber)
        // $cur.revision) as $from
    | [.[] | select(.revision < $from and .status == "superseded"
        and .app_version != $cur.app_version)]
    | last | .revision // empty')} || fail "the history of hds could not be read; nothing was changed"
printf '%s' "$target" | grep -Eqx '[0-9]+' \
    || fail "no earlier version to roll back to; nothing was changed. See: helm -n $ns history hds"
say "rolling back to Helm revision $target"

flux -n "$flux_ns" suspend helmrelease hds \
    || fail "Flux could not suspend the hds HelmRelease; nothing was rolled back"
suspended=$(kubectl -n "$flux_ns" get helmrelease hds -o jsonpath='{.spec.suspend}') || suspended=""
[ "$suspended" = true ] \
    || fail "the hds HelmRelease is not suspended, so Flux could undo a rollback; nothing was rolled back"
say "Flux is suspended for hds"

helm -n "$ns" rollback hds "$target" --wait --timeout "$timeout" \
    || fail "the rollback failed; Flux stays suspended. See: helm -n $ns history hds"

meta=$(helm -n "$ns" get metadata hds) || fail "the rollback finished, but its revision could not be read"
version=$(printf '%s\n' "$meta" | sed -n 's/^APP_VERSION: //p')
revision=$(printf '%s\n' "$meta" | sed -n 's/^REVISION: //p')
say "hds now runs $version (Helm revision $revision)"
say "Flux stays suspended for hds. Run make ops-resume once a fixed version is released:"
say "Flux then installs the newest version."
