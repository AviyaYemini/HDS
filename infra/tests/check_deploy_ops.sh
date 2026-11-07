#!/usr/bin/env bash
# Checks scripts/ops/rollback.sh, resume.sh and restore.sh (make ops-rollback, ops-resume,
# ops-restore; specs/006 T049, T049b)
# against fake flux, helm and kubectl on PATH, which play the cluster's answers and record each call
# in order. No cluster is needed. Run by the CI lint job: infra/tests/check_deploy_ops.sh
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
fake=$(mktemp -d)
trap 'rm -rf -- "${fake:?}"' EXIT
for tool in flux helm kubectl; do
    cat > "$fake/$tool" <<EOF
#!/bin/sh
printf '%s %s\n' "$tool" "\$*" >> "\$FAKE_LOG"
exec "$fake/answer" "$tool" "\$@"
EOF
    chmod +x "$fake/$tool"
done
cat > "$fake/answer" <<'EOF'
#!/bin/sh
# FAKE_FAIL names the one call that fails: suspend, resume, rollback.
tool=$1; shift
case "$tool $*" in
    "flux "*"suspend helmrelease hds"*) [ "${FAKE_FAIL:-}" != suspend ] && touch "$FAKE_DIR/suspended" ;;
    "flux "*"resume helmrelease hds"*) [ "${FAKE_FAIL:-}" != resume ] ;;
    # FAKE_PG_WAS: whether the hds-postgres HelmRelease was suspended (unset: not).
    "kubectl "*"get helmrelease hds-postgres"*) printf '%s' "${FAKE_PG_WAS:-}" ;;
    # Before the suspend call, FAKE_FLUX_WAS (unset: not suspended); after it, FAKE_SUSPENDED.
    "kubectl "*"get helmrelease hds "*)
        if [ -f "$FAKE_DIR/suspended" ]; then printf '%s' "${FAKE_SUSPENDED-true}";
        else printf '%s' "${FAKE_FLUX_WAS:-}"; fi ;;
    "helm "*"rollback hds"*) [ "${FAKE_FAIL:-}" != rollback ] ;;
    "helm "*"history hds"*) printf '%s' "${FAKE_HISTORY:-$PLAIN}" ;;
    "helm "*"get metadata hds"*) printf 'NAME: hds\nAPP_VERSION: abc1234\nREVISION: 4\n' ;;
    # make ops-restore. FAKE_CHECK, FAKE_RUN, FAKE_MIGRATE: each Job's condition (Complete); with
    # Failed, FAKE_<JOB>_EXIT is its container's exit status. FAKE_CRON: hds-backup's suspend.
    # FAKE_TERMINATING: how many polls still find a pod. FAKE_ACTIVE: running backup Jobs.
    # The first listing answers FAKE_ACTIVE, later ones FAKE_ACTIVE_LATER (a backup that started).
    "kubectl "*"get jobs -l "*)
        if [ -f "$FAKE_DIR/listed" ]; then printf '%s' "${FAKE_ACTIVE_LATER:-}";
        else touch "$FAKE_DIR/listed"; printf '%s' "${FAKE_ACTIVE:-}"; fi ;;
    # FAKE_SAVED: what an earlier run left in hds-restore-saved ("api worker cron flux").
    "kubectl "*"get configmap hds-restore-saved"*) printf '%s' "${FAKE_SAVED:-}" ;;
    "kubectl "*"create configmap hds-restore-saved"* | "kubectl "*"delete configmap hds-restore-saved"*) ;;
    "kubectl "*"get cronjob hds-backup -o jsonpath={.spec.suspend}") printf '%s' "${FAKE_CRON:-}" ;;
    "kubectl "*"patch cronjob hds-backup"*) ;;
    "kubectl "*"get deployment hds-api"*) printf 2 ;;
    "kubectl "*"get deployment hds-worker"*) printf 1 ;;
    "kubectl "*"scale deployment"* | "kubectl "*"rollout status"*) ;;
    "kubectl "*"get pods -l app.kubernetes.io/instance=hds"*)
        n=$(cat "$FAKE_DIR/polls" 2> /dev/null || echo "${FAKE_TERMINATING:-0}")
        if [ "$n" -gt 0 ]; then echo "pod/hds-api-1"; echo $((n - 1)) > "$FAKE_DIR/polls"; fi ;;
    "kubectl "*"create job "*"--dry-run=client"*) echo '{}' ;;
    "kubectl patch --local"*) echo '{}' ;;
    "kubectl "*"create --dry-run=client"*"jsonpath"*) printf '%s' "${FAKE_HOOKS-Job/hds-migrate}" ;;
    "kubectl "*"create --dry-run=client"*) echo '{}' ;;
    "kubectl "*"create -f "*) ;;
    "kubectl "*"get job hds-restore-"*)
        case "$*" in
            *check*) printf '%s' "${FAKE_CHECK:-Complete}" ;;
            *migrate*) printf '%s' "${FAKE_MIGRATE:-Complete}" ;;
            *) printf '%s' "${FAKE_RUN:-Complete}" ;;
        esac ;;
    "kubectl "*"get pods -l job-name=hds-restore-"*)
        case "$*" in
            *check*) printf '%s' "${FAKE_CHECK_EXIT:-1}" ;;
            *migrate*) printf '%s' "${FAKE_MIGRATE_EXIT:-1}" ;;
            *) printf '%s' "${FAKE_RUN_EXIT:-1}" ;;
        esac ;;
    "kubectl "*"logs job/"*) echo "a log line" ;;
    "helm "*"get hooks hds"*) printf -- '---\nkind: Job\n' ;;
    *) echo "fake $tool: unexpected: $*" >&2; exit 2 ;;
esac
EOF
chmod +x "$fake/answer"
# Helm histories: revision, status, app version, description.
h() { printf '{"revision":%s,"status":"%s","app_version":"%s","description":"%s"}' "$@"; }
PLAIN="[$(h 1 superseded aaa "Install complete"),$(h 2 deployed bbb "Upgrade complete")]"
AFTER_ROLLBACK="[$(h 4 superseded zzz x),$(h 5 superseded aaa x),$(h 6 superseded bbb x),$(h 7 deployed aaa "Rollback to 5")]"
AFTER_REMEDIATION="[$(h 4 superseded zzz x),$(h 5 superseded aaa x),$(h 6 failed bbb x),$(h 7 deployed aaa "Rollback to 5")]"
ONLY_ONE="[$(h 1 deployed aaa "Install complete")]"
export PLAIN
export PATH="$fake:$PATH" FAKE_LOG="$fake/calls" FAKE_DIR="$fake" HDS_POLL=0

failures=0
pass() { printf 'ok    %s\n' "$1"; }
flunk() { printf 'FAIL  %s\n' "$1"; failures=$((failures + 1)); }

# check LABEL WANTED_STATUS WANTED_TEXT [VAR=VALUE...] -- SCRIPT [ARGS...]
check() {
    local label=$1 want_status=$2 want_text=$3 status out
    shift 3
    local vars=()
    while [ "$1" != -- ]; do vars+=("$1"); shift; done
    shift
    : > "$FAKE_LOG"
    rm -f "$FAKE_DIR/polls" "$FAKE_DIR/suspended" "$FAKE_DIR/listed"
    set +e
    out=$(env ${vars[@]+"${vars[@]}"} "$ROOT/scripts/ops/$1" "${@:2}" 2>&1)
    status=$?
    set -e
    if [ "$status" = "$want_status" ] && grep -qF -- "$want_text" <<< "$out"; then
        pass "$label"
    else
        flunk "$label"
        printf '      status %s (wanted %s); output: %s\n' "$status" "$want_status" "$out"
    fi
}

# called TEXT: whether a recorded call contains TEXT; ends TEXT: whether one ends with it.
called() { grep -qF -- "$1" "$FAKE_LOG"; }
ends() { awk -v t="$1" 'substr($0, length($0) - length(t) + 1) == t { found = 1 } END { exit !found }' "$FAKE_LOG"; }
# holds LABEL COMMAND... and never LABEL COMMAND...: the command succeeds, or fails.
holds() {
    local label=$1; shift
    if "$@"; then pass "$label"; else flunk "$label; the calls were: $(tr '\n' ';' < "$FAKE_LOG")"; fi
}
never() {
    local label=$1; shift
    if "$@"; then flunk "$label; the calls were: $(tr '\n' ';' < "$FAKE_LOG")"; else pass "$label"; fi
}

# line TEXT: the number of the first recorded call containing TEXT, or 0.
line() { grep -nF -- "$1" "$FAKE_LOG" | head -n 1 | cut -d: -f1 || true; }

echo "== rollback"
check "a rollback says what runs now" 0 "hds now runs abc1234 (Helm revision 4)" -- rollback.sh
suspend_at=$(line "suspend helmrelease hds") rollback_at=$(line "rollback hds")
if [ -n "$suspend_at" ] && [ -n "$rollback_at" ] && [ "$suspend_at" -lt "$rollback_at" ]; then
    pass "Flux is suspended before the rollback"
else
    flunk "Flux is suspended before the rollback; the calls were: $(tr '\n' ';' < "$FAKE_LOG")"
fi
holds "the HelmRelease is in flux-system" called "flux -n flux-system suspend helmrelease hds"
holds "helm rolls back in hds and waits" called "helm -n hds rollback hds 1 --wait"
never "a rollback leaves Flux suspended" called "resume"

holds "it goes to the revision before the one running" called "rollback hds 1 --wait"
check "after a rollback, a second goes further back" 0 "rolling back to Helm revision 4" \
    FAKE_HISTORY="$AFTER_ROLLBACK" -- rollback.sh
never "never to the version just rolled back from" called "rollback hds 6"
check "after Flux's remediation, the failed revision is skipped" 0 "rolling back to Helm revision 4" \
    FAKE_HISTORY="$AFTER_REMEDIATION" -- rollback.sh
check "with one revision there is nothing to go back to" 1 "no earlier version" \
    FAKE_HISTORY="$ONLY_ONE" -- rollback.sh
never "and Flux is not suspended for nothing" called "suspend"
check "REVISION picks the target" 0 "rolling back to Helm revision 3" REVISION=3 -- rollback.sh
holds "helm rolls back to it" called "rollback hds 3 --wait"

check "a failed suspend stops before the rollback" 1 "nothing was rolled back" \
    FAKE_FAIL=suspend -- rollback.sh
never "the failed suspend rolled nothing back" called "rollback hds"

check "a suspend that does not hold stops before the rollback" 1 "is not suspended" \
    FAKE_SUSPENDED=false -- rollback.sh
never "the unsuspended release was not rolled back" called "rollback hds"

check "a failed rollback says Flux stays suspended" 1 "Flux stays suspended" \
    FAKE_FAIL=rollback -- rollback.sh
never "the failed rollback left Flux suspended" called "resume"

echo "== resume"
check "a resume hands hds back to Flux" 0 "hds now runs abc1234 (Helm revision 4)" -- resume.sh
holds "Flux resumes the hds HelmRelease" called "flux -n flux-system resume helmrelease hds"
check "a failed resume exits 1" 1 "could not resume" FAKE_FAIL=resume -- resume.sh

echo "== restore"
B=BACKUP=hds-20261105T033000Z
# before A B: whether the first call containing A comes before the first containing B.
before() {
    local a b
    a=$(line "$1") b=$(line "$2")
    [ -n "$a" ] && [ -n "$b" ] && [ "$a" -lt "$b" ]
}
check "a restore runs to the end and says so" 0 "restored hds-20261105T033000Z" \
    "$B" CONFIRM=replace FAKE_TERMINATING=2 -- restore.sh
holds "the check runs before Flux is suspended" before "create -f" "suspend helmrelease hds"
holds "Flux is suspended before anything is scaled" before "suspend helmrelease hds" "scale deployment hds-api --replicas=0"
holds "the CronJob is suspended before the scale-down" \
    before 'patch cronjob hds-backup --type=merge -p {"spec":{"suspend":true}}' "scale deployment hds-api --replicas=0"
holds "it waits until no pod is left, terminating ones too" \
    test "$(grep -c "get pods -l app.kubernetes.io/instance=hds" "$FAKE_LOG")" -eq 3
last_poll=$(grep -nF "get pods -l app.kubernetes.io/instance=hds" "$FAKE_LOG" | tail -n 1 | cut -d: -f1)
holds "the restore Job is made after the last pod is gone" test "$last_poll" -lt "$(line '"value": ["restore.sh", "hds-')"
holds "migrate runs after the restore" before "get job hds-restore-run" "get job hds-restore-migrate"
holds "the restore Job runs restore.sh on the backup" called '"value": ["restore.sh", "hds-20261105T033000Z"]'
holds "the check runs restore.sh --check" called '"value": ["restore.sh", "--check", "hds-20261105T033000Z"]'
holds "CONFIRM reaches the Jobs" called '"value": {"name": "CONFIRM", "value": "replace"}'
holds "migrate is the release's own hook, renamed" called '"name": "hds-restore-migrate-'
holds "the API goes back to its 2 replicas after migrate" \
    before "get job hds-restore-migrate" "scale deployment hds-api --replicas=2"
holds "the worker goes back to its 1 replica" called "scale deployment hds-worker --replicas=1"
holds "an unsuspended CronJob is unsuspended again" called 'patch cronjob hds-backup --type=merge -p {"spec":{"suspend":false}}'
holds "Flux resumes after the scale-up" before "scale deployment hds-worker --replicas=1" "resume helmrelease hds"
holds "and resumes hds-postgres too" ends "resume helmrelease hds-postgres"
holds "hds-postgres is suspended before the scale-down" \
    before "suspend helmrelease hds-postgres" "scale deployment hds-api --replicas=0"
holds "the Jobs lose the CronJob as their owner" called '{"op": "remove", "path": "/metadata/ownerReferences"}'
holds "and are kept 7 days after they finish" called '"path": "/spec/ttlSecondsAfterFinished", "value": 604800'
holds "and its instantiate annotation" \
    called '{"op": "remove", "path": "/metadata/annotations/cronjob.kubernetes.io~1instantiate"}'
holds "the Job is component=restore" \
    called '{"op": "add", "path": "/metadata/labels/app.kubernetes.io~1component", "value": "restore"}'
holds "and so is its pod" called '"path": "/spec/template/metadata/labels/app.kubernetes.io~1component"'
never "the database-client label is left alone" called 'hds~1database-client'
holds "the guard counts running restores too" called "component in (backup,restore-test,restore)"

holds "what runs now is saved before anything is suspended" \
    before "create configmap hds-restore-saved --from-literal=api=2 --from-literal=worker=1 --from-literal=cron=false --from-literal=flux=false --from-literal=pgflux=false" "suspend helmrelease hds"
holds "the saved state is deleted at the end" called "delete configmap hds-restore-saved"

check "a run after one that stopped midway sets back what that one saved" 0 "hds-api 3 and hds-worker 2 are running" \
    "$B" CONFIRM=replace FAKE_SAVED="3 2 true false false" -- restore.sh
holds "the API goes back to the first run's 3" called "scale deployment hds-api --replicas=3"
holds "the CronJob goes back to the first run's suspend" \
    test "$(grep -F "patch cronjob hds-backup" "$FAKE_LOG" | tail -n 1 | sed 's/.* -p //')" = '{"spec":{"suspend":true}}'
never "it saves nothing over the first run's values" called "create configmap"
check "saved values that are not five fail before anything changes" 1 "not five values" \
    "$B" FAKE_SAVED="0" -- restore.sh
never "nothing was suspended then" called "suspend helmrelease"

check "Flux suspended before (ops-rollback) stays suspended" 0 "Flux stays suspended for hds, as it was" \
    "$B" FAKE_FLUX_WAS=true -- restore.sh
never "Flux is not resumed then" ends "resume helmrelease hds"
holds "hds-postgres, not suspended before, is resumed" ends "resume helmrelease hds-postgres"
check "hds-postgres suspended before stays suspended" 0 "restored" "$B" FAKE_PG_WAS=true -- restore.sh
never "hds-postgres is not resumed then" ends "resume helmrelease hds-postgres"

check "a backup that starts during the check stops it, everything set back" 1 \
    "a backup started: hds-backup-29001. Wait for it and run again; nothing was changed" \
    "$B" FAKE_ACTIVE_LATER="hds-backup-29001 " -- restore.sh
never "nothing was scaled" called "scale deployment"
holds "the CronJob is set back" called '{"spec":{"suspend":false}}'
holds "Flux is resumed for hds" ends "resume helmrelease hds"
holds "and the saved state deleted" called "delete configmap hds-restore-saved"
check "after an earlier run stopped midway, a started backup leaves its state alone" 1 \
    "stay suspended from the earlier run" \
    "$B" FAKE_SAVED="3 2 false false false" FAKE_ACTIVE_LATER="hds-backup-29001 " -- restore.sh
never "the earlier run's state is kept" called "delete configmap"

check "CONFIRM is replace or nothing" 1 "CONFIRM is replace or unset" "$B" CONFIRM=yes -- restore.sh
never "a bad CONFIRM made no Job" called "create -f"

check "a CronJob the runbook suspended stays suspended" 0 "suspend is true again" \
    "$B" FAKE_CRON=true -- restore.sh
never "it is never set to false" called '{"spec":{"suspend":false}}'

check "a refused check changes and stops nothing" 1 "nothing was changed or stopped" \
    "$B" FAKE_CHECK=Failed FAKE_CHECK_EXIT=3 -- restore.sh
never "the refused check suspended nothing" called "suspend"
never "the refused check scaled nothing" called "scale deployment"

check "a restore that refuses starts everything again" 1 "nothing was changed, and everything was started again" \
    "$B" FAKE_RUN=Failed FAKE_RUN_EXIT=3 -- restore.sh
holds "the API is back at 2" called "scale deployment hds-api --replicas=2"
holds "Flux is resumed" called "resume helmrelease hds"
never "no migration ran" called "hds-restore-migrate"

check "a restore that failed midway leaves everything stopped" 1 "stay at 0 and Flux stays suspended" \
    "$B" FAKE_RUN=Failed FAKE_RUN_EXIT=1 -- restore.sh
never "nothing was scaled back up" called "scale deployment hds-api --replicas=2"
never "Flux was not resumed" called "resume helmrelease"

check "a failed migration leaves everything stopped" 1 "restored but not migrated" \
    "$B" FAKE_MIGRATE=Failed -- restore.sh
never "nothing was scaled back up after it" called "scale deployment hds-api --replicas=2"
never "Flux was not resumed after it" called "resume helmrelease"

check "hooks that are not one migrate Job stop it before migrating" 1 "not one Job hds-migrate" \
    "$B" FAKE_HOOKS="List/" -- restore.sh
never "no migration Job was made from them" called "hds-restore-migrate-"

check "pods that never go start everything again" 1 "pods were still running" \
    "$B" FAKE_TERMINATING=99 HDS_STOP_TIMEOUT=0 -- restore.sh
never "no restore ran while pods were left" called "hds-restore-run-"
holds "Flux is resumed then" called "resume helmrelease hds"

check "a running backup refuses it" 1 "is still running: hds-backup-x" \
    "$B" FAKE_ACTIVE="hds-backup-x " -- restore.sh
never "the refused restore made no Job" called "create -f"
check "BACKUP is required" 1 "usage: make ops-restore" -- restore.sh
check "BACKUP must be a backup's name" 1 "usage: make ops-restore" BACKUP="x; rm" -- restore.sh

[ "$failures" -eq 0 ] || { echo "deploy op checks: $failures failed" >&2; exit 1; }
echo "deploy op checks: all passed"
