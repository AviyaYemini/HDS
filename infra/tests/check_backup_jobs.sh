#!/usr/bin/env bash
# Checks scripts/ops/backup_jobs.sh (make ops-backup, ops-restore-test; specs/006 T045b) against a
# fake kubectl on PATH, which plays the cluster's answers and records each call. No cluster is
# needed. Run by the CI lint job: infra/tests/check_backup_jobs.sh
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../.." && pwd)
fake=$(mktemp -d)
trap 'rm -rf -- "${fake:?}"' EXIT
cat > "$fake/kubectl" <<'EOF'
#!/bin/sh
# The fake: FAKE_NO_CRONJOB, FAKE_ACTIVE (running Jobs), FAKE_STATE (the Job's true conditions).
printf '%s\n' "$*" >> "$FAKE_LOG"
case "$*" in
    *"get cronjob"*) [ -z "${FAKE_NO_CRONJOB:-}" ] ;;
    *"get jobs -l app.kubernetes.io/component in "*) printf '%s' "${FAKE_ACTIVE:-}" ;;
    *"create job"*) ;;
    *"get job "*) printf '%s' "${FAKE_STATE:-}" ;;
    *"logs job/"*) echo "[pod/x/backup] a log line" ;;
    *) echo "fake kubectl: unexpected: $*" >&2; exit 2 ;;
esac
EOF
chmod +x "$fake/kubectl"
export PATH="$fake:$PATH" FAKE_LOG="$fake/calls"

failures=0
# check LABEL WANTED_STATUS WANTED_TEXT [VAR=VALUE...] -- ARGS...
check() {
    local label=$1 want_status=$2 want_text=$3 status out
    shift 3
    local vars=()
    while [ "$1" != -- ]; do vars+=("$1"); shift; done
    shift
    : > "$FAKE_LOG"
    set +e
    out=$(env ${vars[@]+"${vars[@]}"} "$ROOT/scripts/ops/backup_jobs.sh" "$@" 2>&1)
    status=$?
    set -e
    if [ "$status" = "$want_status" ] && grep -qF -- "$want_text" <<< "$out"; then
        printf 'ok    %s\n' "$label"
    else
        printf 'FAIL  %s\n      status %s (wanted %s); output: %s\n' "$label" "$status" "$want_status" "$out"
        failures=$((failures + 1))
    fi
}

check "a backup that completes succeeds and shows its logs" 0 "a log line" \
    FAKE_STATE="SuccessCriteriaMet Complete" -- run backup
check "a restore test that fails exits 1" 1 "failed; its pod stays" \
    FAKE_STATE="FailureTarget Failed" -- run restore-test
check "its logs are shown when it fails too" 1 "a log line" FAKE_STATE=Failed -- run restore-test
check "a running Job of the CronJob refuses a second" 1 "is still running: hds-backup-x" \
    FAKE_ACTIVE="hds-backup-x " -- run backup
if grep -q "create job" "$FAKE_LOG"; then
    printf 'FAIL  the refused run created a Job\n'; failures=$((failures + 1))
else
    printf 'ok    the refused run created no Job\n'
fi
if grep -qF -- "get jobs -l app.kubernetes.io/component in (backup,restore)" "$FAKE_LOG"; then
    printf 'ok    a running restore refuses a backup too\n'
else
    printf 'FAIL  the guard was: %s\n' "$(grep 'get jobs' "$FAKE_LOG")"; failures=$((failures + 1))
fi
check "a Job that never finishes times out" 1 "did not finish within 0s" \
    HDS_JOB_TIMEOUT=0 -- run backup
check "no CronJob, no Job" 1 "no CronJob hds-backup" FAKE_NO_CRONJOB=1 -- run backup
check "only backup and restore-test can be run" 1 "usage:" -- run vault-snapshot
check "the namespace is hds unless set" 0 "succeeded" FAKE_STATE=Complete -- run backup
if grep -q -- "-n hds create job hds-backup-manual-.* --from=cronjob/hds-backup" "$FAKE_LOG"; then
    printf 'ok    the Job is made in hds from cronjob/hds-backup\n'
else
    printf 'FAIL  the create call was: %s\n' "$(grep 'create job' "$FAKE_LOG")"
    failures=$((failures + 1))
fi

[ "$failures" -eq 0 ] || { echo "backup job checks: $failures failed" >&2; exit 1; }
echo "backup job checks: all passed"
