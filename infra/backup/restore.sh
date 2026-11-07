#!/bin/sh
# Restore a backup into the application's database (specs/006-deploy-operations FR-013, SC-005,
# research R11; docs/runbooks/restore.md).
#
#   restore.sh [NAME]           restore the backup NAME, or the newest, from HDS_RESTORE_FROM
#   restore.sh --check [NAME]   only the checks: the guard below and the download and checksum
#
# It exits 3 when it failed having changed nothing (the guard refused, or the backup could not be
# read or verified), and 1 when it failed after emptying the schema; make ops-restore starts the
# API and the worker again only after a 3.
#
# It refuses a database with data unless CONFIRM=replace is set; then the public schema is
# emptied first, so tables a newer release added cannot get in the way. The dump is checked
# against its manifest before anything changes.
#
# It only restores. Stopping the API and the worker first, migrating after, and starting them
# again are the caller's: in the cluster, `make ops-restore` (Group I, T049b) does them from the
# operator's machine, with Flux suspended for the whole run; outside it, run migrate yourself.
#
# Environment: as lib.sh; PG* name the application's database. HDS_RESTORE_FROM is offsite-crypt
# (the default) or local-crypt.
set -eu
# shellcheck source-path=SCRIPTDIR source=lib.sh
. "$(dirname "$0")/lib.sh"
: "${HDS_RESTORE_FROM:=offsite-crypt}"

HDS_FAIL_STATUS=3
check_only=""
if [ "${1:-}" = --check ]; then
    check_only=1
    shift
fi

work_dir
# Any failure before the schema is emptied, set -e's too (a jq that cannot read a manifest, say),
# exits 3: nothing was changed (review R118).
on_exit() {
    code=$?
    rm -rf "$HDS_WORK"
    [ "$code" -eq 0 ] || [ "$HDS_FAIL_STATUS" != 3 ] || exit 3
}
trap on_exit EXIT
rclone_config

name=${1:-$(newest_backup "$HDS_RESTORE_FROM")}
[ -n "$name" ] || fail "no backup on $HDS_RESTORE_FROM:; nothing was changed"
if has_data && [ "${CONFIRM:-}" != replace ]; then
    fail "$PGDATABASE on $PGHOST holds data; nothing was changed. Set CONFIRM=replace to replace it"
fi
fetch_verified "$HDS_RESTORE_FROM" "$name"  # its failures exit 3 too
if [ -n "$check_only" ]; then
    log "$name: verified; it can be restored into $PGDATABASE on $PGHOST; nothing was changed"
    exit 0
fi

HDS_FAIL_STATUS=1
psql -X -q -v ON_ERROR_STOP=1 -c 'DROP SCHEMA public CASCADE' -c 'CREATE SCHEMA public' \
    || fail "the public schema could not be emptied"
pg_restore --exit-on-error --single-transaction --no-owner --no-privileges \
    --dbname="$PGDATABASE" "$HDS_WORK/$name.dump" \
    || fail "$name: pg_restore failed; the database is now empty"
log "$name: restored into $PGDATABASE on $PGHOST; run migrate next"
