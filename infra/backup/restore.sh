#!/bin/sh
# Restore a backup into the application's database (specs/006-deploy-operations FR-013, SC-005,
# research R11; docs/runbooks/restore.md).
#
#   restore.sh [NAME]   restore the backup NAME, or the newest, from HDS_RESTORE_FROM
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

work_dir
rclone_config

name=${1:-$(newest_backup "$HDS_RESTORE_FROM")}
[ -n "$name" ] || fail "no backup on $HDS_RESTORE_FROM:"
if has_data && [ "${CONFIRM:-}" != replace ]; then
    fail "$PGDATABASE on $PGHOST holds data; nothing was changed. Set CONFIRM=replace to replace it"
fi
fetch_verified "$HDS_RESTORE_FROM" "$name"

psql -X -q -v ON_ERROR_STOP=1 -c 'DROP SCHEMA public CASCADE' -c 'CREATE SCHEMA public' \
    || fail "the public schema could not be emptied"
pg_restore --exit-on-error --single-transaction --no-owner --no-privileges \
    --dbname="$PGDATABASE" "$HDS_WORK/$name.dump" \
    || fail "$name: pg_restore failed; the database is now empty"
log "$name: restored into $PGDATABASE on $PGHOST; run migrate next"
