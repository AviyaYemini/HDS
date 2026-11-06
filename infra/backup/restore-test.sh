#!/bin/sh
# The weekly restore test, the backup image's part (specs/006-deploy-operations FR-014, R11).
#
# It takes the newest backup from offsite-crypt: (Drive), never the local copy, since that is the
# one a lost machine leaves. It checks the dump's SHA-256 against its manifest, restores it with
# pg_restore into the empty database the PG* variables name (the pod's own throwaway sidecar,
# never the application's database), then compares the restored migration revision and every
# table's row count with the manifest. This runs before migrate, which may change both.
#
# The API image's part follows: python -m hds.scripts.migrate, then python -m
# hds.scripts.check_restore, on the same copy (the chart's restore-test CronJob).
#
# Environment: as lib.sh; PG* name the throwaway database. HDS_BACKUP picks a backup by name.
set -eu
# shellcheck source-path=SCRIPTDIR source=lib.sh
. "$(dirname "$0")/lib.sh"

work_dir
rclone_config

name=${HDS_BACKUP:-$(newest_backup offsite-crypt)}
[ -n "$name" ] || fail "no backup on offsite-crypt:"
fetch_verified offsite-crypt "$name"
manifest="$HDS_WORK/$name.manifest.json"

if has_data; then
    fail "$PGDATABASE on $PGHOST is not empty; the restore test needs an empty database"
fi
pg_restore --exit-on-error --no-owner --no-privileges --dbname="$PGDATABASE" \
    "$HDS_WORK/$name.dump" || fail "$name: pg_restore failed"
log "$name: restored into $PGDATABASE on $PGHOST"

psql -X -q -At -v ON_ERROR_STOP=1 -c "$STATE_SQL" > "$HDS_WORK/restored.json" \
    || fail "$name: the restored database could not be read"
restored=$(jq -r .revision "$HDS_WORK/restored.json")
expected=$(jq -r .alembic_revision "$manifest")
[ "$restored" = "$expected" ] \
    || fail "$name: the restored revision is $restored, the manifest's is $expected"
differences=$(jq -r --slurpfile m "$manifest" '
    .tables as $got | $m[0].tables as $want
    | ($got + $want | keys_unsorted | unique)[]
    | select($got[.] != $want[.])
    | "\(.) has \($got[.] // "no table") rows restored, \($want[.] // "no table") in the manifest"
' "$HDS_WORK/restored.json")
[ -z "$differences" ] || fail "$name: the row counts differ: $differences"
log "$name: revision $restored and $(jq '.tables | length' "$manifest") tables' row counts match"
log "restore test passed for $name; migrate and check_restore come next"
