#!/bin/sh
# The daily backup (specs/006-deploy-operations FR-011, FR-012, research R11): one consistent dump
# of the application database and its manifest, written through rclone crypt to the machine
# (local-crypt:) and to Drive (offsite-crypt:).
#
# The dump and the manifest's revision and row counts come from one snapshot: a read-only
# transaction exports it, pg_dump reads at it, and the counts are taken in that transaction, so a
# write in between changes neither. In each place the dump is written first and the manifest last,
# so a run cut short leaves no valid backup there (data-model.md). If either place fails the
# script exits 1 (the alert), and the other place's copy is kept.
#
# Environment: as lib.sh. HDS_VERSION, the image's version, goes in the manifest.
set -eu
# shellcheck source-path=SCRIPTDIR source=lib.sh
. "$(dirname "$0")/lib.sh"

work_dir
rclone_config

at=$(now)
name="hds-$(date -u -d "@$at" +%Y%m%dT%H%M%SZ)"
taken_at=$(date -u -d "@$at" +%Y-%m-%dT%H:%M:%SZ)
expires_on=$(date -u -d "@$((at + HDS_RETENTION_DAYS * 86400))" +%Y-%m-%d)
HDS_DUMP="$HDS_WORK/$name.dump"
export HDS_DUMP

log "$name: dumping $PGDATABASE on $PGHOST"
psql -X -q -At -v ON_ERROR_STOP=1 -v state_file="$HDS_WORK/state.json" > /dev/null <<EOF
BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;
SELECT pg_export_snapshot() AS snapshot \gset
\setenv HDS_SNAPSHOT :snapshot
\! pg_dump --format=custom --snapshot="\$HDS_SNAPSHOT" --file="\$HDS_DUMP" && touch "\$HDS_DUMP.ok"
\o :state_file
$STATE_SQL;
\o
COMMIT;
EOF
[ -f "$HDS_DUMP.ok" ] || fail "$name: pg_dump failed"

jq -n \
    --arg name "$name" \
    --arg taken_at "$taken_at" \
    --arg version "${HDS_VERSION:-unknown}" \
    --arg sha256 "$(sha256sum "$HDS_DUMP" | cut -d ' ' -f 1)" \
    --argjson size_bytes "$(wc -c < "$HDS_DUMP")" \
    --arg expires_on "$expires_on" \
    --slurpfile state "$HDS_WORK/state.json" \
    '{name: $name, taken_at: $taken_at, version: $version,
      alembic_revision: $state[0].revision, tables: $state[0].tables,
      size_bytes: $size_bytes, sha256: $sha256, expires_on: $expires_on}' \
    > "$HDS_WORK/$name.manifest.json"

size=$(wc -c < "$HDS_DUMP")
failed=""
for remote in local-crypt offsite-crypt; do
    if rclone copyto "$HDS_DUMP" "$remote:$name.dump" \
        && rclone copyto "$HDS_WORK/$name.manifest.json" "$remote:$name.manifest.json"; then
        log "$name: written to $remote"
    else
        log "$name: could not be written to $remote"
        failed="$failed $remote"
    fi
done
[ -z "$failed" ] || fail "$name: not stored in:$failed"
log "$name: done ($size bytes)"
