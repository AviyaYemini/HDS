#!/bin/sh
# Retention (specs/006-deploy-operations FR-011, research R11), run after each backup.
#
# In each place (local-crypt:, offsite-crypt:) a backup past its manifest's expires_on is deleted,
# except the newest backup whose dump matches its manifest's checksum, which is never deleted, even
# when expired. A dump with no manifest (a run cut short) goes once its name is past the retention.
# Vault's snapshots (vault-<time>.snap.age, no manifest) on the plain local: and offsite: remotes:
# the newest HDS_VAULT_KEEP (8) are kept, so the newest is never deleted.
#
# The pruning functions run under `||`, where set -e is off, so each step checks its own status.
#
# Environment: as lib.sh, and HDS_VAULT_KEEP.
set -eu
# shellcheck source-path=SCRIPTDIR source=lib.sh
. "$(dirname "$0")/lib.sh"
: "${HDS_VAULT_KEEP:=8}"

work_dir
rclone_config

today=$(date -u -d "@$(now)" +%Y-%m-%d)
oldest_kept=$(date -u -d "@$(($(now) - HDS_RETENTION_DAYS * 86400))" +%Y%m%dT%H%M%SZ)
failed=""

# Whether the backup NAME on REMOTE has a dump matching its manifest.
valid() {
    want=$(rclone cat "$1:$2.manifest.json" 2>/dev/null | jq -r .sha256) || return 1
    got=$(rclone cat "$1:$2.dump" 2>/dev/null | sha256sum | cut -d ' ' -f 1) || return 1
    [ -n "$want" ] && [ "$want" = "$got" ]
}

# Whether date A (yyyy-mm-dd) is before date B.
before() {
    [ "$1" != "$2" ] && [ "$(printf '%s\n%s\n' "$1" "$2" | sort | head -n 1)" = "$1" ]
}

# Whether a backup's name (hds-<timestamp>) is older than the retention.
past_retention() {
    stamp=${1#hds-}
    [ "$stamp" != "$oldest_kept" ] \
        && [ "$(printf '%s\n%s\n' "$stamp" "$oldest_kept" | sort | head -n 1)" = "$stamp" ]
}

prune_backups() {
    remote=$1
    rclone mkdir "$remote:" || return 1  # a first run finds no folder yet
    listing=$(rclone lsf "$remote:" --files-only --include 'hds-*') || return 1
    names=$(printf '%s\n' "$listing" | sed -n 's/\.manifest\.json$//p' | sort -r)
    keep=""
    for name in $names; do
        if valid "$remote" "$name"; then keep=$name; break; fi
        log "$remote: $name does not verify"
    done
    [ -n "$keep" ] && log "$remote: keeping $keep, the newest valid backup"
    for name in $names; do
        [ "$name" = "$keep" ] && continue
        expires=$(rclone cat "$remote:$name.manifest.json" | jq -er .expires_on) || expires=""
        if [ -z "$expires" ]; then
            # An unreadable manifest must not stop pruning there; it goes once its name is old.
            if ! past_retention "$name"; then
                log "$remote: $name has an unreadable manifest; kept for now"
                continue
            fi
            expires="unreadable"
        fi
        if [ "$expires" = unreadable ] || before "$expires" "$today"; then
            rclone deletefile "$remote:$name.dump" 2>/dev/null || true
            rclone deletefile "$remote:$name.manifest.json" || return 1
            log "$remote: deleted $name (expired $expires)"
        fi
    done
    # Dumps whose manifest was never written.
    for dump in $(printf '%s\n' "$listing" | grep '\.dump$'); do
        name=${dump%.dump}
        printf '%s\n' "$names" | grep -qx "$name" && continue
        if past_retention "$name"; then
            rclone deletefile "$remote:$dump" || return 1
            log "$remote: deleted $dump (no manifest, past the retention)"
        fi
    done
}

prune_vault() {
    place=$1
    count=0
    rclone mkdir "$place" || return 1
    files=$(rclone lsf "$place" --files-only --include 'vault-*.snap.age') || return 1
    for file in $(printf '%s\n' "$files" | sort -r); do
        count=$((count + 1))
        [ "$count" -le "$HDS_VAULT_KEEP" ] && continue
        rclone deletefile "$place/$file" || return 1
        log "$place: deleted $file (more than $HDS_VAULT_KEEP snapshots)"
    done
}

for remote in local-crypt offsite-crypt; do
    prune_backups "$remote" || failed="$failed $remote"
done
for place in "local:$HDS_BACKUP_DIR/vault" "offsite:$HDS_OFFSITE_PATH/vault"; do
    prune_vault "$place" || failed="$failed $place"
done
[ -z "$failed" ] || fail "pruning failed in:$failed"
log "done"
