#!/usr/bin/env bash
# The backup and restore test (specs/006-deploy-operations T038, FR-011 to FR-014; the CI
# backup-restore job). Compose runs the application's database, an empty one to restore into, the
# API image and the backup image (compose.yml); the remotes are encrypted folders under a temporary
# folder. Every password is made here for this run. The script stops at the first unmet step.
#
# Run from anywhere: infra/backup/tests/run.sh. Needs Docker and openssl.
set -euo pipefail

cd "$(dirname "$0")"
work=$(mktemp -d)
mkdir -p "$work/local" "$work/offsite" "$work/files"
HDS_TEST_DIR=$work
HDS_UID=$(id -u)
HDS_GID=$(id -g)
HDS_TEST_DB_PASSWORD=$(openssl rand -hex 16)
HDS_TEST_JWT_SECRET=$(openssl rand -hex 32)
HDS_CRYPT_PASSWORD=$(openssl rand -hex 24)
HDS_CRYPT_SALT=$(openssl rand -hex 24)
export HDS_TEST_DIR HDS_UID HDS_GID HDS_TEST_DB_PASSWORD HDS_TEST_JWT_SECRET HDS_CRYPT_PASSWORD \
    HDS_CRYPT_SALT

dc() { docker compose -f compose.yml "$@"; }
cleanup() {
    dc --profile run down -v --remove-orphans > /dev/null 2>&1 || true
    rm -rf -- "${work:?}"
}
trap cleanup EXIT

step() { printf '\n== step %s\n' "$*"; }
fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }
ok() { printf 'ok    %s\n' "$*"; }

# backup_run [-e NAME=VALUE]... COMMAND...: a command in the backup image; the -e options go to
# docker compose run, before the service.
backup_run() {
    local options=()
    while [ "${1:-}" = -e ]; do
        options+=("$1" "$2")
        shift 2
    done
    dc run --rm -T ${options[@]+"${options[@]}"} backup "$@"
}
rcl() { backup_run hds-rclone.sh "$@"; }
sql() { dc exec -T db psql -X -q -At -U hds -d hds -v ON_ERROR_STOP=1 -c "$1"; }
fresh_scratch() { dc up -d --wait --force-recreate --renew-anon-volumes scratch > /dev/null 2>&1; }
# Expect a command to fail with MESSAGE in its output: refuses LABEL MESSAGE COMMAND...
refuses() {
    local label=$1 message=$2 out status
    shift 2
    set +e
    out=$("$@" 2>&1)
    status=$?
    set -e
    [ "$status" -ne 0 ] || fail "$label: it succeeded; the output was: $out"
    grep -qF -- "$message" <<< "$out" || fail "$label: the output lacks '$message': $out"
    ok "$label"
}
SHIFTS="SELECT count(*) || ':' || md5(string_agg(id::text || starts_at::text, ',' ORDER BY id))
    FROM shifts"
SCRATCH_URL="postgresql+asyncpg://hds:${HDS_TEST_DB_PASSWORD}@scratch:5432/hds"

dc --profile run build -q api backup
dc up -d --wait db scratch > /dev/null 2>&1

step "1: a seeded database with known shifts, backed up to both places"
dc run --rm -T api sh -c 'python -m alembic upgrade head && python -m hds.scripts.seed_dev' > /dev/null
sql "INSERT INTO shifts (id, organization_id, project_id, shift_type_id, starts_at, ends_at,
        headcount, status, created_at, updated_at)
    SELECT gen_random_uuid(), p.organization_id, p.id,
        (SELECT st.id FROM shift_types st WHERE st.organization_id = p.organization_id
         ORDER BY st.name LIMIT 1),
        date_trunc('hour', now()) - n * interval '1 day',
        date_trunc('hour', now()) - n * interval '1 day' + interval '8 hours', 2, 'draft', now(), now()
    FROM projects p, generate_series(1, 3) n" > /dev/null
shifts_at_backup=$(sql "$SHIFTS")
[ "${shifts_at_backup%%:*}" -ge 3 ] || fail "the known shifts were not created"
backup_run backup.sh
name=$(rcl lsf offsite-crypt: | sed -n 's/\.manifest\.json$//p' | sort -r | head -n 1)
[ -n "$name" ] || fail "no manifest on offsite-crypt:"
for remote in local-crypt offsite-crypt; do
    files=$(rcl lsf "$remote:" | sort | tr '\n' ' ')
    [ "$files" = "$name.dump $name.manifest.json " ] || fail "$remote holds: $files"
done
rcl cat "offsite-crypt:$name.manifest.json" > "$work/files/manifest.json"
jq -e '(.name | test("^hds-[0-9]{8}T[0-9]{6}Z$")) and (.taken_at | test("^[0-9-]{10}T[0-9:]{8}Z$"))
    and (.version | type == "string") and (.alembic_revision | length > 0)
    and (.tables | type == "object") and (.tables.shifts >= 3) and (.tables.users == 5)
    and (.size_bytes > 0) and (.sha256 | test("^[0-9a-f]{64}$"))
    and (.expires_on | test("^[0-9]{4}-[0-9]{2}-[0-9]{2}$"))' "$work/files/manifest.json" > /dev/null \
    || fail "the manifest lacks a field of data-model.md: $(cat "$work/files/manifest.json")"
sum=$(rcl cat "offsite-crypt:$name.dump" | sha256sum | cut -d ' ' -f 1)
[ "$sum" = "$(jq -r .sha256 "$work/files/manifest.json")" ] || fail "the dump's SHA-256 is not the manifest's"
ok "$name in both places; the manifest has every field and its sha256 matches the dump"

step "2: the stored files show no table name or address"
if grep -r -a -i -l -E 'shifts|users|organisations|alembic|@hds\.test|hds-[0-9]{8}T' \
    "$work/local" "$work/offsite"; then
    fail "a readable name is stored (the files above)"
fi
ok "nothing readable under either remote"

step "3: the restore test restores the offsite copy into an empty database and passes"
fresh_scratch
backup_run -e PGHOST=scratch restore-test.sh
dc run --rm -T -e DATABASE_URL="$SCRATCH_URL" api \
    sh -c 'python -m hds.scripts.migrate && python -m hds.scripts.check_restore'
ok "restore-test.sh, migrate and check_restore pass on the restored copy"
# The test must use the offsite copy: with the local copy gone it still passes.
rcl moveto "local-crypt:$name.dump" "local-crypt:$name.dump.away"
fresh_scratch
backup_run -e PGHOST=scratch restore-test.sh > /dev/null 2>&1 || fail "it needed the local copy"
rcl moveto "local-crypt:$name.dump.away" "local-crypt:$name.dump"
ok "it reads the offsite copy, not the local one"

step "4: a manifest that disagrees with the restored copy fails it, before migrate"
put_manifest() { rcl rcat "offsite-crypt:$name.manifest.json" < "$1"; }
jq '.tables.shifts += 1' "$work/files/manifest.json" > "$work/files/counts.json"
put_manifest "$work/files/counts.json"
fresh_scratch
refuses "a changed row count names the table" "shifts has" \
    backup_run -e PGHOST=scratch restore-test.sh
revision=$(jq -r .alembic_revision "$work/files/manifest.json")
jq '.alembic_revision = "000_not_this_one"' "$work/files/manifest.json" > "$work/files/revision.json"
put_manifest "$work/files/revision.json"
fresh_scratch
refuses "a changed revision names both" "the restored revision is $revision, the manifest's is 000_not_this_one" \
    backup_run -e PGHOST=scratch restore-test.sh
put_manifest "$work/files/manifest.json"

step "5: a corrupted offsite dump fails on the checksum"
rcl cat "offsite-crypt:$name.dump" > "$work/files/good.dump"
cp "$work/files/good.dump" "$work/files/bad.dump"
printf 'X' | dd of="$work/files/bad.dump" bs=1 seek=200 conv=notrunc 2> /dev/null
rcl rcat "offsite-crypt:$name.dump" < "$work/files/bad.dump"
fresh_scratch
refuses "a corrupted dump" "checksum mismatch" backup_run -e PGHOST=scratch restore-test.sh
rcl rcat "offsite-crypt:$name.dump" < "$work/files/good.dump"

step "6: an unreachable offsite remote fails the backup, and the local copy is kept"
later=$(($(date -u +%s) + 120))
refuses "offsite unreachable" "not stored in: offsite-crypt" backup_run \
    -e HDS_NOW="$later" -e RCLONE_CONFIG_OFFSITE_TYPE=sftp -e RCLONE_CONFIG_OFFSITE_HOST=127.0.0.1 \
    -e RCLONE_CONFIG_OFFSITE_PORT=1 -e RCLONE_CONFIG_OFFSITE_USER=nobody -e RCLONE_CONTIMEOUT=2s \
    -e RCLONE_RETRIES=1 -e RCLONE_LOW_LEVEL_RETRIES=1 backup.sh
local_only=$(rcl lsf local-crypt: | sed -n 's/\.manifest\.json$//p' | sort -r | head -n 1)
[ "$local_only" != "$name" ] || fail "no local copy of the second backup"
ok "the local copy $local_only exists"

step "6b: a dump that is not stored leaves no manifest: the manifest is written last"
# A transfer limit of 10 kB fails the dump (tens of kB) and lets the manifest (2 kB) through.
skipped=$(backup_run date -u -d "@$((later + 60))" +hds-%Y%m%dT%H%M%SZ)
refuses "a dump skipped by rclone" "not stored in: local-crypt offsite-crypt" backup_run \
    -e HDS_NOW="$((later + 60))" -e RCLONE_MAX_TRANSFER=10k backup.sh
for remote in local-crypt offsite-crypt; do
    if rcl lsf "$remote:" | grep -qF "$skipped.manifest.json"; then
        fail "$remote holds a manifest for $skipped, whose dump was never stored"
    fi
done
ok "no manifest for $skipped in either place"

step "7: pruning 31 days on keeps the newest valid backup in each place, and 8 Vault snapshots"
# A newer, broken backup on the machine: its dump does not match its manifest.
# GNU date, from the image: the runner may be a Mac.
broken=$(backup_run date -u -d "@$((later + 120))" +hds-%Y%m%dT%H%M%SZ)
rcl cat "local-crypt:$local_only.manifest.json" | jq --arg n "$broken" '.name = $n' \
    > "$work/files/broken.json"
rcl rcat "local-crypt:$broken.manifest.json" < "$work/files/broken.json"
printf 'not a dump\n' | rcl rcat "local-crypt:$broken.dump"
# An old backup whose manifest cannot be read: it must not stop the pruning, and it goes.
printf 'not json\n' | rcl rcat "local-crypt:hds-20261001T000000Z.manifest.json"
for i in 01 02 03 04 05 06 07 08 09 10; do
    printf 'age\n' | rcl rcat "local:/backups/vault/vault-202610${i}T000000Z.snap.age"
    printf 'age\n' | rcl rcat "offsite:/offsite/hds/vault/vault-202610${i}T000000Z.snap.age"
done
backup_run -e HDS_NOW="$((later + 31 * 86400))" prune.sh || fail "prune.sh failed"
files=$(rcl lsf local-crypt: | sort | tr '\n' ' ')
[ "$files" = "$local_only.dump $local_only.manifest.json " ] || fail "local-crypt holds: $files"
files=$(rcl lsf offsite-crypt: | sort | tr '\n' ' ')
[ "$files" = "$name.dump $name.manifest.json " ] || fail "offsite-crypt holds: $files"
ok "each place keeps only its newest valid backup, though expired; the broken ones are gone"
for place in local:/backups/vault offsite:/offsite/hds/vault; do
    files=$(rcl lsf "$place" | sort | tr '\n' ' ')
    want=$(for i in 03 04 05 06 07 08 09 10; do printf 'vault-202610%sT000000Z.snap.age ' "$i"; done)
    [ "$files" = "$want" ] || fail "$place holds: $files"
done
ok "the newest 8 Vault snapshots are kept on each side"

step "7b: when no backup in a place verifies, pruning there deletes nothing and fails"
# Every dump on Drive is now corrupt (one backup there, all past expiry): with nothing to keep,
# deleting the expired ones would leave Drive empty (review R90).
before_prune=$(rcl lsf offsite-crypt: | sort | tr '\n' ' ')
rcl rcat "offsite-crypt:$name.dump" < "$work/files/bad.dump"
refuses "pruning with no valid backup" "offsite-crypt: no backup verifies; nothing is pruned there" \
    backup_run -e HDS_NOW="$((later + 31 * 86400))" prune.sh
[ "$(rcl lsf offsite-crypt: | sort | tr '\n' ' ')" = "$before_prune" ] \
    || fail "offsite-crypt lost files: $(rcl lsf offsite-crypt: | tr '\n' ' ')"
ok "offsite-crypt still holds $before_prune"
rcl rcat "offsite-crypt:$name.dump" < "$work/files/good.dump"

step "8: restore.sh refuses a database with data unless CONFIRM=replace"
refuses "a database with data" "holds data; nothing was changed" backup_run restore.sh "$name"
[ "$(sql "$SHIFTS")" = "$shifts_at_backup" ] || fail "the refused restore changed the shifts"
sql "DELETE FROM shifts WHERE id = (SELECT min(id::text)::uuid FROM shifts)" > /dev/null
[ "$(sql "$SHIFTS")" != "$shifts_at_backup" ] || fail "the shifts did not change"
backup_run -e CONFIRM=replace restore.sh "$name"
dc run --rm -T api python -m hds.scripts.migrate > /dev/null
[ "$(sql "$SHIFTS")" = "$shifts_at_backup" ] || fail "the restored shifts differ from the backup's"
ok "with CONFIRM=replace it restores, and the shifts match the backup"

printf '\nbackup and restore checks: all passed\n'
