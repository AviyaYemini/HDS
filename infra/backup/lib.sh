# shellcheck shell=sh
# Shared by the backup scripts (specs/006-deploy-operations research R11); sourced, never run.
# POSIX sh. Nothing here prints a secret: passwords reach rclone through a private, rendered
# config file, and never through a command line.
#
# The environment (from the chart, and the hds-backup Secret):
#   PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE  the database, as libpq reads them
#   HDS_CRYPT_PASSWORD, HDS_CRYPT_SALT              the crypt remotes' password and salt
#   HDS_DRIVE_TOKEN, HDS_DRIVE_FOLDER_ID            the Drive remote (HDS_DRIVE_CLIENT_ID and
#                                                   HDS_DRIVE_CLIENT_SECRET when not rclone's own)
#   HDS_BACKUP_DIR      the backup volume (default /backups)
#   HDS_OFFSITE_PATH    the folder on the off-machine remote (default hds)
#   HDS_RETENTION_DAYS  how long a backup is kept (default 30)
#   HDS_NOW             seconds since the epoch, standing in for now (tests only)

HDS_LIB_DIR=$(cd "$(dirname "$0")" && pwd)
: "${HDS_BACKUP_DIR:=/backups}" "${HDS_OFFSITE_PATH:=hds}" "${HDS_RETENTION_DAYS:=30}"
export HDS_BACKUP_DIR HDS_OFFSITE_PATH
# Fail in seconds, not minutes, when a remote is down; a daily job needs no long retries.
: "${RCLONE_CONTIMEOUT:=15s}" "${RCLONE_RETRIES:=3}" "${RCLONE_LOW_LEVEL_RETRIES:=3}"
export RCLONE_CONTIMEOUT RCLONE_RETRIES RCLONE_LOW_LEVEL_RETRIES

log() {
    printf '%s %s: %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$(basename "$0")" "$*" >&2
}

fail() {
    log "FAILED: $*"
    exit 1
}

now() {
    printf '%s\n' "${HDS_NOW:-$(date -u +%s)}"
}

# A temporary folder only this run can read, removed when the script exits.
work_dir() {
    umask 077
    HDS_WORK=$(mktemp -d)
    trap 'rm -rf "$HDS_WORK"' EXIT
}

# Render rclone.conf.tmpl into the work folder: each ${NAME} from the environment, the crypt
# passwords obscured first. awk reads the values from ENVIRON, so none is ever an argument.
rclone_config() {
    [ -n "${HDS_CRYPT_PASSWORD:-}" ] || fail "HDS_CRYPT_PASSWORD is not set"
    [ -n "${HDS_CRYPT_SALT:-}" ] || fail "HDS_CRYPT_SALT is not set"
    HDS_CRYPT_PASSWORD_OBSCURED=$(printf '%s' "$HDS_CRYPT_PASSWORD" | rclone obscure -)
    HDS_CRYPT_SALT_OBSCURED=$(printf '%s' "$HDS_CRYPT_SALT" | rclone obscure -)
    export HDS_CRYPT_PASSWORD_OBSCURED HDS_CRYPT_SALT_OBSCURED
    awk '{
        while (match($0, /\$\{[A-Z0-9_]+\}/)) {
            name = substr($0, RSTART + 2, RLENGTH - 3)
            $0 = substr($0, 1, RSTART - 1) ENVIRON[name] substr($0, RSTART + RLENGTH)
        }
        print
    }' "$HDS_LIB_DIR/rclone.conf.tmpl" > "$HDS_WORK/rclone.conf"
    RCLONE_CONFIG="$HDS_WORK/rclone.conf"
    export RCLONE_CONFIG
}

# The database's migration revision and every public table's row count, as one JSON object, read
# in the caller's transaction: {"revision": "...", "tables": {"name": count, ...}}.
# shellcheck disable=SC2034 # used by the scripts that source this file
STATE_SQL="SELECT json_build_object(
    'revision', (SELECT string_agg(version_num, ',' ORDER BY version_num) FROM alembic_version),
    'tables', (SELECT coalesce(json_object_agg(c.relname, (xpath('/row/n/text()', query_to_xml(
        format('SELECT count(*) AS n FROM %I.%I', n.nspname, c.relname), false, true, '')))[1]
        ::text::bigint ORDER BY c.relname), '{}'::json)
      FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = 'public' AND c.relkind = 'r'))"

# The newest backup name (hds-<timestamp>) with a manifest on a remote, or nothing. A remote that
# cannot be listed stops the script: it is never taken for an empty one.
newest_backup() {
    listing=$(rclone lsf "$1:" --files-only --include 'hds-*.manifest.json') \
        || fail "$1: the backups could not be listed"
    printf '%s\n' "$listing" | sed -n 's/\.manifest\.json$//p' | sort -r | head -n 1
}

# Download a backup and check its dump against its manifest: fetch_verified REMOTE NAME.
fetch_verified() {
    rclone copyto "$1:$2.manifest.json" "$HDS_WORK/$2.manifest.json" \
        || fail "$2: the manifest could not be read from $1"
    rclone copyto "$1:$2.dump" "$HDS_WORK/$2.dump" || fail "$2: the dump could not be read from $1"
    want=$(jq -r .sha256 "$HDS_WORK/$2.manifest.json")
    got=$(sha256sum "$HDS_WORK/$2.dump" | cut -d ' ' -f 1)
    [ "$want" = "$got" ] || fail "$2: checksum mismatch: the dump is $got, the manifest says $want"
    log "$2: downloaded from $1; the checksum matches"
}

# Whether the database holds data: any public table other than alembic_version with a row. A
# query that fails stops the script: it never counts as an empty database.
HAS_DATA_SQL="SELECT coalesce(bool_or((xpath('/row/n/text()', query_to_xml(
        format('SELECT count(*) AS n FROM %I.%I', n.nspname, c.relname), false, true, '')))[1]
        ::text::bigint > 0), false)
      FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
      WHERE n.nspname = 'public' AND c.relkind = 'r' AND c.relname <> 'alembic_version'"

has_data() {
    answer=$(psql -X -q -At -v ON_ERROR_STOP=1 -c "$HAS_DATA_SQL") \
        || fail "the target database could not be read"
    [ "$answer" = t ]
}
