#!/bin/sh
# rclone with the backup remotes (specs/006-deploy-operations research R11): the config is rendered
# from the environment for this one call, as the backup scripts do. For the runbook and the tests:
#   hds-rclone.sh lsf offsite-crypt:     list the database backups on Drive
#   hds-rclone.sh lsf local:/backups/vault
set -eu
# shellcheck source-path=SCRIPTDIR source=lib.sh
. "$(dirname "$0")/lib.sh"

work_dir
rclone_config
rclone "$@"
