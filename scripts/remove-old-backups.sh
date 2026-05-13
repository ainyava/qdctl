#!/usr/bin/env bash
set -euo pipefail

# Required env vars:
#   RCLONE_REMOTE        e.g. s3:my-bucket/qdrant-backups  (any rclone remote:path)
# Optional:
#   KEEP_BACKUPS_COUNT   number of most-recent backups to keep (default: 3)

: "${RCLONE_REMOTE:?RCLONE_REMOTE is required}"
KEEP_BACKUPS_COUNT="${KEEP_BACKUPS_COUNT:-3}"

echo "==> Checking remote path ${RCLONE_REMOTE}"
if ! rclone lsl "${RCLONE_REMOTE}" > /dev/null 2>&1; then
    echo "ERROR: Cannot access ${RCLONE_REMOTE}" >&2
    exit 1
fi

echo "==> Keeping ${KEEP_BACKUPS_COUNT} most-recent backups, removing the rest from ${RCLONE_REMOTE}"
rclone lsl "${RCLONE_REMOTE}" |
    sort -rk 2,3 |
    awk '{ $1=$2=$3=""; print substr($0, 4) }' |
    tail -n +"$((KEEP_BACKUPS_COUNT + 1))" |
    xargs -t --no-run-if-empty -I @ rclone deletefile "${RCLONE_REMOTE}/@"

echo "==> Done"
