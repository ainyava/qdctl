#!/usr/bin/env bash
set -euo pipefail

# Required env vars:
#   QDRANT_URL        e.g. http://qdrant:6334
#   RCLONE_REMOTE     e.g. s3:my-bucket/qdrant-backups  (any rclone remote:path)
# Optional:
#   QDRANT_API_KEY    API key for Qdrant
#   QDCTL_BATCH_SIZE  points per scroll request (default: 1000)

: "${QDRANT_URL:?QDRANT_URL is required}"
: "${RCLONE_REMOTE:?RCLONE_REMOTE is required}"

DATE=$(date -u +%Y-%m-%d)
BACKUP_DIR="/tmp/qdrant-backup-${DATE}"
ARCHIVE="/tmp/qdrant-backup-${DATE}.tar.gz"

echo "==> Backing up Qdrant at ${QDRANT_URL}"

QDCTL_ARGS=(--url "${QDRANT_URL}" --output-dir "${BACKUP_DIR}")
[[ -n "${QDRANT_API_KEY:-}" ]] && QDCTL_ARGS+=(--api-key "${QDRANT_API_KEY}")
[[ -n "${QDCTL_BATCH_SIZE:-}" ]] && QDCTL_ARGS+=(--batch-size "${QDCTL_BATCH_SIZE}")

qdctl backup "${QDCTL_ARGS[@]}"

echo "==> Creating archive ${ARCHIVE}"
tar -czf "${ARCHIVE}" -C /tmp "qdrant-backup-${DATE}"

echo "==> Uploading to ${RCLONE_REMOTE}"
rclone copy "${ARCHIVE}" "${RCLONE_REMOTE}" --progress

echo "==> Done. Uploaded $(basename "${ARCHIVE}") to ${RCLONE_REMOTE}"

rm -rf "${BACKUP_DIR}" "${ARCHIVE}"
