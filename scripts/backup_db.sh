#!/usr/bin/env bash
set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib_paths.sh"

if [ -f "${PLUTONIA_ROOT}/.env" ]; then
  set -a
  # shellcheck disable=SC1091
  source "${PLUTONIA_ROOT}/.env"
  set +a
fi

BACKUP_DIR="${BACKUP_DIR:-${PLUTONIA_BACKUP_DIR}}"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
mkdir -p "$BACKUP_DIR"

export PGPASSWORD="${POSTGRES_PASSWORD:-}"
pg_dump \
  -h "${POSTGRES_HOST:-127.0.0.1}" \
  -p "${POSTGRES_PORT:-5432}" \
  -U "${POSTGRES_USER:-python}" \
  "${POSTGRES_DB:-plutonia}" | gzip > "$BACKUP_DIR/plutonia_$TIMESTAMP.sql.gz"
find "$BACKUP_DIR" -name "*.sql.gz" -mtime +7 -delete

echo "Backup completed: plutonia_$TIMESTAMP.sql.gz"
