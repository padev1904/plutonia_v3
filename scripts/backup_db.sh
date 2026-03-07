#!/usr/bin/env bash
set -euo pipefail

BACKUP_DIR="/opt/plutonia/ai-news-portal/backups"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
mkdir -p "$BACKUP_DIR"

docker exec plutonia-postgres-1 pg_dump -U python plutonia | gzip > "$BACKUP_DIR/plutonia_$TIMESTAMP.sql.gz"
find "$BACKUP_DIR" -name "*.sql.gz" -mtime +7 -delete

echo "Backup completed: plutonia_$TIMESTAMP.sql.gz"
