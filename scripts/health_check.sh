#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "===== $(date -Is) ====="
echo "--- Docker ---"
docker compose -f "${ROOT_DIR}/docker-compose.yml" ps
echo "--- GPU ---"
nvidia-smi --query-gpu=memory.used,memory.total,utilization.gpu --format=csv,noheader || true
echo "--- Disk ---"
df -h "${ROOT_DIR}" || true
echo "--- DB Size (plutonia) ---"
docker exec plutonia-postgres-1 psql -U python -d plutonia -c "SELECT pg_size_pretty(pg_database_size('plutonia'));"
echo "--- Article Count ---"
docker exec plutonia-postgres-1 psql -U python -d plutonia -c "SELECT COUNT(*) FROM news_article;" || true
