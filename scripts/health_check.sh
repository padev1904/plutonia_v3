#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT_DIR}/scripts/lib_paths.sh"

if [ -f "${ROOT_DIR}/.env" ]; then
  set -a
  # shellcheck disable=SC1091
  source "${ROOT_DIR}/.env"
  set +a
fi

echo "===== $(date -Is) ====="
echo "--- Services ---"
for svc in plutonia-portal plutonia-monitor nginx postgresql ollama searxng; do
  if command -v systemctl >/dev/null 2>&1; then
    systemctl is-active "${svc}" 2>/dev/null || true
  fi
done
echo "--- GPU ---"
nvidia-smi --query-gpu=memory.used,memory.total,utilization.gpu --format=csv,noheader || true
echo "--- Disk ---"
df -h "${ROOT_DIR}" || true
echo "--- HTTP ---"
curl -fsS http://127.0.0.1:8000/healthz || true
curl -fsS http://127.0.0.1:8001/healthz || true
echo "--- DB Size (plutonia) ---"
PGPASSWORD="${POSTGRES_PASSWORD:-}" psql \
  -h "${POSTGRES_HOST:-127.0.0.1}" \
  -p "${POSTGRES_PORT:-5432}" \
  -U "${POSTGRES_USER:-python}" \
  -d "${POSTGRES_DB:-plutonia}" \
  -c "SELECT pg_size_pretty(pg_database_size('${POSTGRES_DB:-plutonia}'));"
echo "--- Article Count ---"
PGPASSWORD="${POSTGRES_PASSWORD:-}" psql \
  -h "${POSTGRES_HOST:-127.0.0.1}" \
  -p "${POSTGRES_PORT:-5432}" \
  -U "${POSTGRES_USER:-python}" \
  -d "${POSTGRES_DB:-plutonia}" \
  -c "SELECT COUNT(*) FROM news_article;" || true
