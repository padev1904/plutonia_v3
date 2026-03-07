#!/usr/bin/env bash
set -euo pipefail

echo "===== $(date -Is) ====="
echo "--- Docker ---"
docker compose -f /opt/plutonia/ai-news-portal/docker-compose.yml ps
echo "--- GPU ---"
nvidia-smi --query-gpu=memory.used,memory.total,utilization.gpu --format=csv,noheader || true
echo "--- Disk ---"
df -h /opt/plutonia/ai-news-portal || true
echo "--- DB Size (plutonia) ---"
docker exec plutonia-postgres-1 psql -U python -d plutonia -c "SELECT pg_size_pretty(pg_database_size('plutonia'));"
echo "--- Article Count ---"
docker exec plutonia-postgres-1 psql -U python -d plutonia -c "SELECT COUNT(*) FROM news_article;" || true
