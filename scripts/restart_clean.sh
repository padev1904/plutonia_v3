#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"
COMPOSE_CMD=(docker compose -p plutonia-ainews -f "${ROOT_DIR}/docker-compose.yml")
ROOT_COMPOSE_CMD=(docker compose -p plutonia -f /opt/plutonia/docker-compose.yml)
LEGACY_PROJECT_CMD=(docker compose -p ai-news-portal -f "${ROOT_DIR}/docker-compose.yml")

echo "[clean-restart] removing legacy ai-news-portal project containers (name-collision guard)..."
"${LEGACY_PROJECT_CMD[@]}" down --remove-orphans >/dev/null 2>&1 || true
if [ -f /home/python/ai-news-portal/docker-compose.yml ]; then
  docker compose -p ai-news-portal -f /home/python/ai-news-portal/docker-compose.yml down --remove-orphans >/dev/null 2>&1 || true
fi
for cname in ainews-portal ainews-nginx ainews-gmail-monitor ainews-cloudflared; do
  docker rm -f "${cname}" >/dev/null 2>&1 || true
done

echo "[clean-restart] ensuring core compose services and removing orphans..."
"${COMPOSE_CMD[@]}" up -d --remove-orphans portal nginx cloudflared >/dev/null

echo "[clean-restart] waiting for portal service..."
portal_ready=0
for i in $(seq 1 40); do
  if "${COMPOSE_CMD[@]}" exec -T portal python manage.py check >/dev/null 2>&1; then
    portal_ready=1
    break
  fi
  sleep 1
done
test "${portal_ready}" -eq 1

echo "[clean-restart] preflight snapshot..."
"${COMPOSE_CMD[@]}" exec -T portal python manage.py shell -c "
from news.models import Newsletter, Article, ProcessingLog, Resource, Category
print({
  'newsletters': Newsletter.objects.count(),
  'newsletters_completed': Newsletter.objects.filter(status='completed').count(),
  'newsletters_deleted_post_published': Newsletter.objects.filter(status='eliminada_pos_publicada').count(),
  'newsletters_transient': Newsletter.objects.exclude(status__in=['completed', 'eliminada_pos_publicada']).count(),
  'articles': Article.objects.count(),
  'articles_public': Article.objects.filter(is_review_approved=True, editorial_status='approved').count(),
  'articles_transient': Article.objects.exclude(is_review_approved=True, editorial_status='approved').count(),
  'processing_logs': ProcessingLog.objects.count(),
  'resources': Resource.objects.count(),
  'resources_public': Resource.objects.filter(review_status='approved', is_active=True).count(),
  'resources_transient': Resource.objects.exclude(review_status='approved', is_active=True).count(),
  'categories': Category.objects.count(),
  'review_status_count': Newsletter.objects.filter(status='review').count(),
})
"

echo "[clean-restart] stopping gmail-monitor..."
"${COMPOSE_CMD[@]}" stop gmail-monitor

echo "[clean-restart] cleaning transient rows (preserving public published content)..."
"${COMPOSE_CMD[@]}" exec -T portal python manage.py shell -c "
from django.db import connection
from django.utils import timezone
from news.models import Newsletter, Article, ProcessingLog, Resource, Category

public_articles_qs = Article.objects.filter(is_review_approved=True, editorial_status='approved')
preserved_article_ids = list(public_articles_qs.values_list('id', flat=True))
preserved_newsletter_ids = set(public_articles_qs.values_list('newsletter_id', flat=True).distinct())
deleted_post_publish_newsletter_ids = set(
    Newsletter.objects.filter(status='eliminada_pos_publicada').values_list('id', flat=True)
)
preserved_newsletter_ids |= deleted_post_publish_newsletter_ids

public_resources_qs = Resource.objects.filter(review_status='approved', is_active=True)
preserved_resource_ids = list(public_resources_qs.values_list('id', flat=True))

deleted_articles = Article.objects.exclude(id__in=preserved_article_ids).delete()[0]
deleted_newsletters = Newsletter.objects.exclude(id__in=preserved_newsletter_ids).delete()[0]
deleted_logs = ProcessingLog.objects.all().delete()[0]
deleted_resources = Resource.objects.exclude(id__in=preserved_resource_ids).delete()[0]

now = timezone.now()
for nl in Newsletter.objects.filter(id__in=preserved_newsletter_ids).only('id', 'status', 'processed_at', 'error_message', 'news_count'):
    public_count = Article.objects.filter(newsletter_id=nl.id, is_review_approved=True, editorial_status='approved').count()
    is_deleted_post_publish = nl.id in deleted_post_publish_newsletter_ids
    target_status = 'eliminada_pos_publicada' if is_deleted_post_publish else 'completed'
    update_fields = []
    if nl.status != target_status:
        nl.status = target_status
        update_fields.append('status')
    if nl.processed_at is None:
        nl.processed_at = now
        update_fields.append('processed_at')
    if nl.error_message:
        nl.error_message = ''
        update_fields.append('error_message')
    if nl.news_count != public_count:
        nl.news_count = public_count
        update_fields.append('news_count')
    if update_fields:
        nl.save(update_fields=update_fields)

deleted_orphan_categories = Category.objects.filter(articles__isnull=True).delete()[0]

with connection.cursor() as c:
    for table in ['news_newsletter', 'news_article', 'news_processinglog', 'news_resource', 'news_category']:
        c.execute(f'SELECT MAX(id) FROM {table}')
        max_id = c.fetchone()[0]
        c.execute(
            f\"SELECT setval(pg_get_serial_sequence('{table}','id'), %s, %s)\",
            [max_id if max_id is not None else 1, bool(max_id)],
        )

print({
  'status':'ok',
  'cleanup':'selective',
  'preserved': {
    'newsletters': len(preserved_newsletter_ids),
    'newsletters_deleted_post_published': len(deleted_post_publish_newsletter_ids),
    'articles': len(preserved_article_ids),
    'resources': len(preserved_resource_ids),
  },
  'deleted': {
    'newsletters': deleted_newsletters,
    'articles': deleted_articles,
    'processing_logs': deleted_logs,
    'resources': deleted_resources,
    'orphan_categories': deleted_orphan_categories,
  },
})
"

echo "[clean-restart] clearing review artifacts..."
find review -maxdepth 1 -type f \( -name 'newsletter_*' -o -name 'resource_*' \) -delete

remaining_review_files="$(find review -maxdepth 1 -type f \( -name 'newsletter_*' -o -name 'resource_*' \) | wc -l)"
echo "[clean-restart] review artifacts remaining=${remaining_review_files}"
test "${remaining_review_files}" -eq 0

echo "[clean-restart] rotating OpenClaw main sessions..."
if docker ps -a --format '{{.Names}}' | grep -q '^plutonia-openclaw$'; then
  docker exec plutonia-openclaw sh -lc '
set -e
d="/root/.openclaw/agents/main/sessions"
mkdir -p "${d}/archive"
ts="$(date -u +%Y%m%dT%H%M%SZ)"
for f in "${d}"/*.jsonl; do
  [ -f "${f}" ] || continue
  mv "${f}" "${d}/archive/$(basename "${f}").${ts}"
done
printf "{}\n" > "${d}/sessions.json"
'
  docker restart plutonia-openclaw >/dev/null
  session_jsonl_count="$(docker exec plutonia-openclaw sh -lc 'find /root/.openclaw/agents/main/sessions -maxdepth 1 -type f -name \"*.jsonl\" | wc -l')"
  echo "[clean-restart] openclaw active session files=${session_jsonl_count}"
  test "${session_jsonl_count}" -eq 0

  echo "[clean-restart] validating OpenClaw review hard-guard marker..."
  hardguard_hits="$(docker exec plutonia-openclaw sh -lc 'grep -R "__PLUTONIA_REVIEW_NO_REPLY_GUARD_V6__" -n /usr/local/lib/node_modules/openclaw/dist/reply-*.js /usr/local/lib/node_modules/openclaw/dist/plugin-sdk/reply-*.js | wc -l')"
  if [ "${hardguard_hits}" -lt 2 ]; then
    echo "[clean-restart] hard-guard marker V6 missing -> rebuilding openclaw image..."
    "${ROOT_COMPOSE_CMD[@]}" build openclaw
    "${ROOT_COMPOSE_CMD[@]}" up -d --no-deps --force-recreate openclaw
    hardguard_hits="$(docker exec plutonia-openclaw sh -lc 'grep -R "__PLUTONIA_REVIEW_NO_REPLY_GUARD_V6__" -n /usr/local/lib/node_modules/openclaw/dist/reply-*.js /usr/local/lib/node_modules/openclaw/dist/plugin-sdk/reply-*.js | wc -l')"
  fi
  echo "[clean-restart] openclaw hard-guard marker hits=${hardguard_hits}"
  test "${hardguard_hits}" -ge 2
fi

echo "[clean-restart] starting gmail-monitor..."
# Recreate from base compose to avoid inheriting debug container runtime
# (e.g., DEBUGPY_WAIT_FOR_CLIENT=true from docker-compose.debug.yml sessions).
"${COMPOSE_CMD[@]}" up -d --no-deps --force-recreate gmail-monitor

echo "[clean-restart] post-check: db counters..."
"${COMPOSE_CMD[@]}" exec -T portal python manage.py shell -c "
from news.models import Newsletter, Article, ProcessingLog, Resource, Category
state = {
  'newsletters_total': Newsletter.objects.count(),
  'newsletters_pending': Newsletter.objects.filter(status='pending').count(),
  'newsletters_processing': Newsletter.objects.filter(status='processing').count(),
  'newsletters_review': Newsletter.objects.filter(status='review').count(),
  'articles_total': Article.objects.count(),
  'articles_public': Article.objects.filter(is_review_approved=True, editorial_status='approved').count(),
  'articles_non_public': Article.objects.exclude(is_review_approved=True, editorial_status='approved').count(),
  'processing_logs': ProcessingLog.objects.count(),
  'resources_total': Resource.objects.count(),
  'resources_public': Resource.objects.filter(review_status='approved', is_active=True).count(),
  'resources_non_public': Resource.objects.exclude(review_status='approved', is_active=True).count(),
  'categories': Category.objects.count(),
}
print(state)
assert state['newsletters_pending'] == 0, state
assert state['newsletters_processing'] == 0, state
assert state['newsletters_review'] == 0, state
assert state['articles_non_public'] == 0, state
assert state['processing_logs'] == 0, state
assert state['resources_non_public'] == 0, state
"

echo "[clean-restart] post-check: review API health..."
health_ok=0
for i in $(seq 1 20); do
  if docker exec plutonia-openclaw curl -fsS -m 3 http://ainews-gmail-monitor:8001/healthz >/tmp/review_health.json 2>/dev/null; then
    cat /tmp/review_health.json
    health_ok=1
    break
  fi
  sleep 1
done
test "${health_ok}" -eq 1

echo "[clean-restart] done."
