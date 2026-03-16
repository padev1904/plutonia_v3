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

PYTHON_BIN="${PYTHON_BIN:-${ROOT_DIR}/.venv/bin/python}"
MANAGE_PY="${ROOT_DIR}/portal/manage.py"
PORTAL_SERVICE="${PORTAL_SERVICE:-plutonia-portal}"
MONITOR_SERVICE="${MONITOR_SERVICE:-plutonia-monitor}"

if [ ! -x "${PYTHON_BIN}" ]; then
  echo "[clean-restart] missing python interpreter: ${PYTHON_BIN}" >&2
  exit 1
fi

if [ ! -f "${MANAGE_PY}" ]; then
  echo "[clean-restart] missing manage.py: ${MANAGE_PY}" >&2
  exit 1
fi

service_exists() {
  command -v systemctl >/dev/null 2>&1 && systemctl list-unit-files "$1.service" >/dev/null 2>&1
}

service_stop() {
  if service_exists "$1"; then
    systemctl stop "$1"
  fi
}

service_start() {
  if service_exists "$1"; then
    systemctl start "$1"
  fi
}

echo "[clean-restart] ensuring runtime directories..."
mkdir -p "${PLUTONIA_REVIEW_DIR}" "${PLUTONIA_LOG_DIR}" "${PLUTONIA_RUN_DIR}" "${PLUTONIA_BACKUP_DIR}" "${PLUTONIA_STATIC_DIR}"

echo "[clean-restart] waiting for portal checks..."
portal_ready=0
for i in $(seq 1 40); do
  if "${PYTHON_BIN}" "${MANAGE_PY}" check >/dev/null 2>&1; then
    portal_ready=1
    break
  fi
  sleep 1
done
test "${portal_ready}" -eq 1

echo "[clean-restart] preflight snapshot..."
"${PYTHON_BIN}" "${MANAGE_PY}" shell -c "
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

echo "[clean-restart] stopping monitor service..."
service_stop "${MONITOR_SERVICE}"

echo "[clean-restart] cleaning transient rows (preserving public published content)..."
"${PYTHON_BIN}" "${MANAGE_PY}" shell -c "
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
find "${PLUTONIA_REVIEW_DIR}" -maxdepth 1 -type f \( -name 'newsletter_*' -o -name 'resource_*' \) -delete

remaining_review_files="$(find "${PLUTONIA_REVIEW_DIR}" -maxdepth 1 -type f \( -name 'newsletter_*' -o -name 'resource_*' \) | wc -l)"
echo "[clean-restart] review artifacts remaining=${remaining_review_files}"
test "${remaining_review_files}" -eq 0

echo "[clean-restart] starting monitor service..."
service_start "${MONITOR_SERVICE}"

echo "[clean-restart] post-check: db counters..."
"${PYTHON_BIN}" "${MANAGE_PY}" shell -c "
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
health_file="${PLUTONIA_RUN_DIR}/review_health.json"
for i in $(seq 1 20); do
  if curl -fsS -m 3 http://127.0.0.1:8001/healthz >"${health_file}" 2>/dev/null; then
    cat "${health_file}"
    health_ok=1
    break
  fi
  sleep 1
done
test "${health_ok}" -eq 1

echo "[clean-restart] done."
