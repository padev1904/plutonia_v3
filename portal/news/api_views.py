import json
import logging
import re
from datetime import UTC
from functools import wraps

from django.conf import settings
from django.db.models.functions import Coalesce
from django.http import JsonResponse
from django.utils import timezone
from django.utils.dateparse import parse_datetime
from django.utils.text import slugify
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_GET, require_POST

from .models import Article, Category, Newsletter, ProcessingLog, Resource

logger = logging.getLogger("news")


def _clean_keyword(value: str) -> str:
    return " ".join(str(value).split()).strip()[:100]


def _parse_datetime_utc(value, *, default=None):
    raw = str(value or "").strip()
    if not raw:
        return default
    parsed = parse_datetime(raw)
    if parsed is None:
        return default
    if timezone.is_naive(parsed):
        parsed = timezone.make_aware(parsed, UTC)
    return parsed


def _parse_bool(value, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _resolve_article_body_from_payload(article_data: dict) -> str:
    body = str(article_data.get("article_body", "")).strip()
    if body:
        return body
    summary = str(article_data.get("summary", "")).strip()
    if summary:
        return summary
    return ""


def _resource_summary_fallback(*, title: str, section: str, category: str, subcategory: str) -> str:
    title_text = str(title or "").strip() or "Untitled resource"
    section_text = str(section or "").strip() or "n/a"
    category_text = str(category or "").strip() or "n/a"
    subcategory_text = str(subcategory or "").strip() or "n/a"
    return (
        f"{title_text}. "
        f"This resource is classified under {section_text} / {category_text} / {subcategory_text}. "
        "Full source details are available via the Open Source link."
    )


def _resource_article_body_fallback(*, title: str, summary: str, section: str, category: str, subcategory: str) -> str:
    title_text = str(title or "").strip() or "Untitled resource"
    section_text = str(section or "").strip() or "n/a"
    category_text = str(category or "").strip() or "n/a"
    subcategory_text = str(subcategory or "").strip() or "n/a"
    parts = [
        (
            f"{title_text} is cataloged under {section_text} / {category_text} / {subcategory_text} "
            "and is handled as a practical learning asset."
        ),
        (
            "From an editorial standpoint, the expected value is not just conceptual context but also "
            "actionable guidance that can be translated into implementation work."
        ),
        (
            "Readers should use the source as the primary reference to inspect scope, prerequisites, "
            "and the concrete methods, examples, or exercises covered by the material."
        ),
        (
            "This entry is structured to support decision-making before opening the source: what problem "
            "space it addresses, who benefits most, and where it fits in an AI learning path."
        ),
        (
            "If the source metadata is limited, details can evolve as additional public information becomes "
            "available, while preserving the same taxonomy placement."
        ),
    ]
    return "\n\n".join(parts)


def _resource_article_body_is_weak(body: str, summary: str) -> bool:
    value = str(body or "").strip()
    if not value:
        return True
    if value.strip() == str(summary or "").strip():
        return True
    if len(value) < 420:
        return True
    paragraphs = [p.strip() for p in re.split(r"\n\s*\n+", value) if p.strip()]
    if len(paragraphs) < 5:
        return True
    summary_norm = " ".join(str(summary or "").split()).strip().lower()
    value_norm = " ".join(value.split()).strip().lower()
    if summary_norm and value_norm.startswith(summary_norm) and (len(value_norm) - len(summary_norm) < 260):
        return True
    low = value.lower()
    weak_markers = (
        "access to all",
        "subscribe",
        "sign in",
        "cookie",
        "privacy policy",
    )
    return any(marker in low for marker in weak_markers)


def _resolve_category_objects(raw_categories) -> list[Category]:
    category_objects: list[Category] = []
    seen_keywords: set[str] = set()
    for cat_name in (raw_categories or [])[:10]:
        cat_name_clean = _clean_keyword(cat_name)
        if not cat_name_clean:
            continue
        category_key = cat_name_clean.casefold()
        if category_key in seen_keywords:
            continue
        seen_keywords.add(category_key)
        slug_base = slugify(cat_name_clean)[:100]
        if not slug_base:
            continue
        cat, created = Category.objects.get_or_create(slug=slug_base, defaults={"name": cat_name_clean})
        if not created and cat.name != cat_name_clean and cat.name.casefold() == cat_name_clean.casefold():
            cat.name = cat_name_clean
            cat.save(update_fields=["name"])
        category_objects.append(cat)
    return category_objects


def _refresh_category_counts() -> None:
    for cat in Category.objects.all():
        cat.news_count = cat.articles.filter(is_review_approved=True, editorial_status="approved").count()
        cat.save(update_fields=["news_count"])


def _refresh_newsletter_editorial_state(newsletter: Newsletter) -> None:
    approved_qs = Article.objects.filter(newsletter=newsletter, is_review_approved=True)
    pending_count = approved_qs.filter(editorial_status="pending").count()
    approved_count = approved_qs.filter(editorial_status="approved").count()

    newsletter.news_count = approved_count
    if pending_count > 0:
        newsletter.status = "review"
        newsletter.error_message = f"Editorial review pending for {pending_count} article(s)"
        newsletter.processed_at = None
    else:
        newsletter.status = "completed"
        newsletter.error_message = ""
        newsletter.processed_at = timezone.now()
    newsletter.save(update_fields=["status", "error_message", "processed_at", "news_count"])


def _article_editorial_payload(article: Article) -> dict:
    return {
        "id": article.id,
        "newsletter_id": article.newsletter_id,
        "draft_article_index": article.draft_article_index,
        "title": article.title,
        "summary": article.summary,
        "article_body": article.enrichment_context,
        "original_url": article.original_url,
        "editorial_status": article.editorial_status,
        "preview_token": str(article.preview_token),
        "preview_path": f"/preview/{article.preview_token}/",
        "preview_card_path": f"/preview/card/{article.preview_token}/",
        "published_at": article.published_at.isoformat(),
        "section": article.section,
        "category": article.category,
        "subcategory": article.subcategory,
        "categories": [c.name for c in article.categories.all().order_by("name")],
        "public_visible": bool(article.is_review_approved and article.editorial_status == "approved"),
        "telegram_triage_status": article.telegram_triage_status,
        "content_profile": article.content_profile,
    }


def _resource_payload(resource: Resource) -> dict:
    return {
        "id": resource.id,
        "title": resource.title,
        "summary": resource.summary,
        "article_body": resource.article_body,
        "description": resource.description,
        "resource_url": resource.resource_url,
        "image_url": resource.image_url,
        "section": resource.section,
        "category": resource.category,
        "subcategory": resource.subcategory,
        "is_featured": bool(resource.is_featured),
        "is_active": bool(resource.is_active),
        "review_status": resource.review_status,
        "review_requested_at": resource.review_requested_at.isoformat() if resource.review_requested_at else "",
        "review_notified_at": resource.review_notified_at.isoformat() if resource.review_notified_at else "",
        "review_decided_at": resource.review_decided_at.isoformat() if resource.review_decided_at else "",
        "review_comment": resource.review_comment,
        "preview_token": str(resource.preview_token),
        "preview_path": f"/resources/preview/{resource.preview_token}/",
        "source_published_at": resource.source_published_at.isoformat() if resource.source_published_at else "",
        "published_at": resource.published_at.isoformat(),
        "source_name": resource.source_name,
    }


def api_key_required(view_func):
    @wraps(view_func)
    def wrapper(request, *args, **kwargs):
        api_key = request.headers.get("X-API-Key", "")
        if api_key != settings.AGENT_API_KEY:
            return JsonResponse({"error": "Invalid API key"}, status=401)
        return view_func(request, *args, **kwargs)

    return wrapper


@csrf_exempt
@require_POST
@api_key_required
def register_newsletter(request):
    try:
        data = json.loads(request.body)
        now_value = timezone.now()
        received_at = _parse_datetime_utc(data.get("received_at", ""), default=now_value) or now_value
        original_sent_at = _parse_datetime_utc(data.get("original_sent_at", ""), default=None)

        defaults = {
            "gmail_uid": str(data.get("gmail_uid", "")).strip()[:50],
            "sender_name": str(data.get("sender_name", "Unknown Sender")).strip()[:255],
            "sender_email": str(data.get("sender_email", "unknown@example.com")).strip()[:254],
            "original_sender_name": str(data.get("original_sender_name", "")).strip()[:255],
            "original_sender_email": str(data.get("original_sender_email", "")).strip()[:255],
            "subject": str(data.get("subject", "(no subject)")).strip()[:500],
            "original_sent_at_raw": str(data.get("original_sent_at_raw", "")).strip()[:255],
            "original_sent_at": original_sent_at,
            "received_at": received_at,
            "raw_html": str(data.get("raw_html", "")),
            "status": "pending",
        }
        newsletter, created = Newsletter.objects.get_or_create(
            gmail_message_id=data["gmail_message_id"],
            defaults=defaults,
        )

        if not created:
            update_fields: list[str] = []
            for field_name in (
                "gmail_uid",
                "sender_name",
                "sender_email",
                "original_sender_name",
                "original_sender_email",
                "subject",
                "original_sent_at_raw",
            ):
                value = defaults[field_name]
                if value and getattr(newsletter, field_name) != value:
                    setattr(newsletter, field_name, value)
                    update_fields.append(field_name)
            if defaults["original_sent_at"] and newsletter.original_sent_at != defaults["original_sent_at"]:
                newsletter.original_sent_at = defaults["original_sent_at"]
                update_fields.append("original_sent_at")
            if defaults["raw_html"] and newsletter.raw_html != defaults["raw_html"]:
                newsletter.raw_html = defaults["raw_html"]
                update_fields.append("raw_html")
            if update_fields:
                newsletter.save(update_fields=sorted(set(update_fields)))
            return JsonResponse(
                {"status": "duplicate", "newsletter_id": newsletter.id, "message": "Newsletter already registered"},
                status=200,
            )

        return JsonResponse({"status": "created", "newsletter_id": newsletter.id}, status=201)
    except Exception as exc:
        logger.exception("Error registering newsletter")
        return JsonResponse({"error": str(exc)}, status=400)


@csrf_exempt
@require_POST
@api_key_required
def publish_articles(request):
    try:
        data = json.loads(request.body)
        newsletter = Newsletter.objects.get(id=data["newsletter_id"])
        mode = str(data.get("mode", "preview")).strip().lower()
        if mode not in {"preview", "public"}:
            return JsonResponse({"error": "Invalid mode. Use 'preview' or 'public'."}, status=400)

        created_articles = []
        created_payload = []
        keep_indices: set[int] = set()
        prune_missing = str(data.get("prune_missing", "true")).strip().lower() in {"1", "true", "yes", "on"}
        default_published_at = newsletter.received_at or timezone.now()
        now_value = timezone.now()
        for idx, article_data in enumerate(data.get("articles", []), start=1):
            if not isinstance(article_data, dict):
                continue
            category_objects = _resolve_category_objects(article_data.get("categories", []))

            published_at = default_published_at
            published_raw = str(article_data.get("published_at", "")).strip()
            if published_raw:
                parsed = parse_datetime(published_raw)
                if parsed is not None:
                    if timezone.is_naive(parsed):
                        parsed = timezone.make_aware(parsed, UTC)
                    published_at = parsed

            editorial_status = "approved" if mode == "public" else "pending"
            editorial_reviewed_at = now_value if mode == "public" else None
            draft_article_index = article_data.get("_article_index") or article_data.get("draft_article_index") or idx
            try:
                draft_article_index = int(draft_article_index)
            except Exception:
                draft_article_index = idx
            keep_indices.add(draft_article_index)

            defaults = {
                "title": article_data.get("title", "Untitled")[:500],
                "summary": article_data.get("summary", ""),
                "original_url": article_data.get("original_url", ""),
                "image_url": article_data.get("source_image_url", "") or article_data.get("image_url", ""),
                "section": str(article_data.get("section", "")).strip()[:120],
                "category": str(article_data.get("category", "")).strip()[:120],
                "subcategory": str(article_data.get("subcategory", "")).strip()[:120],
                "enrichment_context": _resolve_article_body_from_payload(article_data),
                "published_at": published_at,
                "is_review_approved": True,
                "review_approved_at": now_value,
                "editorial_status": editorial_status,
                "editorial_reviewed_at": editorial_reviewed_at,
                "editorial_comment": str(article_data.get("editorial_comment", "")).strip(),
            }
            article, _created = Article.objects.update_or_create(
                newsletter=newsletter,
                draft_article_index=draft_article_index,
                defaults=defaults,
            )
            if category_objects:
                article.categories.set(category_objects)
            else:
                article.categories.clear()
            created_articles.append(article.id)
            created_payload.append(_article_editorial_payload(article))

        if prune_missing and keep_indices:
            stale_qs = Article.objects.filter(newsletter=newsletter, is_review_approved=True).exclude(
                draft_article_index__in=sorted(keep_indices)
            )
            stale_count = stale_qs.count()
            if stale_count:
                stale_qs.delete()
                logger.info(
                    "pruned stale newsletter articles newsletter_id=%s removed=%s kept=%s",
                    newsletter.id,
                    stale_count,
                    sorted(keep_indices),
                )

        if mode == "public":
            newsletter.status = "completed"
            newsletter.processed_at = now_value
            newsletter.error_message = ""
            newsletter.news_count = len(created_articles)
            newsletter.save(update_fields=["status", "processed_at", "error_message", "news_count"])
        else:
            newsletter.status = "review"
            newsletter.processed_at = None
            newsletter.error_message = f"Editorial review pending for {len(created_articles)} article(s)"
            newsletter.news_count = 0
            newsletter.save(update_fields=["status", "processed_at", "error_message", "news_count"])

        _refresh_category_counts()

        status_value = "published" if mode == "public" else "editorial_review"
        return JsonResponse(
            {
                "status": status_value,
                "mode": mode,
                "newsletter_id": newsletter.id,
                "articles_created": len(created_articles),
                "article_ids": created_articles,
                "articles": created_payload,
            },
            status=201,
        )

    except Newsletter.DoesNotExist:
        return JsonResponse({"error": "Newsletter not found"}, status=404)
    except Exception as exc:
        logger.exception("Error publishing articles")
        return JsonResponse({"error": str(exc)}, status=400)


@csrf_exempt
@require_POST
@api_key_required
def update_newsletter_status(request):
    try:
        data = json.loads(request.body)
        newsletter = Newsletter.objects.get(id=data["newsletter_id"])
        status_value = str(data["status"]).strip().lower()
        newsletter.status = status_value
        update_fields = ["status"]
        if "error_message" in data:
            newsletter.error_message = str(data.get("error_message", ""))
            update_fields.append("error_message")
        if status_value == "completed":
            newsletter.processed_at = timezone.now()
            update_fields.append("processed_at")
        elif status_value == "eliminada_pos_publicada":
            newsletter.processed_at = timezone.now()
            update_fields.append("processed_at")
        elif status_value in {"pending", "processing", "review", "error"}:
            newsletter.processed_at = None
            update_fields.append("processed_at")
        newsletter.save(update_fields=sorted(set(update_fields)))
        return JsonResponse({"status": "updated", "newsletter_id": newsletter.id})
    except Newsletter.DoesNotExist:
        return JsonResponse({"error": "Newsletter not found"}, status=404)
    except Exception as exc:
        return JsonResponse({"error": str(exc)}, status=400)


@require_GET
@api_key_required
def get_ingest_cursor(_request):
    max_uid = 0
    for raw_uid in Newsletter.objects.values_list("gmail_uid", flat=True).iterator():
        try:
            uid_value = int(str(raw_uid).strip())
        except Exception:
            continue
        if uid_value > max_uid:
            max_uid = uid_value
    return JsonResponse({"max_gmail_uid": max_uid, "newsletters_total": Newsletter.objects.count()})


@require_GET
@api_key_required
def get_pending_newsletters(request):
    status_value = str(request.GET.get("status", "pending")).strip().lower() or "pending"
    allowed_status = {"pending", "review", "processing"}
    if status_value not in allowed_status:
        return JsonResponse({"error": f"invalid status: {status_value}"}, status=400)
    limit_raw = str(request.GET.get("limit", "50")).strip()
    try:
        limit = max(1, min(200, int(limit_raw)))
    except Exception:
        limit = 50

    base_qs = Newsletter.objects.filter(status=status_value)
    pending_qs = (
        base_qs.annotate(sort_original_sent_at=Coalesce("original_sent_at", "received_at"))
        .order_by("sort_original_sent_at", "received_at", "id")
        .values(
            "id",
            "gmail_uid",
            "gmail_message_id",
            "sender_name",
            "sender_email",
            "subject",
            "received_at",
            "original_sent_at",
            "original_sent_at_raw",
            "original_sender_name",
            "original_sender_email",
            "status",
        )[:limit]
    )

    return JsonResponse(
        {
            "status": status_value,
            "pending_count": base_qs.count(),
            "newsletters": list(pending_qs),
        }
    )


@require_GET
@api_key_required
def get_newsletter_raw(_request, newsletter_id: int):
    try:
        newsletter = Newsletter.objects.get(id=int(newsletter_id))
    except Newsletter.DoesNotExist:
        return JsonResponse({"error": "Newsletter not found"}, status=404)

    return JsonResponse(
        {
            "id": newsletter.id,
            "gmail_uid": newsletter.gmail_uid,
            "gmail_message_id": newsletter.gmail_message_id,
            "sender_name": newsletter.sender_name,
            "sender_email": newsletter.sender_email,
            "subject": newsletter.subject,
            "received_at": newsletter.received_at.isoformat(),
            "original_sender_name": newsletter.original_sender_name,
            "original_sender_email": newsletter.original_sender_email,
            "original_sent_at_raw": newsletter.original_sent_at_raw,
            "original_sent_at": newsletter.original_sent_at.isoformat() if newsletter.original_sent_at else "",
            "status": newsletter.status,
            "raw_html": newsletter.raw_html,
        }
    )


@require_GET
@api_key_required
def get_article_editorial_data(request):
    article_id_raw = request.GET.get("article_id", "").strip()
    if not article_id_raw:
        return JsonResponse({"error": "article_id is required"}, status=400)
    try:
        article_id = int(article_id_raw)
    except ValueError:
        return JsonResponse({"error": "article_id must be an integer"}, status=400)

    try:
        article = (
            Article.objects.select_related("newsletter")
            .prefetch_related("categories")
            .get(id=article_id, is_review_approved=True)
        )
    except Article.DoesNotExist:
        return JsonResponse({"error": "Article not found"}, status=404)

    return JsonResponse({"status": "ok", "article": _article_editorial_payload(article)})


@require_GET
@api_key_required
def get_pending_article_editorial(request):
    mode = str(request.GET.get("mode", "latest")).strip().lower() or "latest"
    if mode not in {"latest", "oldest"}:
        return JsonResponse({"error": f"invalid mode: {mode}"}, status=400)

    pending_qs = (
        Article.objects.select_related("newsletter")
        .prefetch_related("categories")
        .filter(is_review_approved=True, editorial_status="pending")
    )

    # When the Telegram inline bot is polling, exclude articles already sent for triage.
    # This prevents the triage job from stalling on the first article.
    exclude_tg = str(request.GET.get("exclude_tg_triaged", "")).strip().lower()
    if exclude_tg in {"1", "true", "yes", "on"}:
        pending_qs = pending_qs.filter(telegram_triage_status="not_sent")

    pending_count = pending_qs.count()
    if pending_count == 0:
        return JsonResponse(
            {
                "status": "no_pending_context",
                "reason": "no_pending_editorial_articles",
                "mode": mode,
                "pending_editorial_count": 0,
                "article": None,
            }
        )

    if mode == "oldest":
        article = pending_qs.order_by("published_at", "id").first()
    else:
        article = pending_qs.order_by("-published_at", "-id").first()

    return JsonResponse(
        {
            "status": "ok",
            "mode": mode,
            "pending_editorial_count": pending_count,
            "article": _article_editorial_payload(article),
        }
    )


@csrf_exempt
@require_POST
@api_key_required
def apply_article_editorial_decision(request):
    try:
        data = json.loads(request.body)
    except Exception:
        return JsonResponse({"error": "invalid json"}, status=400)

    article_id = data.get("article_id")
    if article_id is None:
        return JsonResponse({"error": "article_id is required"}, status=400)
    try:
        article_id = int(article_id)
    except Exception:
        return JsonResponse({"error": "article_id must be an integer"}, status=400)

    decision = str(data.get("decision", "")).strip().lower()
    decision_to_status = {
        "approve": "approved",
        "approved": "approved",
        "hold": "on_hold",
        "on_hold": "on_hold",
        "reject": "changes_requested",
        "changes": "changes_requested",
        "changes_requested": "changes_requested",
        "request_changes": "changes_requested",
        "revise": "pending",
    }
    if decision not in decision_to_status:
        return JsonResponse({"error": "invalid decision"}, status=400)

    try:
        article = (
            Article.objects.select_related("newsletter")
            .prefetch_related("categories")
            .get(id=article_id, is_review_approved=True)
        )
    except Article.DoesNotExist:
        return JsonResponse({"error": "Article not found"}, status=404)

    update_fields: list[str] = []
    for field_name, max_len in [
        ("title", 500),
        ("section", 120),
        ("category", 120),
        ("subcategory", 120),
    ]:
        if field_name in data:
            value = str(data.get(field_name, "")).strip()
            setattr(article, field_name, value[:max_len] if max_len else value)
            update_fields.append(field_name)

    if "summary" in data:
        article.summary = str(data.get("summary", "")).strip()
        update_fields.append("summary")
    if "article_body" in data:
        article.enrichment_context = str(data.get("article_body", "")).strip()
        update_fields.append("enrichment_context")
    if "original_url" in data:
        article.original_url = str(data.get("original_url", "")).strip()
        update_fields.append("original_url")

    comment = str(data.get("comment", "")).strip()
    if comment:
        article.editorial_comment = comment
        update_fields.append("editorial_comment")

    target_status = decision_to_status[decision]
    article.editorial_status = target_status
    update_fields.append("editorial_status")

    if target_status in {"approved", "on_hold", "changes_requested"}:
        article.editorial_reviewed_at = timezone.now()
    else:
        article.editorial_reviewed_at = None
    update_fields.append("editorial_reviewed_at")

    if update_fields:
        article.save(update_fields=sorted(set(update_fields)))

    if "categories" in data:
        category_objects = _resolve_category_objects(data.get("categories", []))
        article.categories.set(category_objects)

    _refresh_newsletter_editorial_state(article.newsletter)
    _refresh_category_counts()
    article.refresh_from_db()

    pending_count = Article.objects.filter(
        newsletter=article.newsletter,
        is_review_approved=True,
        editorial_status="pending",
    ).count()

    return JsonResponse(
        {
            "status": "ok",
            "decision": decision,
            "article": _article_editorial_payload(article),
            "newsletter_id": article.newsletter_id,
            "newsletter_status": article.newsletter.status,
            "pending_editorial_count": pending_count,
        }
    )


@csrf_exempt
@require_POST
@api_key_required
def publish_resource(request):
    try:
        data = json.loads(request.body)
    except Exception:
        return JsonResponse({"error": "invalid json"}, status=400)

    resource_url = str(data.get("resource_url", "")).strip()
    if not resource_url:
        return JsonResponse({"error": "resource_url is required"}, status=400)
    if not resource_url.startswith(("http://", "https://")):
        return JsonResponse({"error": "resource_url must start with http:// or https://"}, status=400)

    existing = Resource.objects.filter(resource_url=resource_url).only(
        "id",
        "title",
        "summary",
        "article_body",
        "description",
        "image_url",
        "section",
        "category",
        "subcategory",
        "is_featured",
        "is_active",
        "review_status",
        "review_requested_at",
        "review_notified_at",
        "review_decided_at",
        "review_comment",
        "source_published_at",
        "published_at",
    ).first()

    title_in = str(data.get("title", "")).strip()[:500]
    title = title_in or (existing.title if existing and existing.title else resource_url)

    summary_in = str(data.get("summary", "")).strip() or str(data.get("description", "")).strip()
    summary = summary_in or (existing.summary if existing else "") or (existing.description if existing else "")

    article_body_in = str(data.get("article_body", "")).strip()
    article_body = article_body_in or (existing.article_body if existing else "")

    image_in = str(data.get("image_url", "")).strip()[:2000]
    image_url = image_in or (existing.image_url if existing else "")

    section_in = str(data.get("section", "")).strip()[:120]
    section = section_in or (existing.section if existing else "")

    category_in = str(data.get("category", "")).strip()[:120]
    category = category_in or (existing.category if existing else "")

    subcategory_in = str(data.get("subcategory", "")).strip()[:120]
    subcategory = subcategory_in or (existing.subcategory if existing else "")

    if not summary:
        summary = _resource_summary_fallback(
            title=title,
            section=section,
            category=category,
            subcategory=subcategory,
        )
    if not article_body:
        article_body = _resource_article_body_fallback(
            title=title,
            summary=summary,
            section=section,
            category=category,
            subcategory=subcategory,
        )
    elif existing is None and not article_body_in and _resource_article_body_is_weak(article_body, summary):
        article_body = _resource_article_body_fallback(
            title=title,
            summary=summary,
            section=section,
            category=category,
            subcategory=subcategory,
        )

    # Legacy compatibility: keep `description` aligned to teaser summary.
    description = summary
    source_published_at = _parse_datetime_utc(data.get("source_published_at"), default=None) or (
        existing.source_published_at if existing else None
    )
    published_default = source_published_at or (existing.published_at if existing else timezone.now())
    published_at = _parse_datetime_utc(data.get("published_at"), default=published_default) or published_default
    review_status = (
        str(data.get("review_status", "")).strip().lower()
        or (existing.review_status if existing else "")
        or "approved"
    )
    if review_status not in {"pending", "approved", "rejected"}:
        return JsonResponse({"error": "review_status must be pending|approved|rejected"}, status=400)

    defaults = {
        "title": title,
        "summary": summary,
        "article_body": article_body,
        "description": description,
        "image_url": image_url,
        "section": section,
        "category": category,
        "subcategory": subcategory,
        "is_featured": _parse_bool(data.get("is_featured"), default=bool(existing.is_featured) if existing else False),
        "is_active": _parse_bool(data.get("is_active"), default=bool(existing.is_active) if existing else True),
        "review_status": review_status,
        "review_requested_at": _parse_datetime_utc(
            data.get("review_requested_at"),
            default=(existing.review_requested_at if existing else None),
        ),
        "review_notified_at": _parse_datetime_utc(
            data.get("review_notified_at"),
            default=(existing.review_notified_at if existing else None),
        ),
        "review_decided_at": _parse_datetime_utc(
            data.get("review_decided_at"),
            default=(existing.review_decided_at if existing else None),
        ),
        "review_comment": str(data.get("review_comment", "")).strip() or (existing.review_comment if existing else ""),
        "source_published_at": source_published_at,
        "published_at": published_at,
    }

    resource, created = Resource.objects.update_or_create(
        resource_url=resource_url,
        defaults=defaults,
    )
    status_value = "created" if created else "updated"
    return JsonResponse(
        {"status": status_value, "resource": _resource_payload(resource)},
        status=201 if created else 200,
    )


@require_GET
@api_key_required
def get_pending_resource_reviews(request):
    mode = str(request.GET.get("mode", "latest_notified")).strip().lower() or "latest_notified"
    limit_raw = str(request.GET.get("limit", "20")).strip()
    try:
        limit = max(1, min(200, int(limit_raw)))
    except Exception:
        limit = 20

    qs = Resource.objects.filter(review_status="pending")
    if mode == "latest_notified":
        qs = qs.exclude(review_notified_at__isnull=True).order_by("-review_notified_at", "-id")
    elif mode == "next_unnotified":
        qs = qs.filter(review_notified_at__isnull=True).order_by("review_requested_at", "id")
    else:
        return JsonResponse({"error": f"invalid mode: {mode}"}, status=400)

    rows = list(qs[:limit])
    return JsonResponse(
        {
            "status": "ok",
            "mode": mode,
            "pending_count": Resource.objects.filter(review_status="pending").count(),
            "resources": [_resource_payload(row) for row in rows],
        }
    )


@csrf_exempt
@require_POST
@api_key_required
def mark_resource_review_notified(request):
    try:
        data = json.loads(request.body)
    except Exception:
        return JsonResponse({"error": "invalid json"}, status=400)

    resource_id = data.get("resource_id")
    if resource_id is None:
        return JsonResponse({"error": "resource_id is required"}, status=400)
    try:
        resource_id = int(resource_id)
    except Exception:
        return JsonResponse({"error": "resource_id must be an integer"}, status=400)

    try:
        resource = Resource.objects.get(id=resource_id)
    except Resource.DoesNotExist:
        return JsonResponse({"error": "Resource not found"}, status=404)

    resource.review_notified_at = timezone.now()
    if resource.review_status != "pending":
        resource.review_status = "pending"
    resource.save(update_fields=["review_notified_at", "review_status"])
    return JsonResponse({"status": "ok", "resource": _resource_payload(resource)})


@csrf_exempt
@require_POST
@api_key_required
def apply_resource_review_decision(request):
    try:
        data = json.loads(request.body)
    except Exception:
        return JsonResponse({"error": "invalid json"}, status=400)

    resource_id = data.get("resource_id")
    if resource_id is None:
        return JsonResponse({"error": "resource_id is required"}, status=400)
    try:
        resource_id = int(resource_id)
    except Exception:
        return JsonResponse({"error": "resource_id must be an integer"}, status=400)

    decision = str(data.get("decision", "")).strip().lower()
    aliases = {"approved": "approve", "rejected": "reject"}
    decision = aliases.get(decision, decision)
    if decision not in {"approve", "reject"}:
        return JsonResponse({"error": "decision must be approve|reject"}, status=400)

    try:
        resource = Resource.objects.get(id=resource_id)
    except Resource.DoesNotExist:
        return JsonResponse({"error": "Resource not found"}, status=404)

    comment = str(data.get("comment", "")).strip()
    now_value = timezone.now()
    if decision == "approve":
        resource.review_status = "approved"
        resource.is_active = True
    else:
        resource.review_status = "rejected"
        resource.is_active = False
    resource.review_decided_at = now_value
    if comment:
        resource.review_comment = comment
    resource.save(update_fields=["review_status", "is_active", "review_decided_at", "review_comment"])

    pending_count = Resource.objects.filter(review_status="pending").count()
    return JsonResponse(
        {
            "status": "ok",
            "decision": decision,
            "resource": _resource_payload(resource),
            "pending_review_count": pending_count,
        }
    )


@csrf_exempt
@require_POST
@api_key_required
def log_processing(request):
    try:
        data = json.loads(request.body)
        newsletter = None
        if data.get("newsletter_id"):
            newsletter = Newsletter.objects.get(id=data["newsletter_id"])

        ProcessingLog.objects.create(
            newsletter=newsletter,
            action=data.get("action", "unknown"),
            status=data.get("status", "started"),
            message=data.get("message", ""),
            duration_seconds=data.get("duration_seconds"),
        )
        return JsonResponse({"status": "logged"}, status=201)
    except Exception as exc:
        return JsonResponse({"error": str(exc)}, status=400)


@csrf_exempt
@require_POST
@api_key_required
def update_article_link_validation(request):
    """Persist link validation results on an Article (Fase 2)."""
    try:
        data = json.loads(request.body)
    except Exception:
        return JsonResponse({"error": "invalid json"}, status=400)

    article_id = data.get("article_id")
    if article_id is None:
        return JsonResponse({"error": "article_id is required"}, status=400)

    try:
        article = Article.objects.get(id=int(article_id))
    except Article.DoesNotExist:
        return JsonResponse({"error": "Article not found"}, status=404)

    update_fields: list[str] = []

    status_val = str(data.get("link_validation_status", "")).strip()
    if status_val in ("not_checked", "valid", "uncertain", "invalid"):
        article.link_validation_status = status_val
        update_fields.append("link_validation_status")

    if "link_validation_confidence" in data:
        try:
            article.link_validation_confidence = max(0.0, min(1.0, float(data["link_validation_confidence"])))
            update_fields.append("link_validation_confidence")
        except (ValueError, TypeError):
            pass

    if "link_validation_reason" in data:
        article.link_validation_reason = str(data["link_validation_reason"])[:2000]
        update_fields.append("link_validation_reason")

    if "source_link_origin" in data:
        origin = str(data["source_link_origin"]).strip()
        if origin in ("email", "search", "user"):
            article.source_link_origin = origin
            update_fields.append("source_link_origin")

    if "link_candidates_json" in data:
        article.link_candidates_json = str(data["link_candidates_json"])
        update_fields.append("link_candidates_json")

    if "original_url" in data:
        article.original_url = str(data["original_url"]).strip()
        update_fields.append("original_url")

    if "proposed_title" in data:
        article.proposed_title = str(data["proposed_title"]).strip()[:500]
        update_fields.append("proposed_title")

    if "content_profile" in data:
        profile = str(data["content_profile"]).strip()
        if profile in ("news", "resource"):
            article.content_profile = profile
            update_fields.append("content_profile")

    if "telegram_triage_status" in data:
        tg_status = str(data["telegram_triage_status"]).strip()
        if tg_status in ("not_sent", "awaiting_triage", "awaiting_llm", "pending_approval",
                         "waiting_user_input", "waiting_edit", "blocked_llm", "approved", "rejected"):
            article.telegram_triage_status = tg_status
            update_fields.append("telegram_triage_status")

    if update_fields:
        article.link_validated_at = timezone.now()
        update_fields.append("link_validated_at")
        article.save(update_fields=sorted(set(update_fields)))

    return JsonResponse({
        "status": "ok",
        "article_id": article.id,
        "updated_fields": sorted(set(update_fields)),
    })
