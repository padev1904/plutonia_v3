import hashlib
import hmac
import json
import secrets
import time

import requests
from django.conf import settings
from django.core.paginator import Paginator
from django.db.models import Count, Q
from django.db.models.functions import TruncDate
from django.http import Http404, HttpRequest, HttpResponse
from django.shortcuts import get_object_or_404, render
from django.views.decorators.http import require_GET, require_http_methods, require_POST
from django.contrib.postgres.search import SearchQuery, SearchRank

from .models import Article, Category, Newsletter, Resource


def _base_queryset():
    return (
        Article.objects.filter(is_review_approved=True, editorial_status="approved")
        .select_related("newsletter")
        .prefetch_related("categories")
    )


def _editorial_queryset():
    return (
        Article.objects.filter(is_review_approved=True)
        .select_related("newsletter")
        .prefetch_related("categories")
    )


def _approved_categories_queryset():
    return (
        Category.objects.annotate(
            approved_count=Count(
                "articles",
                filter=Q(articles__is_review_approved=True, articles__editorial_status="approved"),
                distinct=True,
            )
        )
        .filter(approved_count__gt=0)
        .order_by("name")
    )


def _resource_queryset():
    return Resource.objects.filter(is_active=True, review_status="approved")


def _review_api_base_url() -> str:
    value = str(getattr(settings, "REVIEW_API_BASE_URL", "http://ainews-gmail-monitor:8001")).strip().rstrip("/")
    return value or "http://ainews-gmail-monitor:8001"


def _sign_resource_submit(resource_url: str) -> dict[str, str | int]:
    secret = str(getattr(settings, "REVIEW_SIGNATURE_SECRET", "")).strip()
    if not secret:
        raise RuntimeError("REVIEW_SIGNATURE_SECRET is missing")
    ts = int(time.time())
    nonce = secrets.token_urlsafe(12)
    resource_hash = hashlib.sha256(resource_url.encode("utf-8")).hexdigest()
    canonical = f"resource|submit|{resource_hash}|{ts}|{nonce}"
    sig = hmac.new(secret.encode("utf-8"), canonical.encode("utf-8"), hashlib.sha256).hexdigest()
    return {"sig_ts": ts, "sig_nonce": nonce, "sig": sig}


@require_GET
def article_list(request: HttpRequest) -> HttpResponse:
    qs = _base_queryset()

    category_slug = request.GET.get("category", "").strip()
    favorites_only = request.GET.get("favorites", "").strip() == "1"
    unread_only = request.GET.get("unread", "").strip() == "1"
    query_text = request.GET.get("q", "").strip()

    if category_slug:
        qs = qs.filter(categories__slug=category_slug)
    if favorites_only:
        qs = qs.filter(is_favorite=True)
    if unread_only:
        qs = qs.filter(is_read=False)
    if query_text:
        sq = SearchQuery(query_text)
        qs = qs.annotate(rank=SearchRank("search_vector", sq)).filter(rank__gt=0.001).order_by("-rank", "-published_at")

    paginator = Paginator(qs.distinct(), 20)
    page_obj = paginator.get_page(request.GET.get("page", 1))

    context = {
        "page_obj": page_obj,
        "categories": _approved_categories_queryset(),
        "filters": {
            "category": category_slug,
            "favorites": favorites_only,
            "unread": unread_only,
            "q": query_text,
        },
    }

    if getattr(request, "htmx", False):
        return render(request, "news/partials/article_cards.html", context)
    return render(request, "news/article_list.html", context)


@require_GET
def article_detail(request: HttpRequest, pk: int) -> HttpResponse:
    article = get_object_or_404(_base_queryset(), pk=pk)
    if not article.is_read:
        article.is_read = True
        article.save(update_fields=["is_read"])
    back_url = request.META.get("HTTP_REFERER", "").strip() or "/"
    return render(request, "news/article_detail.html", {"article": article, "back_url": back_url})


@require_GET
def article_preview(request: HttpRequest, token) -> HttpResponse:
    article = _editorial_queryset().filter(preview_token=token).order_by("-id").first()
    if article is None:
        raise Http404("Article not found")
    if not article.is_read:
        article.is_read = True
        article.save(update_fields=["is_read"])
    back_url = request.META.get("HTTP_REFERER", "").strip() or "/dashboard/"
    return render(
        request,
        "news/article_detail.html",
        {
            "article": article,
            "back_url": back_url,
            "is_preview": True,
        },
    )


@require_GET
def article_card_preview(request: HttpRequest, token) -> HttpResponse:
    article = _editorial_queryset().filter(preview_token=token).order_by("-id").first()
    if article is None:
        raise Http404("Article not found")

    context_cards = list(_base_queryset().exclude(id=article.id).order_by("-published_at")[:5])
    cards = [article, *context_cards]
    placeholder_count = max(0, 6 - len(cards))

    return render(
        request,
        "news/article_card_preview.html",
        {
            "article": article,
            "is_preview": True,
            "detail_preview_path": f"/preview/{article.preview_token}/",
            "cards": cards,
            "target_id": article.id,
            "placeholder_range": range(placeholder_count),
        },
    )


@require_GET
def article_card_public(request: HttpRequest, pk: int) -> HttpResponse:
    article = get_object_or_404(_base_queryset(), pk=pk)
    context_cards = list(_base_queryset().exclude(id=article.id).order_by("-published_at")[:5])
    cards = [article, *context_cards]
    placeholder_count = max(0, 6 - len(cards))

    return render(
        request,
        "news/article_card_preview.html",
        {
            "article": article,
            "is_preview": False,
            "detail_preview_path": f"/article/{article.id}/",
            "cards": cards,
            "target_id": article.id,
            "placeholder_range": range(placeholder_count),
        },
    )


@require_POST
def toggle_favorite(request: HttpRequest, pk: int) -> HttpResponse:
    article = get_object_or_404(Article.objects.filter(is_review_approved=True, editorial_status="approved"), pk=pk)
    article.is_favorite = not article.is_favorite
    article.save(update_fields=["is_favorite"])
    return render(request, "news/partials/favorite_button.html", {"article": article})


@require_GET
def category_list(request: HttpRequest) -> HttpResponse:
    categories = (
        Category.objects.annotate(
            article_count=Count(
                "articles",
                filter=Q(articles__is_review_approved=True, articles__editorial_status="approved"),
                distinct=True,
            )
        )
        .filter(article_count__gt=0)
        .order_by("name")
    )
    return render(request, "news/category_list.html", {"categories": categories})


@require_GET
def category_detail(request: HttpRequest, slug: str) -> HttpResponse:
    category = get_object_or_404(Category, slug=slug)
    qs = _base_queryset().filter(categories=category).distinct()
    paginator = Paginator(qs, 20)
    page_obj = paginator.get_page(request.GET.get("page", 1))
    return render(
        request,
        "news/article_list.html",
        {
            "page_obj": page_obj,
            "categories": _approved_categories_queryset(),
            "filters": {"category": slug, "favorites": False, "unread": False, "q": ""},
            "selected_category": category,
        },
    )


@require_GET
def favorites(request: HttpRequest) -> HttpResponse:
    qs = _base_queryset().filter(is_favorite=True)
    paginator = Paginator(qs, 20)
    page_obj = paginator.get_page(request.GET.get("page", 1))
    return render(
        request,
        "news/favorites.html",
        {
            "page_obj": page_obj,
            "categories": _approved_categories_queryset(),
            "filters": {"category": "", "favorites": True, "unread": False, "q": ""},
        },
    )


@require_GET
def dashboard(request: HttpRequest) -> HttpResponse:
    newsletter_total = Newsletter.objects.count()
    article_total = Article.objects.filter(is_review_approved=True, editorial_status="approved").count()
    preview_pending = Article.objects.filter(is_review_approved=True, editorial_status="pending").count()
    pending = Newsletter.objects.filter(status="pending").count()
    review = Newsletter.objects.filter(status="review").count()
    completed = Newsletter.objects.filter(status="completed").count()

    top_categories = (
        Category.objects.annotate(
            approved_count=Count(
                "articles",
                filter=Q(articles__is_review_approved=True, articles__editorial_status="approved"),
                distinct=True,
            )
        )
        .filter(approved_count__gt=0)
        .order_by("-approved_count", "name")[:10]
    )
    by_day = (
        Article.objects.filter(is_review_approved=True, editorial_status="approved")
        .annotate(day=TruncDate("published_at"))
        .values("day")
        .annotate(total=Count("id"))
        .order_by("-day")[:14]
    )

    return render(
        request,
        "news/dashboard.html",
        {
            "newsletter_total": newsletter_total,
            "article_total": article_total,
            "preview_pending": preview_pending,
            "pending": pending,
            "review": review,
            "completed": completed,
            "top_categories": top_categories,
            "by_day": list(reversed(by_day)),
        },
    )


@require_GET
def resource_list(request: HttpRequest) -> HttpResponse:
    qs = _resource_queryset()

    query_text = request.GET.get("q", "").strip()
    section_value = request.GET.get("section", "").strip()
    category_value = request.GET.get("category", "").strip()
    subcategory_value = request.GET.get("subcategory", "").strip()
    featured_only = request.GET.get("featured", "").strip() == "1"

    if query_text:
        qs = qs.filter(
            Q(title__icontains=query_text)
            | Q(summary__icontains=query_text)
            | Q(article_body__icontains=query_text)
            | Q(description__icontains=query_text)
        )
    if section_value:
        qs = qs.filter(section=section_value)
    if category_value:
        qs = qs.filter(category=category_value)
    if subcategory_value:
        qs = qs.filter(subcategory=subcategory_value)
    if featured_only:
        qs = qs.filter(is_featured=True)

    paginator = Paginator(qs, 20)
    page_obj = paginator.get_page(request.GET.get("page", 1))

    base = _resource_queryset()
    sections = list(base.exclude(section="").values_list("section", flat=True).distinct().order_by("section"))
    categories = list(base.exclude(category="").values_list("category", flat=True).distinct().order_by("category"))
    subcategories = list(base.exclude(subcategory="").values_list("subcategory", flat=True).distinct().order_by("subcategory"))

    return render(
        request,
        "news/resource_list.html",
        {
            "page_obj": page_obj,
            "sections": sections,
            "categories": categories,
            "subcategories": subcategories,
            "filters": {
                "q": query_text,
                "section": section_value,
                "category": category_value,
                "subcategory": subcategory_value,
                "featured": featured_only,
            },
        },
    )


@require_GET
def resource_detail(request: HttpRequest, pk: int) -> HttpResponse:
    resource = get_object_or_404(_resource_queryset(), pk=pk)
    back_url = request.META.get("HTTP_REFERER", "").strip() or "/resources/"
    return render(
        request,
        "news/resource_detail.html",
        {
            "resource": resource,
            "back_url": back_url,
        },
    )


@require_GET
def resource_preview(request: HttpRequest, token) -> HttpResponse:
    resource = Resource.objects.filter(preview_token=token).order_by("-id").first()
    if resource is None:
        raise Http404("Resource not found")
    back_url = request.META.get("HTTP_REFERER", "").strip() or "/resources/"
    return render(
        request,
        "news/resource_preview.html",
        {
            "resource": resource,
            "back_url": back_url,
            "is_preview": True,
        },
    )


@require_http_methods(["GET", "POST"])
def resource_submit(request: HttpRequest) -> HttpResponse:
    context: dict[str, object] = {
        "form": {
            "resource_url": "",
            "title": "",
            "description": "",
            "section": "",
            "category": "",
            "subcategory": "",
        }
    }

    if request.method == "GET":
        return render(request, "news/resource_submit.html", context)

    resource_url = request.POST.get("resource_url", "").strip()
    title = request.POST.get("title", "").strip()
    description = request.POST.get("description", "").strip()
    section = request.POST.get("section", "").strip()
    category = request.POST.get("category", "").strip()
    subcategory = request.POST.get("subcategory", "").strip()

    context["form"] = {
        "resource_url": resource_url,
        "title": title,
        "description": description,
        "section": section,
        "category": category,
        "subcategory": subcategory,
    }

    if not resource_url.startswith(("http://", "https://")):
        context["error"] = "Invalid URL. Use http:// or https://."
        return render(request, "news/resource_submit.html", context)

    payload: dict[str, object] = {
        "resource_url": resource_url,
        "review_required": True,
    }
    if title:
        payload["title"] = title
    if description:
        payload["description"] = description
    if section:
        payload["section"] = section
    if category:
        payload["category"] = category
    if subcategory:
        payload["subcategory"] = subcategory

    try:
        payload.update(_sign_resource_submit(resource_url))
        resp = requests.post(
            f"{_review_api_base_url()}/api/review/resource-submit",
            json=payload,
            timeout=90,
        )
        data = resp.json()
    except Exception as exc:
        context["error"] = f"Submission failed: {exc}"
        return render(request, "news/resource_submit.html", context)

    if resp.status_code < 200 or resp.status_code >= 300:
        context["error"] = str(data.get("error", "Submission failed"))
        return render(request, "news/resource_submit.html", context)

    context["success"] = "Resource processed and queued for Telegram approval."
    context["result"] = data
    context["result_pretty"] = json.dumps(data, ensure_ascii=False, indent=2)
    return render(request, "news/resource_submit.html", context)
