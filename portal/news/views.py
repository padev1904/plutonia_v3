import hashlib
import hmac
import json
import secrets
import time

import requests
from django.conf import settings
from django.core.paginator import Paginator
from django.db.models import Case, Count, F, IntegerField, Q, Value, When, Window
from django.db.models.functions import Coalesce, TruncDate
from django.http import Http404, HttpRequest, HttpResponse
from django.shortcuts import get_object_or_404, render
from django.utils.text import slugify
from django.views.decorators.http import require_GET, require_http_methods, require_POST
from django.contrib.postgres.search import SearchQuery, SearchRank

from .models import Article, Category, Newsletter, Resource


def _base_queryset():
    return (
        Article.objects.filter(
            is_review_approved=True,
            editorial_status="approved",
            content_profile="news",
        )
        .select_related("newsletter")
        .prefetch_related("categories")
    )


def _annotate_equivalent_score(qs, *, url_field: str):
    return qs.annotate(
        _equivalent_group_count=Window(
            expression=Count("id"),
            partition_by=[F(url_field)],
        )
    ).annotate(
        equivalent_score=Case(
            When(**{f"{url_field}__isnull": True}, then=Value(1)),
            When(**{url_field: ""}, then=Value(1)),
            default=F("_equivalent_group_count"),
            output_field=IntegerField(),
        )
    )


def _news_card_queryset():
    return _annotate_equivalent_score(
        _base_queryset().annotate(sort_published_at=Coalesce("newsletter__original_sent_at", "newsletter__received_at", "published_at")),
        url_field="original_url",
    ).order_by("-sort_published_at", "-id")


def _editorial_queryset():
    return (
        Article.objects.filter(is_review_approved=True)
        .select_related("newsletter")
        .prefetch_related("categories")
    )


def _approved_categories_queryset():
    return (
        _base_queryset()
        .exclude(category="")
        .values("category")
        .annotate(approved_count=Count("id"))
        .order_by("-approved_count", "category")
    )


def _public_category_rows():
    return [
        {
            "name": str(row["category"]).strip(),
            "slug": slugify(str(row["category"]).strip()),
            "article_count": int(row["approved_count"]),
        }
        for row in _approved_categories_queryset()
        if str(row.get("category", "")).strip()
    ]


def _resource_queryset():
    return _annotate_equivalent_score(
        Resource.objects.filter(is_active=True, review_status="approved").annotate(
            sort_published_at=Coalesce("source_published_at", "published_at")
        ),
        url_field="resource_url",
    ).order_by("-sort_published_at", "-id")


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
    qs = _news_card_queryset()
    query_text = request.GET.get("q", "").strip()

    if query_text:
        sq = SearchQuery(query_text)
        qs = qs.annotate(rank=SearchRank("search_vector", sq)).filter(rank__gt=0.001).order_by("-rank", "-sort_published_at", "-id")

    paginator = Paginator(qs, 24)
    page_obj = paginator.get_page(request.GET.get("page", 1))

    context = {
        "page_obj": page_obj,
        "filters": {
            "q": query_text,
        },
        "page_heading": "News",
        "page_intro": "Published news cards with image, title, summary, taxonomy, original date, and source score.",
    }

    if getattr(request, "htmx", False):
        return render(request, "news/partials/article_cards.html", context)
    return render(request, "news/article_list.html", context)


@require_GET
def article_detail(request: HttpRequest, pk: int) -> HttpResponse:
    article = _news_card_queryset().filter(pk=pk).first()
    if article is None:
        article = (
            Article.objects.filter(
                is_review_approved=True,
                editorial_status="approved",
                content_profile="product",
            )
            .select_related("newsletter")
            .prefetch_related("categories")
            .filter(pk=pk)
            .first()
        )
    if article is None:
        raise Http404("Article not found")
    if not article.is_read:
        article.is_read = True
        article.save(update_fields=["is_read"])
    back_url = request.META.get("HTTP_REFERER", "").strip() or "/"
    return render(request, "news/article_detail.html", {"article": article, "back_url": back_url})


@require_GET
def product_detail(request: HttpRequest, pk: int) -> HttpResponse:
    """View for product detail page"""
    # Filter for approved products only
    article = get_object_or_404(
        Article.objects.filter(
            is_review_approved=True,
            editorial_status="approved",
            content_profile="product",
        ).select_related("newsletter").prefetch_related("categories"),
        pk=pk
    )
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
    article = _news_card_queryset().filter(pk=pk).first()
    detail_path = ""
    if article is None:
        article = (
            Article.objects.filter(
                is_review_approved=True,
                editorial_status="approved",
                content_profile="product",
            )
            .select_related("newsletter")
            .prefetch_related("categories")
            .filter(pk=pk)
            .first()
        )
        if article is None:
            raise Http404("Article not found")
        context_cards = list(
            Article.objects.filter(
                is_review_approved=True,
                editorial_status="approved",
                content_profile="product",
            )
            .exclude(id=article.id)
            .order_by("-published_at")[:5]
        )
        detail_path = f"/products/{article.id}/"
    else:
        context_cards = list(_base_queryset().exclude(id=article.id).order_by("-published_at")[:5])
        detail_path = f"/article/{article.id}/"
    cards = [article, *context_cards]
    placeholder_count = max(0, 6 - len(cards))

    return render(
        request,
        "news/article_card_preview.html",
        {
            "article": article,
            "is_preview": False,
            "detail_preview_path": detail_path,
            "cards": cards,
            "target_id": article.id,
            "placeholder_range": range(placeholder_count),
        },
    )


@require_GET
def product_card_public(request: HttpRequest, pk: int) -> HttpResponse:
    """Public card view for product articles"""
    # Filter for approved products only
    article = get_object_or_404(
        Article.objects.filter(
            is_review_approved=True,
            editorial_status="approved",
            content_profile="product",
        ).select_related("newsletter").prefetch_related("categories"),
        pk=pk
    )
    context_cards = list(
        Article.objects.filter(
            is_review_approved=True,
            editorial_status="approved",
            content_profile="product",
        ).exclude(id=article.id).order_by("-published_at")[:5]
    )
    cards = [article, *context_cards]
    placeholder_count = max(0, 6 - len(cards))

    return render(
        request,
        "news/article_card_preview.html",
        {
            "article": article,
            "is_preview": False,
            "detail_preview_path": f"/products/{article.id}/",
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
    return render(request, "news/category_list.html", {"categories": _public_category_rows()})


@require_GET
def category_detail(request: HttpRequest, slug: str) -> HttpResponse:
    public_categories = _public_category_rows()
    category = next((row for row in public_categories if row["slug"] == slug), None)
    if not category:
        raise Http404("Category not found")
    qs = _news_card_queryset().filter(category=category["name"])
    paginator = Paginator(qs, 24)
    page_obj = paginator.get_page(request.GET.get("page", 1))
    return render(
        request,
        "news/article_list.html",
        {
            "page_obj": page_obj,
            "filters": {"q": ""},
            "selected_category": category,
            "page_heading": category["name"],
            "page_intro": "Filtered news cards for the selected category.",
        },
    )


@require_GET
def favorites(request: HttpRequest) -> HttpResponse:
    qs = _news_card_queryset().filter(is_favorite=True)
    paginator = Paginator(qs, 24)
    page_obj = paginator.get_page(request.GET.get("page", 1))
    return render(
        request,
        "news/favorites.html",
        {
            "page_obj": page_obj,
            "filters": {"q": ""},
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

    if query_text:
        qs = qs.filter(
            Q(title__icontains=query_text)
            | Q(summary__icontains=query_text)
            | Q(article_body__icontains=query_text)
            | Q(description__icontains=query_text)
        )

    paginator = Paginator(qs, 24)
    page_obj = paginator.get_page(request.GET.get("page", 1))

    return render(
        request,
        "news/resource_list.html",
        {
            "page_obj": page_obj,
            "filters": {
                "q": query_text,
            },
            "page_heading": "Resources",
            "page_intro": "Study materials and deep-dive resources with image, summary, taxonomy, original date, and source score.",
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


@require_GET
def product_list(request: HttpRequest) -> HttpResponse:
    """View for the Products section"""
    # Filter articles by content_profile="product" and is_review_approved=True
    qs = (
        Article.objects.filter(
            is_review_approved=True,
            editorial_status="approved",
            content_profile="product",
        )
        .select_related("newsletter")
        .prefetch_related("categories")
        .annotate(sort_published_at=Coalesce("newsletter__original_sent_at", "newsletter__received_at", "published_at"))
        .order_by("-sort_published_at", "-id")
    )
    
    query_text = request.GET.get("q", "").strip()

    if query_text:
        sq = SearchQuery(query_text)
        qs = qs.annotate(rank=SearchRank("search_vector", sq)).filter(rank__gt=0.001).order_by("-rank", "-sort_published_at", "-id")

    paginator = Paginator(qs, 24)
    page_obj = paginator.get_page(request.GET.get("page", 1))

    context = {
        "page_obj": page_obj,
        "filters": {
            "q": query_text,
        },
        "page_heading": "Products",
        "page_intro": "Our AI-powered products and SaaS offerings.",
    }

    if getattr(request, "htmx", False):
        return render(request, "news/partials/article_cards.html", context)
    
    return render(request, "news/product_list.html", context)
