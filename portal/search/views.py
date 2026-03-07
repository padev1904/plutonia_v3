from django.contrib.postgres.search import SearchQuery, SearchRank
from django.shortcuts import render
from django.views.decorators.http import require_GET

from news.models import Article
from .searxng_client import SearXNGClient


@require_GET
def search_view(request):
    query = request.GET.get("q", "").strip()
    local_results = []

    if query:
        sq = SearchQuery(query)
        local_results = list(
            Article.objects.annotate(rank=SearchRank("search_vector", sq))
            .filter(is_review_approved=True, editorial_status="approved")
            .filter(rank__gt=0.001)
            .order_by("-rank", "-published_at")[:50]
            .select_related("newsletter")
            .prefetch_related("categories")
        )

    web_mode = request.GET.get("web", "0") == "1"
    if web_mode and getattr(request, "htmx", False):
        web_results = SearXNGClient().search(query, categories="general", max_results=8)
        return render(request, "search/web_results.html", {"query": query, "web_results": web_results})

    return render(request, "search/search.html", {"query": query, "local_results": local_results})
