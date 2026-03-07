from __future__ import annotations

import requests
from django.conf import settings


class SearXNGClient:
    def __init__(self) -> None:
        self.base_url = settings.SEARXNG_URL.rstrip("/")

    def search(self, query: str, categories: str = "general", max_results: int = 5) -> list[dict]:
        if not query.strip():
            return []
        try:
            response = requests.get(
                f"{self.base_url}/search",
                params={"q": query, "format": "json", "categories": categories},
                timeout=10,
            )
            response.raise_for_status()
            data = response.json()
            results: list[dict] = []
            for item in data.get("results", [])[:max_results]:
                results.append(
                    {
                        "title": item.get("title", ""),
                        "url": item.get("url", item.get("link", "")),
                        "content": item.get("content", ""),
                    }
                )
            return results
        except Exception:
            return []
