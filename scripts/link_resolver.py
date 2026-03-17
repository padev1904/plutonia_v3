"""Link resolution and validation via SearXNG.

Pipeline:
  1. If link is missing → search SearXNG to find original URL
  2. If link exists but is a tracking/redirect URL → resolve to final URL
  3. Validate final URL: fetch page title, compare with article title
  4. If mismatch → search SearXNG for the correct URL

Entry point: resolve_article_links(result, searxng_url)
"""

from __future__ import annotations

import logging
import os
import re
from difflib import SequenceMatcher
from typing import TYPE_CHECKING
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

if TYPE_CHECKING:
    from newsletter_revh_parser import EmailParseResult

LOG = logging.getLogger("link_resolver")

SEARXNG_URL = os.getenv("SEARXNG_URL", "http://127.0.0.1:8888")
FETCH_TIMEOUT = int(os.getenv("LINK_FETCH_TIMEOUT", "8"))
VALID_THRESHOLD = float(os.getenv("LINK_VALID_THRESHOLD", "0.55"))
SEARCH_MAX_RESULTS = int(os.getenv("LINK_SEARCH_MAX_RESULTS", "5"))

_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)

# Domínios de tracking/redirect que escondem o URL final
_TRACKING_DOMAINS = frozenset({
    "elink535.humanintheloop.online",
    "elink.beehiiv.com",
    "link.beehiiv.com",
    "click.convertkit-mail.com",
    "click.mailchimpapp.com",
    "list-manage.com",
    "mailchi.mp",
    "t.co",
    "bit.ly",
    "tinyurl.com",
    "ow.ly",
    "buff.ly",
    "redirect.substack.com",
    "substack.com/redirect",
    "link.tldr.tech",
    "tracking.tldrnewsletter.com",
})

# Domínios que NUNCA são URLs de artigos reais
_NOISE_DOMAINS = frozenset({
    "unsubscribe",
    "mailto",
    "substack.com",
    "beehiiv.com",
    "convertkit.com",
    "mailchimp.com",
    "twitter.com",
    "x.com",
    "linkedin.com",
    "facebook.com",
    "instagram.com",
    "youtube.com",
    "apple.com",
    "google.com",
    "plutoanalytics.com",
})


def _is_tracking_url(url: str) -> bool:
    """Devolve True se o URL é um redirect/tracking e precisa de ser resolvido."""
    try:
        host = urlparse(url).netloc.lower().lstrip("www.")
        return host in _TRACKING_DOMAINS or any(d in host for d in _TRACKING_DOMAINS)
    except Exception:
        return False


def _is_noise_url(url: str) -> bool:
    """Devolve True se o URL não é um artigo (infra, redes sociais, etc.)."""
    try:
        host = urlparse(url).netloc.lower().lstrip("www.")
        return any(d in host for d in _NOISE_DOMAINS)
    except Exception:
        return True


def _resolve_final_url(url: str) -> str:
    """Segue redirects e devolve o URL final. Mantém o original se falhar."""
    if not url:
        return url
    try:
        resp = requests.head(
            url,
            timeout=FETCH_TIMEOUT,
            headers={"User-Agent": _BROWSER_UA},
            allow_redirects=True,
        )
        final = resp.url
        return final if final.startswith("http") else url
    except Exception as e:
        LOG.debug("resolve_final_url failed for %s: %s", url, e)
        return url


def _fetch_page_title(url: str) -> str:
    """Faz fetch do URL e extrai o título da página."""
    if not url or not url.startswith("http"):
        return ""
    try:
        resp = requests.get(
            url,
            timeout=FETCH_TIMEOUT,
            headers={"User-Agent": _BROWSER_UA},
            allow_redirects=True,
        )
        if resp.status_code != 200:
            return ""
        soup = BeautifulSoup(resp.text[:40_000], "html.parser")
        title_tag = soup.find("title")
        if title_tag:
            return title_tag.get_text(" ", strip=True)
        h1 = soup.find("h1")
        if h1:
            return h1.get_text(" ", strip=True)
        return ""
    except Exception as e:
        LOG.debug("fetch_page_title failed for %s: %s", url, e)
        return ""


def _title_sim(a: str, b: str) -> float:
    """Similaridade normalizada entre dois títulos (0.0–1.0)."""
    if not a or not b:
        return 0.0
    a_norm = re.sub(r"[^\w\s]", " ", a.lower())
    b_norm = re.sub(r"[^\w\s]", " ", b.lower())
    ratio = SequenceMatcher(None, a_norm, b_norm).ratio()
    # Bonus: verificar se as palavras-chave do artigo aparecem no título da página
    words_a = set(w for w in a_norm.split() if len(w) > 3)
    words_b = set(w for w in b_norm.split() if len(w) > 3)
    if words_a and words_b:
        overlap = len(words_a & words_b) / max(len(words_a), len(words_b))
        ratio = max(ratio, overlap * 0.9)
    return ratio


def _searxng_search(query: str, *, base_url: str = SEARXNG_URL, max_results: int = SEARCH_MAX_RESULTS) -> list[dict]:
    """Pesquisa no SearXNG e devolve lista de {title, url, content}."""
    if not query.strip() or not base_url:
        return []
    try:
        resp = requests.get(
            f"{base_url.rstrip('/')}/search",
            params={
                "q": query,
                "format": "json",
                "engines": "google,bing,duckduckgo",
            },
            timeout=15,
        )
        resp.raise_for_status()
        results = []
        for item in resp.json().get("results", [])[:max_results]:
            url = item.get("url", "")
            if not url or _is_noise_url(url):
                continue
            results.append({
                "title": item.get("title", ""),
                "url": url,
                "content": item.get("content", ""),
            })
        return results
    except Exception as e:
        LOG.warning("SearXNG search failed (url=%s): %s", base_url, e)
        return []


def _find_url_via_search(article_title: str, article_snippet: str, *, searxng_url: str) -> str | None:
    """Pesquisa o URL original no SearXNG com base no título e snippet."""
    queries = [article_title]
    if article_snippet:
        first_words = " ".join(article_snippet.split()[:8])
        queries.append(f"{article_title} {first_words}")

    best_url = None
    best_score = 0.0

    for query in queries:
        candidates = _searxng_search(query, base_url=searxng_url)
        for c in candidates:
            score = _title_sim(article_title, c.get("title", ""))
            if score > best_score:
                best_score = score
                best_url = c["url"]
        if best_score >= VALID_THRESHOLD:
            break

    if best_url and best_score >= VALID_THRESHOLD:
        LOG.info("search found url (score=%.2f): %s", best_score, best_url)
        return best_url

    LOG.debug("search: no confident match (best=%.2f) for %r", best_score, article_title)
    return None


def _validate_and_resolve(
    url: str,
    article_title: str,
    article_snippet: str,
    *,
    searxng_url: str,
) -> tuple[str, str]:
    """Valida o URL existente. Devolve (final_url, status).

    status: 'valid' | 'replaced' | 'kept' | 'missing'
    """
    # 1. Resolver tracking URLs
    working_url = url
    if _is_tracking_url(url):
        resolved = _resolve_final_url(url)
        if resolved != url:
            LOG.info("resolved tracking url: %s → %s", url[:60], resolved[:60])
            working_url = resolved

    # 2. Validar por similaridade de título
    page_title = _fetch_page_title(working_url)
    if page_title:
        sim = _title_sim(article_title, page_title)
        LOG.debug("title sim=%.2f  article=%r  page=%r", sim, article_title[:50], page_title[:50])
        if sim >= VALID_THRESHOLD:
            return working_url, "valid"
    else:
        LOG.debug("could not fetch page title for %s", working_url[:60])

    # 3. Link inválido ou não confirmado → pesquisar alternativa
    found = _find_url_via_search(article_title, article_snippet, searxng_url=searxng_url)
    if found and found != working_url:
        return found, "replaced"

    # 4. Manter o URL original (sem confirmação)
    return working_url, "kept"


def resolve_article_links(
    result: "EmailParseResult",
    *,
    searxng_url: str = SEARXNG_URL,
) -> None:
    """Resolve e valida os links de todos os artigos do resultado.

    Modifica result.articles in-place:
    - Artigos sem link → pesquisa no SearXNG
    - Artigos com link de tracking → resolve o URL final
    - Artigos com link inválido → substitui pelo resultado da pesquisa
    Adiciona nota ao article.notes com o status da resolução.
    """
    if not searxng_url:
        LOG.warning("resolve_article_links: SEARXNG_URL não configurado, a saltar.")
        return

    for article in result.articles:
        title = article.title or ""
        snippet = (article.text or "")[:300]
        link = article.source_link or ""

        if not link or link == "NEEDS_USER_LINK":
            # Sem link → pesquisar
            found = _find_url_via_search(title, snippet, searxng_url=searxng_url)
            if found:
                article.source_link = found
                if "link_searched" not in (article.notes or []):
                    article.notes.append("link_searched")
                LOG.info("[%s] link found via search: %s", title[:40], found[:60])
            else:
                LOG.info("[%s] no link found via search", title[:40])
        else:
            # Link existe → validar
            final_url, status = _validate_and_resolve(link, title, snippet, searxng_url=searxng_url)
            if status == "replaced":
                article.source_link = final_url
                if "link_replaced" not in (article.notes or []):
                    article.notes.append("link_replaced")
                LOG.info("[%s] link replaced: %s → %s", title[:40], link[:50], final_url[:60])
            elif status == "valid" and final_url != link:
                article.source_link = final_url
                if "link_resolved" not in (article.notes or []):
                    article.notes.append("link_resolved")
                LOG.info("[%s] tracking resolved: %s → %s", title[:40], link[:50], final_url[:60])
