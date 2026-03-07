#!/usr/bin/env python3
"""HTTP API server for review decisions.

Runs as a daemon thread inside the gmail-monitor container,
exposing article review endpoints so that OpenClaw (or any
container on the same Docker network) can trigger decisions
via HTTP instead of ``docker exec``.

Endpoints
---------
POST /api/review/article-decision   — Phase 1 (approve / reject one article)
POST /api/review/content-decision   — Phase 2 (editorial decision for one article)
POST /api/review/resource-submit    — Resource submission (classify + publish)
POST /api/review/resource-decision  — Resource editorial approval (approve / reject)
GET  /healthz                       — Liveness probe
"""

from __future__ import annotations

import json
import hmac
import hashlib
import logging
import os
import re
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from process_newsletter import Config

LOG = logging.getLogger("review_api")

REVIEW_API_PORT = int(os.getenv("REVIEW_API_PORT", "8001"))
REVIEW_SIGNATURE_REQUIRED = os.getenv("REVIEW_SIGNATURE_REQUIRED", "false").strip().lower() in {"1", "true", "yes", "on"}
REVIEW_SIGNATURE_SECRET = os.getenv("REVIEW_SIGNATURE_SECRET", "").strip()
REVIEW_SIGNATURE_MAX_AGE_SECONDS = int(os.getenv("REVIEW_SIGNATURE_MAX_AGE_SECONDS", "300"))
REVIEW_NEXT_NOTIFICATION_DELAY_SECONDS = float(os.getenv("REVIEW_NEXT_NOTIFICATION_DELAY_SECONDS", "15"))
REVIEW_AUTO_SOURCE_DISCOVERY = os.getenv("REVIEW_AUTO_SOURCE_DISCOVERY", "true").strip().lower() in {"1", "true", "yes", "on"}
REVIEW_AUTO_SOURCE_MIN_SCORE = int(os.getenv("REVIEW_AUTO_SOURCE_MIN_SCORE", "30"))
REVIEW_RESOURCE_EDITORIAL_REWRITE = os.getenv("REVIEW_RESOURCE_EDITORIAL_REWRITE", "true").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
REVIEW_RESOURCE_EDITORIAL_MIN_SOURCE_CHARS = int(os.getenv("REVIEW_RESOURCE_EDITORIAL_MIN_SOURCE_CHARS", "800"))
_SEEN_NONCES: dict[str, int] = {}
_NONCE_LOCK = threading.Lock()

REVIEW_RESOURCE_CLASSIFICATION_PROMPT = """Classify one AI resource into taxonomy fields.

Return STRICT JSON object only:
{{
  "section": "...",
  "category": "...",
  "subcategory": "..."
}}

Rules:
- English only.
- Keep each field concise (max 5 words).
- Prefer practical learning taxonomy for AI resources.
- Avoid generic outputs like "Misc".

Resource URL: {resource_url}
Resource title: {title}
Resource description: {description}
"""

REVIEW_RESOURCE_EDITORIAL_PROMPT = """You are preparing a publish-ready AI resource entry with two distinct scopes.

Return STRICT JSON object only:
{{
  "summary": "...",
  "article_body": "..."
}}

Resource metadata
- title: {title}
- source_url: {source_url}
- source_name: {source_name}
- source_published_at: {source_published_at}
- section: {section}
- category: {category}
- subcategory: {subcategory}

Seed teaser (may be weak): {seed_summary}
Seed body (may be weak): {seed_article_body}

Source material:
---
{source_material}
---

Rules:
- `summary` is a teaser for resource cards: 2-3 complete sentences, concise and specific.
- `article_body` is the long-form detail page: 6-9 short paragraphs separated by blank lines.
- Keep each paragraph readable (typically 2-4 sentences) and avoid dense text walls.
- `article_body` must be significantly richer than `summary` and must not repeat it verbatim.
- Cover: context, what the resource contains, who it is for, practical value, and limitations/caveats when present.
- Keep factual and neutral. No speculation. No marketing tone.
- English only. No markdown headings. No bullet lists. No raw URL dumps.
"""

_MONTHS = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
    # Portuguese aliases (no accents to keep parsing ASCII-friendly).
    "janeiro": 1,
    "fevereiro": 2,
    "marco": 3,
    "abril": 4,
    "maio": 5,
    "junho": 6,
    "julho": 7,
    "agosto": 8,
    "setembro": 9,
    "outubro": 10,
    "novembro": 11,
    "dezembro": 12,
}
_MONTH_PATTERN = "|".join(sorted((re.escape(k) for k in _MONTHS.keys()), key=len, reverse=True))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _read_json_body(handler: BaseHTTPRequestHandler) -> dict:
    content_length = int(handler.headers.get("Content-Length", 0))
    if content_length == 0:
        return {}
    raw = handler.rfile.read(content_length)
    return json.loads(raw.decode("utf-8"))


def _send_json(handler: BaseHTTPRequestHandler, data: dict, status: int = 200) -> None:
    body = json.dumps(data, ensure_ascii=False).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def _normalize_article_decision(value: str) -> str:
    aliases = {
        "approve": "approve",
        "approved": "approve",
        "accept": "approve",
        "accepted": "approve",
        "yes": "approve",
        "sim": "approve",
        "ok": "approve",
        "aprovar": "approve",
        "aprovado": "approve",
        "aceitar": "approve",
        "aceite": "approve",
        "reject": "reject",
        "rejected": "reject",
        "decline": "reject",
        "declined": "reject",
        "no": "reject",
        "nao": "reject",
        "não": "reject",
        "rejeitar": "reject",
        "rejeitado": "reject",
        "recusar": "reject",
    }
    return aliases.get(str(value or "").strip().lower(), str(value or "").strip().lower())


def _normalize_content_decision(value: str) -> str:
    decision = str(value or "").strip().lower()
    aliases = {"reject": "request_changes", "changes": "request_changes"}
    return aliases.get(decision, decision)


def _normalize_resource_decision(value: str) -> str:
    aliases = {
        "approve": "approve",
        "approved": "approve",
        "accept": "approve",
        "accepted": "approve",
        "yes": "approve",
        "sim": "approve",
        "ok": "approve",
        "aprovar": "approve",
        "aprovado": "approve",
        "aceitar": "approve",
        "aceite": "approve",
        "reject": "reject",
        "rejected": "reject",
        "decline": "reject",
        "declined": "reject",
        "no": "reject",
        "nao": "reject",
        "não": "reject",
        "rejeitar": "reject",
        "rejeitado": "reject",
        "recusar": "reject",
    }
    return aliases.get(str(value or "").strip().lower(), str(value or "").strip().lower())


def _sig_payload_for_article(data: dict, normalized_decision: str) -> str:
    newsletter_id = data.get("newsletter_id")
    article_index = data.get("article_index")
    nid = "" if newsletter_id is None else str(int(newsletter_id))
    aidx = "" if article_index is None else str(int(article_index))
    source_url = str(data.get("source_url", "")).strip()
    source_url_hash = hashlib.sha256(source_url.encode("utf-8")).hexdigest() if source_url else ""
    return f"article|{normalized_decision}|{nid}|{aidx}|{source_url_hash}"


def _sig_payload_for_content(data: dict, normalized_decision: str) -> str:
    article_id = data.get("article_id")
    aid = "" if article_id is None else str(int(article_id))
    return f"content|{normalized_decision}|{aid}"


def _normalize_url(value: str) -> str:
    url = str(value or "").strip()
    if not url:
        return ""
    if url.startswith(("http://", "https://")):
        return url.rstrip(" \t\r\n.,)")
    return ""


def _normalized_host_from_url(value: str) -> str:
    url = _normalize_url(value)
    if not url:
        return ""
    try:
        host = (urlparse(url).hostname or "").lower().strip(".")
    except Exception:
        return ""
    if host.startswith("www."):
        host = host[4:]
    return host


def _is_generic_source_url(value: str) -> bool:
    url = _normalize_url(value)
    if not url:
        return True
    try:
        parsed = urlparse(url)
    except Exception:
        return True

    host = (parsed.hostname or "").lower().strip(".")
    if host.startswith("www."):
        host = host[4:]
    path = (parsed.path or "").strip().lower()
    query = (parsed.query or "").strip()
    fragment = (parsed.fragment or "").strip()

    if path in {"", "/"} and not query and not fragment:
        return True

    if host == "linkedin.com":
        if "/activity-" in path:
            return False
        if path.startswith(("/posts/", "/feed/update/", "/pulse/", "/article/", "/newsletters/")):
            return False
        return True

    if host == "substack.com" or host.endswith(".substack.com"):
        if path.startswith("/p/") or "/note/" in path:
            return False
        return True

    if path in {"", "/", "/home", "/home/", "/feed", "/feed/", "/search", "/search/", "/explore", "/explore/", "/topics", "/topics/"}:
        return True
    if path.startswith(("/tag/", "/tags/", "/topic/", "/topics/", "/category/", "/categories/")):
        return True
    return False


def _title_from_url(url: str) -> str:
    value = _normalize_url(url)
    if not value:
        return ""
    try:
        parsed = urlparse(value)
    except Exception:
        return ""
    chunks = [chunk for chunk in (parsed.path or "").split("/") if chunk]
    if not chunks:
        return ""
    slug = chunks[-1]
    slug = re.sub(r"\.[A-Za-z0-9]{2,6}$", "", slug)
    slug = slug.replace("-", " ").replace("_", " ").strip()
    slug = re.sub(r"\s+", " ", slug)
    if not slug:
        return ""
    if len(slug) < 4:
        return ""
    return slug[:200].title()


def _source_name_from_url(url: str) -> str:
    value = _normalize_url(url)
    if not value:
        return ""
    try:
        host = (urlparse(value).hostname or "").lower().strip(".")
    except Exception:
        return ""
    if not host:
        return ""
    if host.startswith("www."):
        host = host[4:]
    chunks = [chunk for chunk in host.split(".") if chunk]
    if not chunks:
        return ""
    if len(chunks) >= 3 and chunks[-1] in {"uk", "pt", "br", "au", "jp", "in", "es", "fr", "de", "it", "nl"} and chunks[-2] in {"co", "com", "org", "gov", "edu"}:
        base = chunks[-3]
    elif len(chunks) >= 2 and chunks[-2] in {"co", "com", "org", "net", "gov", "edu"}:
        base = chunks[-3] if len(chunks) >= 3 else chunks[-2]
    elif len(chunks) >= 2:
        base = chunks[-2]
    else:
        base = chunks[0]
    return base.replace("-", " ").replace("_", " ").strip().title()


def _sig_payload_for_resource(data: dict, action: str = "submit") -> str:
    resource_url = _normalize_url(str(data.get("resource_url", "")).strip())
    resource_hash = hashlib.sha256(resource_url.encode("utf-8")).hexdigest() if resource_url else ""
    return f"resource|{action}|{resource_hash}"


def _sig_payload_for_resource_decision(data: dict, normalized_decision: str) -> str:
    resource_id = data.get("resource_id")
    rid = "" if resource_id is None else str(int(resource_id))
    return f"resource|{normalized_decision}|{rid}"


def _as_bool(value: Any, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _parse_json_object(raw: str) -> dict[str, Any]:
    text = str(raw or "").strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        pass
    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        try:
            parsed = json.loads(text[start : end + 1])
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def _iso_from_parts(year: int, month: int, day: int = 1) -> str:
    try:
        dt = datetime(year, month, day, 0, 0, 0, tzinfo=timezone.utc)
    except Exception:
        return ""
    return dt.isoformat()


def _parse_date_string(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    try:
        from process_newsletter import _normalize_datetime

        normalized = _normalize_datetime(raw)
        if normalized:
            return normalized
    except Exception:
        pass

    m = re.search(fr"\b({_MONTH_PATTERN})\s+(\d{{4}})\b", raw, flags=re.IGNORECASE)
    if m:
        month = _MONTHS.get(m.group(1).lower(), 0)
        year = int(m.group(2))
        if month > 0:
            return _iso_from_parts(year, month, 1)

    m = re.search(fr"\b({_MONTH_PATTERN})\s+(\d{{1,2}}),?\s+(\d{{4}})\b", raw, flags=re.IGNORECASE)
    if m:
        month = _MONTHS.get(m.group(1).lower(), 0)
        day = int(m.group(2))
        year = int(m.group(3))
        if month > 0:
            return _iso_from_parts(year, month, day)

    m = re.search(fr"\b(\d{{1,2}})\s+de\s+({_MONTH_PATTERN})\s+de\s+(\d{{4}})\b", raw, flags=re.IGNORECASE)
    if m:
        day = int(m.group(1))
        month = _MONTHS.get(m.group(2).lower(), 0)
        year = int(m.group(3))
        if month > 0:
            return _iso_from_parts(year, month, day)

    m = re.search(fr"\b(\d{{1,2}})\s+({_MONTH_PATTERN})\s+(\d{{4}})\b", raw, flags=re.IGNORECASE)
    if m:
        day = int(m.group(1))
        month = _MONTHS.get(m.group(2).lower(), 0)
        year = int(m.group(3))
        if month > 0:
            return _iso_from_parts(year, month, day)

    m = re.search(r"\b(\d{4})\b", raw)
    if m:
        return _iso_from_parts(int(m.group(1)), 1, 1)

    return ""


def _extract_date_candidates_from_text(text: str) -> list[dict[str, Any]]:
    value = str(text or "")
    if not value:
        return []

    patterns = [
        re.compile(
            fr"\b({_MONTH_PATTERN})\s+\d{{4}}\b",
            flags=re.IGNORECASE,
        ),
        re.compile(
            fr"\b({_MONTH_PATTERN})\s+\d{{1,2}},?\s+\d{{4}}\b",
            flags=re.IGNORECASE,
        ),
        re.compile(r"\b\d{4}-\d{2}-\d{2}\b"),
        re.compile(fr"\b\d{{1,2}}\s+(?:{_MONTH_PATTERN})\s+\d{{4}}\b", flags=re.IGNORECASE),
        re.compile(fr"\b\d{{1,2}}\s+de\s+(?:{_MONTH_PATTERN})\s+de\s+\d{{4}}\b", flags=re.IGNORECASE),
    ]

    scored: dict[str, dict[str, Any]] = {}
    lower = value.lower()
    for pattern in patterns:
        for match in pattern.finditer(value):
            raw = match.group(0)
            normalized = _parse_date_string(raw)
            if not normalized:
                continue

            start, end = match.span()
            ctx = lower[max(0, start - 35) : min(len(lower), end + 35)]

            score = 1
            if "last updated" in ctx:
                score += 16
            elif "updated" in ctx:
                score += 8
            if "published" in ctx or "publication" in ctx or "released" in ctx or "release date" in ctx:
                score += 5
            if "estimated" in ctx or "estimate" in ctx:
                score -= 8
            if "meap began" in ctx or "isbn" in ctx or "pages" in ctx:
                score -= 10
            elif "began" in ctx:
                score -= 4
            if "summer" in ctx or "spring" in ctx or "fall" in ctx or "winter" in ctx:
                score -= 6

            prev = scored.get(normalized)
            if prev is None:
                scored[normalized] = {
                    "value": normalized,
                    "score": score,
                    "hits": 1,
                    "raw_sample": raw,
                }
            else:
                prev["score"] = int(prev.get("score", 0)) + score
                prev["hits"] = int(prev.get("hits", 0)) + 1

    ranked = sorted(
        scored.values(),
        key=lambda row: (int(row.get("score", 0)), int(row.get("hits", 0)), str(row.get("value", ""))),
        reverse=True,
    )
    return ranked


def _clean_taxonomy_label(value: str) -> str:
    text = " ".join(str(value or "").split()).strip()
    text = text.strip(" /|-")
    if not text:
        return ""
    return text[:120]


def _extract_source_taxonomy(text: str) -> dict[str, str]:
    raw = str(text or "")
    if not raw:
        return {}

    pattern = re.compile(
        r"catalog\s*/\s*([^/\n]{2,80})\s*/\s*([^/\n]{2,80})\s*/\s*([^/\n]{2,80})",
        flags=re.IGNORECASE,
    )
    m = pattern.search(raw)
    if not m:
        return {}

    section = _clean_taxonomy_label(m.group(1))
    category = _clean_taxonomy_label(m.group(2))
    subcategory = _clean_taxonomy_label(m.group(3))
    if not section or not category or not subcategory:
        return {}
    return {
        "section": section,
        "category": category,
        "subcategory": subcategory,
    }


def _same_host_family(host_a: str, host_b: str) -> bool:
    a = str(host_a or "").lower().strip(".")
    b = str(host_b or "").lower().strip(".")
    if not a or not b:
        return False
    return a == b or a.endswith(f".{b}") or b.endswith(f".{a}")


def _add_date_vote(
    votes: dict[str, dict[str, Any]],
    *,
    value: str,
    score: int,
    evidence: str,
    same_host: bool = False,
) -> None:
    date_value = str(value or "").strip()
    if not date_value:
        return
    item = votes.get(date_value)
    if item is None:
        votes[date_value] = {
            "value": date_value,
            "score": int(score),
            "votes": 1,
            "same_host_votes": 1 if same_host else 0,
            "evidence": [evidence] if evidence else [],
        }
        return
    item["score"] = int(item.get("score", 0)) + int(score)
    item["votes"] = int(item.get("votes", 0)) + 1
    if same_host:
        item["same_host_votes"] = int(item.get("same_host_votes", 0)) + 1
    if evidence and evidence not in item.get("evidence", []) and len(item.get("evidence", [])) < 5:
        item["evidence"].append(evidence)


def _discover_resource_source_date(
    cfg: Config,
    *,
    resource_url: str,
    title: str,
) -> dict[str, Any]:
    normalized_url = _normalize_url(resource_url)
    try:
        source_host = (urlparse(normalized_url).hostname or "").lower()
    except Exception:
        source_host = ""

    queries: list[str] = []
    if title:
        queries.extend([title, f"\"{title}\"", f"{title} release date"])
    if normalized_url:
        queries.append(normalized_url)
    if title and source_host:
        queries.insert(0, f"site:{source_host} {title}")

    deduped_queries: list[str] = []
    seen_queries: set[str] = set()
    for query in queries:
        candidate = str(query or "").strip()
        if not candidate:
            continue
        key = candidate.casefold()
        if key in seen_queries:
            continue
        seen_queries.add(key)
        deduped_queries.append(candidate)

    if not deduped_queries:
        return {"attempted": False, "source_published_at": "", "origin": "", "candidates": []}

    votes: dict[str, dict[str, Any]] = {}
    fetch_scores: dict[str, int] = {}

    for q_idx, query in enumerate(deduped_queries[:4]):
        try:
            resp = requests.get(
                f"{cfg.searxng_url.rstrip('/')}/search",
                params={"q": query, "format": "json"},
                timeout=12,
            )
            resp.raise_for_status()
            payload = resp.json()
        except Exception as exc:
            LOG.warning("resource date discovery search failed query=%r err=%s", query, exc)
            continue

        rows = payload.get("results", []) if isinstance(payload, dict) else []
        if not isinstance(rows, list):
            continue

        for idx, row in enumerate(rows[:10]):
            if not isinstance(row, dict):
                continue
            result_url = _normalize_url(str(row.get("url") or row.get("link") or "").strip())
            if not result_url:
                continue
            result_title = str(row.get("title", "")).strip()
            snippet = str(row.get("content", "")).strip()
            host = ""
            try:
                host = (urlparse(result_url).hostname or "").lower()
            except Exception:
                host = ""
            is_same_host = bool(source_host and _same_host_family(host, source_host))

            token_overlap = 0
            title_tokens = _tokenize_title(title)
            if title_tokens:
                hay = f"{result_title}\n{snippet}".lower()
                token_overlap = sum(1 for token in title_tokens if token in hay)
            overlap_ratio = (token_overlap / max(1, len(title_tokens))) if title_tokens else 0.0

            if source_host and (not is_same_host) and overlap_ratio < 0.6:
                continue

            for field_name in ("publishedDate", "published_date", "date", "published"):
                raw_value = str(row.get(field_name, "")).strip()
                normalized = _parse_date_string(raw_value)
                if not normalized:
                    continue
                score = 24 - (idx * 2) - q_idx
                if is_same_host:
                    score += 8
                else:
                    score -= 4
                _add_date_vote(
                    votes,
                    value=normalized,
                    score=max(1, score),
                    evidence=f"searx:{field_name}:{result_url}",
                    same_host=is_same_host,
                )

            candidate_text = "\n".join(part for part in (result_title, snippet) if part)
            for rank, cand in enumerate(_extract_date_candidates_from_text(candidate_text)[:2]):
                score = int(cand.get("score", 0)) + max(0, 10 - idx - (rank * 2))
                if is_same_host:
                    score += 6
                else:
                    score -= 3
                _add_date_vote(
                    votes,
                    value=str(cand.get("value", "")),
                    score=max(1, score),
                    evidence=f"snippet:{result_url}",
                    same_host=is_same_host,
                )

            fetch_score = 20 - idx - q_idx
            if is_same_host:
                fetch_score += 12
            else:
                fetch_score -= 4
                if overlap_ratio >= 0.8:
                    fetch_score += 4
            prev_score = fetch_scores.get(result_url)
            if prev_score is None or fetch_score > prev_score:
                fetch_scores[result_url] = fetch_score

    ranked_fetch_urls = [
        url
        for url, _score in sorted(fetch_scores.items(), key=lambda item: int(item[1]), reverse=True)
        if int(_score) > 8
    ]

    for idx, url in enumerate(ranked_fetch_urls[:3]):
        try:
            meta = _extract_resource_source_metadata(url)
        except Exception as exc:
            LOG.warning("resource date discovery fetch failed url=%s err=%s", url, exc)
            continue
        from_source = str(meta.get("source_published_at", "")).strip()
        if not from_source:
            continue
        host = ""
        try:
            host = (urlparse(url).hostname or "").lower()
        except Exception:
            host = ""
        score = 28 - (idx * 3)
        is_same_host = bool(source_host and _same_host_family(host, source_host))
        if is_same_host:
            score += 10
        _add_date_vote(votes, value=from_source, score=score, evidence=f"page:{url}", same_host=is_same_host)

    ranked = sorted(
        votes.values(),
        key=lambda row: (
            int(row.get("same_host_votes", 0)),
            int(row.get("votes", 0)),
            int(row.get("score", 0)),
            str(row.get("value", "")),
        ),
        reverse=True,
    )
    if source_host:
        same_host_ranked = [row for row in ranked if int(row.get("same_host_votes", 0)) > 0]
        if same_host_ranked:
            ranked = same_host_ranked
    selected = str(ranked[0].get("value", "")).strip() if ranked else ""
    origin = ""
    if selected:
        top_votes = int(ranked[0].get("votes", 0))
        top_same_host_votes = int(ranked[0].get("same_host_votes", 0))
        if top_votes >= 2 and (not source_host or top_same_host_votes > 0):
            origin = "web_consensus"
        else:
            origin = "web_best_effort"

    return {
        "attempted": True,
        "source_published_at": selected,
        "origin": origin,
        "candidates": ranked[:3],
    }


def _collect_resource_web_snippets(cfg: Config, *, title: str, resource_url: str) -> str:
    normalized_url = _normalize_url(resource_url)
    try:
        source_host = (urlparse(normalized_url).hostname or "").lower()
    except Exception:
        source_host = ""

    queries: list[str] = []
    if title:
        queries.extend([title, f"\"{title}\""])
    if source_host and title:
        queries.insert(0, f"site:{source_host} {title}")
    if normalized_url:
        queries.append(normalized_url)

    dedup_queries: list[str] = []
    seen_queries: set[str] = set()
    for query in queries:
        q = str(query or "").strip()
        if not q:
            continue
        key = q.casefold()
        if key in seen_queries:
            continue
        seen_queries.add(key)
        dedup_queries.append(q)

    if not dedup_queries:
        return ""

    chunks: list[str] = []
    seen_chunks: set[str] = set()

    for q_idx, query in enumerate(dedup_queries[:4]):
        try:
            resp = requests.get(
                f"{cfg.searxng_url.rstrip('/')}/search",
                params={"q": query, "format": "json"},
                timeout=12,
            )
            resp.raise_for_status()
            payload = resp.json()
        except Exception as exc:
            LOG.warning("resource snippet discovery search failed query=%r err=%s", query, exc)
            continue

        rows = payload.get("results", []) if isinstance(payload, dict) else []
        if not isinstance(rows, list):
            continue

        for idx, row in enumerate(rows[:10]):
            if not isinstance(row, dict):
                continue
            result_url = _normalize_url(str(row.get("url") or row.get("link") or "").strip())
            if not result_url:
                continue
            result_title = " ".join(str(row.get("title", "")).split()).strip()
            snippet = " ".join(str(row.get("content", "")).split()).strip()
            if len(snippet) < 80:
                continue

            try:
                host = (urlparse(result_url).hostname or "").lower()
            except Exception:
                host = ""
            if source_host and host and (not _same_host_family(host, source_host)):
                title_tokens = _tokenize_title(title)
                hay = f"{result_title}\n{snippet}".lower()
                overlap = sum(1 for token in title_tokens if token in hay) if title_tokens else 0
                overlap_ratio = overlap / max(1, len(title_tokens)) if title_tokens else 0.0
                if overlap_ratio < 0.6:
                    continue

            chunk = f"{result_title}\n{snippet}\nSource: {result_url}".strip()
            key = chunk.casefold()
            if key in seen_chunks:
                continue
            seen_chunks.add(key)
            chunks.append(chunk)
            if len(chunks) >= 10:
                break
        if len(chunks) >= 10:
            break

    text = "\n\n".join(chunks).strip()
    return text[:14000]


def _verify_signature(data: dict, *, payload_prefix: str) -> tuple[dict, int] | None:
    if not REVIEW_SIGNATURE_REQUIRED:
        return None
    if not REVIEW_SIGNATURE_SECRET:
        return {"error": "signature validation misconfigured: REVIEW_SIGNATURE_SECRET missing"}, 500

    sig = str(data.get("sig", "")).strip().lower()
    nonce = str(data.get("sig_nonce", "")).strip()
    ts_raw = data.get("sig_ts")

    if not sig or not nonce or ts_raw is None:
        return {"error": "signature required"}, 401
    if not re.fullmatch(r"[a-f0-9]{64}", sig):
        return {"error": "invalid signature format"}, 401
    if not re.fullmatch(r"[A-Za-z0-9_-]{8,80}", nonce):
        return {"error": "invalid nonce format"}, 401

    try:
        ts = int(ts_raw)
    except (TypeError, ValueError):
        return {"error": "invalid sig_ts"}, 401

    now = int(time.time())
    max_age = max(30, REVIEW_SIGNATURE_MAX_AGE_SECONDS)
    if ts > now + 5 or now - ts > max_age:
        return {"error": "signature expired"}, 401

    canonical = f"{payload_prefix}|{ts}|{nonce}"
    expected = hmac.new(
        REVIEW_SIGNATURE_SECRET.encode("utf-8"),
        canonical.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    if not hmac.compare_digest(expected, sig):
        return {"error": "invalid signature"}, 401

    nonce_key = f"{payload_prefix}:{nonce}"
    with _NONCE_LOCK:
        expired = [k for k, expiry in _SEEN_NONCES.items() if expiry <= now]
        for key in expired:
            _SEEN_NONCES.pop(key, None)
        if nonce_key in _SEEN_NONCES:
            return {"error": "signature replay detected"}, 409
        _SEEN_NONCES[nonce_key] = now + max_age

    return None


def _queue_notification_after_response(sender_fn, *, context: str) -> dict:
    delay = max(0.0, REVIEW_NEXT_NOTIFICATION_DELAY_SECONDS)

    def _runner() -> None:
        if delay > 0:
            time.sleep(delay)
        try:
            result = sender_fn()
            LOG.info("deferred next notification sent context=%s result=%s", context, result)
        except Exception as exc:
            LOG.warning("deferred next notification failed context=%s err=%s", context, exc)

    threading.Thread(
        target=_runner,
        name=f"deferred-next-notification:{context}",
        daemon=True,
    ).start()

    return {
        "sent": False,
        "queued": True,
        "reason": "deferred_after_decision_response",
        "delay_seconds": delay,
    }


def _tokenize_title(value: str) -> list[str]:
    stop = {
        "the", "and", "for", "with", "from", "this", "that", "into", "new", "how",
        "uma", "com", "para", "de", "da", "do", "das", "dos", "que", "em",
    }
    out: list[str] = []
    for token in re.findall(r"[A-Za-z0-9]+", str(value or "").lower()):
        if len(token) < 3 or token in stop:
            continue
        out.append(token)
    return out


def _score_source_candidate(*, article_title: str, url: str, result_title: str, snippet: str) -> int:
    if _is_generic_source_url(url):
        return -10_000
    try:
        parsed = urlparse(url)
    except Exception:
        return -10_000
    host = (parsed.hostname or "").lower()
    path = (parsed.path or "").lower()
    if not host:
        return -10_000

    blocked_hosts = {"smry.ai", "removepaywalls.com", "substackcdn.com"}
    if any(host == b or host.endswith(f".{b}") for b in blocked_hosts):
        return -10_000
    if "unsubscribe" in path or "notification/unsubscribe" in path:
        return -10_000

    score = 0
    if "substack.com" in host:
        score += 30
        if path.startswith("/p/"):
            score += 25
        if "/note/" in path:
            score += 18
    if "linkedin.com" in host:
        score += 18
        if "/posts/" in path or "/activity-" in path:
            score += 8

    title_tokens = _tokenize_title(article_title)
    hay = f"{result_title}\n{snippet}".lower()
    overlap = sum(1 for t in title_tokens if t in hay)
    score += overlap * 4
    if title_tokens:
        ratio = overlap / max(1, len(title_tokens))
        if ratio >= 0.6:
            score += 20
        elif ratio >= 0.4:
            score += 10

    return score


def _discover_source_url(cfg: Config, article: dict[str, Any]) -> dict[str, Any]:
    title = str(article.get("title", "")).strip()
    if not title:
        return {"attempted": False, "selected_url": "", "reason": "missing_title", "candidates": []}

    preferred_host = _normalized_host_from_url(str(article.get("original_url", "")).strip())
    queries = [
        title,
        f"{title} substack",
        f"{title} linkedin",
    ]
    scored: dict[str, dict[str, Any]] = {}

    # Prefer URLs already surfaced in pipeline enrichment context.
    context_text = str(article.get("enrichment_context", "")).strip()
    for url in re.findall(r"https?://[^\s\]\)\"'<>]+", context_text):
        score = _score_source_candidate(
            article_title=title,
            url=url,
            result_title="",
            snippet=context_text,
        )
        candidate_host = _normalized_host_from_url(url)
        if preferred_host and candidate_host:
            if candidate_host == preferred_host:
                score += 35
            else:
                score -= 15
        if score < REVIEW_AUTO_SOURCE_MIN_SCORE:
            continue
        try:
            parsed = urlparse(url)
            host = (parsed.hostname or "").lower()
            path = (parsed.path or "").lower()
        except Exception:
            host = ""
            path = ""
        score += 24
        if "substack.com" in host and "/note/" in path:
            score += 24
        elif "substack.com" in host and path.startswith("/p/"):
            score += 20
        elif "linkedin.com" in host and ("/posts/" in path or "/activity-" in path):
            score += 18
        prev = scored.get(url)
        if prev is None or int(prev.get("score", -10_000)) < score:
            scored[url] = {
                "url": url,
                "score": score,
                "title": "context_candidate",
                "query": "enrichment_context",
            }

    for query in queries:
        try:
            resp = requests.get(
                f"{cfg.searxng_url.rstrip('/')}/search",
                params={"q": query, "format": "json"},
                timeout=12,
            )
            resp.raise_for_status()
            payload = resp.json()
        except Exception as exc:
            LOG.warning("auto source discovery search failed query=%r err=%s", query, exc)
            continue

        rows = payload.get("results", []) if isinstance(payload, dict) else []
        if not isinstance(rows, list):
            continue

        for row in rows[:8]:
            if not isinstance(row, dict):
                continue
            url = str(row.get("url") or row.get("link") or "").strip()
            if not url.startswith(("http://", "https://")):
                continue
            result_title = str(row.get("title", "")).strip()
            snippet = str(row.get("content", "")).strip()
            score = _score_source_candidate(
                article_title=title,
                url=url,
                result_title=result_title,
                snippet=snippet,
            )
            candidate_host = _normalized_host_from_url(url)
            if preferred_host and candidate_host:
                if candidate_host == preferred_host:
                    score += 35
                else:
                    score -= 15
            if score < REVIEW_AUTO_SOURCE_MIN_SCORE:
                continue
            prev = scored.get(url)
            if prev is None or int(prev.get("score", -10_000)) < score:
                scored[url] = {
                    "url": url,
                    "score": score,
                    "title": result_title,
                    "query": query,
                }

    ranked = sorted(scored.values(), key=lambda r: int(r.get("score", 0)), reverse=True)
    selected = str(ranked[0]["url"]) if ranked else ""

    # Validate discovered URLs for actual content before selecting one.
    # If top-scored link is a 404/empty page, try next candidates.
    try:
        from process_newsletter import _get_source_snapshot, source_snapshot_title_match  # Lazy import.
    except Exception:
        _get_source_snapshot = None  # type: ignore[assignment]
        source_snapshot_title_match = None  # type: ignore[assignment]

    validated_candidates: list[tuple[str, int, bool]] = []
    if _get_source_snapshot is not None:
        for row in ranked[:8]:
            candidate_url = _normalize_url(str(row.get("url", "")).strip())
            if not candidate_url:
                continue
            try:
                snapshot = _get_source_snapshot(
                    candidate_url,
                    cfg.source_rewrite_max_chars,
                    cfg.source_open_min_chars,
                )
            except Exception:
                continue
            text = str(snapshot.get("text", "")).strip()
            chars = int(snapshot.get("chars", 0) or 0)
            error = str(snapshot.get("error", "")).strip()
            if error:
                continue
            if chars < max(120, int(cfg.source_presence_min_chars)):
                continue
            if "not found" in text[:1000].lower():
                continue
            if source_snapshot_title_match is not None:
                semantic = source_snapshot_title_match(
                    title,
                    snapshot,
                    min_overlap=float(getattr(cfg, "source_title_overlap_min_ratio", 0.35)),
                )
                if not bool(semantic.get("ok")):
                    continue
            resolved_url = _normalize_url(str(snapshot.get("url", "")).strip()) or candidate_url
            resolved_host = _normalized_host_from_url(resolved_url)
            is_preferred = bool(preferred_host and resolved_host == preferred_host)
            row_score = int(row.get("score", 0) or 0)
            semantic_ratio = float(semantic.get("overlap_ratio", 0.0) or 0.0) if source_snapshot_title_match is not None else 0.0
            final_score = row_score + int(semantic_ratio * 100)
            if is_preferred:
                final_score += 25
            validated_candidates.append((resolved_url, final_score, is_preferred))

    validated_selected = ""
    if validated_candidates:
        preferred_rows = [row for row in validated_candidates if row[2]]
        if preferred_rows:
            preferred_rows.sort(key=lambda row: row[1], reverse=True)
            validated_selected = preferred_rows[0][0]
        else:
            validated_candidates.sort(key=lambda row: row[1], reverse=True)
            validated_selected = validated_candidates[0][0]

    if validated_selected:
        selected = validated_selected
        reason = "ok"
    elif selected:
        selected = ""
        reason = "candidate_unavailable"
    else:
        reason = "not_found"
    return {
        "attempted": True,
        "selected_url": selected,
        "reason": reason,
        "candidates": ranked[:8],
    }


def _extract_resource_source_metadata(resource_url: str) -> dict[str, Any]:
    normalized = _normalize_url(resource_url)
    if not normalized:
        return {
            "final_url": "",
            "title": "",
            "summary": "",
            "article_body": "",
            "description": "",
            "image_url": "",
            "source_text_excerpt": "",
            "source_published_at": "",
            "source_published_at_origin": "",
            "source_taxonomy": {},
        }

    try:
        resp = requests.get(
            normalized,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
                )
            },
            allow_redirects=True,
            timeout=25,
        )
        resp.raise_for_status()
    except Exception as exc:
        LOG.warning("resource metadata fetch failed url=%s err=%s", normalized, exc)
        return {
            "final_url": normalized,
            "title": "",
            "summary": "",
            "article_body": "",
            "description": "",
            "image_url": "",
            "source_text_excerpt": "",
            "source_published_at": "",
            "source_published_at_origin": "",
            "source_taxonomy": {},
        }

    final_url = _normalize_url(resp.url) or normalized
    soup = BeautifulSoup(resp.text or "", "lxml")

    def _meta(*queries: tuple[str, str]) -> str:
        for attr_name, attr_value in queries:
            tag = soup.find("meta", attrs={attr_name: attr_value})
            if tag and tag.get("content"):
                value = str(tag.get("content", "")).strip()
                if value:
                    return value
        return ""

    title = (
        _meta(("property", "og:title"), ("name", "og:title"), ("name", "twitter:title"))
        or str(soup.title.get_text(" ", strip=True) if soup.title else "").strip()
    )
    description = _meta(
        ("property", "og:description"),
        ("name", "og:description"),
        ("name", "description"),
        ("name", "twitter:description"),
    )

    def _compact_ws(value: str) -> str:
        return " ".join(str(value or "").split()).strip()

    def _collect_blocks(limit: int = 8) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()

        def _looks_like_keyword_dump(text: str) -> bool:
            words = re.findall(r"[A-Za-z][A-Za-z0-9+.-]*", text)
            if len(words) < 10:
                return False
            punctuation_hits = len(re.findall(r"[.!?]", text))
            title_like = sum(1 for word in words if word[:1].isupper() or word.isupper())
            lower_like = sum(1 for word in words if word[:1].islower())
            title_ratio = title_like / max(1, len(words))
            lower_ratio = lower_like / max(1, len(words))
            if punctuation_hits <= 1 and title_ratio > 0.65:
                return True
            if lower_ratio < 0.25 and title_ratio > 0.55:
                return True
            return False

        for tag in soup.find_all(["p", "li"], limit=220):
            text = _compact_ws(tag.get_text(" ", strip=True))
            if len(text) < 60:
                continue
            if not re.search(r"[.!?]", text):
                # Ignore menu-like token lists that are not prose.
                continue
            low = text.lower()
            if any(noise in low for noise in ("cookie", "privacy", "terms", "sign in", "subscribe", "javascript")):
                continue
            if _looks_like_keyword_dump(text):
                continue
            key = text.casefold()
            if key in seen:
                continue
            seen.add(key)
            out.append(text)
            if len(out) >= limit:
                break
        return out

    def _make_summary(primary: str, blocks: list[str]) -> str:
        base = _compact_ws(primary)
        if not base and blocks:
            base = blocks[0]
        if not base:
            return ""
        sentences = [s.strip() for s in re.split(r"(?<=[.!?])\s+", base) if s.strip()]
        if not sentences:
            return base[:320]
        chosen: list[str] = []
        total = 0
        for sentence in sentences:
            if total >= 260:
                break
            chosen.append(sentence)
            total += len(sentence) + 1
            if len(chosen) >= 2:
                break
        summary = " ".join(chosen).strip() or base[:320]
        if summary and summary[-1] not in ".!?":
            summary = f"{summary}."
        return summary[:360]

    blocks = _collect_blocks(limit=10)
    summary = _make_summary(description, blocks)
    if not summary and blocks:
        summary = _make_summary(blocks[0], blocks)
    article_body = "\n\n".join(blocks[:6]).strip()
    if article_body:
        words = re.findall(r"[A-Za-z][A-Za-z0-9+.-]*", article_body)
        punctuation_hits = len(re.findall(r"[.!?]", article_body))
        title_like = sum(1 for word in words if word[:1].isupper() or word.isupper())
        title_ratio = (title_like / max(1, len(words))) if words else 0.0
        if punctuation_hits <= 1 and title_ratio > 0.6:
            article_body = ""
    if not article_body and summary:
        article_body = summary
    if not description:
        description = summary
    image = _meta(("property", "og:image"), ("name", "og:image"), ("name", "twitter:image"))
    image_url = _normalize_url(image)
    if not image_url and image:
        image_url = _normalize_url(urljoin(final_url, image))
    source_published_at = ""
    source_published_at_origin = ""
    try:
        from process_newsletter import _extract_published_at_from_soup

        source_published_at = str(_extract_published_at_from_soup(soup, final_url) or "").strip()
        if source_published_at:
            source_published_at_origin = "source_structured"
    except Exception as exc:
        LOG.warning("resource source date extraction failed url=%s err=%s", final_url, exc)

    page_text = ""
    try:
        page_text = soup.get_text("\n", strip=True)[:140000]
    except Exception:
        page_text = ""

    source_text_excerpt = ""
    if blocks:
        source_text_excerpt = "\n\n".join(blocks[:10]).strip()
    if description:
        desc = _compact_ws(description)
        if desc and desc.casefold() not in source_text_excerpt.casefold():
            source_text_excerpt = f"{desc}\n\n{source_text_excerpt}".strip()
    if len(source_text_excerpt) < 700 and page_text:
        extra_chunks: list[str] = []
        seen_chunks: set[str] = set()
        for raw_line in page_text.splitlines():
            line = _compact_ws(raw_line)
            if len(line) < 70:
                continue
            if not re.search(r"[.!?]", line):
                continue
            low = line.lower()
            if any(noise in low for noise in ("cookie", "privacy", "terms", "sign in", "subscribe")):
                continue
            key = line.casefold()
            if key in seen_chunks:
                continue
            seen_chunks.add(key)
            extra_chunks.append(line)
            if len(extra_chunks) >= 12:
                break
        if extra_chunks:
            merged = "\n\n".join(extra_chunks).strip()
            source_text_excerpt = f"{source_text_excerpt}\n\n{merged}".strip() if source_text_excerpt else merged
    source_text_excerpt = source_text_excerpt[:14000]

    if not source_published_at:
        candidates = _extract_date_candidates_from_text(page_text)
        if candidates:
            source_published_at = str(candidates[0].get("value", "")).strip()
            if source_published_at:
                source_published_at_origin = "source_text"

    source_taxonomy = _extract_source_taxonomy(page_text)

    return {
        "final_url": final_url,
        "title": title[:500],
        "summary": summary,
        "article_body": article_body,
        "description": description,
        "image_url": image_url,
        "source_text_excerpt": source_text_excerpt,
        "source_published_at": source_published_at,
        "source_published_at_origin": source_published_at_origin,
        "source_taxonomy": source_taxonomy,
    }


def _fallback_resource_taxonomy(title: str, description: str, resource_url: str) -> dict[str, str]:
    hay = f"{title} {description} {resource_url}".lower()
    section = "Learning Resources"
    category = "General AI"
    subcategory = "General"

    if any(token in hay for token in ("book", "ebook", "manning.com/books", "oreilly", "packtpub")):
        category = "Books"
        subcategory = "AI & ML Books"
    elif any(token in hay for token in ("course", "tutorial", "bootcamp", "curriculum")):
        category = "Courses"
        subcategory = "Online Courses"
    elif any(token in hay for token in ("github.com", "repository", "open source", "toolkit")):
        category = "Tools"
        subcategory = "Open Source"
    elif any(token in hay for token in ("paper", "arxiv", "research")):
        category = "Research"
        subcategory = "Papers"

    if "reasoning" in hay:
        subcategory = "Reasoning Models"
    elif "agent" in hay:
        subcategory = "AI Agents"
    elif "llm" in hay or "language model" in hay:
        subcategory = "LLMs"

    return {
        "section": section,
        "category": category,
        "subcategory": subcategory,
        "classification_origin": "fallback",
    }


def _classify_resource_taxonomy(cfg: Config, *, title: str, description: str, resource_url: str) -> dict[str, str]:
    from process_newsletter import _llm_generate

    prompt = REVIEW_RESOURCE_CLASSIFICATION_PROMPT.format(
        resource_url=resource_url[:1000],
        title=(title or "")[:500],
        description=(description or "")[:1200],
    )
    try:
        raw = _llm_generate(cfg, prompt)
    except Exception as exc:
        LOG.warning("resource taxonomy llm failed url=%s err=%s", resource_url, exc)
        return _fallback_resource_taxonomy(title, description, resource_url)

    parsed = _parse_json_object(raw)
    section = str(parsed.get("section", "")).strip()[:120]
    category = str(parsed.get("category", "")).strip()[:120]
    subcategory = str(parsed.get("subcategory", "")).strip()[:120]
    if not section or not category or not subcategory:
        return _fallback_resource_taxonomy(title, description, resource_url)
    return {
        "section": section,
        "category": category,
        "subcategory": subcategory,
        "classification_origin": "llm",
    }


def _resource_token_set(text: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[A-Za-z][A-Za-z0-9+._-]{2,}", str(text or "").lower())
        if len(token) >= 3
    }


def _resource_editorial_is_weak(summary: str, article_body: str) -> bool:
    normalized_summary = " ".join(str(summary or "").split()).strip()
    normalized_body = " ".join(str(article_body or "").split()).strip()
    if not normalized_body:
        return True
    if normalized_summary and normalized_body.casefold() == normalized_summary.casefold():
        return True
    if len(normalized_body) < 420:
        return True

    paragraphs = [p.strip() for p in re.split(r"\n\s*\n+", str(article_body or "").strip()) if p.strip()]
    if len(paragraphs) < 5:
        return True

    if normalized_summary:
        summary_tokens = _resource_token_set(normalized_summary)
        body_tokens = _resource_token_set(normalized_body)
        if summary_tokens:
            overlap = len(summary_tokens & body_tokens) / max(1, len(summary_tokens))
            if overlap > 0.95 and len(normalized_body) < 900:
                return True
        if normalized_body.startswith(normalized_summary):
            if len(normalized_body) - len(normalized_summary) < 260:
                return True
    return False


def _format_resource_article_body_for_readability(article_body: str) -> str:
    raw = str(article_body or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    if not raw:
        return ""

    blocks = [re.sub(r"\s+", " ", block).strip() for block in re.split(r"\n\s*\n+", raw) if block.strip()]
    if not blocks:
        return ""

    formatted: list[str] = []
    seen: set[str] = set()
    for block in blocks:
        sentences = [s.strip() for s in re.split(r"(?<=[.!?])\s+", block) if s.strip()]
        if not sentences:
            sentences = [block]
        chunk: list[str] = []
        chunk_len = 0
        for sentence in sentences:
            chunk.append(sentence)
            chunk_len += len(sentence) + 1
            # Keep paragraphs compact: 2-3 sentences or ~230 chars.
            if len(chunk) >= 2 and (chunk_len >= 230 or len(chunk) >= 3):
                paragraph = " ".join(chunk).strip()
                key = paragraph.casefold()
                if paragraph and key not in seen:
                    seen.add(key)
                    formatted.append(paragraph)
                chunk = []
                chunk_len = 0
        if chunk:
            paragraph = " ".join(chunk).strip()
            key = paragraph.casefold()
            if paragraph and key not in seen:
                seen.add(key)
                formatted.append(paragraph)

    if not formatted:
        return ""
    return "\n\n".join(formatted[:10])


def _synthesize_resource_editorial(
    cfg: Config,
    *,
    title: str,
    source_url: str,
    source_name: str,
    source_published_at: str,
    section: str,
    category: str,
    subcategory: str,
    seed_summary: str,
    seed_article_body: str,
    source_material: str,
) -> dict[str, str]:
    from process_newsletter import (
        _assert_required_summary_model,
        _llm_generate,
        _normalize_article_body_text,
        _normalize_summary_text,
    )

    material = str(source_material or "").strip()
    if len(material) < REVIEW_RESOURCE_EDITORIAL_MIN_SOURCE_CHARS:
        return {}

    prompt = REVIEW_RESOURCE_EDITORIAL_PROMPT.format(
        title=str(title or "")[:500],
        source_url=str(source_url or "")[:1000],
        source_name=str(source_name or "")[:120],
        source_published_at=str(source_published_at or "")[:64] or "unknown",
        section=str(section or "")[:120] or "General",
        category=str(category or "")[:120] or "General",
        subcategory=str(subcategory or "")[:120] or "General",
        seed_summary=str(seed_summary or "")[:1200],
        seed_article_body=str(seed_article_body or "")[:1800],
        source_material=material[:14000],
    )

    try:
        _assert_required_summary_model(cfg)
        raw = _llm_generate(cfg, prompt)
    except Exception as exc:
        LOG.warning("resource editorial llm failed url=%s err=%s", source_url, exc)
        return {}

    parsed = _parse_json_object(raw)
    summary = _normalize_summary_text(str(parsed.get("summary", "")).strip())
    article_body = _format_resource_article_body_for_readability(
        _normalize_article_body_text(str(parsed.get("article_body", "")).strip())
    )
    if not summary:
        summary = _normalize_summary_text(seed_summary)
    if _resource_editorial_is_weak(summary, article_body):
        return {}
    return {"summary": summary, "article_body": article_body}


def _portal_api_post(cfg: Config, path: str, payload: dict[str, Any]) -> dict[str, Any]:
    headers = {"Content-Type": "application/json", "X-API-Key": cfg.agent_api_key}
    resp = requests.post(
        f"{cfg.portal_api_url.rstrip('/')}/{path.lstrip('/')}",
        headers=headers,
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def _portal_api_get(cfg: Config, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    headers = {"X-API-Key": cfg.agent_api_key}
    resp = requests.get(
        f"{cfg.portal_api_url.rstrip('/')}/{path.lstrip('/')}",
        headers=headers,
        params=params or {},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def _build_external_preview_url(preview_path: str) -> str:
    path = str(preview_path or "").strip()
    if not path:
        return ""
    try:
        from publish_review_draft import _build_preview_url

        return _build_preview_url(path)
    except Exception:
        return path


def _build_external_public_url(path: str) -> str:
    return _build_external_preview_url(path)


def _is_http_url(value: str) -> bool:
    lowered = str(value or "").strip().lower()
    return lowered.startswith("http://") or lowered.startswith("https://")


def _title_present_in_html(title: str, html: str) -> bool:
    tokens = _tokenize_title(title)
    if not tokens:
        return True
    hay = str(html or "").lower()
    overlap = sum(1 for token in tokens if token in hay)
    ratio = overlap / max(1, len(tokens))
    return ratio >= 0.4


def _http_validate_url(url: str, *, expected_title: str = "", timeout: int = 12) -> tuple[bool, str]:
    target = str(url or "").strip()
    if not _is_http_url(target):
        return False, "not_http_url"
    try:
        resp = requests.get(
            target,
            timeout=timeout,
            allow_redirects=True,
            headers={"User-Agent": "PlutoniaNewsroom/1.0"},
        )
    except Exception as exc:
        return False, f"request_failed:{exc}"
    if resp.status_code >= 400:
        return False, f"http_{resp.status_code}"
    body = resp.text or ""
    if len(body) < 200:
        return False, "response_too_short"
    if expected_title and not _title_present_in_html(expected_title, body):
        return False, "title_not_found_in_page"
    return True, "ok"


def _notify_public_article_links(cfg: Config, result: dict[str, Any]) -> dict[str, Any]:
    try:
        from gmail_monitor import _resolve_telegram_config, _send_telegram_message
    except Exception as exc:
        return {"sent": False, "reason": f"telegram_import_failed:{exc}"}

    article = result.get("article") if isinstance(result, dict) else None
    if not isinstance(article, dict):
        return {"sent": False, "reason": "missing_article_payload"}

    article_id = article.get("id")
    if not article_id:
        return {"sent": False, "reason": "missing_article_id"}

    title = str(article.get("title", "")).strip() or "Untitled article"
    newsletter_id = article.get("newsletter_id")
    draft_index = article.get("draft_article_index")
    detail_url = _build_external_public_url(f"/article/{int(article_id)}/")
    card_url = _build_external_public_url(f"/article/{int(article_id)}/card/")

    if not (_is_http_url(detail_url) and _is_http_url(card_url)):
        return {
            "sent": False,
            "reason": "public_base_unresolved",
            "detail_url": detail_url,
            "card_url": card_url,
        }

    parsed = urlparse(detail_url)
    site_url = f"{parsed.scheme}://{parsed.netloc}/" if parsed.scheme and parsed.netloc else ""
    site_ok, site_reason = _http_validate_url(site_url, timeout=10)
    if not site_ok:
        return {"sent": False, "reason": f"site_unavailable:{site_reason}", "site_url": site_url}

    card_ok, card_reason = _http_validate_url(card_url, expected_title=title, timeout=12)
    if not card_ok:
        return {"sent": False, "reason": f"card_link_invalid:{card_reason}", "card_url": card_url}

    detail_ok, detail_reason = _http_validate_url(detail_url, expected_title=title, timeout=12)
    if not detail_ok:
        return {"sent": False, "reason": f"detail_link_invalid:{detail_reason}", "detail_url": detail_url}

    token, chat_id = _resolve_telegram_config(cfg)
    if not token or not chat_id:
        return {"sent": False, "reason": "missing_telegram_config"}

    lines = [
        f"Article published: Newsletter #{newsletter_id} / Article #{draft_index}",
        f"Title: {title}",
        f"Card: {card_url}",
        f"Article: {detail_url}",
    ]
    text = "\n".join(lines).strip()
    if len(text) > 3900:
        text = text[:3890].rstrip() + "\n\n[truncated]"
    _send_telegram_message(token, chat_id, text)
    return {
        "sent": True,
        "newsletter_id": newsletter_id,
        "article_index": draft_index,
        "article_id": article_id,
        "card_url": card_url,
        "detail_url": detail_url,
        "site_url": site_url,
    }


def _notify_resource_review(cfg: Config, resource: dict[str, Any]) -> dict[str, Any]:
    try:
        from gmail_monitor import _resolve_telegram_config, _send_telegram_message
    except Exception as exc:
        return {"sent": False, "reason": f"telegram_import_failed: {exc}"}

    token, chat_id = _resolve_telegram_config(cfg)
    if not token or not chat_id:
        return {"sent": False, "reason": "missing_telegram_config"}

    rid = resource.get("id")
    title = str(resource.get("title", "")).strip() or "Untitled Resource"
    source = str(resource.get("resource_url", "")).strip() or "n/a"
    section = str(resource.get("section", "")).strip() or "n/a"
    category = str(resource.get("category", "")).strip() or "n/a"
    subcategory = str(resource.get("subcategory", "")).strip() or "n/a"
    preview_url = _build_external_preview_url(str(resource.get("preview_path", "")).strip()) or "n/a"
    source_date = str(resource.get("source_published_at", "")).strip() or "n/a"
    source_date_origin = str(resource.get("source_date_origin", "")).strip()
    taxonomy_origin = str(resource.get("classification_origin", "")).strip()
    source_date_line = source_date if not source_date_origin else f"{source_date} ({source_date_origin})"
    taxonomy_line = f"{section} / {category} / {subcategory}"
    if taxonomy_origin:
        taxonomy_line = f"{taxonomy_line} ({taxonomy_origin})"

    lines = [
        f"Resource review required: Resource #{rid}",
        f"1) Title: {title}",
        f"2) source: {source}",
        f"3) source_date: {source_date_line}",
        f"4) taxonomy: {taxonomy_line}",
        f"5) preview: {preview_url}",
        "",
        "Reply approved or rejected.",
    ]
    text = "\n".join(lines).strip()
    if len(text) > 3900:
        text = text[:3890].rstrip() + "\n\n[truncated]"
    _send_telegram_message(token, chat_id, text)

    if rid is not None:
        try:
            _portal_api_post(cfg, "resources/review-notified/", {"resource_id": int(rid)})
        except Exception as exc:
            LOG.warning("resource review notify mark failed resource_id=%s err=%s", rid, exc)

    return {"sent": True, "resource_id": rid, "preview_url": preview_url}


def _notify_next_pending_resource_review(cfg: Config) -> dict[str, Any]:
    try:
        payload = _portal_api_get(cfg, "resources/review-pending/", {"mode": "next_unnotified", "limit": 1})
    except Exception as exc:
        return {"sent": False, "reason": f"pending_lookup_failed: {exc}"}

    rows = payload.get("resources", []) if isinstance(payload, dict) else []
    if not isinstance(rows, list) or not rows:
        return {"sent": False, "reason": "no_pending_resources"}
    row0 = rows[0] if isinstance(rows[0], dict) else {}
    if not row0:
        return {"sent": False, "reason": "invalid_pending_payload"}
    return _notify_resource_review(cfg, row0)


# ---------------------------------------------------------------------------
# Phase 1 — Article approve / reject
# ---------------------------------------------------------------------------

def _handle_article_decision(data: dict) -> tuple[dict, int]:
    """Replicates ``review_article_decision.py:main()`` logic.

    Expected JSON body keys
    -----------------------
    newsletter_id : int, optional
    article_index : int, optional
    decision      : str  — ``"approve"`` or ``"reject"``
    comment       : str, optional
    source_url    : str, optional
    source_text   : str, optional
    """
    from review_article_decision import (
        MIN_AUTO_SOURCE_TEXT_CHARS,
        _decision_counts,
        _guard_decision_reentry,
        _mark_decision,
        _publish_single_article_preview,
        _resolve_latest_notified_pending_target,
        _resolve_target,
        _save_draft,
        _send_editorial_notification,
        _send_next_article_notification,
    )
    from process_newsletter import (
        _apply_source_metadata,
        _assert_required_summary_model,
        _get_source_snapshot,
        _snapshot_has_meaningful_content,
        article_preview_quality_issues,
        enrich_article,
        rewrite_article_from_source,
        source_snapshot_title_match,
    )
    from review_apply_manual_source import _rewrite_from_manual_text

    cfg = Config()

    newsletter_id = data.get("newsletter_id")
    if newsletter_id is not None:
        newsletter_id = int(newsletter_id)

    article_index = data.get("article_index")
    if article_index is not None:
        article_index = int(article_index)

    decision_raw = str(data.get("decision", "")).strip().lower()
    comment = str(data.get("comment", "")).strip()
    source_url = str(data.get("source_url", "")).strip()
    source_text = str(data.get("source_text", "")).strip()
    manual_source_text_supplied = bool(source_text)
    auto_source_discovery: dict[str, Any] | None = None

    decision = _normalize_article_decision(decision_raw)

    # Auto-infer approve when source material is provided.
    if not decision:
        if source_url or source_text:
            decision = "approve"
        else:
            return {"error": "decision is required"}, 400

    if decision not in ("approve", "reject"):
        return {"error": "decision must be 'approve' or 'reject'"}, 400

    sig_err = _verify_signature(data, payload_prefix=_sig_payload_for_article(data, decision))
    if sig_err is not None:
        return sig_err

    # Resolve target draft + article.
    # Idempotent behavior: if user sends an extra no-ID decision after all notified
    # pending items are already decided, return 200/no_pending instead of 400.
    try:
        if newsletter_id is None and article_index is None:
            resolved_nid, resolved_aidx, draft_path, draft = _resolve_latest_notified_pending_target(cfg)
        else:
            resolved_nid, resolved_aidx, draft_path, draft = _resolve_target(
                cfg=cfg,
                newsletter_id=newsletter_id,
                article_index=article_index,
                draft_file=None,
            )
    except SystemExit as exc:
        msg = str(exc).strip()
        msg_lower = msg.lower()
        if newsletter_id is None and article_index is None and (
            "missing review context" in msg_lower
            or "no draft with actionable article found" in msg_lower
        ):
            return {
                "status": "no_pending_context",
                "decision": "ignored",
                "reason": "no_pending_articles",
                "message": "No pending article currently awaiting decision.",
                "next_review_notification": {
                    "sent": False,
                    "reason": "no_pending_articles",
                },
            }, 200
        raise
    articles = draft.get("articles", [])
    idx = resolved_aidx - 1
    if idx < 0 or idx >= len(articles):
        return {"error": f"article index out of range: {resolved_aidx} (1..{len(articles)})"}, 400

    current = articles[idx]
    if not isinstance(current, dict):
        return {"error": "selected article is invalid"}, 400

    current_source_url = _normalize_url(str(current.get("original_url", "")).strip())
    current_source_generic = _is_generic_source_url(current_source_url) if current_source_url else True

    source_discovery_meta = current.get("source_discovery", {}) if isinstance(current.get("source_discovery"), dict) else {}
    direct_url_has_content = bool(source_discovery_meta.get("direct_url_has_content", True))
    direct_url_semantic_match = bool(source_discovery_meta.get("direct_url_semantic_match", True))

    # Optional auto-discovery for source URL when article source is missing/invalid.
    if (
        decision == "approve"
        and REVIEW_AUTO_SOURCE_DISCOVERY
        and not source_text
        and not source_url
        and (
            bool(current.get("manual_review_required"))
            or not current_source_url
            or current_source_generic
            or not direct_url_has_content
            or not direct_url_semantic_match
        )
    ):
        auto_source_discovery = _discover_source_url(cfg, current)
        picked_url = str(auto_source_discovery.get("selected_url", "")).strip()
        if picked_url:
            source_url = picked_url

    # Auto-fetch source text from URL when not provided inline.
    if decision == "approve" and not source_text and source_url:
        snapshot = _get_source_snapshot(
            source_url, cfg.source_rewrite_max_chars, cfg.source_open_min_chars,
        )
        candidate = str(snapshot.get("text", "")).strip()
        if len(candidate) >= MIN_AUTO_SOURCE_TEXT_CHARS:
            source_text = candidate

    duplicate = _guard_decision_reentry(
        article=current,
        decision=decision,
        newsletter_id=resolved_nid,
        article_index=resolved_aidx,
        articles=articles,
        draft_path=draft_path,
    )
    if duplicate is not None:
        return duplicate

    # ---- REJECT ----
    if decision == "reject":
        updated = _mark_decision(current, "rejected", comment)
        articles[idx] = updated
        draft["articles"] = articles
        _save_draft(draft_path, draft)

        next_notif = _queue_notification_after_response(
            lambda: _send_next_article_notification(cfg, resolved_nid, str(draft_path)),
            context=f"phase1_reject_newsletter_{resolved_nid}",
        )
        return {
            "newsletter_id": resolved_nid,
            "article_index": resolved_aidx,
            "decision": "rejected",
            "title": str(updated.get("title", "")).strip(),
            "decision_counts": _decision_counts([r for r in articles if isinstance(r, dict)]),
            "next_review_notification": next_notif,
            "draft_file": str(draft_path),
        }, 200

    # ---- APPROVE ----
    updated = dict(current)

    if source_text or source_url:
        _assert_required_summary_model(cfg)

    if source_text:
        if source_url:
            updated["original_url"] = source_url
            updated = _apply_source_metadata(updated)
        if len(source_text) < 300:
            return {"error": "source text too short (need >= 300 chars)"}, 400
        updated = _rewrite_from_manual_text(cfg, updated, source_text)
    elif source_url:
        updated["original_url"] = source_url
        updated = _apply_source_metadata(updated)
        updated = rewrite_article_from_source(cfg, updated)
        updated = enrich_article(cfg, updated)

    if bool(updated.get("manual_review_required")):
        return {
            "error": (
                "approve blocked: article requires manual source. "
                "Provide source_text / source_url before approval. "
                "Safety override allow_unresolved_manual is disabled."
            ),
        }, 400

    quality_issues = article_preview_quality_issues(updated)
    resolved_source_url = str(updated.get("original_url", "")).strip()
    if resolved_source_url:
        snapshot = _get_source_snapshot(
            resolved_source_url,
            cfg.source_rewrite_max_chars,
            cfg.source_open_min_chars,
        )
        if not _snapshot_has_meaningful_content(snapshot, cfg.source_presence_min_chars):
            if not manual_source_text_supplied:
                quality_issues.append("source_url_has_no_meaningful_content")
        else:
            semantic = source_snapshot_title_match(
                str(updated.get("title", "")).strip(),
                snapshot,
                min_overlap=cfg.source_title_overlap_min_ratio,
            )
            if not bool(semantic.get("ok")):
                quality_issues.append("source_url_semantic_mismatch")
    elif not source_text:
        quality_issues.append("missing_source_material")

    if quality_issues:
        return {
            "error": "approve blocked: preview quality gate failed",
            "quality_issues": sorted(set(quality_issues)),
        }, 400

    updated = _mark_decision(updated, "approved", comment)
    articles[idx] = updated
    draft["articles"] = articles
    _save_draft(draft_path, draft)

    publish_result = _publish_single_article_preview(cfg, resolved_nid, resolved_aidx, updated)
    editorial_notif = _send_editorial_notification(cfg, resolved_nid, publish_result)

    # Persist portal article metadata back into draft.
    published_rows = publish_result.get("articles", []) if isinstance(publish_result, dict) else []
    if isinstance(published_rows, list) and published_rows:
        row0 = published_rows[0] if isinstance(published_rows[0], dict) else {}
        if row0:
            updated["portal_article_id"] = row0.get("id")
            updated["editorial_status"] = row0.get("editorial_status")
            updated["editorial_preview_path"] = row0.get("preview_path")
            updated["editorial_preview_token"] = row0.get("preview_token")
            articles[idx] = updated
            draft["articles"] = articles
            _save_draft(draft_path, draft)

    return {
        "newsletter_id": resolved_nid,
        "article_index": resolved_aidx,
        "decision": "approved",
        "title": str(updated.get("title", "")).strip(),
        "manual_review_required": bool(updated.get("manual_review_required")),
        "summary_source_mode": str(updated.get("summary_source_mode", "")).strip(),
        "auto_source_discovery": auto_source_discovery or {"attempted": False},
        "decision_counts": _decision_counts([r for r in articles if isinstance(r, dict)]),
        "preview_publish_result": publish_result,
        "editorial_notification": editorial_notif,
        "next_review_notification": {
            "sent": False,
            "reason": "blocked_until_editorial_public_confirmation",
        },
        "draft_file": str(draft_path),
    }, 200


# ---------------------------------------------------------------------------
# Phase 2 — Editorial content decision
# ---------------------------------------------------------------------------

def _handle_content_decision(data: dict) -> tuple[dict, int]:
    """Replicates ``review_content_decision.py:main()`` logic.

    Expected JSON body keys
    -----------------------
    article_id       : int, optional (when omitted, resolves latest pending editorial article)
    decision         : str  — ``"approve"``, ``"revise"``, ``"hold"``,
                        ``"changes"``, ``"request_changes"``, ``"reject"``
    comment          : str, optional
    instructions     : str, required when decision is ``"revise"``
    source_text      : str, optional (used by revise flow)
    max_source_chars : int, optional (default 12000)
    """
    from review_content_decision import (
        _api_get,
        _api_post,
        _build_revision_payload,
        _notify_next_article_review,
    )

    cfg = Config()
    if not cfg.agent_api_key:
        return {"error": "AGENT_API_KEY missing"}, 500

    article_id = data.get("article_id")
    if article_id is not None:
        try:
            article_id = int(article_id)
        except (TypeError, ValueError):
            return {"error": "article_id must be an integer"}, 400

    decision = _normalize_content_decision(str(data.get("decision", "")).strip().lower())
    valid = {"approve", "revise", "hold", "changes", "request_changes", "reject"}
    if decision not in valid:
        return {"error": f"invalid decision: {decision}"}, 400

    sig_err = _verify_signature(data, payload_prefix=_sig_payload_for_content(data, decision))
    if sig_err is not None:
        return sig_err

    comment = str(data.get("comment", "")).strip()
    instructions = str(data.get("instructions", "")).strip()
    source_text = str(data.get("source_text", "")).strip()
    max_source_chars = int(data.get("max_source_chars", 12000))

    if article_id is None:
        pending_payload = _api_get(cfg, "articles/editorial-pending/", {"mode": "latest"})
        pending_article = pending_payload.get("article") if isinstance(pending_payload, dict) else None
        if not isinstance(pending_article, dict):
            return {
                "status": "no_pending_context",
                "decision": "ignored",
                "reason": "no_pending_editorial_articles",
                "message": "No pending editorial article currently awaiting decision.",
                "next_review_notification": {
                    "sent": False,
                    "reason": "no_pending_editorial_articles",
                },
            }, 200
        try:
            article_id = int(pending_article.get("id"))
        except (TypeError, ValueError):
            return {"error": "invalid pending editorial article id"}, 500

    # Fetch current article data from portal.
    fetched = _api_get(cfg, "articles/editorial-data/", {"article_id": article_id})
    article = fetched.get("article") if isinstance(fetched, dict) else None
    if not isinstance(article, dict):
        return {"error": "failed to load article editorial data"}, 404

    # Normalise decision aliases.
    aliases = {"reject": "request_changes", "changes": "request_changes"}
    decision = aliases.get(decision, decision)

    payload: dict = {
        "article_id": article_id,
        "decision": decision,
        "comment": comment,
    }

    if decision == "revise":
        if not instructions:
            return {"error": "decision=revise requires instructions"}, 400
        revision = _build_revision_payload(
            cfg, article, instructions, source_text, max_source_chars,
        )
        payload.update(revision)

    result = _api_post(cfg, "articles/editorial-decision/", payload)

    # After editorial approve → notify next pending article.
    next_notif = None
    publication_notif = None
    if decision == "approve":
        publication_notif = _notify_public_article_links(cfg, result if isinstance(result, dict) else {})
        newsletter_id = result.get("newsletter_id")
        if newsletter_id is not None:
            next_notif = _queue_notification_after_response(
                lambda: _notify_next_article_review(cfg, int(newsletter_id)),
                context=f"phase2_approve_newsletter_{int(newsletter_id)}",
            )
        else:
            next_notif = {"sent": False, "reason": "missing_newsletter_id"}

    output = dict(result) if isinstance(result, dict) else {"status": "ok"}
    output["resolved_article_id"] = article_id
    if publication_notif is not None:
        output["publication_notification"] = publication_notif
    if next_notif is not None:
        output["next_review_notification"] = next_notif

    return output, 200


# ---------------------------------------------------------------------------
# Resource submit — classify + publish
# ---------------------------------------------------------------------------

def _handle_resource_submit(data: dict) -> tuple[dict, int]:
    cfg = Config()
    if not cfg.agent_api_key:
        return {"error": "AGENT_API_KEY missing"}, 500

    resource_url = _normalize_url(str(data.get("resource_url", "")).strip())
    if not resource_url:
        return {"error": "resource_url is required"}, 400

    sig_err = _verify_signature(data, payload_prefix=_sig_payload_for_resource({"resource_url": resource_url}, "submit"))
    if sig_err is not None:
        return sig_err

    provided_title = str(data.get("title", "")).strip()
    provided_summary = str(data.get("summary", "")).strip()
    provided_description = str(data.get("description", "")).strip()
    provided_article_body = str(data.get("article_body", "")).strip()
    provided_image_url = _normalize_url(str(data.get("image_url", "")).strip())
    provided_source_published_at = _parse_date_string(str(data.get("source_published_at", "")).strip())

    source_meta = _extract_resource_source_metadata(resource_url)
    resolved_url = _normalize_url(source_meta.get("final_url", "")) or resource_url
    resolved_title = (
        provided_title
        or str(source_meta.get("title", "")).strip()
        or _title_from_url(resolved_url)
        or resolved_url
    )[:500]
    resolved_summary = (
        provided_summary
        or provided_description
        or str(source_meta.get("summary", "")).strip()
        or str(source_meta.get("description", "")).strip()
    )
    resolved_article_body = (
        provided_article_body
        or str(source_meta.get("article_body", "")).strip()
        or resolved_summary
    )
    resolved_image_url = provided_image_url or _normalize_url(str(source_meta.get("image_url", "")).strip())
    source_meta_date_found = bool(str(source_meta.get("source_published_at", "")).strip())
    source_published_at = str(source_meta.get("source_published_at", "")).strip()
    source_date_origin = str(source_meta.get("source_published_at_origin", "")).strip()
    web_date_lookup: dict[str, Any] = {"attempted": False, "source_published_at": "", "origin": "", "candidates": []}

    if provided_source_published_at:
        source_published_at = provided_source_published_at
        source_date_origin = "manual_override"
    elif not source_published_at:
        web_date_lookup = _discover_resource_source_date(cfg, resource_url=resolved_url, title=resolved_title)
        source_published_at = str(web_date_lookup.get("source_published_at", "")).strip()
        if source_published_at:
            source_date_origin = str(web_date_lookup.get("origin", "")).strip() or "web_consensus"

    provided_section = str(data.get("section", "")).strip()
    provided_category = str(data.get("category", "")).strip()
    provided_subcategory = str(data.get("subcategory", "")).strip()
    auto_classify = _as_bool(data.get("auto_classify"), default=True)
    source_taxonomy = source_meta.get("source_taxonomy") if isinstance(source_meta.get("source_taxonomy"), dict) else {}
    source_section = _clean_taxonomy_label(str(source_taxonomy.get("section", "")))
    source_category = _clean_taxonomy_label(str(source_taxonomy.get("category", "")))
    source_subcategory = _clean_taxonomy_label(str(source_taxonomy.get("subcategory", "")))

    if provided_section and provided_category and provided_subcategory:
        taxonomy = {
            "section": provided_section[:120],
            "category": provided_category[:120],
            "subcategory": provided_subcategory[:120],
            "classification_origin": "manual",
        }
    elif source_section and source_category and source_subcategory:
        taxonomy = {
            "section": source_section[:120],
            "category": source_category[:120],
            "subcategory": source_subcategory[:120],
            "classification_origin": "source_taxonomy",
        }
    elif auto_classify:
        taxonomy = _classify_resource_taxonomy(
            cfg,
            title=resolved_title,
            description=resolved_summary,
            resource_url=resolved_url,
        )
    else:
        taxonomy = _fallback_resource_taxonomy(resolved_title, resolved_summary, resolved_url)
        taxonomy["classification_origin"] = "auto_disabled_fallback"

    editorial_rewrite_applied = False
    editorial_rewrite = _as_bool(data.get("editorial_rewrite"), default=REVIEW_RESOURCE_EDITORIAL_REWRITE)
    snippet_material_used = False
    if editorial_rewrite:
        source_material = str(source_meta.get("source_text_excerpt", "")).strip()
        if len(source_material) < REVIEW_RESOURCE_EDITORIAL_MIN_SOURCE_CHARS:
            snippet_material = _collect_resource_web_snippets(
                cfg,
                title=resolved_title,
                resource_url=resolved_url,
            )
            if len(snippet_material) >= REVIEW_RESOURCE_EDITORIAL_MIN_SOURCE_CHARS:
                source_material = snippet_material
                snippet_material_used = True
        rewritten = _synthesize_resource_editorial(
            cfg,
            title=resolved_title,
            source_url=resolved_url,
            source_name=_source_name_from_url(resolved_url),
            source_published_at=source_published_at,
            section=taxonomy.get("section", ""),
            category=taxonomy.get("category", ""),
            subcategory=taxonomy.get("subcategory", ""),
            seed_summary=resolved_summary,
            seed_article_body=resolved_article_body,
            source_material=source_material,
        )
        if rewritten:
            resolved_summary = str(rewritten.get("summary", "")).strip() or resolved_summary
            resolved_article_body = str(rewritten.get("article_body", "")).strip() or resolved_article_body
            editorial_rewrite_applied = True

    resolved_article_body = _format_resource_article_body_for_readability(str(resolved_article_body or ""))
    if not resolved_article_body and resolved_summary:
        resolved_article_body = resolved_summary

    payload: dict[str, Any] = {
        "resource_url": resolved_url,
        "title": resolved_title,
        "summary": resolved_summary,
        "description": provided_description or str(source_meta.get("description", "")).strip() or resolved_summary,
        "article_body": resolved_article_body,
        "image_url": resolved_image_url,
        "section": taxonomy["section"],
        "category": taxonomy["category"],
        "subcategory": taxonomy["subcategory"],
        "is_featured": _as_bool(data.get("is_featured"), default=False),
    }
    if source_published_at:
        payload["source_published_at"] = source_published_at
    review_required = _as_bool(data.get("review_required"), default=True)
    now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    if review_required:
        payload["is_active"] = False
        payload["review_status"] = "pending"
        payload["review_requested_at"] = now_iso
        payload["review_decided_at"] = ""
        payload["review_comment"] = ""
    else:
        payload["is_active"] = _as_bool(data.get("is_active"), default=True)
        payload["review_status"] = "approved" if payload["is_active"] else "pending"
    if data.get("published_at"):
        payload["published_at"] = str(data.get("published_at")).strip()

    try:
        publish_result = _portal_api_post(cfg, "resources/publish/", payload)
    except Exception as exc:
        return {"error": f"resource publish failed: {exc}"}, 502

    resource_row = publish_result.get("resource") if isinstance(publish_result, dict) else None
    review_notification = {"sent": False, "reason": "not_requested"}
    if review_required and isinstance(resource_row, dict):
        resource_row["source_date_origin"] = source_date_origin
        resource_row["classification_origin"] = taxonomy.get("classification_origin", "fallback")
        review_notification = _notify_resource_review(cfg, resource_row)

    return {
        "status": "ok",
        "resource_publish_result": publish_result,
        "review_required": review_required,
        "review_notification": review_notification,
        "classification_origin": taxonomy.get("classification_origin", "fallback"),
        "source_date_origin": source_date_origin,
        "editorial_rewrite_applied": editorial_rewrite_applied,
        "editorial_snippet_material_used": snippet_material_used,
        "source_taxonomy_detected": bool(source_section and source_category and source_subcategory),
        "web_date_lookup": web_date_lookup,
        "source_metadata": {
            "final_url": resolved_url,
            "metadata_title_found": bool(source_meta.get("title")),
            "metadata_summary_found": bool(source_meta.get("summary")),
            "metadata_article_body_found": bool(source_meta.get("article_body")),
            "metadata_description_found": bool(source_meta.get("description")),
            "metadata_image_found": bool(source_meta.get("image_url")),
            "metadata_source_published_at_found": source_meta_date_found,
            "source_taxonomy_found": bool(source_section and source_category and source_subcategory),
            "metadata_source_text_excerpt_found": bool(str(source_meta.get("source_text_excerpt", "")).strip()),
        },
    }, 200


def _handle_resource_decision(data: dict) -> tuple[dict, int]:
    cfg = Config()
    if not cfg.agent_api_key:
        return {"error": "AGENT_API_KEY missing"}, 500

    decision = _normalize_resource_decision(str(data.get("decision", "")).strip().lower())
    if decision not in {"approve", "reject"}:
        return {"error": "decision must be approve|reject"}, 400

    sig_err = _verify_signature(data, payload_prefix=_sig_payload_for_resource_decision(data, decision))
    if sig_err is not None:
        return sig_err

    resource_id = data.get("resource_id")
    if resource_id is None:
        try:
            pending = _portal_api_get(cfg, "resources/review-pending/", {"mode": "latest_notified", "limit": 1})
        except Exception as exc:
            return {"error": f"failed to resolve resource context: {exc}"}, 502
        rows = pending.get("resources", []) if isinstance(pending, dict) else []
        if not isinstance(rows, list) or not rows or not isinstance(rows[0], dict):
            return {"error": "missing resource review context; provide Resource #<RID>"}, 400
        resource_id = rows[0].get("id")

    try:
        rid = int(resource_id)
    except Exception:
        return {"error": "resource_id must be an integer"}, 400

    comment = str(data.get("comment", "")).strip()
    try:
        decision_result = _portal_api_post(
            cfg,
            "resources/review-decision/",
            {"resource_id": rid, "decision": decision, "comment": comment},
        )
    except Exception as exc:
        return {"error": f"resource decision failed: {exc}"}, 502

    pending_count = int(decision_result.get("pending_review_count", 0)) if isinstance(decision_result, dict) else 0
    next_notif = {"sent": False, "reason": "no_pending_resources"}
    if pending_count > 0:
        next_notif = _queue_notification_after_response(
            lambda: _notify_next_pending_resource_review(cfg),
            context=f"resource_{decision}_next_notification",
        )

    out = dict(decision_result) if isinstance(decision_result, dict) else {"status": "ok"}
    out["next_resource_review_notification"] = next_notif
    return out, 200


# ---------------------------------------------------------------------------
# HTTP Server
# ---------------------------------------------------------------------------

class ReviewAPIHandler(BaseHTTPRequestHandler):
    """Request handler for the review API."""

    def log_message(self, format, *args):  # noqa: A002
        LOG.info("review_api %s", format % args)

    def do_GET(self):  # noqa: N802
        if self.path == "/healthz":
            _send_json(self, {"status": "ok"})
        else:
            _send_json(self, {"error": "not found"}, 404)

    def do_POST(self):  # noqa: N802
        try:
            data = _read_json_body(self)
        except Exception as exc:
            _send_json(self, {"error": f"invalid JSON body: {exc}"}, 400)
            return

        try:
            if self.path == "/api/review/article-decision":
                result, status = _handle_article_decision(data)
            elif self.path == "/api/review/content-decision":
                result, status = _handle_content_decision(data)
            elif self.path == "/api/review/resource-submit":
                result, status = _handle_resource_submit(data)
            elif self.path == "/api/review/resource-decision":
                result, status = _handle_resource_decision(data)
            else:
                result, status = {"error": "not found"}, 404
        except SystemExit as exc:
            # The underlying scripts raise SystemExit for validation errors.
            result, status = {"error": str(exc)}, 400
        except Exception as exc:
            LOG.exception("review api error path=%s", self.path)
            result, status = {"error": str(exc)}, 500

        _send_json(self, result, status)


def start_review_api_server(port: int = 0) -> threading.Thread:
    """Start the review API server as a background daemon thread.

    Parameters
    ----------
    port : int
        TCP port to listen on.  Defaults to ``REVIEW_API_PORT`` env var (8001).

    Returns
    -------
    threading.Thread
        The running daemon thread (already started).
    """
    listen_port = port or REVIEW_API_PORT
    server = HTTPServer(("0.0.0.0", listen_port), ReviewAPIHandler)
    thread = threading.Thread(
        target=server.serve_forever,
        name="review-api-server",
        daemon=True,
    )
    thread.start()
    LOG.info("review API server listening on port %s", listen_port)
    return thread
