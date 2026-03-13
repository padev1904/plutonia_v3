#!/usr/bin/env python3
"""Pipeline de processamento de newsletters.

Fluxo:
1) clean_html
2) classify_newsletter (digest|article)
3) extract_articles via OpenClaw (Qwen) ou Ollama fallback
4) resolve original source metadata (url/data/imagem)
5) enrich_article via SearXNG
6) gerar draft para revisão manual (ou publicar via API Django)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import shutil
import subprocess
import time
import unicodedata
import uuid
import urllib.parse
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from html import unescape
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup, NavigableString, Tag
from newsletter_revh_parser import ParsedArticle, parse_email_articles, unwrap as revh_unwrap


LOG = logging.getLogger("process_newsletter")

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
HTTP_HEADERS = {"User-Agent": USER_AGENT}
URL_RE = re.compile(r"https?://[^\s<>()\"']+", flags=re.IGNORECASE)
ANGLE_BRACKET_URL_RE = re.compile(r"<(https?://[^>\s]+)>", flags=re.IGNORECASE)
MEDIUM_ID_RE = re.compile(r"-[0-9a-f]{10,}$", flags=re.IGNORECASE)
URL_DATE_RE = re.compile(r"/(20\d{2})/(0[1-9]|1[0-2])/([0-3]\d)(?:/|$)")
BOOL_TRUE = {"1", "true", "yes", "on"}
MEDIUM_CUSTOM_HOSTS = {
    "towardsdatascience.com",
    "betterprogramming.pub",
    "uxdesign.cc",
    "itnext.io",
    "levelup.gitconnected.com",
    "ai.plainenglish.io",
    "javascript.plainenglish.io",
    "towardsai.net",
}
NOISE_HOSTS = {
    "plutoanalytics.com",
    "www.plutoanalytics.com",
    "instagram.com",
    "www.instagram.com",
    "facebook.com",
    "www.facebook.com",
    "substackcdn.com",
    "www.substackcdn.com",
    "eotrx.substackcdn.com",
    "smry.ai",
    "www.smry.ai",
    "removepaywalls.com",
    "www.removepaywalls.com",
}
WEB_IMAGE_BLOCKED_PAGE_HOSTS = {
    "x.com",
    "twitter.com",
    "facebook.com",
    "instagram.com",
    "linkedin.com",
    "reddit.com",
    "youtube.com",
    "youtu.be",
    "tiktok.com",
    "pinterest.com",
    "bing.com",
    "duckduckgo.com",
    "qwant.com",
    "startpage.com",
}
WEB_IMAGE_BLOCKED_ASSET_HOSTS = {
    "mm.bing.net",
    "bing.com",
    "duckduckgo.com",
    "qwant.com",
    "startpage.com",
    "pinterest.com",
}
WEB_IMAGE_NOISE_TERMS = (
    "logo",
    "avatar",
    "icon",
    "favicon",
    "profile",
    "sprite",
    "emoji",
    "placeholder",
    "default-image",
    "default_image",
)
GENERIC_NAV_PATHS = {
    "",
    "/",
    "/home",
    "/home/",
    "/about",
    "/about/",
    "/feed",
    "/feed/",
    "/search",
    "/search/",
    "/explore",
    "/explore/",
    "/topics",
    "/topics/",
    "/subscribe",
    "/subscribe/",
    "/signup",
    "/signup/",
    "/advertise",
    "/advertise/",
    "/terms",
    "/terms/",
    "/privacy",
    "/privacy/",
}
PROMO_PATTERNS = [
    r"would you like to be featured",
    r"thanks for reading",
    r"subscribe for free",
    r"pledge your support",
    r"\bunsubscribe\b",
    r"\bread in app\b",
    r"\blike this post\b",
    r"\bleave a comment\b",
    r"\brestack this post\b",
]
PAYWALL_PATTERNS = [
    "subscribe to continue reading",
    "this content is for subscribers",
    "subscriber-only",
    "for subscribers only",
    "already a subscriber",
    "sign in to continue",
    "subscription required",
    "unlock this article",
    "continue reading with a subscription",
    "become a member",
    "paywall",
    "just a moment",
    "verify you are human",
    "attention required",
    "checking your browser",
    "cloudflare",
    "cf-challenge",
]
SOURCE_UNAVAILABLE_PATTERNS = [
    "404",
    "page not found",
    "not found",
    "this page doesn",
    "doesn't exist",
    "page you requested does not exist",
    "sorry, we couldn't find",
    "an error occurred",
]
PUBLISHED_META_FIELDS: tuple[tuple[str, str, str], ...] = (
    ("meta", "property", "article:published_time"),
    ("meta", "name", "article:published_time"),
    ("meta", "name", "publish_date"),
    ("meta", "name", "publish-date"),
    ("meta", "name", "pubdate"),
    ("meta", "itemprop", "datePublished"),
    ("meta", "name", "date"),
)
IMAGE_META_FIELDS: tuple[tuple[str, str, str], ...] = (
    ("meta", "property", "og:image"),
    ("meta", "name", "og:image"),
    ("meta", "property", "twitter:image"),
    ("meta", "name", "twitter:image"),
    ("meta", "itemprop", "image"),
)

SOURCE_META_CACHE: dict[str, tuple[str, str, str]] = {}
PUBLISHED_AT_CACHE: dict[str, str] = {}
IMAGE_URL_CACHE: dict[str, str] = {}
IMAGE_PROBE_CACHE: dict[str, dict[str, Any]] = {}
SOURCE_TEXT_CACHE: dict[str, str] = {}
SOURCE_SNAPSHOT_CACHE: dict[str, dict[str, Any]] = {}
WEB_IMAGE_SEARCH_CACHE: dict[str, dict[str, Any]] = {}
ANCHOR_NOISE_TEXT_RE = re.compile(
    r"(?:\bread online\b|\bview in browser\b|\bsign ?up\b|\bsubscribe\b|\badvertise\b|"
    r"\bpartner with us\b|\bget started\b|\bupdate your email preferences\b|"
    r"\bforwarded this\??\b|\bmanage preferences\b|\bunsubscribe\b|\bclick to execute\b)",
    re.IGNORECASE,
)
INVALID_TITLE_VALUES = {"", "sem titulo", "untitled", "none", "null", "n/a", "na"}
EMAIL_PHONE_RE = re.compile(r"^\+\d[\d\s\-]{6,}$")
EMAIL_MIN_VISIBLE_CHARS = 5
EMAIL_FORWARD_LABELS = {
    "From:", "Sent:", "To:", "Subject:", "Date:",
    "De:", "Enviado:", "Para:", "Assunto:", "Data:",
}
EMAIL_FORWARD_SUB_LABELS = {"On Behalf Of"}
EMAIL_FORWARD_HEADER_PREFIXES = (
    "From: ", "Sent: ", "To: ", "Subject: ", "Date: ",
    "De: ", "Enviado: ", "Para: ", "Assunto: ", "Data: ",
)
EMAIL_DISCARD_PATTERNS = (
    "CONFIDENTIALITY NOTICE",
    "Substack Inc",
    "Market Street",
    "Unsubscribe",
    "Start writing",
    "See more notes in the Substack app",
    "e-mail transmission and eventual attached files",
)
EMAIL_DISCARD_ENDSWITH = (" liked",)
EMAIL_DISCARD_EXACT = {"You follow"}
EMAIL_DISCARD_CONTAINS = ("posted new notes",)
EMAIL_IMAGE_MIN_SIZE = 50
EMAIL_IMAGE_DISCARD_PATTERNS = (
    "open.substack.com",
    "pixel",
    "track",
    "beacon",
    "1x1",
    "email.mg-",
    "cid:",
)

TITLE_PROPOSAL_PROMPT = """You are a title generation assistant.
Generate a single concise and engaging title for the content below.
The title must be based only on the text provided.
Ignore newsletter wrapper text, email headers, sender names, author bylines, and phrases such as
"posted new notes", "is now available", "highlighted in newsletter", or similar digest framing.
Focus on the main subject of the article itself.
Do not include the author/publication name unless the article is actually about that person/publication.
Prefer concrete topic nouns over generic wording like newsletter, update, note, roundup, or developments.

Output ONLY raw JSON with this exact structure:
{{
  "title": "..."
}}

Profile: {content_profile}
Text:
{article_text}
"""

GENERIC_TITLE_HINTS = (
    "newsletter",
    "highlighted in newsletter",
    "posted new notes",
    "news roundup",
    "digest",
    "multiple ai developments",
    "emerging ai developments",
)

CLASSIFY_PROMPT = """Classify this newsletter content as either "digest" or "article".

- "digest": Contains multiple distinct news items, links, or summaries (like a curated list)
- "article": Contains one main long-form piece (editorial, analysis, deep-dive)

NEWSLETTER CONTENT (first 500 chars):
---
{newsletter_preview}
---

Respond with exactly one word: "digest" or "article"
"""

EXTRACT_DIGEST_PROMPT = """You are an AI news extraction specialist. Your task is to extract individual news items from a newsletter.

NEWSLETTER CONTENT:
---
{newsletter_text}
---

INSTRUCTIONS:
1. Identify each distinct news item, announcement, or story in the newsletter.
2. For each item, extract:
   - title: A clear, concise headline (create one if not explicit)
   - summary: A 2-4 sentence summary capturing the key facts
   - original_url: The URL link if present (empty string if not)
   - categories: 3-10 concise keywords in English (topics, companies, technologies, sectors)

RULES:
- Extract ALL distinct news items, do not skip any
- Each news item should be self-contained
- Do NOT include promotional content, ads, or calls-to-action
- Do NOT include newsletter meta-content
- Summaries must be factual and neutral
- If a URL is relative or broken, set original_url to empty string

OUTPUT FORMAT (strict JSON array):
[
  {
    "title": "...",
    "summary": "...",
    "original_url": "...",
    "categories": ["...", "..."]
  }
]

Output ONLY the JSON array. No preamble, no explanation.
"""

EXTRACT_ARTICLE_PROMPT = """You are an AI news extraction specialist. Your task is to summarize a long-form newsletter article into a structured news item.

NEWSLETTER CONTENT:
---
{newsletter_text}
---

INSTRUCTIONS:
1. Create a clear, concise headline that captures the main topic
2. Write a 3-6 sentence summary covering the main argument and significance
3. Extract the primary URL if present
4. Assign 3-10 concise keywords in English

OUTPUT FORMAT (strict JSON array with single item):
[
  {
    "title": "...",
    "summary": "...",
    "original_url": "...",
    "categories": ["...", "..."]
  }
]

Output ONLY the JSON array. No preamble, no explanation.
"""

SOURCE_REWRITE_PROMPT = """You are preparing one publish-ready AI news article from the ORIGINAL source.

Use the source content below as the primary evidence. Keep a factual, neutral tone.
Discard newsletter promos, referral pitches, and unrelated calls-to-action.

CURRENT ITEM:
- title: {existing_title}
- summary: {existing_summary}
- categories: {existing_categories}
- source_url: {source_url}
- source_published_at: {source_published_at}

SOURCE CONTENT:
---
{source_text}
---

Return STRICT JSON object only:
{{
  "title": "...",
  "summary": "...",
  "article_body": "...",
  "section": "...",
  "category": "...",
  "subcategory": "...",
  "categories": ["...", "..."]
}}

Rules:
- `summary` must be a concise teaser with 2-3 sentences (complete and self-contained, no truncation markers).
- `article_body` must be a journalistic text with 5-8 short paragraphs, clear narrative flow, no bullet lists, and no raw URL dumps.
- Separate paragraphs with blank lines; each paragraph should read like newsroom prose (not notes).
- `section`, `category`, and `subcategory` must be in English.
- `categories` must contain 3-10 concise keywords in English.
- If source content is partial, preserve reliable facts and avoid speculation.
- Do not include markdown or explanations outside JSON.
"""


@dataclass
class Config:
    ollama_url: str = os.getenv("OLLAMA_HOST", "http://ollama:11434")
    ollama_model: str = os.getenv("OLLAMA_MODEL", "qwen3:14b")
    searxng_url: str = os.getenv("SEARXNG_URL", "http://searxng:8080")
    portal_api_url: str = os.getenv("PORTAL_API_URL", "http://portal:8000/api/agent")
    agent_api_key: str = os.getenv("AGENT_API_KEY", "")
    llm_backend: str = os.getenv("LLM_BACKEND", "ollama").strip().lower()
    openclaw_gateway_url: str = os.getenv("OPENCLAW_GATEWAY_URL", "").strip()
    openclaw_gateway_token: str = os.getenv("OPENCLAW_GATEWAY_TOKEN", "").strip()
    openclaw_gateway_password: str = os.getenv("OPENCLAW_GATEWAY_PASSWORD", "").strip()
    openclaw_timeout_seconds: int = int(os.getenv("OPENCLAW_TIMEOUT_SECONDS", "180"))
    openclaw_session_prefix: str = os.getenv("OPENCLAW_SESSION_PREFIX", "agent:main:newsletter")
    openclaw_fallback_ollama: bool = os.getenv("OPENCLAW_FALLBACK_OLLAMA", "false").strip().lower() in BOOL_TRUE
    review_before_publish: bool = os.getenv("REVIEW_BEFORE_PUBLISH", "true").strip().lower() in BOOL_TRUE
    review_output_dir: str = os.getenv("REVIEW_OUTPUT_DIR", "/review")
    source_rewrite_enabled: bool = os.getenv("SOURCE_REWRITE_ENABLED", "true").strip().lower() in BOOL_TRUE
    source_rewrite_max_chars: int = int(os.getenv("SOURCE_REWRITE_MAX_CHARS", "12000"))
    source_open_min_chars: int = int(os.getenv("SOURCE_OPEN_MIN_CHARS", "180"))
    source_discovery_enabled: bool = os.getenv("SOURCE_DISCOVERY_ENABLED", "true").strip().lower() in BOOL_TRUE
    source_discovery_min_score: int = int(os.getenv("SOURCE_DISCOVERY_MIN_SCORE", "30"))
    source_presence_min_chars: int = int(os.getenv("SOURCE_PRESENCE_MIN_CHARS", "220"))
    source_title_overlap_min_ratio: float = float(os.getenv("SOURCE_TITLE_OVERLAP_MIN_RATIO", "0.35"))
    web_image_search_enabled: bool = os.getenv("WEB_IMAGE_SEARCH_ENABLED", "true").strip().lower() in BOOL_TRUE
    web_image_search_min_score: int = int(os.getenv("WEB_IMAGE_SEARCH_MIN_SCORE", "36"))
    web_image_search_max_candidates: int = int(os.getenv("WEB_IMAGE_SEARCH_MAX_CANDIDATES", "6"))
    required_summary_model: str = os.getenv("REQUIRED_SUMMARY_MODEL", "qwen3:14b").strip()
    require_manual_article_approval: bool = os.getenv("REQUIRE_MANUAL_ARTICLE_APPROVAL", "true").strip().lower() in BOOL_TRUE
    # Per-task model configuration (Fase 6 — migrated from local agent)
    ollama_model_summary: str = os.getenv("OLLAMA_MODEL_SUMMARY", "").strip()
    ollama_model_title: str = os.getenv("OLLAMA_MODEL_TITLE", "").strip()
    ollama_model_link_validation: str = os.getenv("OLLAMA_MODEL_LINK_VALIDATION", "").strip()


OLLAMA_PARAMS = {
    # Keep extraction deterministic across resets for identical input.
    "temperature": float(os.getenv("OLLAMA_TEMPERATURE", "0")),
    "num_predict": int(os.getenv("OLLAMA_NUM_PREDICT", "2048")),
    "top_p": float(os.getenv("OLLAMA_TOP_P", "1")),
    "repeat_penalty": float(os.getenv("OLLAMA_REPEAT_PENALTY", "1.1")),
    "seed": int(os.getenv("OLLAMA_SEED", "42")),
}


def _resolve_manage_py_path() -> Path | None:
    candidates = [
        Path(__file__).resolve().parent.parent / "portal" / "manage.py",
        Path("/app/manage.py"),
        Path.cwd() / "portal" / "manage.py",
        Path.cwd() / "manage.py",
    ]
    for candidate in candidates:
        try:
            if candidate.exists():
                return candidate
        except Exception:
            continue
    return None


def _safe_json_array(text: str) -> list[dict[str, Any]]:
    text = text.strip()
    if not text:
        return []

    # Strict first
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return [item for item in parsed if isinstance(item, dict)]
    except json.JSONDecodeError:
        pass

    # Try fenced block / substring
    m = re.search(r"\[.*\]", text, flags=re.DOTALL)
    if m:
        try:
            parsed = json.loads(m.group(0))
            if isinstance(parsed, list):
                return [item for item in parsed if isinstance(item, dict)]
        except json.JSONDecodeError:
            return []
    return []


def _safe_json_object(text: str) -> dict[str, Any]:
    value = (text or "").strip()
    if not value:
        return {}
    try:
        parsed = json.loads(value)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass

    m = re.search(r"\{.*\}", value, flags=re.DOTALL)
    if not m:
        return {}
    try:
        parsed = json.loads(m.group(0))
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        return {}
    return {}


def _normalize_url(url: str) -> str:
    out = unescape((url or "").strip())
    out = out.strip("<>.,);]")
    return out


def _normalized_host_from_url(url: str) -> str:
    normalized = _normalize_url(url)
    if not normalized:
        return ""
    try:
        host = (urlparse(normalized).hostname or "").lower().strip(".")
    except Exception:
        return ""
    if host.startswith("www."):
        host = host[4:]
    return host


def _is_generic_source_url(url: str) -> bool:
    normalized = _normalize_url(url)
    if not normalized:
        return True
    try:
        parsed = urlparse(normalized)
    except Exception:
        return True

    host = (parsed.hostname or "").lower().strip(".")
    if not host:
        return True
    if host.startswith("www."):
        host = host[4:]

    path = (parsed.path or "").strip()
    path_lower = path.lower()
    query = (parsed.query or "").strip()
    fragment = (parsed.fragment or "").strip()

    # Hard rule: naked host/homepage is never a valid article source.
    if path_lower in {"", "/"} and not query and not fragment:
        return True

    # LinkedIn: only post/article-like deep links are valid source links.
    if host == "linkedin.com":
        if "/activity-" in path_lower:
            return False
        if path_lower.startswith(("/posts/", "/feed/update/", "/pulse/", "/article/", "/newsletters/")):
            return False
        return True

    # Substack: root/profile/navigation links are generic; prefer /p/ or /note/.
    if host == "substack.com" or host.endswith(".substack.com"):
        if path_lower.startswith("/p/") or "/note/" in path_lower:
            return False
        return True

    if path_lower in GENERIC_NAV_PATHS:
        return True
    if path_lower.startswith(("/tag/", "/tags/", "/topic/", "/topics/", "/category/", "/categories/")):
        return True
    return False


def _source_name_from_url(url: str) -> str:
    normalized = _normalize_url(url)
    if not normalized:
        return ""
    try:
        host = (urlparse(normalized).hostname or "").lower().strip(".")
    except Exception:
        return ""
    if not host:
        return ""
    if host.startswith("www."):
        host = host[4:]
    parts = [p for p in host.split(".") if p]
    if not parts:
        return ""

    if len(parts) >= 3 and parts[-1] in {"uk", "pt", "br", "au", "jp", "in", "es", "fr", "de", "it", "nl"} and parts[-2] in {"co", "com", "org", "gov", "edu"}:
        base = parts[-3]
    elif len(parts) >= 2 and parts[-2] in {"co", "com", "org", "net", "gov", "edu"}:
        base = parts[-3] if len(parts) >= 3 else parts[-2]
    elif len(parts) >= 2:
        base = parts[-2]
    else:
        base = parts[0]

    special = {
        "bbc": "BBC",
        "cnbc": "CNBC",
        "wsj": "WSJ",
        "nytimes": "The New York Times",
        "ft": "Financial Times",
    }
    if base in special:
        return special[base]
    return base.replace("-", " ").replace("_", " ").strip().title()


def _normalize_keywords(values: list[Any], limit: int = 10) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        label = _compact_text(str(value))
        if not label:
            continue
        label = label[:100]
        key = label.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(label)
        if len(out) >= limit:
            break
    return out


def _normalize_summary_text(text: str) -> str:
    cleaned = _compact_text(text)
    if not cleaned:
        return ""
    cleaned = re.sub(r"\s*(?:\.\.\.|…)+\s*$", "", cleaned).strip()
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if cleaned and cleaned[-1] not in ".!?":
        cleaned = f"{cleaned}."
    return cleaned


def _normalize_article_body_text(text: str) -> str:
    raw = (text or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    if not raw:
        return ""

    paragraphs: list[str] = []
    for block in re.split(r"\n\s*\n+", raw):
        lines: list[str] = []
        for line in block.split("\n"):
            candidate = re.sub(r"^[-*•]+\s*", "", line.strip())
            if not candidate:
                continue
            if re.fullmatch(r"https?://\S+", candidate):
                continue
            lines.append(candidate)
        paragraph = re.sub(r"\s+", " ", " ".join(lines)).strip()
        if paragraph:
            paragraphs.append(paragraph)

    if not paragraphs:
        fallback = _compact_text(raw)
        if fallback:
            paragraphs = [re.sub(r"^[-*•]+\s*", "", fallback).strip()]

    deduped: list[str] = []
    for paragraph in paragraphs:
        if deduped and deduped[-1].casefold() == paragraph.casefold():
            continue
        deduped.append(paragraph)

    if len(deduped) == 1:
        sentences = [s.strip() for s in re.split(r"(?<=[.!?])\s+", deduped[0]) if s.strip()]
        if len(sentences) >= 5:
            chunked: list[str] = []
            current: list[str] = []
            for sentence in sentences:
                current.append(sentence)
                if len(current) >= 2:
                    chunked.append(" ".join(current).strip())
                    current = []
            if current:
                chunked.append(" ".join(current).strip())
            deduped = chunked

    return "\n\n".join(deduped[:8])


def _derive_section(category_value: str, title: str) -> str:
    text = f"{category_value} {title}".lower()
    if any(k in text for k in ("policy", "regulation", "government", "law", "ethics", "safety")):
        return "Policy & Governance"
    if any(k in text for k in ("chip", "hardware", "infrastructure", "data center", "gpu", "semiconductor")):
        return "Infrastructure & Hardware"
    if any(k in text for k in ("funding", "acquisition", "merger", "startup", "investment")):
        return "Business & Finance"
    if any(k in text for k in ("research", "model", "llm", "benchmark", "open source")):
        return "Models & Research"
    if any(k in text for k in ("tool", "application", "product", "platform")):
        return "Products & Applications"
    return "Industry & Markets"


def _taxonomy_defaults(article: dict[str, Any]) -> dict[str, Any]:
    out = dict(article)
    section = str(out.get("section", "")).strip()[:120]
    category_value = str(out.get("category", "")).strip()[:120]
    subcategory = str(out.get("subcategory", "")).strip()[:120]
    categories = _normalize_keywords(out.get("categories", []), limit=10)

    if not category_value and categories:
        category_value = categories[0][:120]
    if not category_value:
        category_value = "General AI"
    if not subcategory:
        subcategory = category_value
    if not section:
        section = _derive_section(category_value, str(out.get("title", "")))

    out["section"] = section
    out["category"] = category_value
    out["subcategory"] = subcategory
    out["categories"] = categories
    return out


def _looks_like_medium_post(url: str) -> bool:
    normalized = _normalize_url(url)
    if not normalized:
        return False
    try:
        parsed = urlparse(normalized)
    except Exception:
        return False
    host = (parsed.hostname or "").lower()
    if "medium.com" in host:
        return True
    if host in MEDIUM_CUSTOM_HOSTS:
        return True
    # Heuristic slug-only detection must be restricted to known Medium hosts.
    return False


def _rewrite_medium_url(url: str) -> str:
    normalized = _normalize_url(url)
    if not normalized:
        return ""
    if normalized.startswith("https://freedium-mirror.cfd/"):
        return normalized
    try:
        host = (urlparse(normalized).hostname or "").lower()
    except Exception:
        return normalized
    if "medium.com" in host or _looks_like_medium_post(normalized):
        return f"https://freedium-mirror.cfd/{normalized}"
    return normalized


def _extract_urls(text: str) -> list[str]:
    urls = [_normalize_url(m.group(0)) for m in URL_RE.finditer(text or "")]
    out: list[str] = []
    seen: set[str] = set()
    for url in urls:
        if not url or url in seen:
            continue
        seen.add(url)
        out.append(url)
    return out


def _extract_raw_angle_bracket_urls(raw_html: str) -> list[str]:
    urls = [_normalize_url(m.group(1)) for m in ANGLE_BRACKET_URL_RE.finditer(raw_html or "")]
    out: list[str] = []
    seen: set[str] = set()
    for url in urls:
        if not url or url in seen:
            continue
        seen.add(url)
        out.append(url)
    return out


def _extract_forwarded_subject_hint(raw_html: str) -> str:
    if not raw_html:
        return ""
    try:
        text = BeautifulSoup(raw_html, "lxml").get_text("\n", strip=True)
    except Exception:
        text = raw_html
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    header_re = re.compile(r"^(from|de|sent|date|enviado|to|para|cc|bcc|subject|assunto)\s*:", re.IGNORECASE)
    label_like_re = re.compile(r"^[A-Za-zÀ-ÖØ-öø-ÿ][A-Za-zÀ-ÖØ-öø-ÿ0-9 _-]{0,32}:\s*", re.IGNORECASE)
    for idx, line in enumerate(lines):
        m = re.match(r"^(subject|assunto)\s*:\s*(.*)$", line, flags=re.IGNORECASE)
        if not m:
            continue
        parts = [m.group(2).strip()] if m.group(2).strip() else []
        cursor = idx + 1
        continuation_count = 0
        while cursor < len(lines):
            nxt = lines[cursor].strip()
            if not nxt:
                break
            if header_re.match(nxt):
                break
            if label_like_re.match(nxt):
                break
            parts.append(nxt)
            continuation_count += 1
            cursor += 1
            if continuation_count >= 2 or len(" ".join(parts)) >= 320:
                break
        candidate = _compact_text(" ".join(parts))[:320]
        if candidate:
            return candidate

    for pattern in (r"^\s*Subject\s*:\s*(.+)$", r"^\s*Assunto\s*:\s*(.+)$"):
        m = re.search(pattern, text, flags=re.IGNORECASE | re.MULTILINE)
        if m:
            candidate = _compact_text(m.group(1))[:320]
            if candidate:
                return candidate
    return ""


def _extract_anchor_context_text(anchor: Tag) -> str:
    text = _compact_text(anchor.get_text(" ", strip=True))
    if text and len(re.findall(r"[A-Za-z0-9]+", text)) >= 2 and not ANCHOR_NOISE_TEXT_RE.search(text):
        return text[:320]

    img = anchor.find("img")
    if isinstance(img, Tag):
        alt = _compact_text(str(img.get("alt", "")))
        if alt and len(re.findall(r"[A-Za-z0-9]+", alt)) >= 2 and not ANCHOR_NOISE_TEXT_RE.search(alt):
            return alt[:320]

    container = anchor
    allowed = {"td", "div", "tr", "li", "p", "section", "article", "body"}
    while isinstance(container, Tag):
        if container.name in allowed:
            context = _compact_text(container.get_text(" ", strip=True))
            if context and len(re.findall(r"[A-Za-z0-9]+", context)) >= 2 and not ANCHOR_NOISE_TEXT_RE.search(context):
                return context[:320]
        container = container.parent
    return ""


def _extract_email_anchor_candidates(raw_html: str, *, limit: int = 250) -> list[dict[str, str]]:
    if not raw_html:
        return []
    try:
        soup = BeautifulSoup(raw_html, "lxml")
    except Exception:
        return []

    out: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for anchor in soup.find_all("a", href=True):
        href = _normalize_url(str(anchor.get("href", "")).strip())
        if not href.startswith(("http://", "https://")):
            continue
        if _is_noise_link(href):
            continue

        text = _extract_anchor_context_text(anchor)
        if not text or len(text) < 10:
            continue
        if len(re.findall(r"[A-Za-z0-9]+", text)) < 2:
            continue
        if ANCHOR_NOISE_TEXT_RE.search(text):
            continue

        key = (text.casefold(), href)
        if key in seen:
            continue
        seen.add(key)
        out.append({"text": text[:320], "url": href})
        if len(out) >= limit:
            break
    return out


def _text_overlap_ratio(reference_text: str, candidate_text: str) -> float:
    ref_tokens = _tokenize_title_for_source_match(reference_text)
    if not ref_tokens:
        return 0.0
    candidate_blob = str(candidate_text or "").lower()
    matches = {token for token in ref_tokens if token in candidate_blob}
    return len(matches) / max(1, len(ref_tokens))


def _attach_email_anchor_source(
    article: dict[str, Any],
    anchor_candidates: list[dict[str, str]],
    *,
    subject_hint: str = "",
    single_article_mode: bool = False,
) -> dict[str, Any]:
    out = dict(article)
    if not anchor_candidates:
        if subject_hint and single_article_mode:
            out["newsletter_subject_hint"] = subject_hint
        return out

    existing = _normalize_url(str(out.get("original_url", "")).strip())
    existing_host = _normalized_host_from_url(existing)
    if existing and not _is_generic_source_url(existing) and not existing_host.startswith("link.mail."):
        if subject_hint and single_article_mode:
            out["newsletter_subject_hint"] = subject_hint
        return out

    article_title = str(out.get("title", "")).strip()
    best_row: dict[str, Any] | None = None
    for row in anchor_candidates:
        anchor_text = str(row.get("text", "")).strip()
        anchor_url = _normalize_url(str(row.get("url", "")).strip())
        if not anchor_text or not anchor_url:
            continue
        title_ratio = _text_overlap_ratio(article_title, anchor_text)
        subject_ratio = _text_overlap_ratio(subject_hint, anchor_text) if subject_hint else 0.0
        score = int(title_ratio * 100) + int(subject_ratio * 80)
        if title_ratio >= 0.45:
            score += 20
        if subject_ratio >= 0.45:
            score += 16
        if existing and anchor_url == existing:
            score += 8
        if not best_row or score > int(best_row.get("score", -1)):
            best_row = {
                "url": anchor_url,
                "text": anchor_text,
                "score": score,
                "title_ratio": title_ratio,
                "subject_ratio": subject_ratio,
            }

    if not best_row:
        if subject_hint and single_article_mode:
            out["newsletter_subject_hint"] = subject_hint
        return out

    title_ratio = float(best_row.get("title_ratio", 0.0) or 0.0)
    subject_ratio = float(best_row.get("subject_ratio", 0.0) or 0.0)
    allow = title_ratio >= 0.35 or subject_ratio >= 0.45 or (single_article_mode and subject_ratio >= 0.30)
    if not allow:
        if subject_hint and single_article_mode:
            out["newsletter_subject_hint"] = subject_hint
        return out

    out["original_url"] = str(best_row["url"])
    out["source_origin"] = "direct_email_anchor"
    out["source_anchor_text"] = str(best_row["text"])
    out["source_anchor_title_overlap"] = round(title_ratio, 3)
    out["source_anchor_subject_overlap"] = round(subject_ratio, 3)
    if subject_hint and single_article_mode:
        out["newsletter_subject_hint"] = subject_hint
    return out


def _is_noise_link(url: str) -> bool:
    try:
        parsed = urlparse(url)
    except Exception:
        return True
    host = (parsed.hostname or "").lower()
    path = parsed.path.lower()
    if host in NOISE_HOSTS:
        return True

    if "linkedin.com" in host:
        # Blocklist exaustiva: bloqueia perfis, empresas, vagas, eventos e páginas de sistema.
        # Deixa passar /pulse/, /article/, /newsletters/, /posts/ e /feed/update/.
        blocked_paths = (
            "/in/", "/company/", "/jobs/", "/events/", "/school/",
            "/groups/", "/learning/", "/services/", "/sales/",
            "/talent/", "/premium/", "/help/", "/policy/",
            "/legal/", "/pub/", "/directory/", "/showcase/",
            "/login/", "/signup/", "/auth/"
        )
        if path.startswith(blocked_paths):
            return True

    if "substack.com" in host and path.startswith("/redirect/2/"):
        return True
    if "utm_campaign=email-reaction" in url or "utm_campaign=email-share" in url:
        return True
    if "disable_email" in url or "signup" in url:
        return True
    return False


def _pick_primary_content_url(text: str) -> str:
    urls = _extract_urls(text)
    if not urls:
        return ""

    preferred = [u for u in urls if "substack.com/app-link/post" in u and "post-email-title" in u]
    if preferred:
        return preferred[0]

    preferred = [u for u in urls if "substack.com/app-link/post" in u]
    if preferred:
        return preferred[0]

    preferred = [u for u in urls if "open.substack.com" in u and "/p/" in u]
    if preferred:
        return preferred[0]

    preferred = [u for u in urls if "substack.com/p/" in u]
    if preferred:
        return preferred[0]

    for url in urls:
        if _is_noise_link(url):
            continue
        if _is_generic_source_url(url):
            continue
        return url
    # If all URLs are noise/tracking/UI links, do not force a primary fetch target.
    return ""


def _is_promo_text(text: str) -> bool:
    value = (text or "").strip().lower()
    if not value:
        return True
    return any(re.search(pat, value, re.IGNORECASE) for pat in PROMO_PATTERNS)


def _compact_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


def _fetch_url(url: str, retries: int = 3, timeout: int = 30) -> requests.Response:
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, headers=HTTP_HEADERS, allow_redirects=True, timeout=timeout)
            resp.raise_for_status()
            return resp
        except Exception as exc:
            last_err = exc
            LOG.warning("fetch failed attempt=%s url=%s err=%s", attempt, url, exc)
            time.sleep(1.2 * attempt)
    raise RuntimeError(f"failed to fetch {url}: {last_err}")


def _normalize_datetime(value: str) -> str:
    raw = _compact_text(value)
    if not raw:
        return ""

    if raw.endswith("Z"):
        iso_candidate = raw[:-1] + "+00:00"
    else:
        iso_candidate = raw

    try:
        dt = datetime.fromisoformat(iso_candidate)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except Exception:
        pass

    try:
        dt = parsedate_to_datetime(raw)
        if dt is not None:
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.isoformat()
    except Exception:
        pass

    if re.match(r"^\d{4}-\d{2}-\d{2}$", raw):
        return f"{raw}T00:00:00+00:00"
    return ""


def _extract_published_at_from_url(url: str) -> str:
    m = URL_DATE_RE.search(url or "")
    if not m:
        return ""
    yyyy, mm, dd = m.groups()
    return f"{yyyy}-{mm}-{dd}T00:00:00+00:00"


def _extract_published_at_from_json_ld(soup: BeautifulSoup) -> str:
    fields = ("datePublished", "dateCreated", "uploadDate", "dateModified")

    def _walk(node: Any) -> list[dict[str, Any]]:
        if isinstance(node, dict):
            out = [node]
            for value in node.values():
                out.extend(_walk(value))
            return out
        if isinstance(node, list):
            out: list[dict[str, Any]] = []
            for item in node:
                out.extend(_walk(item))
            return out
        return []

    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = script.string or script.get_text("\n", strip=True)
        if not raw:
            continue
        try:
            parsed = json.loads(raw)
        except Exception:
            continue
        for obj in _walk(parsed):
            for field in fields:
                value = str(obj.get(field, "")).strip()
                normalized = _normalize_datetime(value)
                if normalized:
                    return normalized
    return ""


def _extract_published_at_from_soup(soup: BeautifulSoup, source_url: str) -> str:
    for tag_name, attr_name, attr_value in PUBLISHED_META_FIELDS:
        tag = soup.find(tag_name, attrs={attr_name: attr_value})
        if tag and tag.get("content"):
            normalized = _normalize_datetime(str(tag.get("content")))
            if normalized:
                return normalized

    for time_tag in soup.find_all("time"):
        value = str(time_tag.get("datetime", "")).strip() or time_tag.get_text(" ", strip=True)
        normalized = _normalize_datetime(value)
        if normalized:
            return normalized

    from_json_ld = _extract_published_at_from_json_ld(soup)
    if from_json_ld:
        return from_json_ld

    return _extract_published_at_from_url(source_url)


def _same_effective_url(left: str, right: str) -> bool:
    left_norm = _normalize_url(left)
    right_norm = _normalize_url(right)
    if not left_norm or not right_norm:
        return False
    try:
        left_parsed = urlparse(left_norm)
        right_parsed = urlparse(right_norm)
    except Exception:
        return left_norm.rstrip("/") == right_norm.rstrip("/")

    left_host = (left_parsed.hostname or "").lower().strip(".")
    right_host = (right_parsed.hostname or "").lower().strip(".")
    left_path = (left_parsed.path or "").rstrip("/")
    right_path = (right_parsed.path or "").rstrip("/")
    return left_host == right_host and left_path == right_path


def _fallback_published_at_via_searxng(source_url: str, searxng_url: str) -> str:
    normalized_source = _normalize_url(source_url)
    base = str(searxng_url or "").strip()
    if not normalized_source or not base:
        return ""

    try:
        resp = requests.get(
            f"{base.rstrip('/')}/search",
            params={"q": normalized_source, "format": "json"},
            headers=HTTP_HEADERS,
            timeout=12,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        LOG.warning("searxng source date fallback failed url=%s err=%s", normalized_source, exc)
        return ""

    results = data.get("results", []) if isinstance(data, dict) else []
    if not isinstance(results, list):
        return ""

    for row in results:
        if not isinstance(row, dict):
            continue
        candidate_url = _normalize_url(str(row.get("url") or row.get("link") or "").strip())
        if not candidate_url:
            continue
        if not _same_effective_url(candidate_url, normalized_source):
            continue
        for field_name in ("publishedDate", "published_date"):
            normalized = _normalize_datetime(str(row.get(field_name, "")).strip())
            if normalized:
                return normalized
    return ""


def _resolve_relative_url(url: str, base_url: str) -> str:
    value = _normalize_url(url)
    if not value:
        return ""
    if value.startswith("http://") or value.startswith("https://"):
        return value
    if value.startswith("//"):
        scheme = urlparse(base_url).scheme or "https"
        return f"{scheme}:{value}"
    return ""


def _extract_image_from_json_ld(soup: BeautifulSoup, source_url: str) -> str:
    def _walk(node: Any) -> list[dict[str, Any]]:
        if isinstance(node, dict):
            out = [node]
            for value in node.values():
                out.extend(_walk(value))
            return out
        if isinstance(node, list):
            out: list[dict[str, Any]] = []
            for item in node:
                out.extend(_walk(item))
            return out
        return []

    def _from_image_field(value: Any) -> str:
        if isinstance(value, str):
            return _resolve_relative_url(value, source_url)
        if isinstance(value, list):
            for item in value:
                resolved = _from_image_field(item)
                if resolved:
                    return resolved
        if isinstance(value, dict):
            for key in ("url", "contentUrl", "thumbnailUrl"):
                resolved = _resolve_relative_url(str(value.get(key, "")).strip(), source_url)
                if resolved:
                    return resolved
        return ""

    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = script.string or script.get_text("\n", strip=True)
        if not raw:
            continue
        try:
            parsed = json.loads(raw)
        except Exception:
            continue
        for obj in _walk(parsed):
            resolved = _from_image_field(obj.get("image"))
            if resolved:
                return resolved
    return ""


def _extract_image_from_soup(soup: BeautifulSoup, source_url: str) -> str:
    for tag_name, attr_name, attr_value in IMAGE_META_FIELDS:
        tag = soup.find(tag_name, attrs={attr_name: attr_value})
        if tag and tag.get("content"):
            resolved = _resolve_relative_url(str(tag.get("content")), source_url)
            if resolved:
                return resolved

    from_json_ld = _extract_image_from_json_ld(soup, source_url)
    if from_json_ld:
        return from_json_ld

    root = soup.find("article") or soup.find("main") or soup
    for img in root.find_all("img", src=True):
        src = _resolve_relative_url(str(img.get("src", "")), source_url)
        if not src:
            continue
        lower = src.lower()
        if any(noise in lower for noise in ("logo", "avatar", "icon", "pixel")):
            continue
        return src
    return ""


def _is_external_content_link(url: str) -> bool:
    normalized = _normalize_url(url)
    if not normalized:
        return False
    if not normalized.startswith("http"):
        return False
    if _is_noise_link(normalized):
        return False
    try:
        host = (urlparse(normalized).hostname or "").lower()
    except Exception:
        return False
    if any(
        blocked in host
        for blocked in (
            "freedium-mirror.cfd",
            "aibuzz.me",
            "aibuzz.news",
            "medium.com",
            "substack.com",
            "patreon.com",
            "ko-fi.com",
            "liberapay.com",
            "instagram.com",
            "facebook.com",
            "twitter.com",
            "x.com",
        )
    ):
        return False
    return True


def _find_source_link_in_medium_page(root: Tag) -> str:
    for anchor in root.find_all("a", href=True):
        href = _normalize_url(anchor.get("href", ""))
        if not _is_external_content_link(href):
            continue
        context = _compact_text(anchor.parent.get_text(" ", strip=True) if anchor.parent else "").lower()
        if "source" in context:
            return href

    for text_node in root.find_all(string=re.compile(r"^\s*source\s*:?\s*$", re.IGNORECASE)):
        parent = text_node.parent
        if parent is None:
            continue
        for anchor in parent.find_all_next("a", href=True, limit=6):
            href = _normalize_url(anchor.get("href", ""))
            if _is_external_content_link(href):
                return href

    outbound: list[str] = []
    seen: set[str] = set()
    for anchor in root.find_all("a", href=True):
        href = _normalize_url(anchor.get("href", ""))
        if not _is_external_content_link(href):
            continue
        if href in seen:
            continue
        seen.add(href)
        outbound.append(href)
    if len(outbound) == 1:
        return outbound[0]
    return ""


def _get_published_at(url: str) -> str:
    normalized = _normalize_url(url)
    if not normalized:
        return ""
    if normalized in PUBLISHED_AT_CACHE:
        return PUBLISHED_AT_CACHE[normalized]
    try:
        resp = _fetch_url(normalized)
        soup = BeautifulSoup(resp.text, "lxml")
        value = _extract_published_at_from_soup(soup, normalized)
    except Exception as exc:
        LOG.warning("published_at resolution failed url=%s err=%s", normalized, exc)
        value = _extract_published_at_from_url(normalized)
    PUBLISHED_AT_CACHE[normalized] = value
    return value


def _get_source_image(url: str) -> str:
    normalized = _normalize_url(url)
    if not normalized:
        return ""
    if normalized in IMAGE_URL_CACHE:
        return IMAGE_URL_CACHE[normalized]
    try:
        resp = _fetch_url(normalized)
        soup = BeautifulSoup(resp.text, "lxml")
        value = _extract_image_from_soup(soup, normalized)
    except Exception as exc:
        LOG.warning("image resolution failed url=%s err=%s", normalized, exc)
        value = ""
    IMAGE_URL_CACHE[normalized] = value
    return value


def _host_matches_blocklist(host: str, blocked_hosts: set[str]) -> bool:
    normalized_host = str(host or "").strip().lower().lstrip(".")
    if not normalized_host:
        return False
    return any(normalized_host == blocked or normalized_host.endswith(f".{blocked}") for blocked in blocked_hosts)


def _is_blocked_web_image_page(url: str) -> bool:
    host = _normalized_host_from_url(url)
    return _host_matches_blocklist(host, WEB_IMAGE_BLOCKED_PAGE_HOSTS)


def _is_blocked_web_image_asset(url: str) -> bool:
    normalized = _normalize_url(url)
    if not normalized:
        return True
    host = _normalized_host_from_url(normalized)
    if _host_matches_blocklist(host, WEB_IMAGE_BLOCKED_ASSET_HOSTS):
        return True
    lower = normalized.lower()
    return any(term in lower for term in WEB_IMAGE_NOISE_TERMS)


def _probe_image_url(url: str) -> dict[str, Any]:
    normalized = _normalize_url(url)
    if not normalized:
        return {"ok": False, "url": "", "final_url": "", "content_type": "", "content_length": 0}

    cached = IMAGE_PROBE_CACHE.get(normalized)
    if cached is not None:
        return cached

    result = {
        "ok": False,
        "url": normalized,
        "final_url": normalized,
        "content_type": "",
        "content_length": 0,
    }
    if _is_blocked_web_image_asset(normalized):
        IMAGE_PROBE_CACHE[normalized] = result
        return result

    response: requests.Response | None = None
    try:
        response = requests.get(
            normalized,
            headers=HTTP_HEADERS,
            allow_redirects=True,
            timeout=15,
            stream=True,
        )
        final_url = _normalize_url(str(response.url or "").strip()) or normalized
        content_type = str(response.headers.get("Content-Type", "")).split(";", 1)[0].strip().lower()
        try:
            content_length = int(str(response.headers.get("Content-Length", "0")).strip() or 0)
        except Exception:
            content_length = 0
        ok = bool(response.ok and content_type.startswith("image/"))
        if ok and content_length and content_length < 5_000:
            ok = False
        result = {
            "ok": ok,
            "url": normalized,
            "final_url": final_url,
            "content_type": content_type,
            "content_length": content_length,
        }
    except Exception:
        pass
    finally:
        if response is not None:
            response.close()

    IMAGE_PROBE_CACHE[normalized] = result
    final_url = _normalize_url(str(result.get("final_url", "")).strip())
    if final_url and final_url != normalized:
        IMAGE_PROBE_CACHE[final_url] = result
    return result


def _image_resolution_area(value: str) -> int:
    raw = str(value or "").replace(chr(215), "x")
    match = re.search(r"(\d{2,5})\s*x\s*(\d{2,5})", raw, flags=re.IGNORECASE)
    if not match:
        return 0
    try:
        width = int(match.group(1))
        height = int(match.group(2))
    except Exception:
        return 0
    return max(0, width) * max(0, height)


def _content_length_bonus(content_length: int) -> int:
    size = int(content_length or 0)
    if size >= 250_000:
        return 12
    if size >= 100_000:
        return 8
    if size >= 40_000:
        return 4
    return 0


def _web_image_search_cache_key(article: dict[str, Any]) -> str:
    categories_raw = article.get("categories", []) or []
    if not isinstance(categories_raw, list):
        categories_raw = [categories_raw]
    payload = {
        "title": str(article.get("title", "")).strip(),
        "summary": str(article.get("summary", "")).strip()[:300],
        "article_body": str(article.get("article_body", "")).strip()[:800],
        "enrichment_context": str(article.get("enrichment_context", "")).strip()[:800],
        "original_url": _normalize_url(str(article.get("original_url", "")).strip()),
        "categories": [str(v).strip() for v in categories_raw[:4] if str(v).strip()],
        "source_name": str(article.get("source_name", "")).strip(),
    }
    return json.dumps(payload, sort_keys=True)


def _article_has_resolved_image(article: dict[str, Any]) -> bool:
    image_url = _normalize_url(str(article.get("image_url", "")).strip())
    if image_url and not _is_blocked_web_image_asset(image_url):
        return True
    source_image_url = _normalize_url(str(article.get("source_image_url", "")).strip())
    if source_image_url and not _is_blocked_web_image_asset(source_image_url):
        return True
    email_images = article.get("email_images", []) or []
    if not isinstance(email_images, list):
        email_images = [email_images]
    for row in email_images:
        candidate = _normalize_url(str(row).strip())
        if candidate and not _is_blocked_web_image_asset(candidate):
            return True
    return False


def _build_web_image_queries(article: dict[str, Any]) -> list[str]:
    title = _compact_text(str(article.get("title", "")).strip())
    if not title:
        return []

    title_lower = title.lower()
    queries: list[str] = [title]
    source_name = _compact_text(str(article.get("source_name", "")).strip())
    categories_raw = article.get("categories", []) or []
    if not isinstance(categories_raw, list):
        categories_raw = [categories_raw]
    categories = [str(v).strip() for v in categories_raw if str(v).strip()]
    extras: list[str] = []

    if source_name and source_name.lower() not in title_lower:
        queries.append(f"{title} {source_name}")

    for category in categories[:3]:
        compact = _compact_text(category)
        if compact and compact.lower() not in title_lower and compact not in extras:
            extras.append(compact)

    text_blob = "\n".join(
        [
            str(article.get("summary", "")).strip(),
            str(article.get("article_body", "")).strip()[:800],
            str(article.get("enrichment_context", "")).strip()[:800],
        ]
    ).strip()
    title_tokens = set(_tokenize_title_for_source_match(title))
    for token in _tokenize_title_for_source_match(text_blob):
        if token in title_tokens:
            continue
        if token in extras:
            continue
        if len(token) < 6:
            continue
        extras.append(token)
        if len(extras) >= 4:
            break

    if extras:
        queries.append(f"{title} {' '.join(extras[:2])}")
    if source_name and extras:
        queries.append(f"{title} {source_name} {extras[0]}")

    deduped: list[str] = []
    seen: set[str] = set()
    for query in queries:
        compact = _compact_text(query)
        if not compact:
            continue
        key = compact.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(compact)
    return deduped[:4]


def _searxng_search(cfg: Config, query: str, *, categories: str = "") -> list[dict[str, Any]]:
    base = str(cfg.searxng_url or "").strip()
    compact_query = _compact_text(query)
    if not base or not compact_query:
        return []

    params = {"q": compact_query, "format": "json"}
    if categories:
        params["categories"] = categories

    try:
        resp = requests.get(
            f"{base.rstrip('/')}/search",
            params=params,
            headers=HTTP_HEADERS,
            timeout=12,
        )
        resp.raise_for_status()
        payload = resp.json()
    except Exception as exc:
        LOG.warning("searxng query failed query=%r categories=%s err=%s", compact_query, categories or "general", exc)
        return []

    rows = payload.get("results", []) if isinstance(payload, dict) else []
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def _score_web_image_page_result(
    *,
    article_title: str,
    original_url: str,
    page_url: str,
    result_title: str,
    snippet: str,
) -> int:
    normalized_page = _normalize_url(page_url)
    if not normalized_page:
        return -10_000
    if _is_generic_source_url(normalized_page):
        return -10_000
    if _is_blocked_web_image_page(normalized_page):
        return -10_000

    score = _score_discovered_source_url(
        article_title=article_title,
        url=normalized_page,
        result_title=result_title,
        snippet=snippet,
    )
    if score <= -10_000:
        return score

    overlap_ratio = _text_overlap_ratio(article_title, f"{result_title}\n{snippet}")
    score += int(overlap_ratio * 100)

    preferred_host = _normalized_host_from_url(original_url)
    candidate_host = _normalized_host_from_url(normalized_page)
    if original_url and _same_effective_url(normalized_page, original_url):
        score += 80
    elif preferred_host and candidate_host == preferred_host:
        score += 18
    return score


def _score_web_image_result(
    *,
    article_title: str,
    original_url: str,
    page_url: str,
    image_url: str,
    result_title: str,
    snippet: str,
    resolution: str,
) -> int:
    normalized_image = _normalize_url(image_url)
    if not normalized_image or _is_blocked_web_image_asset(normalized_image):
        return -10_000

    normalized_page = _normalize_url(page_url)
    if normalized_page and _is_blocked_web_image_page(normalized_page):
        return -10_000

    basis_url = normalized_page or normalized_image
    score = _score_discovered_source_url(
        article_title=article_title,
        url=basis_url,
        result_title=result_title,
        snippet=snippet,
    )
    if score <= -10_000:
        return score

    overlap_ratio = _text_overlap_ratio(article_title, f"{result_title}\n{snippet}\n{normalized_page}")
    score += int(overlap_ratio * 100)

    preferred_host = _normalized_host_from_url(original_url)
    candidate_host = _normalized_host_from_url(normalized_page)
    if original_url and normalized_page and _same_effective_url(normalized_page, original_url):
        score += 90
    elif preferred_host and candidate_host == preferred_host:
        score += 24

    area = _image_resolution_area(resolution)
    if area >= 1_200 * 630:
        score += 16
    elif area >= 800 * 420:
        score += 10
    elif area >= 500 * 260:
        score += 4

    noise_blob = f"{normalized_page}\n{normalized_image}\n{result_title}".lower()
    if any(term in noise_blob for term in WEB_IMAGE_NOISE_TERMS):
        score -= 40
    return score


def _find_web_image_from_search_pages(cfg: Config, article: dict[str, Any], queries: list[str]) -> dict[str, Any]:
    title = _compact_text(str(article.get("title", "")).strip())
    original_url = _normalize_url(str(article.get("original_url", "")).strip())
    candidates: list[dict[str, Any]] = []
    seen_pages: set[str] = set()

    for query in queries:
        rows = _searxng_search(cfg, query)
        for row in rows[: max(1, cfg.web_image_search_max_candidates)]:
            page_url = _normalize_url(str(row.get("url") or row.get("link") or "").strip())
            if not page_url or page_url in seen_pages:
                continue
            seen_pages.add(page_url)

            result_title = _compact_text(str(row.get("title", "")).strip())
            snippet = _compact_text(str(row.get("content", "")).strip())
            score = _score_web_image_page_result(
                article_title=title,
                original_url=original_url,
                page_url=page_url,
                result_title=result_title,
                snippet=snippet,
            )
            if score < cfg.web_image_search_min_score:
                continue

            page_image_url = _get_source_image(page_url)
            if not page_image_url or _is_blocked_web_image_asset(page_image_url):
                continue

            probe = _probe_image_url(page_image_url)
            if not bool(probe.get("ok")):
                continue

            final_image_url = _normalize_url(str(probe.get("final_url", "")).strip()) or page_image_url
            final_score = score + _content_length_bonus(int(probe.get("content_length", 0) or 0))
            candidate = {
                "query": query,
                "page_url": page_url,
                "image_url": final_image_url,
                "score": final_score,
                "title": result_title,
                "strategy": "search_results_page_image",
            }
            if original_url and _same_effective_url(page_url, original_url):
                return {
                    "selected_url": str(candidate.get("image_url", "")).strip(),
                    "selected_page_url": str(candidate.get("page_url", "")).strip(),
                    "selected_score": int(candidate.get("score", 0) or 0),
                    "query": str(candidate.get("query", "")).strip(),
                    "strategy": str(candidate.get("strategy", "")).strip(),
                    "candidates": [candidate],
                }
            candidates.append(candidate)

    if not candidates:
        return {}

    candidates.sort(key=lambda row: int(row.get("score", 0) or 0), reverse=True)
    best = candidates[0]
    return {
        "selected_url": str(best.get("image_url", "")).strip(),
        "selected_page_url": str(best.get("page_url", "")).strip(),
        "selected_score": int(best.get("score", 0) or 0),
        "query": str(best.get("query", "")).strip(),
        "strategy": str(best.get("strategy", "")).strip(),
        "candidates": candidates[:5],
    }


def _find_web_image_from_image_results(cfg: Config, article: dict[str, Any], queries: list[str]) -> dict[str, Any]:
    title = _compact_text(str(article.get("title", "")).strip())
    original_url = _normalize_url(str(article.get("original_url", "")).strip())
    candidates: list[dict[str, Any]] = []
    seen_images: set[str] = set()

    for query in queries:
        rows = _searxng_search(cfg, query, categories="images")
        for row in rows[: max(1, cfg.web_image_search_max_candidates)]:
            page_url = _normalize_url(str(row.get("url") or row.get("link") or "").strip())
            image_url = _normalize_url(str(row.get("img_src") or row.get("image") or "").strip())
            if not image_url or image_url in seen_images:
                continue
            seen_images.add(image_url)

            result_title = _compact_text(str(row.get("title", "")).strip())
            snippet = _compact_text(str(row.get("content", "")).strip())
            score = _score_web_image_result(
                article_title=title,
                original_url=original_url,
                page_url=page_url,
                image_url=image_url,
                result_title=result_title,
                snippet=snippet,
                resolution=str(row.get("resolution", "")).strip(),
            )
            if score < cfg.web_image_search_min_score:
                continue

            probe = _probe_image_url(image_url)
            if not bool(probe.get("ok")):
                continue

            final_image_url = _normalize_url(str(probe.get("final_url", "")).strip()) or image_url
            final_score = score + _content_length_bonus(int(probe.get("content_length", 0) or 0))
            candidate = {
                "query": query,
                "page_url": page_url,
                "image_url": final_image_url,
                "score": final_score,
                "title": result_title,
                "strategy": "search_results_image_asset",
            }
            if original_url and page_url and _same_effective_url(page_url, original_url):
                return {
                    "selected_url": str(candidate.get("image_url", "")).strip(),
                    "selected_page_url": str(candidate.get("page_url", "")).strip(),
                    "selected_score": int(candidate.get("score", 0) or 0),
                    "query": str(candidate.get("query", "")).strip(),
                    "strategy": str(candidate.get("strategy", "")).strip(),
                    "candidates": [candidate],
                }
            candidates.append(candidate)

    if not candidates:
        return {}

    candidates.sort(key=lambda row: int(row.get("score", 0) or 0), reverse=True)
    best = candidates[0]
    return {
        "selected_url": str(best.get("image_url", "")).strip(),
        "selected_page_url": str(best.get("page_url", "")).strip(),
        "selected_score": int(best.get("score", 0) or 0),
        "query": str(best.get("query", "")).strip(),
        "strategy": str(best.get("strategy", "")).strip(),
        "candidates": candidates[:5],
    }


def discover_web_image_for_article(cfg: Config, article: dict[str, Any]) -> dict[str, Any]:
    if not cfg.web_image_search_enabled:
        return {"attempted": False, "reason": "disabled", "selected_url": ""}
    if _article_has_resolved_image(article):
        return {"attempted": False, "reason": "existing_image", "selected_url": ""}

    title = _compact_text(str(article.get("title", "")).strip())
    if not title:
        return {"attempted": False, "reason": "missing_title", "selected_url": ""}

    cache_key = _web_image_search_cache_key(article)
    cached = WEB_IMAGE_SEARCH_CACHE.get(cache_key)
    if cached is not None:
        return cached

    queries = _build_web_image_queries(article)
    if not queries:
        result = {"attempted": False, "reason": "missing_query", "selected_url": ""}
        WEB_IMAGE_SEARCH_CACHE[cache_key] = result
        return result

    selection = _find_web_image_from_search_pages(cfg, article, queries)
    if not selection:
        selection = _find_web_image_from_image_results(cfg, article, queries)

    if selection:
        result = {
            "attempted": True,
            "reason": "ok",
            "strategy": str(selection.get("strategy", "")).strip(),
            "query": str(selection.get("query", "")).strip(),
            "queries": queries,
            "selected_url": str(selection.get("selected_url", "")).strip(),
            "selected_page_url": str(selection.get("selected_page_url", "")).strip(),
            "selected_score": int(selection.get("selected_score", 0) or 0),
            "candidates": selection.get("candidates", []),
        }
    else:
        result = {
            "attempted": True,
            "reason": "not_found",
            "strategy": "",
            "query": "",
            "queries": queries,
            "selected_url": "",
            "selected_page_url": "",
            "selected_score": 0,
            "candidates": [],
        }

    WEB_IMAGE_SEARCH_CACHE[cache_key] = result
    return result


def _attach_web_image_fallback(cfg: Config, article: dict[str, Any]) -> dict[str, Any]:
    item = dict(article)
    discovery = discover_web_image_for_article(cfg, item)
    if bool(discovery.get("attempted")):
        item["image_search"] = discovery

    selected_url = _normalize_url(str(discovery.get("selected_url", "")).strip())
    if selected_url and not _article_has_resolved_image(item):
        item["image_url"] = selected_url
        item["image_origin"] = "web_search"
        selected_page_url = _normalize_url(str(discovery.get("selected_page_url", "")).strip())
        if selected_page_url:
            item["image_source_page"] = selected_page_url
    return item


def _prefer_source_published_at(cfg: Config, article: dict[str, Any]) -> dict[str, Any]:
    item = dict(article)
    source_published_at = _normalize_datetime(str(item.get("source_published_at", "")).strip())
    original_url = _normalize_url(str(item.get("original_url", "")).strip())

    if not source_published_at and original_url:
        _, resolved_published_at, _ = _resolve_source_metadata(
            original_url,
            searxng_url=cfg.searxng_url,
        )
        source_published_at = _normalize_datetime(str(resolved_published_at or "").strip())

    if source_published_at:
        item["source_published_at"] = source_published_at
        item["published_at"] = source_published_at
    return item


def _resolve_source_metadata(url: str, *, searxng_url: str = "") -> tuple[str, str, str]:
    normalized = _normalize_url(url)
    if not normalized:
        return "", "", ""
    if normalized in SOURCE_META_CACHE:
        return SOURCE_META_CACHE[normalized]

    resolved_url = _rewrite_medium_url(normalized)
    published_at = ""
    image_url = ""

    try:
        resp = _fetch_url(resolved_url)
        final_url = _normalize_url(resp.url) or resolved_url
        soup = BeautifulSoup(resp.text, "lxml")
        root = soup.find("article") or soup.find("main") or soup.body or soup

        if _looks_like_medium_post(normalized) or "freedium-mirror.cfd" in ((urlparse(final_url).hostname or "").lower()):
            source_link = _find_source_link_in_medium_page(root)
            if source_link:
                source_link = _normalize_url(source_link)
                published_at = _get_published_at(source_link)
                image_url = _get_source_image(source_link)
                SOURCE_META_CACHE[normalized] = (source_link, published_at, image_url)
                return SOURCE_META_CACHE[normalized]

        published_at = _extract_published_at_from_soup(soup, final_url)
        if not published_at:
            published_at = _fallback_published_at_via_searxng(final_url, searxng_url)
        image_url = _extract_image_from_soup(soup, final_url)
        SOURCE_META_CACHE[normalized] = (final_url, published_at, image_url)
        return SOURCE_META_CACHE[normalized]
    except Exception as exc:
        LOG.warning("source resolution failed url=%s err=%s", normalized, exc)

    fallback_published_at = _extract_published_at_from_url(resolved_url)
    if not fallback_published_at:
        fallback_published_at = _fallback_published_at_via_searxng(resolved_url, searxng_url)
    SOURCE_META_CACHE[normalized] = (resolved_url, fallback_published_at, "")
    return SOURCE_META_CACHE[normalized]


def _apply_source_metadata(article: dict[str, Any], cfg: Config | None = None) -> dict[str, Any]:
    result = dict(article)
    original_url = _normalize_url(str(result.get("original_url", "")).strip())
    if not original_url:
        result.setdefault("source_origin", "missing")
        return _taxonomy_defaults(result)
    if _is_generic_source_url(original_url):
        result["original_url"] = ""
        result["source_origin"] = "direct_generic"
        result.setdefault(
            "source_discovery",
            {
                "attempted": False,
                "reason": "direct_generic_url",
                "selected_url": "",
            },
        )
        return _taxonomy_defaults(result)

    source_url, published_at, source_image_url = _resolve_source_metadata(
        original_url,
        searxng_url=(cfg.searxng_url if cfg is not None else os.getenv("SEARXNG_URL", "http://searxng:8080")),
    )
    resolved_source = source_url or _rewrite_medium_url(original_url)
    intermediate_url = ""
    if _looks_like_medium_post(original_url):
        intermediate_url = _rewrite_medium_url(original_url)
    elif resolved_source and resolved_source != original_url:
        intermediate_url = original_url

    result["original_url"] = resolved_source
    if intermediate_url and intermediate_url != resolved_source:
        result["intermediate_url"] = intermediate_url

    source_name = _source_name_from_url(resolved_source)
    if source_name:
        result["source_name"] = source_name
    if published_at:
        result["source_published_at"] = published_at
        result["published_at"] = published_at
    if source_image_url:
        result["source_image_url"] = source_image_url
    if not str(result.get("source_origin", "")).strip():
        result["source_origin"] = "direct"
    return _taxonomy_defaults(result)


def _clean_heading_title(text: str) -> str:
    title = _compact_text(text)
    # Remove leading emoji / bullets while preserving normal punctuation in the title body.
    title = re.sub(r"^[^\wA-Za-z]+", "", title).strip()
    return title


def _visible_text_length(text: str) -> int:
    cleaned = re.sub(r"[\u200b\u200c\u200d\ufeff\u034f\xad\s]", "", text or "")
    return len(cleaned)


def _is_valid_title_value(value: str | None) -> bool:
    if value is None:
        return False
    title = str(value).strip()
    if not title:
        return False
    return title.lower() not in INVALID_TITLE_VALUES


def _title_needs_refinement(value: str | None) -> bool:
    if not _is_valid_title_value(value):
        return True
    title = str(value).strip().lower()
    if any(hint in title for hint in GENERIC_TITLE_HINTS):
        return True
    if "highlighted" in title and "newsletter" in title:
        return True
    if "developments" in title and "newsletter" in title:
        return True
    if "updates on" in title:
        return True
    if ("developments" in title or "breakthrough" in title or "breakthroughs" in title) and (" and " in title or "/" in title):
        return True
    if any(token in title for token in ("clawdbot", "openclaw")) and (" and " in title or "/" in title or "," in title):
        return True
    return False


def _clean_title_prompt_text(article_text: str) -> str:
    text = _compact_text(article_text or "")
    if not text:
        return ""

    cut_markers = (
        "on a little tangent:",
        "on a side note:",
        "separately,",
        "meanwhile,",
    )
    lower = text.lower()
    cut_positions = []
    for marker in cut_markers:
        pos = lower.find(marker)
        if pos > 0:
            cut_positions.append(pos)
    if cut_positions:
        text = text[: min(cut_positions)].strip()

    chapter_match = re.search(r"(?:ch\s+\d+|chapter\s+\d+)", text, flags=re.IGNORECASE)
    if chapter_match and chapter_match.start() <= 180:
        text = text[chapter_match.start():].strip()

    text = re.sub(
        r"^(?:[A-Z][A-Za-z0-9&.,'/-]+\s+){2,10}(?=(?:How|Why|What|When|Reinforcement|Claude|BuildML|OpenAI|Anthropic|Google|Meta|NVIDIA|Mistral|Agent|Training|Reasoning|Ch\s+\d+|Chapter\s+\d+))",
        "",
        text,
    ).strip()
    return text[:3000]


def _propose_title_with_llm(cfg: Config, article_text: str, content_profile: str = "news") -> str | None:
    text = _clean_title_prompt_text(article_text)
    if not text:
        return None

    prompt = TITLE_PROPOSAL_PROMPT.format(
        content_profile=content_profile,
        article_text=text,
    )
    try:
        response = _llm_generate(cfg, prompt, model_override=cfg.ollama_model_title or "")
        parsed = _safe_json_object(response)
    except Exception as exc:
        LOG.warning("title proposal failed err=%s", exc)
        return None

    title = str(parsed.get("title", "")).strip()
    if not _is_valid_title_value(title):
        return None
    return title


def _ensure_article_title(cfg: Config, article: dict[str, Any], article_text: str, *, content_profile: str = "news") -> bool:
    raw_title = str(article.get("title", "")).strip()
    if _is_valid_title_value(raw_title):
        article["title"] = raw_title[:500]
        return True

    generated = _propose_title_with_llm(cfg, article_text, content_profile=content_profile)
    if not generated:
        return False

    article["title"] = generated[:500]
    article["title_origin"] = "llm_proposed"
    return True


def _is_email_junk(text: str) -> bool:
    value = str(text or "").strip()
    if _visible_text_length(value) < EMAIL_MIN_VISIBLE_CHARS:
        return True
    if any(pattern in value for pattern in EMAIL_DISCARD_PATTERNS):
        return True
    if any(value.endswith(suffix) for suffix in EMAIL_DISCARD_ENDSWITH):
        return True
    if value in EMAIL_DISCARD_EXACT:
        return True
    if any(fragment in value for fragment in EMAIL_DISCARD_CONTAINS):
        return True
    if EMAIL_PHONE_RE.match(value):
        return True
    return False


def _is_email_forward_header(text: str) -> tuple[str, str] | None:
    value = str(text or "")
    for prefix in EMAIL_FORWARD_HEADER_PREFIXES:
        if value.startswith(prefix):
            return prefix.rstrip(": "), value[len(prefix):].strip()
    return None


def _is_email_forward_label(text: str) -> str | None:
    stripped = str(text or "").strip()
    if stripped in EMAIL_FORWARD_LABELS:
        return stripped.rstrip(":")
    return None


def _get_email_img_dimension(img_tag: Tag) -> int:
    best = 0
    for attr in ("width", "height"):
        value = img_tag.get(attr, "")
        try:
            best = max(best, int(str(value).replace("px", "")))
        except (TypeError, ValueError):
            pass

    if best == 0:
        src = str(img_tag.get("src", ""))
        matches = re.findall(r"[wh]_(\d{2,})", src)
        if matches:
            best = max(int(match) for match in matches)
    return best


def _normalize_email_image_url(src: str) -> str:
    value = str(src or "").strip()
    if "substackcdn.com/image/fetch" in value:
        parts = value.split("/https%3A%2F%2F")
        if len(parts) > 1:
            return "https://" + urllib.parse.unquote(parts[1])
        parts = value.split("/https://")
        if len(parts) > 1:
            return "https://" + parts[1]
    return value


def _is_relevant_email_image(img_tag: Tag) -> bool:
    src = str(img_tag.get("src", "")).strip()
    if not src:
        return False
    src_lower = src.lower()
    if any(pattern in src_lower for pattern in EMAIL_IMAGE_DISCARD_PATTERNS):
        return False
    alt = str(img_tag.get("alt", "")).strip().lower()
    if alt in {"start writing", "substack"}:
        return False
    dim = _get_email_img_dimension(img_tag)
    if 0 < dim < EMAIL_IMAGE_MIN_SIZE:
        return False
    return True


def _collect_post_readmore_images(element: Tag, limit: int = 5) -> list[str]:
    images: list[str] = []
    current = element.parent
    while current and current.name not in {"td", "div", "tr", "body", "[document]"}:
        current = current.parent

    if not current:
        return images

    for sibling in current.find_next_siblings(limit=limit * 3):
        if len(images) >= limit:
            break
        if not isinstance(sibling, Tag):
            continue
        for img in sibling.find_all("img", recursive=True):
            if not _is_relevant_email_image(img):
                continue
            url = _normalize_email_image_url(str(img.get("src", "")))
            if url and url not in images:
                images.append(url)
                if len(images) >= limit:
                    break
    return images


def _extract_read_more_link(element: Tag) -> str:
    if element.name == "a":
        return _normalize_url(str(element.get("href", "")).strip())

    a_parent = element.find_parent("a")
    if isinstance(a_parent, Tag):
        return _normalize_url(str(a_parent.get("href", "")).strip())

    previous = element.find_all_previous("a", limit=1)
    if previous:
        return _normalize_url(str(previous[0].get("href", "")).strip())
    return ""


def _finalize_email_segment(segment: dict[str, Any]) -> dict[str, Any] | None:
    text_lines = [_compact_text(line) for line in segment.get("text", []) if _compact_text(line)]
    if not text_lines:
        return None
    full_text = " ".join(text_lines).strip()
    if _visible_text_length(full_text) <= 20:
        return None
    images = list(dict.fromkeys(str(url).strip() for url in segment.get("images", []) if str(url).strip()))
    return {
        "text": text_lines,
        "full_text": full_text,
        "images": images,
        "original_url": _normalize_url(str(segment.get("original_url", "")).strip()),
    }


def _fallback_title_from_text(text: str) -> str:
    lines = [
        _clean_heading_title(line)
        for line in re.split(r"\n+", str(text or ""))
        if _compact_text(line)
    ]
    for line in lines:
        if len(line) < 12 or _is_promo_text(line):
            continue
        sentence = re.split(r"(?<=[.!?])\s+", line, maxsplit=1)[0].strip()
        candidate = sentence or line
        if len(candidate) > 160:
            candidate = candidate[:159].rstrip() + "..."
        if len(candidate) >= 12:
            return candidate

    words = re.findall(r"\S+", _compact_text(text))
    return " ".join(words[:12]).strip()[:160]


def _fallback_summary_from_text(text: str, max_chars: int = 800) -> str:
    compact = _compact_text(text)
    if not compact:
        return ""
    sentences = [part.strip() for part in re.split(r"(?<=[.!?])\s+", compact) if part.strip()]
    summary = " ".join(sentences[:3]).strip()
    if len(summary) < 120:
        summary = compact[:max_chars].strip()
    if len(summary) > max_chars:
        summary = summary[: max_chars - 3].rstrip() + "..."
    return _normalize_summary_text(summary)


def _segment_article_with_llm(
    cfg: Config,
    segment: dict[str, Any],
    *,
    content_profile: str = "news",
) -> dict[str, Any]:
    segment_text = str(segment.get("full_text", "")).strip()
    extracted = extract_articles(cfg, segment_text[:6000], "article")
    article = dict(extracted[0]) if extracted else {}

    article["summary"] = _normalize_summary_text(str(article.get("summary", "")).strip()) or _fallback_summary_from_text(segment_text)
    article.setdefault("categories", [])
    article.setdefault("section", "")
    article.setdefault("category", "")
    article.setdefault("subcategory", "")
    article["content_profile"] = content_profile if content_profile in {"news", "resource"} else "news"

    original_url = _normalize_url(str(segment.get("original_url", "")).strip())
    if original_url:
        article["original_url"] = original_url
        article["source_origin"] = "direct_email_parser"

    images = list(dict.fromkeys(str(url).strip() for url in segment.get("images", []) if str(url).strip()))
    if images:
        article["email_images"] = images
        article.setdefault("image_url", images[0])

    article["raw_email_segment_text"] = segment_text
    article["enrichment_context"] = segment_text
    article = _taxonomy_defaults(article)

    if _title_needs_refinement(article.get("title")):
        article["title"] = ""
        article["title_origin"] = "needs_refinement"

    if not _ensure_article_title(cfg, article, segment_text, content_profile=content_profile):
        fallback_title = _fallback_title_from_text(segment_text) or "Untitled article"
        article["title"] = fallback_title[:500]
        article["manual_review_required"] = True
        article.setdefault("review_note", "LLM failed to propose a reliable title for this email segment.")
    return article


def _article_from_revh_candidate(
    cfg: Config,
    candidate: ParsedArticle,
    *,
    content_profile: str = "news",
) -> dict[str, Any]:
    segment_text = str(candidate.text or "").strip()
    article = {
        "title": str(candidate.title or "").strip()[:500],
        "summary": _fallback_summary_from_text(segment_text),
        "original_url": _normalize_url(str(candidate.source_link or "").strip()),
        "categories": [],
        "section": "",
        "category": "",
        "subcategory": "",
        "content_profile": content_profile if content_profile in {"news", "resource"} else "news",
        "raw_email_segment_text": segment_text,
        "enrichment_context": segment_text,
        "parser_backend": "revh",
        "parser_family": str(candidate.family or "").strip(),
        "parser_confidence": round(float(candidate.confidence or 0.0), 3),
        "parser_notes": list(candidate.notes or []),
        "parser_published_date_source": str(candidate.published_date_source or "").strip(),
    }
    if article["original_url"]:
        article["source_origin"] = "direct_revh_parser"
    if candidate.published_date:
        article["published_at"] = candidate.published_date
    article = _taxonomy_defaults(article)
    if _title_needs_refinement(article.get("title")):
        article["title"] = ""
        article["title_origin"] = "needs_refinement"
    if not _ensure_article_title(cfg, article, segment_text, content_profile=content_profile):
        fallback_title = _fallback_title_from_text(segment_text) or "Untitled article"
        article["title"] = fallback_title[:500]
        article["manual_review_required"] = True
        article.setdefault("review_note", "revH parser returned no reliable title for this email segment.")
    return article


def _should_use_revh_parse_result(parsed_articles: list[ParsedArticle]) -> bool:
    if not parsed_articles:
        return False
    if len(parsed_articles) > 1:
        return True
    first = parsed_articles[0]
    if str(first.family or "").strip().lower() != "generic":
        return True
    return bool(first.source_link) and float(first.confidence or 0.0) >= 0.70


def _attach_email_segment_images(
    articles: list[dict[str, Any]],
    segments: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    used_indices: set[int] = set()
    for article in articles:
        item = dict(article)
        article_url = _normalize_url(str(item.get("original_url", "")).strip())
        article_title = str(item.get("title", "")).strip()
        best_idx = -1
        best_score = -1.0
        for idx, segment in enumerate(segments):
            if idx in used_indices:
                continue
            segment_url = _normalize_url(str(segment.get("original_url", "")).strip())
            score = 0.0
            if article_url and segment_url and article_url == segment_url:
                score += 1000.0
            if article_title:
                score += _text_overlap_ratio(article_title, str(segment.get("full_text", ""))) * 100.0
            if score > best_score:
                best_score = score
                best_idx = idx
        if best_idx >= 0 and best_score >= 45.0:
            matched = segments[best_idx]
            used_indices.add(best_idx)
            images = list(dict.fromkeys(str(url).strip() for url in matched.get("images", []) if str(url).strip()))
            if images:
                item["email_images"] = images
                item.setdefault("image_url", images[0])
            if not item.get("original_url"):
                matched_url = _normalize_url(str(matched.get("original_url", "")).strip())
                if matched_url:
                    item["original_url"] = matched_url
                    item["source_origin"] = "direct_email_parser"
        out.append(item)
    return out


def _extract_article_email_images(raw_html: str, original_url: str, limit: int = 4) -> list[str]:
    target_url = _normalize_url(str(original_url or "").strip())
    if not raw_html or not target_url:
        return []
    try:
        soup = BeautifulSoup(raw_html, "lxml")
    except Exception:
        return []

    images: list[str] = []

    def _add_from_container(container: Tag | None) -> None:
        if not isinstance(container, Tag):
            return
        for img in container.find_all("img", recursive=True):
            if len(images) >= limit:
                return
            if not _is_relevant_email_image(img):
                continue
            url = _normalize_email_image_url(str(img.get("src", "")))
            if url and url not in images:
                images.append(url)

    for anchor in soup.find_all("a", href=True):
        href = _normalize_url(str(anchor.get("href", "")).strip())
        if not href:
            continue
        if revh_unwrap(href) != target_url and href != target_url:
            continue
        _add_from_container(anchor)
        parent = anchor.parent if isinstance(anchor.parent, Tag) else None
        _add_from_container(parent)
        grandparent = parent.parent if isinstance(parent, Tag) and isinstance(parent.parent, Tag) else None
        _add_from_container(grandparent)
        if isinstance(parent, Tag):
            for sibling in list(parent.find_previous_siblings(limit=2)) + list(parent.find_next_siblings(limit=2)):
                _add_from_container(sibling)
        if len(images) >= limit:
            break
    return images


def _attach_email_images_from_html(articles: list[dict[str, Any]], raw_html: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for article in articles:
        item = dict(article)
        if item.get("email_images"):
            out.append(item)
            continue
        images = _extract_article_email_images(raw_html, str(item.get("original_url", "")).strip())
        if images:
            item["email_images"] = images
            item.setdefault("image_url", images[0])
        out.append(item)
    return out


def _extract_email_segments_from_html(raw_html: str) -> list[dict[str, Any]]:
    if not raw_html:
        return []

    try:
        soup = BeautifulSoup(raw_html, "lxml")
    except Exception:
        return []

    for tag in soup(["style", "script", "head", "meta", "title"]):
        tag.decompose()

    segments: list[dict[str, Any]] = []
    current = {"text": [], "images": [], "original_url": ""}
    pending_forward_label: str | None = None
    skip_next = False

    for node in soup.descendants:
        if isinstance(node, Tag) and node.name == "img":
            if _is_relevant_email_image(node):
                url = _normalize_email_image_url(str(node.get("src", "")))
                if url and url not in current["images"]:
                    current["images"].append(url)
            continue

        if not isinstance(node, NavigableString):
            continue

        text_value = str(node).strip()
        if len(text_value) < 3:
            continue

        parent = getattr(node, "parent", None)
        if parent is None:
            continue

        if skip_next:
            skip_next = False
            continue

        if pending_forward_label:
            pending_forward_label = None
            continue

        forward_label = _is_email_forward_label(text_value)
        if forward_label:
            pending_forward_label = forward_label
            continue

        if text_value.strip() in EMAIL_FORWARD_SUB_LABELS:
            skip_next = True
            continue

        if _is_email_forward_header(text_value):
            continue

        if _is_email_junk(text_value):
            continue

        if text_value.lower().startswith("read more"):
            link = _extract_read_more_link(parent)
            if link and not current["original_url"]:
                current["original_url"] = link
            for img_url in _collect_post_readmore_images(parent):
                if img_url not in current["images"]:
                    current["images"].append(img_url)
            finalized = _finalize_email_segment(current)
            if finalized is not None:
                segments.append(finalized)
            current = {"text": [], "images": [], "original_url": ""}
            continue

        current["text"].append(text_value)

    finalized = _finalize_email_segment(current)
    if finalized is not None:
        segments.append(finalized)

    return segments


def _extract_articles_from_email_html(cfg: Config, raw_html: str) -> list[dict[str, Any]]:
    segments = _extract_email_segments_from_html(raw_html)
    if len(segments) < 2:
        return []

    articles = [_segment_article_with_llm(cfg, segment) for segment in segments]
    return [article for article in articles if str(article.get("summary", "")).strip()]


def _build_summary(chunks: list[str], max_chars: int = 1200) -> str:
    usable = [_compact_text(chunk) for chunk in chunks if _compact_text(chunk)]
    usable = [chunk for chunk in usable if len(chunk) >= 40 and not _is_promo_text(chunk)]
    if not usable:
        return ""
    out = " ".join(usable[:3]).strip()
    if len(out) > max_chars:
        out = out[: max_chars - 1].rstrip() + "…"
    return out


def _extract_section_articles(root: Tag, canonical_url: str, page_title: str) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    headings = root.find_all(["h2", "h3"])
    page_title_norm = _compact_text(page_title).lower()

    for heading in headings:
        title = _clean_heading_title(heading.get_text(" ", strip=True))
        if not title or len(title) < 20:
            continue
        if title.lower() == page_title_norm:
            continue
        if _is_promo_text(title):
            continue

        chunks: list[str] = []
        links: list[str] = []
        for a in heading.find_all("a", href=True):
            links.append(_normalize_url(a["href"]))

        for sib in heading.next_siblings:
            if isinstance(sib, Tag) and sib.name in {"h2", "h3"}:
                break
            if isinstance(sib, NavigableString):
                txt = _compact_text(str(sib))
                if txt:
                    chunks.append(txt)
                continue
            if isinstance(sib, Tag):
                for a in sib.find_all("a", href=True):
                    links.append(_normalize_url(a["href"]))
                txt = _compact_text(sib.get_text(" ", strip=True))
                if txt:
                    chunks.append(txt)

        summary = _build_summary(chunks)
        if not summary:
            continue

        chosen_url = ""
        for link in links:
            if not link or _is_noise_link(link):
                continue
            chosen_url = link
            break
        if not chosen_url:
            chosen_url = canonical_url

        candidates.append(
            _taxonomy_defaults(
                {
                "title": title[:500],
                "summary": summary,
                "original_url": _rewrite_medium_url(chosen_url),
                "categories": [],
                }
            )
        )

    return candidates


def _extract_articles_from_primary_link(cleaned_text: str) -> tuple[list[dict[str, Any]], str]:
    primary_url = _pick_primary_content_url(cleaned_text)
    if not primary_url:
        return [], ""

    try:
        resp = _fetch_url(primary_url)
    except Exception as exc:
        LOG.warning("primary link fetch failed: %s", exc)
        return [], ""

    soup = BeautifulSoup(resp.text, "lxml")
    canonical = ""
    canonical_tag = soup.find("link", attrs={"rel": "canonical"})
    if canonical_tag and canonical_tag.get("href"):
        canonical = _normalize_url(canonical_tag["href"])
    source_url = canonical or _normalize_url(resp.url)

    root = soup.find("article") or soup.find("main") or soup.body
    if root is None:
        # Some open.substack responses are redirect stubs with URL in title/meta refresh.
        redirect_target = ""
        title_text = _compact_text(soup.title.string) if soup.title and soup.title.string else ""
        if title_text.startswith("http://") or title_text.startswith("https://"):
            redirect_target = _normalize_url(title_text)
        if not redirect_target:
            refresh_tag = soup.find("meta", attrs={"http-equiv": re.compile(r"refresh", re.I)})
            refresh_content = _compact_text(str(refresh_tag.get("content", ""))) if refresh_tag else ""
            m = re.search(r"url=(https?://\S+)", refresh_content, flags=re.IGNORECASE)
            if m:
                redirect_target = _normalize_url(m.group(1))
        if not redirect_target:
            canonical_tag = soup.find("link", attrs={"rel": "canonical"})
            if canonical_tag and canonical_tag.get("href"):
                candidate = _normalize_url(str(canonical_tag.get("href", "")).strip())
                if candidate and candidate != source_url:
                    redirect_target = candidate

        if redirect_target and redirect_target != source_url:
            try:
                resp2 = _fetch_url(redirect_target)
                soup = BeautifulSoup(resp2.text, "lxml")
                source_url = _normalize_url(resp2.url) or redirect_target
                root = soup.find("article") or soup.find("main") or soup.body
            except Exception as exc:
                LOG.warning("primary redirect fetch failed target=%s err=%s", redirect_target, exc)

    if root is None:
        return [], ""

    page_title = ""
    if soup.title and soup.title.string:
        page_title = _compact_text(soup.title.string)

    # Capture resolved page text so LLM can fallback on richer content when needed.
    page_text = re.sub(r"\n{3,}", "\n\n", root.get_text("\n", strip=True))

    # Prefer deterministic section extraction for link-only digest newsletters.
    section_articles = _extract_section_articles(root, source_url, page_title)
    if len(section_articles) >= 2:
        LOG.info("primary link section extraction succeeded articles=%s source=%s", len(section_articles), source_url)
        return section_articles, page_text

    return [], page_text


# --- Constantes e helpers para limpeza de texto pós-HTML (usados em clean_html) ---

# Padrão de bloco de headers de forward Outlook/Gmail (EN/PT).
# Exige pelo menos 2 headers consecutivos (From/De + Sent/Date/Enviado/Data)
# para evitar falsos positivos com texto editorial.
_FWD_HEADER_RE = re.compile(
    r"^(From|De)\s*:.*\n"
    r"(?:.*\n){0,3}"
    r"(Sent|Date|Enviado|Data)\s*:.*",
    re.MULTILINE | re.IGNORECASE,
)

# URLs substackcdn.com que são ícones UI, avatares ou overlays de vídeo — não conteúdo editorial.
_SUBSTACK_ICON_URL_RE = re.compile(
    r"\[?\s*https?://substackcdn\.com/image/fetch/[^\]\s]*"
    r"(?:/icon/|play_button|/avatar/|bucketeer-)"
    r"[^\]\s]*\s*\]?",
    re.IGNORECASE,
)


def _strip_unicode_noise(text: str) -> str:
    """Remove caracteres unicode de controlo/formatação sem valor editorial.

    - Cf (format chars): combining grapheme joiners, zero-width spaces, BOM, etc.
    - U+FFFD (replacement character): bytes inválidos na conversão de encoding.
    - Zs (space separators) excepto espaço normal (U+0020): normalizados para espaço comum.
    """
    out: list[str] = []
    for ch in text:
        cat = unicodedata.category(ch)
        if cat == "Cf":
            continue
        if ch == "\uFFFD":
            continue
        if cat == "Zs" and ch != " ":
            out.append(" ")
            continue
        out.append(ch)
    return "".join(out)


def clean_html(raw_html: str) -> str:
    raw_angle_urls = _extract_raw_angle_bracket_urls(raw_html or "")
    soup = BeautifulSoup(raw_html or "", "lxml")

    # 1. Remove tags técnicas e blocos de navegação/rodapé
    for tag in soup(["script", "style", "img", "svg", "noscript", "header", "footer"]):
        tag.decompose()

    # 2. Remove ruído de botões de newsletter e avisos de Substack
    noise_patterns = r"unsubscribe|manage preferences|view in browser|read in app|pledge your support|like this post|leave a comment|restack this post"
    for node in soup.find_all(string=re.compile(noise_patterns, re.I)):
        # BeautifulSoup may return string-like nodes without ".parent" in newer versions.
        parent = getattr(node, "parent", None)
        if parent is not None:
            parent.decompose()

    # 3. Converter links em texto (limpando tracking e Ícones UI)
    for a in soup.find_all("a"):
        href = (a.get("href") or "").strip()
        text = a.get_text(" ", strip=True)
        
        # Descodificar URL para apanhar os %2Ficon%2F escondidos
        decoded_href = urllib.parse.unquote(href)
        
        # Identificar se é lixo da interface do Substack
        is_ui_noise = any(noise in decoded_href.lower() for noise in (
            "substack.com/icon", 
            "share=", 
            "action=share",
            "w_28", # Largura típica de ícone Substack
            "w_32"  # Largura típica de avatar/ícone Substack
        ))
        
        if href and not is_ui_noise:
            clean_href = re.sub(r"[?&]utm_[^&]*", "", href).rstrip("?&")
            # Se tiver texto, guarda o texto e o link
            if text:
                a.replace_with(f"{text} ({clean_href})")
            # Se não tiver texto mas for um link (ex: uma imagem de conteúdo), mantemos a referência
            else:
                a.replace_with(f" [Image/Link: {clean_href}] ")
        else:
            # Se for UI noise ou link vazio, destrói
            a.decompose()

    text = soup.get_text("\n", strip=True)

    # 3b. Remover caracteres unicode de controlo/formatação (padding invisível, replacement chars)
    text = _strip_unicode_noise(text)

    # 3c. Remover assinatura do forwarder + bloco de headers de forward (Outlook EN/PT)
    # Exige pelo menos 2 headers consecutivos para confirmar que é forward, não texto editorial.
    # Metadata já persistida na DB pela fase de ingestão — sem perda de informação.
    fwd_match = _FWD_HEADER_RE.search(text)
    if fwd_match:
        block_end = fwd_match.end()
        remaining = text[block_end:]
        subject_match = re.match(
            r"(?:.*\n){0,4}(Subject|Assunto)\s*:.*\n?",
            remaining,
            re.IGNORECASE,
        )
        if subject_match:
            block_end += subject_match.end()
        text = text[block_end:].lstrip("\n")

    # 3d. Remover URLs substackcdn.com de ícones UI, avatares e overlays de vídeo
    text = _SUBSTACK_ICON_URL_RE.sub("", text)

    # 4. Limpeza de padrões específicos (CIDs e Avisos Legais)
    text = re.sub(r"\[cid:[^\]]+\]", "", text)
    # Aviso legal: limitar ao bloco do notice (max ~1500 chars).
    # get_text('\n', strip=True) nunca produz \n\n, por isso o terminador
    # original (?=\n\n|\Z) com re.DOTALL consumia TUDO até ao fim da string.
    # Correcção: parar em headers de forward EN/PT (\nFrom:, \nDe:, \nSent:, \nDate:, \nEnviado:),
    # em \n\n, ou ao fim de 1500 chars — o que vier primeiro.
    text = re.sub(
        r"(?i)CONFIDENTIALITY NOTICE(?:(?!\nFrom:|\nDe:|\nSent:|\nDate:|\nEnviado:|\n\n).){0,1500}",
        "", text, flags=re.DOTALL,
    )
    
    text = re.sub(r"\n{3,}", "\n\n", text)

    # 4b. Recuperar links de plain-text em formato <https://...> que se perdem no parser HTML.
    # Mantemos apenas links não-noise e ainda ausentes no texto final.
    if raw_angle_urls:
        existing = set(_extract_urls(text))
        recovered: list[str] = []
        for url in raw_angle_urls:
            if not url or url in existing or _is_noise_link(url):
                continue
            existing.add(url)
            recovered.append(url)
            if len(recovered) >= 40:
                break
        if recovered:
            text = f"{text}\n\nRecovered source links:\n" + "\n".join(recovered)

    # Limite generoso: a truncatura para o Ollama (12k) é aplicada no ponto de chamada.
    # Aqui mantemos texto amplo para que _pick_primary_content_url e
    # _extract_articles_from_primary_link encontrem links que possam estar mais abaixo.
    return text[:120000]


def _extract_main_text_from_html(html: str) -> str:
    soup = BeautifulSoup(html or "", "lxml")
    for tag in soup(["script", "style", "noscript", "svg", "img"]):
        tag.decompose()
    root = soup.find("article") or soup.find("main") or soup.body or soup
    text = root.get_text("\n", strip=True)
    lines = [_compact_text(line) for line in text.splitlines()]
    lines = [line for line in lines if line and not _is_promo_text(line)]
    compact = "\n".join(lines)
    return re.sub(r"\n{3,}", "\n\n", compact).strip()


def _get_source_text(url: str, max_chars: int) -> str:
    normalized = _normalize_url(url)
    if not normalized:
        return ""
    if normalized in SOURCE_TEXT_CACHE:
        return SOURCE_TEXT_CACHE[normalized]
    try:
        resp = _fetch_url(normalized)
        text = _extract_main_text_from_html(resp.text)
    except Exception as exc:
        LOG.warning("source text resolution failed url=%s err=%s", normalized, exc)
        text = ""
    if len(text) > max_chars:
        text = text[: max_chars - 1].rstrip() + "…"
    SOURCE_TEXT_CACHE[normalized] = text
    return text


def _is_probably_paywalled(html: str, text: str) -> bool:
    _ = html  # kept for signature compatibility with existing callers
    blob = (text or "").lower()
    return any(marker in blob for marker in PAYWALL_PATTERNS)


def _get_source_snapshot(url: str, max_chars: int, min_open_chars: int) -> dict[str, Any]:
    normalized = _normalize_url(url)
    if not normalized:
        return {"url": "", "text": "", "open_access": False, "paywalled": False, "chars": 0, "error": "empty-url"}
    if normalized in SOURCE_SNAPSHOT_CACHE:
        return SOURCE_SNAPSHOT_CACHE[normalized]

    final_url = normalized
    html = ""
    text = ""
    error = ""
    try:
        resp = _fetch_url(normalized)
        final_url = _normalize_url(resp.url) or normalized
        html = resp.text or ""
        text = _extract_main_text_from_html(html)
    except Exception as exc:
        error = str(exc)
        LOG.warning("source snapshot failed url=%s err=%s", normalized, exc)

    if len(text) > max_chars:
        text = text[: max_chars - 1].rstrip() + "…"

    paywalled = _is_probably_paywalled(html, text)
    open_access = bool(text) and len(text) >= max(120, min_open_chars) and not paywalled
    snapshot = {
        "url": final_url,
        "text": text,
        "open_access": open_access,
        "paywalled": paywalled,
        "chars": len(text),
        "error": error,
    }
    SOURCE_SNAPSHOT_CACHE[normalized] = snapshot
    if final_url and final_url != normalized:
        SOURCE_SNAPSHOT_CACHE[final_url] = snapshot
    return snapshot


def _snapshot_has_meaningful_content(snapshot: dict[str, Any], min_chars: int) -> bool:
    if not isinstance(snapshot, dict):
        return False
    if str(snapshot.get("error", "")).strip():
        return False
    resolved_url = _normalize_url(str(snapshot.get("url", "")).strip())
    if resolved_url and _is_generic_source_url(resolved_url):
        return False
    text = str(snapshot.get("text", "")).strip()
    chars = int(snapshot.get("chars", 0) or 0)
    if not text or chars < max(60, int(min_chars)):
        return False
    head = text[:1000].lower()
    if any(marker in head for marker in SOURCE_UNAVAILABLE_PATTERNS):
        return False
    return True


def source_snapshot_title_match(
    article_title: str,
    snapshot: dict[str, Any] | None,
    *,
    min_overlap: float = 0.35,
) -> dict[str, Any]:
    tokens = _tokenize_title_for_source_match(article_title)
    if not tokens:
        return {"ok": True, "overlap_ratio": 1.0, "matched_tokens": [], "title_tokens": []}

    payload = snapshot if isinstance(snapshot, dict) else {}
    text = str(payload.get("text", "")).strip()
    if not text:
        return {"ok": False, "overlap_ratio": 0.0, "matched_tokens": [], "title_tokens": tokens}

    hay = f"{text[:5000]} {payload.get('url', '')}".lower()
    matched = sorted({token for token in tokens if token in hay})
    ratio = len(matched) / max(1, len(tokens))
    strong_match = any(len(token) >= 8 for token in matched)
    relaxed_threshold = max(0.20, min_overlap - 0.12)
    ok = ratio >= min_overlap or (ratio >= relaxed_threshold and strong_match)
    return {
        "ok": bool(ok),
        "overlap_ratio": float(ratio),
        "matched_tokens": matched,
        "title_tokens": tokens,
    }


def article_preview_quality_issues(article: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    summary = _normalize_summary_text(str(article.get("summary", "")).strip())
    body_raw = str(article.get("article_body", "")).strip() or str(article.get("enrichment_context", "")).strip()
    body = _normalize_article_body_text(body_raw)

    if len(summary) < 90:
        issues.append("summary_too_short")
    if len(body) < 420:
        issues.append("article_body_too_short")

    paragraphs = [p.strip() for p in re.split(r"\n\s*\n+", body) if p.strip()]
    if len(paragraphs) < 4:
        issues.append("article_body_needs_more_paragraphs")

    if summary and body:
        summary_norm = " ".join(summary.split()).strip().lower()
        body_norm = " ".join(body.split()).strip().lower()
        if body_norm == summary_norm:
            issues.append("article_body_equals_summary")
        elif body_norm.startswith(summary_norm) and (len(body_norm) - len(summary_norm) < 260):
            issues.append("article_body_too_close_to_summary")

    low = body.lower()
    technical_markers = (
        "manual decision required",
        "manual review required",
        "primary source appears paid/blocked",
        "source appears paid/blocked",
        "openclaw",
        "searx",
        "tool call",
        "review command",
    )
    if any(marker in low for marker in technical_markers):
        issues.append("article_body_contains_technical_context")

    raw_urls = len(re.findall(r"https?://", body))
    if raw_urls >= 3:
        issues.append("article_body_contains_url_dump")

    return issues


def _pick_discovered_source_with_content(
    cfg: Config,
    discovery: dict[str, Any],
    *,
    article_title: str = "",
    current_url: str = "",
    preferred_host: str = "",
    semantic_reference_titles: list[str] | None = None,
) -> str:
    current_normalized = _normalize_url(current_url)
    preferred_host_norm = preferred_host.strip().lower().lstrip("www.")
    extra_refs = [str(v).strip() for v in (semantic_reference_titles or []) if str(v).strip()]
    seen: set[str] = set()
    ordered_urls: list[str] = []
    candidate_scores: dict[str, int] = {}

    selected = _normalize_url(str(discovery.get("selected_url", "")).strip())
    if selected:
        ordered_urls.append(selected)
        candidate_scores[selected] = int(discovery.get("selected_score", 0) or 0)

    candidates = discovery.get("candidates", [])
    if isinstance(candidates, list):
        for row in candidates:
            if not isinstance(row, dict):
                continue
            candidate = _normalize_url(str(row.get("url", "")).strip())
            if candidate:
                ordered_urls.append(candidate)
                candidate_scores[candidate] = int(row.get("score", 0) or 0)

    validated: list[tuple[str, int, bool]] = []
    for candidate_url in ordered_urls:
        if not candidate_url or candidate_url in seen:
            continue
        if _is_generic_source_url(candidate_url):
            continue
        seen.add(candidate_url)
        if current_normalized and candidate_url == current_normalized:
            continue

        snapshot = _get_source_snapshot(
            candidate_url,
            cfg.source_rewrite_max_chars,
            cfg.source_open_min_chars,
        )
        if _snapshot_has_meaningful_content(snapshot, cfg.source_presence_min_chars):
            semantic = source_snapshot_title_match(
                article_title,
                snapshot,
                min_overlap=cfg.source_title_overlap_min_ratio,
            )
            if article_title and not bool(semantic.get("ok")):
                continue

            extra_ref_best = 0.0
            if extra_refs:
                ref_ok = False
                strict_overlap = max(0.55, cfg.source_title_overlap_min_ratio + 0.10)
                for ref_text in extra_refs:
                    ref_semantic = source_snapshot_title_match(
                        ref_text,
                        snapshot,
                        min_overlap=strict_overlap,
                    )
                    extra_ref_best = max(extra_ref_best, float(ref_semantic.get("overlap_ratio", 0.0) or 0.0))
                    if bool(ref_semantic.get("ok")):
                        ref_ok = True
                if not ref_ok:
                    continue

            resolved_url = _normalize_url(str(snapshot.get("url", "")).strip()) or candidate_url
            resolved_host = _normalized_host_from_url(resolved_url)
            is_preferred_host = bool(preferred_host_norm and resolved_host == preferred_host_norm)
            base_score = int(candidate_scores.get(candidate_url, 0))
            overlap_ratio = float(semantic.get("overlap_ratio", 0.0) or 0.0) if article_title else 0.0
            final_score = base_score + int(overlap_ratio * 100)
            if extra_ref_best > 0:
                final_score += int(extra_ref_best * 90)
            if is_preferred_host:
                final_score += 25
            validated.append((resolved_url, final_score, is_preferred_host))

    if not validated:
        return ""

    preferred_rows = [row for row in validated if row[2]]
    if preferred_rows:
        preferred_rows.sort(key=lambda row: row[1], reverse=True)
        return preferred_rows[0][0]

    validated.sort(key=lambda row: row[1], reverse=True)
    return validated[0][0]


def _select_summary_source(cfg: Config, article: dict[str, Any]) -> dict[str, Any]:
    primary_url = _normalize_url(str(article.get("original_url", "")).strip())
    intermediate_url = _normalize_url(str(article.get("intermediate_url", "")).strip())
    if _looks_like_medium_post(intermediate_url):
        intermediate_url = _rewrite_medium_url(intermediate_url)

    if primary_url:
        primary = _get_source_snapshot(primary_url, cfg.source_rewrite_max_chars, cfg.source_open_min_chars)
        if primary.get("open_access"):
            return {
                "url": str(primary.get("url", "")),
                "text": str(primary.get("text", "")),
                "mode": "primary_open",
                "manual_review_required": False,
                "note": "",
            }

        if intermediate_url and (_looks_like_medium_post(intermediate_url) or "freedium-mirror.cfd" in intermediate_url.lower()):
            inter = _get_source_snapshot(intermediate_url, cfg.source_rewrite_max_chars, cfg.source_open_min_chars)
            if inter.get("open_access"):
                return {
                    "url": str(inter.get("url", "")),
                    "text": str(inter.get("text", "")),
                    "mode": "primary_paid_intermediate_medium_freedium",
                    "manual_review_required": False,
                    "note": "Primary source appears paid/blocked; summary generated from Medium intermediary via Freedium mirror.",
                }
            return {
                "url": str(primary.get("url", "")),
                "text": str(primary.get("text", "")),
                "mode": "manual_review_required",
                "manual_review_required": True,
                "note": "Primary source appears paid/blocked and Medium intermediary via Freedium is also not open. Manual decision required.",
            }

        return {
            "url": str(primary.get("url", "")),
            "text": str(primary.get("text", "")),
            "mode": "manual_review_required",
            "manual_review_required": True,
            "note": "Primary source appears paid/blocked and no eligible Medium intermediary was found. Manual decision required.",
        }

    return {
        "url": "",
        "text": "",
        "mode": "manual_review_required",
        "manual_review_required": True,
        "note": "No primary source URL available. Manual decision required.",
    }


def _tokenize_title_for_source_match(value: str) -> list[str]:
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


def _score_discovered_source_url(*, article_title: str, url: str, result_title: str, snippet: str) -> int:
    normalized = _normalize_url(url)
    if not normalized:
        return -10_000
    if _is_generic_source_url(normalized):
        return -10_000
    try:
        parsed = urlparse(normalized)
    except Exception:
        return -10_000

    host = (parsed.hostname or "").lower()
    path = (parsed.path or "").lower()
    if not host:
        return -10_000

    blocked_hosts = {"smry.ai", "removepaywalls.com", "substackcdn.com"}
    if any(host == b or host.endswith(f".{b}") for b in blocked_hosts):
        return -10_000
    if "unsubscribe" in path or "notification/unsubscribe" in path or "/signup" in path:
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

    title_tokens = _tokenize_title_for_source_match(article_title)
    hay = f"{result_title}\n{snippet}".lower()
    overlap = sum(1 for token in title_tokens if token in hay)
    score += overlap * 4
    if title_tokens:
        ratio = overlap / max(1, len(title_tokens))
        if ratio >= 0.6:
            score += 20
        elif ratio >= 0.4:
            score += 10
    return score


def discover_source_url_for_article(
    cfg: Config,
    article: dict[str, Any],
    *,
    min_score: int | None = None,
    preferred_host: str = "",
) -> dict[str, Any]:
    title = str(article.get("title", "")).strip()
    if not title:
        return {"attempted": False, "selected_url": "", "reason": "missing_title", "candidates": []}

    threshold = int(min_score if min_score is not None else cfg.source_discovery_min_score)
    preferred_host_norm = preferred_host.strip().lower().lstrip("www.")
    scored: dict[str, dict[str, Any]] = {}

    context_text = str(article.get("enrichment_context", "")).strip()
    for url in _extract_urls(context_text):
        score = _score_discovered_source_url(
            article_title=title,
            url=url,
            result_title="",
            snippet=context_text,
        )
        candidate_host = _normalized_host_from_url(url)
        if preferred_host_norm and candidate_host:
            if candidate_host == preferred_host_norm:
                score += 35
            else:
                score -= 15
        if score < threshold:
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

    queries = [
        title,
        f"{title} substack",
        f"{title} linkedin",
    ]
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
            LOG.warning("source discovery search failed query=%r err=%s", query, exc)
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
            score = _score_discovered_source_url(
                article_title=title,
                url=url,
                result_title=result_title,
                snippet=snippet,
            )
            candidate_host = _normalized_host_from_url(url)
            if preferred_host_norm and candidate_host:
                if candidate_host == preferred_host_norm:
                    score += 35
                else:
                    score -= 15
            if score < threshold:
                continue
            prev = scored.get(url)
            if prev is None or int(prev.get("score", -10_000)) < score:
                scored[url] = {
                    "url": url,
                    "score": score,
                    "title": result_title,
                    "query": query,
                }

    ranked = sorted(scored.values(), key=lambda row: int(row.get("score", 0)), reverse=True)
    selected = str(ranked[0]["url"]) if ranked else ""
    selected_score = int(ranked[0].get("score", 0) or 0) if ranked else 0
    return {
        "attempted": True,
        "selected_url": selected,
        "selected_score": selected_score,
        "reason": "ok" if selected else "not_found",
        "candidates": ranked[:8],
    }


def _attach_discovered_source_before_review(cfg: Config, article: dict[str, Any]) -> dict[str, Any]:
    out = dict(article)
    existing = _normalize_url(str(out.get("original_url", "")).strip())
    preferred_host = _normalized_host_from_url(existing)
    current_origin = str(out.get("source_origin", "")).strip().lower()
    anchor_hint = str(out.get("source_anchor_text", "")).strip()
    subject_hint = str(out.get("newsletter_subject_hint", "")).strip()
    semantic_references = [v for v in [anchor_hint, subject_hint] if v]
    existing_is_generic = _is_generic_source_url(existing) if existing else False
    direct_has_content = False
    direct_semantic_match = False
    direct_overlap_ratio = 0.0
    if existing and not existing_is_generic:
        if not str(out.get("source_origin", "")).strip():
            out["source_origin"] = "direct"

        existing_snapshot = _get_source_snapshot(
            existing,
            cfg.source_rewrite_max_chars,
            cfg.source_open_min_chars,
        )
        direct_semantic = source_snapshot_title_match(
            str(out.get("title", "")).strip(),
            existing_snapshot,
            min_overlap=cfg.source_title_overlap_min_ratio,
        )
        direct_semantic_match = bool(direct_semantic.get("ok"))
        direct_overlap_ratio = float(direct_semantic.get("overlap_ratio", 0.0) or 0.0)
        direct_has_content = (
            _snapshot_has_meaningful_content(existing_snapshot, cfg.source_presence_min_chars)
            and direct_semantic_match
        )
        if direct_has_content:
            return out

        preserve_direct_email_source = (
            current_origin in {"direct_email_parser", "direct_email_anchor"}
            and not preferred_host.startswith("link.mail.")
        )
        if preserve_direct_email_source:
            out.setdefault(
                "source_discovery",
                {
                    "attempted": False,
                    "reason": "preserve_direct_email_source",
                    "selected_url": "",
                    "direct_url_checked": True,
                    "direct_url_generic": False,
                    "direct_url_has_content": direct_has_content,
                    "direct_url_semantic_match": direct_semantic_match,
                    "direct_url_title_overlap": round(direct_overlap_ratio, 3),
                },
            )
            return out

    if existing_is_generic:
        out["source_origin"] = "direct_generic"
        out["original_url"] = ""
    elif existing:
        out["source_origin"] = "direct_unavailable"
    else:
        out.setdefault("source_origin", "missing")

    if not cfg.source_discovery_enabled:
        return out

    discovery = discover_source_url_for_article(
        cfg,
        out,
        min_score=cfg.source_discovery_min_score,
        preferred_host=preferred_host,
    )
    selected_url = _pick_discovered_source_with_content(
        cfg,
        discovery,
        article_title=str(out.get("title", "")).strip(),
        current_url=existing,
        preferred_host=preferred_host,
        semantic_reference_titles=semantic_references,
    )
    reason = str(discovery.get("reason", "")).strip()
    if not selected_url:
        reason = "candidate_unavailable" if discovery.get("attempted") else reason
    out["source_discovery"] = {
        "attempted": bool(discovery.get("attempted", False)),
        "reason": reason,
        "selected_url": selected_url,
        "direct_url_checked": bool(existing),
        "direct_url_generic": bool(existing_is_generic),
        "direct_url_has_content": direct_has_content,
        "direct_url_semantic_match": direct_semantic_match,
        "direct_url_title_overlap": round(direct_overlap_ratio, 3),
    }
    if not selected_url:
        if existing_is_generic:
            out["original_url"] = ""
            out["source_origin"] = "direct_generic"
        elif existing:
            out["source_origin"] = "direct_unavailable"
        return out

    # Mark inferred source and retry source rewrite before sending review request.
    out["original_url"] = selected_url
    out["source_origin"] = "inferred"
    out = _apply_source_metadata(out, cfg)
    out["source_origin"] = "inferred"
    out = rewrite_article_from_source(cfg, out)
    out["source_origin"] = "inferred"
    out = enrich_article(cfg, out)
    out["source_origin"] = "inferred"
    return out


def _assert_required_summary_model(cfg: Config) -> None:
    required = (cfg.required_summary_model or "").strip().lower()
    if not required:
        return
    backend = (cfg.llm_backend or "").strip().lower()
    if backend == "openclaw":
        status = _openclaw_gateway_call(cfg, "status", {}, timeout_ms=30000)
        active = str(status.get("sessions", {}).get("defaults", {}).get("model", "")).strip().lower()
        if active != required:
            raise RuntimeError(
                f"OpenClaw default model is '{active or 'unknown'}' but required model is '{cfg.required_summary_model}'"
            )
    elif backend == "ollama":
        active = cfg.ollama_model.strip().lower()
        if active != required:
            raise RuntimeError(
                f"Ollama model is '{active or 'unknown'}' but required model is '{cfg.required_summary_model}'"
            )
    else:
        raise RuntimeError(
            f"Unsupported LLM_BACKEND '{cfg.llm_backend}' for required model validation ({cfg.required_summary_model})"
        )

    if cfg.openclaw_fallback_ollama:
        LOG.warning("disabling OPENCLAW_FALLBACK_OLLAMA to guarantee model consistency")
        cfg.openclaw_fallback_ollama = False


def _openclaw_gateway_call(cfg: Config, method: str, params: dict[str, Any], timeout_ms: int = 120000) -> dict[str, Any]:
    cli = shutil.which("openclaw")
    if not cli:
        raise RuntimeError("openclaw CLI not installed in this container")
    cmd = [
        cli,
        "gateway",
        "call",
        method,
        "--json",
        "--timeout",
        str(timeout_ms),
        "--params",
        json.dumps(params, ensure_ascii=False),
    ]

    gateway_url = str(cfg.openclaw_gateway_url or "").strip()
    if gateway_url:
        cmd.extend(["--url", gateway_url])
        gateway_token = str(cfg.openclaw_gateway_token or "").strip()
        gateway_password = str(cfg.openclaw_gateway_password or "").strip()
        if gateway_token:
            cmd.extend(["--token", gateway_token])
        elif gateway_password:
            cmd.extend(["--password", gateway_password])
        else:
            raise RuntimeError(
                "OPENCLAW_GATEWAY_URL requires OPENCLAW_GATEWAY_TOKEN or OPENCLAW_GATEWAY_PASSWORD; "
                "docker socket execution is no longer supported"
            )

    try:
        exec_result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
            timeout=max(10, int(timeout_ms / 1000) + 10),
        )
    except Exception as exc:
        raise RuntimeError(f"openclaw command failed: {exc}") from exc

    exit_code = int(getattr(exec_result, "returncode", 1))
    stdout = str(exec_result.stdout or "").strip()
    stderr = str(exec_result.stderr or "").strip()
    output = stdout or stderr

    if exit_code != 0:
        err = output or stderr or stdout
        raise RuntimeError(f"openclaw gateway call failed ({method}): {err}")

    if not output:
        return {}

    try:
        return json.loads(output)
    except json.JSONDecodeError:
        start = output.find("{")
        end = output.rfind("}")
        if start >= 0 and end > start:
            return json.loads(output[start : end + 1])
        raise


def _extract_openclaw_assistant_text(message: dict[str, Any]) -> str:
    if not isinstance(message, dict):
        return ""
    content = message.get("content")
    if isinstance(content, list):
        chunks: list[str] = []
        for item in content:
            if not isinstance(item, dict):
                continue
            txt = str(item.get("text", "")).strip()
            if txt:
                chunks.append(txt)
        if chunks:
            return "\n".join(chunks).strip()
    text_field = str(message.get("text", "")).strip()
    if text_field:
        return text_field
    return ""


def _looks_like_openclaw_progress_text(text: str) -> bool:
    normalized = " ".join(str(text or "").split()).strip().lower()
    if not normalized:
        return False
    if normalized.startswith("{") and ("\"name\": \"exec\"" in normalized or "\"command\":" in normalized):
        return True
    progress_markers = (
        "let me ",
        "i need to ",
        "i should ",
        "the user is asking",
        "the api returned an error",
        "invalid ops token",
        "check the environment variable",
        "from tools.md",
        "looking at the internal endpoints",
        "i should call",
        "let me make that call",
        "the deployment command is still running",
        "the reviewer is still running",
        "<|im_start|>user",
    )
    return any(marker in normalized for marker in progress_markers)


def _openclaw_generate(cfg: Config, prompt: str, *, frontend_final_only: bool = False) -> str:
    session_key = f"{cfg.openclaw_session_prefix}:{uuid.uuid4().hex[:12]}"
    run_id = f"nl-{uuid.uuid4().hex[:16]}"
    timeout_ms = max(20000, cfg.openclaw_timeout_seconds * 1000)
    poll_deadline = time.time() + cfg.openclaw_timeout_seconds
    last_progress_text = ""

    send_params = {
        "sessionKey": session_key,
        "message": prompt,
        "deliver": False,
        "idempotencyKey": run_id,
    }
    _openclaw_gateway_call(cfg, "chat.send", send_params, timeout_ms=timeout_ms)

    while time.time() < poll_deadline:
        history = _openclaw_gateway_call(
            cfg,
            "chat.history",
            {"sessionKey": session_key, "limit": 6},
            timeout_ms=30000,
        )
        messages = history.get("messages", [])
        if isinstance(messages, list):
            for msg in reversed(messages):
                if not isinstance(msg, dict):
                    continue
                if str(msg.get("role", "")).lower() != "assistant":
                    continue
                text = _extract_openclaw_assistant_text(msg)
                if text:
                    if frontend_final_only and _looks_like_openclaw_progress_text(text):
                        last_progress_text = text
                        LOG.debug("openclaw frontend ignoring progress-like assistant text: %s", text[:200])
                        break
                    try:
                        _openclaw_gateway_call(
                            cfg,
                            "sessions.delete",
                            {"key": session_key, "deleteTranscript": True},
                            timeout_ms=15000,
                        )
                    except Exception:
                        pass
                    return text
        time.sleep(1.25)

    if frontend_final_only and last_progress_text:
        raise RuntimeError("openclaw did not produce a final user-facing reply before timeout")
    raise RuntimeError("openclaw timeout waiting for assistant response")


def _ollama_generate(cfg: Config, prompt: str, retries: int = 3, model_override: str = "") -> str:
    effective_model = model_override or cfg.ollama_model_summary or cfg.ollama_model
    payload = {
        "model": effective_model,
        "prompt": prompt,
        "stream": False,
        "options": OLLAMA_PARAMS,
    }
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.post(f"{cfg.ollama_url.rstrip('/')}/api/generate", json=payload, timeout=300)
            resp.raise_for_status()
            data = resp.json()
            return data.get("response", "")
        except Exception as exc:
            last_err = exc
            LOG.warning("ollama call failed attempt=%s: %s", attempt, exc)
            time.sleep(1.5 * attempt)
    raise RuntimeError(f"ollama failed after retries: {last_err}")


def _llm_generate(cfg: Config, prompt: str, retries: int = 3, model_override: str = "") -> str:
    backend = cfg.llm_backend
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            if backend == "openclaw":
                return _openclaw_generate(cfg, prompt)
            return _ollama_generate(cfg, prompt, retries=1, model_override=model_override)
        except Exception as exc:
            last_err = exc
            LOG.warning("llm call failed backend=%s attempt=%s err=%s", backend, attempt, exc)
            time.sleep(1.5 * attempt)

    if backend == "openclaw" and cfg.openclaw_fallback_ollama:
        LOG.warning("openclaw unavailable; falling back to ollama for this call")
        return _ollama_generate(cfg, prompt, retries=3, model_override=model_override)

    raise RuntimeError(f"llm backend failed ({backend}): {last_err}")


def classify_newsletter(cfg: Config, text_preview: str) -> str:
    prompt = CLASSIFY_PROMPT.format(newsletter_preview=text_preview[:500])
    response = _llm_generate(cfg, prompt)
    label = response.strip().lower()
    if "digest" in label:
        return "digest"
    if "article" in label:
        return "article"
    return "digest"


def extract_articles(cfg: Config, text: str, newsletter_type: str) -> list[dict[str, Any]]:
    template = EXTRACT_DIGEST_PROMPT if newsletter_type == "digest" else EXTRACT_ARTICLE_PROMPT
    # Use direct placeholder replacement to avoid str.format interpreting JSON braces.
    prompt = template.replace("{newsletter_text}", text)

    for _ in range(3):
        response = _llm_generate(cfg, prompt)
        parsed = _safe_json_array(response)
        if parsed:
            cleaned: list[dict[str, Any]] = []
            for row in parsed:
                normalized_summary = _normalize_summary_text(str(row.get("summary", "")))
                candidate = _taxonomy_defaults(
                    {
                        "title": str(row.get("title", "")).strip()[:500],
                        "summary": normalized_summary,
                        "original_url": str(row.get("original_url", "")).strip(),
                        "categories": _normalize_keywords(row.get("categories", []), limit=10),
                        "section": str(row.get("section", "")).strip(),
                        "category": str(row.get("category", "")).strip(),
                        "subcategory": str(row.get("subcategory", "")).strip(),
                    }
                )
                context_for_title = normalized_summary or text[:3000]
                if not _ensure_article_title(cfg, candidate, context_for_title):
                    LOG.warning("dropping extracted article without reliable title")
                    continue
                if candidate["title"] and candidate["summary"]:
                    cleaned.append(candidate)
            if cleaned:
                return cleaned
    return []


def rewrite_article_from_source(cfg: Config, article: dict[str, Any]) -> dict[str, Any]:
    if not cfg.source_rewrite_enabled:
        return _taxonomy_defaults(article)

    source_choice = _select_summary_source(cfg, article)
    source_url = _normalize_url(str(source_choice.get("url", "")).strip())
    source_text = str(source_choice.get("text", "")).strip()
    out = dict(article)
    # Clear stale manual flags before recalculating current source decision.
    out.pop("manual_review_required", None)
    out.pop("review_note", None)
    out["summary_source_mode"] = str(source_choice.get("mode", "")).strip()
    if source_url:
        out["summary_source_url"] = source_url

    review_note = str(source_choice.get("note", "")).strip()
    if review_note:
        out["review_note"] = review_note
    def _require_manual(note: str) -> dict[str, Any]:
        out["manual_review_required"] = True
        out["summary_source_mode"] = "manual_review_required"
        out["review_note"] = note.strip()
        LOG.info("manual review required for summary source title=%s reason=%s", article.get("title", ""), note)
        return _taxonomy_defaults(out)

    if bool(source_choice.get("manual_review_required")):
        # Stop before LLM rewrite: reviewer must provide accessible source URL/text first.
        return _require_manual(
            review_note or "Source is unavailable or blocked. Provide an accessible source URL/text before approval."
        )

    if not source_url or len(source_text) < 300:
        return _require_manual("Insufficient accessible source text for rewrite. Provide source_text or a valid source URL.")

    semantic = source_snapshot_title_match(
        str(out.get("title", "")).strip(),
        {"url": source_url, "text": source_text},
        min_overlap=cfg.source_title_overlap_min_ratio,
    )
    if not bool(semantic.get("ok")):
        return _require_manual(
            "Source content appears unrelated to the article title. Provide a corrected source URL/text before approval."
        )

    prompt = SOURCE_REWRITE_PROMPT.format(
        existing_title=str(out.get("title", "")).strip()[:500],
        existing_summary=str(out.get("summary", "")).strip(),
        existing_categories=", ".join([str(c).strip() for c in out.get("categories", []) if str(c).strip()]) or "None",
        source_url=source_url,
        source_published_at=str(out.get("published_at", "")).strip() or "unknown",
        source_text=source_text,
    )

    try:
        response = _llm_generate(cfg, prompt)
        parsed = _safe_json_object(response)
    except Exception as exc:
        LOG.warning("source rewrite failed title=%s err=%s", out.get("title", ""), exc)
        return _require_manual("Source rewrite failed in LLM runtime. Retry with explicit source_text before approval.")

    if not parsed:
        LOG.warning("source rewrite returned invalid JSON title=%s", out.get("title", ""))
        return _require_manual("Source rewrite returned invalid JSON. Manual source input is required before approval.")

    title = str(parsed.get("title", "")).strip()
    summary = _normalize_summary_text(str(parsed.get("summary", "")))
    article_body = _normalize_article_body_text(str(parsed.get("article_body", "")))
    section = str(parsed.get("section", "")).strip()
    category_value = str(parsed.get("category", "")).strip()
    subcategory = str(parsed.get("subcategory", "")).strip()
    categories = _normalize_keywords(parsed.get("categories", []), limit=10)

    if title:
        out["title"] = title[:500]
    if summary:
        out["summary"] = summary
    if article_body:
        out["article_body"] = article_body
        # Reuse existing DB field for long-form body in the detail page.
        out["enrichment_context"] = article_body
    if section:
        out["section"] = section[:120]
    if category_value:
        out["category"] = category_value[:120]
    if subcategory:
        out["subcategory"] = subcategory[:120]
    if categories:
        out["categories"] = categories

    title_context = article_body or source_text or summary or str(out.get("summary", "")).strip()
    if not _ensure_article_title(cfg, out, title_context):
        return _require_manual("Model failed to generate a reliable title from the source content.")

    preview_issues = article_preview_quality_issues(out)
    if preview_issues:
        return _require_manual(
            "Generated preview failed quality gate: " + ", ".join(preview_issues) + ". Provide manual source text."
        )

    out["source_rewritten"] = True
    out.pop("manual_review_required", None)
    return _taxonomy_defaults(out)


def enrich_article(cfg: Config, article: dict[str, Any]) -> dict[str, Any]:
    article_body = str(article.get("article_body", "")).strip()
    if article_body:
        article["enrichment_context"] = article_body
        return article

    q = article.get("title", "").strip()
    base_context = str(article.get("enrichment_context", "")).strip()
    review_note = str(article.get("review_note", "")).strip()
    if not q:
        article["enrichment_context"] = "\n".join([v for v in [review_note, base_context] if v]).strip()
        return article

    try:
        resp = requests.get(
            f"{cfg.searxng_url.rstrip('/')}/search",
            params={"q": q, "format": "json"},
            timeout=12,
        )
        resp.raise_for_status()
        data = resp.json()
        snippets = []
        for row in data.get("results", [])[:5]:
            title = (row.get("title") or "").strip()
            url = (row.get("url") or row.get("link") or "").strip()
            content = (row.get("content") or "").strip()
            if title:
                snippets.append(f"- {title}: {content} {url}".strip())
        context_bits = [v for v in [review_note, base_context, "\n".join(snippets[:3])] if v]
        article["enrichment_context"] = "\n".join(context_bits).strip()
    except Exception as exc:
        LOG.warning("searxng enrichment failed: %s", exc)
        article["enrichment_context"] = "\n".join([v for v in [review_note, base_context] if v]).strip()

    return article


def _api_post(cfg: Config, path: str, payload: dict[str, Any]) -> dict[str, Any]:
    headers = {"Content-Type": "application/json", "X-API-Key": cfg.agent_api_key}
    resp = requests.post(f"{cfg.portal_api_url.rstrip('/')}/{path.lstrip('/')}", headers=headers, json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _write_review_bundle(
    cfg: Config,
    newsletter_id: int,
    newsletter_type: str,
    articles: list[dict[str, Any]],
    source_mode: str,
) -> str:
    out_dir = Path(cfg.review_output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "newsletter_id": newsletter_id,
        "newsletter_type": newsletter_type,
        "llm_backend": source_mode,
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "articles": articles,
    }
    out_path = out_dir / f"newsletter_{newsletter_id}_draft.json"
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(out_path)


def _normalize_content_profile(value: Any) -> str:
    profile = str(value or "").strip().lower()
    if profile in {"news", "resource"}:
        return profile
    return "news"


def _source_link_origin_from_article(article: dict[str, Any]) -> str:
    explicit = str(article.get("source_link_origin", "")).strip().lower()
    if explicit in {"email", "search", "user"}:
        return explicit
    source_origin = str(article.get("source_origin", "")).strip().lower()
    if source_origin.startswith("direct") or source_origin in {"email", "direct_email_parser", "direct_email_anchor"}:
        return "email"
    if "search" in source_origin or "discover" in source_origin or source_origin == "inferred":
        return "search"
    return "email"


def _sync_review_articles_to_portal(
    cfg: Config,
    newsletter_id: int,
    articles: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    preview_payload: list[dict[str, Any]] = []
    for idx, row in enumerate(articles, start=1):
        item = dict(row)
        item["_article_index"] = idx
        item["content_profile"] = _normalize_content_profile(item.get("content_profile"))
        preview_payload.append(item)

    result = _api_post(
        cfg,
        "articles/publish/",
        {
            "newsletter_id": int(newsletter_id),
            "articles": preview_payload,
            "mode": "preview",
            "prune_missing": True,
        },
    )

    published_rows = result.get("articles", []) if isinstance(result, dict) else []
    row_by_index: dict[int, dict[str, Any]] = {}
    for row in published_rows:
        if not isinstance(row, dict):
            continue
        try:
            draft_index = int(row.get("draft_article_index") or 0)
        except Exception:
            continue
        if draft_index > 0:
            row_by_index[draft_index] = row

    updated_articles: list[dict[str, Any]] = []
    missing_preview_rows: list[int] = []
    for idx, article in enumerate(articles, start=1):
        item = dict(article)
        row = row_by_index.get(idx)
        if not row:
            missing_preview_rows.append(idx)
            updated_articles.append(item)
            continue

        article_id = row.get("id")
        preview_path = str(row.get("preview_path", "")).strip()
        preview_token = str(row.get("preview_token", "")).strip()
        editorial_status = str(row.get("editorial_status", "")).strip()

        if article_id is not None:
            item["portal_article_id"] = article_id
        if preview_path:
            item["preview_path"] = preview_path
            item["editorial_preview_path"] = preview_path
        if preview_token:
            item["preview_token"] = preview_token
            item["editorial_preview_token"] = preview_token
            item["preview_card_path"] = f"/preview/card/{preview_token}/"
        if editorial_status:
            item["editorial_status"] = editorial_status
        item["content_profile"] = _normalize_content_profile(item.get("content_profile"))
        item["telegram_triage_status"] = "not_sent"

        if article_id is not None:
            persist_payload: dict[str, Any] = {
                "article_id": int(article_id),
                "telegram_triage_status": "not_sent",
                "content_profile": item["content_profile"],
                "source_link_origin": _source_link_origin_from_article(item),
            }
            title_for_telegram = str(item.get("proposed_title", "")).strip() or str(item.get("title", "")).strip()
            if title_for_telegram:
                persist_payload["proposed_title"] = title_for_telegram[:500]
            original_url = str(item.get("original_url", "")).strip()
            if original_url:
                persist_payload["original_url"] = original_url
            status_value = str(item.get("link_validation_status", "")).strip()
            if status_value in {"not_checked", "valid", "uncertain", "invalid"}:
                persist_payload["link_validation_status"] = status_value
            if "link_validation_confidence" in item:
                try:
                    persist_payload["link_validation_confidence"] = float(item.get("link_validation_confidence", 0.0) or 0.0)
                except Exception:
                    pass
            reason = str(item.get("link_validation_reason", "")).strip()
            if reason:
                persist_payload["link_validation_reason"] = reason[:2000]
            try:
                _api_post(cfg, "articles/link-validation/", persist_payload)
            except Exception as exc:
                LOG.warning(
                    "failed to persist preview triage metadata newsletter_id=%s article_id=%s err=%s",
                    newsletter_id,
                    article_id,
                    exc,
                )

        updated_articles.append(item)

    if missing_preview_rows:
        raise RuntimeError(
            "preview article sync missing rows for draft indices "
            + ", ".join(str(value) for value in missing_preview_rows)
        )
    return updated_articles


def _mark_articles_pending_approval(articles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    requested_at = datetime.now(tz=timezone.utc).isoformat()
    out: list[dict[str, Any]] = []
    for row in articles:
        item = dict(row)
        decision = str(item.get("review_decision", "")).strip().lower()
        if decision not in {"approved", "rejected", "pending"}:
            decision = "pending"
        item["review_decision"] = decision
        item["review_required"] = True
        item.setdefault("review_requested_at", requested_at)
        out.append(item)
    return out


def process_single_newsletter(
    cfg: Config,
    newsletter_id: int,
    raw_html: str,
    *,
    email_meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    started = time.time()
    # --- INÍCIO: Chave de Idempotência (Bloqueio de Duplicados) ---
    try:
        manage_py = _resolve_manage_py_path()
        duplicate_detected = False
        if manage_py is not None:
            result = subprocess.run(
                [
                    "python",
                    str(manage_py),
                    "shell",
                    "-c",
                    (
                        "from news.models import Newsletter; "
                        f"n = Newsletter.objects.filter(id={newsletter_id}).first(); "
                        "exists = Newsletter.objects.filter("
                        "original_sent_at=n.original_sent_at, "
                        "original_sender_email=n.original_sender_email"
                        ").exclude(id=n.id).exists() if (n and n.original_sent_at and n.original_sender_email) else False; "
                        "print('DUPE' if exists else 'NO')"
                    ),
                ],
                cwd=str(manage_py.parent),
                capture_output=True,
                text=True,
                check=False,
            )
            duplicate_detected = "DUPE" in (result.stdout or "")
        else:
            LOG.warning("Idempotency check skipped: manage.py not found at %s", manage_py)

        if duplicate_detected:
            LOG.info("Idempotency Key: Duplicate content detected for ID=%s. Skipping.", newsletter_id)
            _api_post(
                cfg,
                "newsletter/status/",
                {
                    "newsletter_id": newsletter_id, 
                    "status": "error", 
                    "error_message": "Auto-rejected: Duplicate original content (Idempotency Key)."
                },
            )
            return {"status": "duplicate", "newsletter_id": newsletter_id, "articles_created": 0}
    except Exception as exc:
        LOG.warning("Idempotency check failed for ID=%s: %s", newsletter_id, exc)
    # --- FIM: Chave de Idempotência ---
        
    try:
        _assert_required_summary_model(cfg)
    except Exception as exc:
        message = f"Model enforcement failed: {exc}"
        _api_post(
            cfg,
            "newsletter/status/",
            {"newsletter_id": newsletter_id, "status": "error", "error_message": message},
        )
        return {"status": "error", "newsletter_id": newsletter_id, "articles_created": 0, "error": message}

    meta = email_meta or {}
    cleaned = clean_html(raw_html)
    email_segments = _extract_email_segments_from_html(raw_html)
    revh_result = parse_email_articles(raw_html, email_meta=meta)
    revh_articles: list[dict[str, Any]] = []
    if _should_use_revh_parse_result(revh_result.articles):
        revh_articles = [_article_from_revh_candidate(cfg, row) for row in revh_result.articles]
        if email_segments:
            revh_articles = _attach_email_segment_images(revh_articles, email_segments)
        revh_articles = _attach_email_images_from_html(revh_articles, raw_html)
    email_segment_articles: list[dict[str, Any]] = []

    # -- Audit: guardar output do clean_html antes de qualquer manipulação --
    try:
        audit_dir = Path(cfg.review_output_dir)
        audit_dir.mkdir(parents=True, exist_ok=True)
        (audit_dir / f"newsletter_{newsletter_id}_cleaned.txt").write_text(cleaned, encoding="utf-8")
        LOG.info("audit: saved cleaned text newsletter_id=%s chars=%s", newsletter_id, len(cleaned))
        revh_audit = {
            "family": revh_result.family,
            "skipped": bool(revh_result.skipped),
            "skip_notes": list(revh_result.skip_notes or []),
            "article_count": len(revh_result.articles),
            "articles": [asdict(row) for row in revh_result.articles],
        }
        (audit_dir / f"newsletter_{newsletter_id}_revh_parse.json").write_text(
            json.dumps(revh_audit, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        LOG.info("audit: saved revh parse newsletter_id=%s articles=%s family=%s", newsletter_id, len(revh_result.articles), revh_result.family)
    except Exception as exc:
        LOG.warning("audit: failed to save cleaned text newsletter_id=%s err=%s", newsletter_id, exc)

    link_articles: list[dict[str, Any]] = []
    link_text = ""
    if revh_articles:
        newsletter_type = "digest" if len(revh_articles) > 1 else "article"
        articles = revh_articles
    elif len(email_segments) >= 2:
        email_segment_articles = [_segment_article_with_llm(cfg, segment) for segment in email_segments]
        newsletter_type = "digest"
        articles = email_segment_articles
    else:
        link_articles, link_text = _extract_articles_from_primary_link(cleaned)
        if link_articles:
            newsletter_type = "digest"
            articles = link_articles
        else:
            # If link extraction did not produce sections, enrich prompt context with fetched page text when available.
            llm_source_text = cleaned
            if link_text:
                llm_source_text = f"{cleaned}\n\n--- RESOLVED LINK CONTENT ---\n{link_text}"

            # -- Audit: guardar o texto final entregue ao Ollama --
            try:
                audit_dir = Path(cfg.review_output_dir)
                audit_dir.mkdir(parents=True, exist_ok=True)
                (audit_dir / f"newsletter_{newsletter_id}_llm_source_text.txt").write_text(llm_source_text, encoding="utf-8")
                LOG.info("audit: saved llm_source_text newsletter_id=%s chars=%s", newsletter_id, len(llm_source_text))
            except Exception as exc:
                LOG.warning("audit: failed to save llm_source_text newsletter_id=%s err=%s", newsletter_id, exc)

            newsletter_type = classify_newsletter(cfg, llm_source_text[:2000])
            # Apply truncation here to avoid LLM timeouts with massive prompts
            articles = extract_articles(cfg, llm_source_text[:12000], newsletter_type)

    subject_hint = _extract_forwarded_subject_hint(raw_html)
    anchor_candidates = _extract_email_anchor_candidates(raw_html)
    if anchor_candidates:
        single_article_mode = len(articles) == 1
        articles = [
            _attach_email_anchor_source(
                article,
                anchor_candidates,
                subject_hint=subject_hint,
                single_article_mode=single_article_mode,
            )
            for article in articles
        ]

    with_source = [_apply_source_metadata(article, cfg) for article in articles]
    rewritten = [rewrite_article_from_source(cfg, article) for article in with_source]
    enriched = [enrich_article(cfg, article) for article in rewritten]
    enriched = [_attach_discovered_source_before_review(cfg, article) for article in enriched]
    enriched = [_prefer_source_published_at(cfg, article) for article in enriched]
    enriched = [_attach_web_image_fallback(cfg, article) for article in enriched]
    enriched = _mark_articles_pending_approval(enriched)
    manual_review_required = sum(1 for row in enriched if bool(row.get("manual_review_required")))

    if not enriched:
        _api_post(
            cfg,
            "newsletter/status/",
            {"newsletter_id": newsletter_id, "status": "error", "error_message": "No articles extracted"},
        )
        return {"status": "error", "newsletter_id": newsletter_id, "articles_created": 0}

    review_path = _write_review_bundle(cfg, newsletter_id, newsletter_type, enriched, cfg.llm_backend)

    if cfg.require_manual_article_approval or cfg.review_before_publish or manual_review_required > 0:
        try:
            enriched = _sync_review_articles_to_portal(cfg, newsletter_id, enriched)
            review_path = _write_review_bundle(cfg, newsletter_id, newsletter_type, enriched, cfg.llm_backend)
        except Exception as exc:
            message = f"Review queue sync failed: {exc}"
            _api_post(
                cfg,
                "newsletter/status/",
                {
                    "newsletter_id": newsletter_id,
                    "status": "error",
                    "error_message": message,
                },
            )
            _api_post(
                cfg,
                "log/",
                {
                    "newsletter_id": newsletter_id,
                    "action": "pipeline",
                    "status": "error",
                    "message": message,
                    "duration_seconds": round(time.time() - started, 3),
                },
            )
            return {
                "status": "error",
                "newsletter_id": newsletter_id,
                "articles_created": len(enriched),
                "review_file": review_path,
                "manual_review_required": manual_review_required,
                "error": message,
            }

        review_message = "Draft ready for article-by-article approval"
        if manual_review_required > 0:
            review_message = (
                f"Draft ready for article-by-article approval; {manual_review_required} article(s) require source decision"
            )
        _api_post(
            cfg,
            "newsletter/status/",
            {
                "newsletter_id": newsletter_id,
                "status": "review",
                "error_message": review_message,
            },
        )
        _api_post(
            cfg,
            "log/",
            {
                "newsletter_id": newsletter_id,
                "action": "pipeline",
                "status": "success",
                "message": f"Draft generated ({newsletter_type}) at {review_path} | manual_review_required={manual_review_required}",
                "duration_seconds": round(time.time() - started, 3),
            },
        )
        return {
            "status": "review",
            "newsletter_id": newsletter_id,
            "articles_created": len(enriched),
            "review_file": review_path,
            "manual_review_required": manual_review_required,
        }

    result = _api_post(cfg, "articles/publish/", {"newsletter_id": newsletter_id, "articles": enriched})

    _api_post(
        cfg,
        "log/",
        {
            "newsletter_id": newsletter_id,
            "action": "pipeline",
            "status": "success",
            "message": f"Processed as {newsletter_type}",
            "duration_seconds": round(time.time() - started, 3),
        },
    )
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Process one newsletter from JSON payload")
    parser.add_argument("--payload", required=True, help="JSON file with newsletter_id + raw_html")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    cfg = Config()

    if not cfg.agent_api_key:
        raise SystemExit("AGENT_API_KEY missing")

    with open(args.payload, "r", encoding="utf-8") as f:
        payload = json.load(f)

    result = process_single_newsletter(cfg, int(payload["newsletter_id"]), str(payload["raw_html"]))
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
