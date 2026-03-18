from __future__ import annotations

import base64
import json
import os
import re
import unicodedata
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from html import unescape
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

from bs4 import BeautifulSoup

PT_MONTHS = {
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
EN_MONTHS = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}
EMAIL_RE = re.compile(r"([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})", re.I)
URL_RE = re.compile(r'https?://[^\s<>"\']+', re.I)
URL_IN_ANGLE_RE = re.compile(r"<(https?://[^>\n]+)>")
FORWARD_HEADER_RE = re.compile(
    r"(?ims)^[ \t]*(?:_{2,}\s*)?(?:from|de):\s*(?P<from>.+?)\s*$.*?"
    r"^[ \t]*(?:sent|date|enviado|data):\s*(?P<sent>.+?)\s*$.*?"
    r"^[ \t]*(?:to|para):\s*(?P<to>.+?)\s*$.*?"
    r"^[ \t]*(?:subject|assunto):\s*(?P<subject>.+?)\s*$"
)
TITLE_LINK_RE = re.compile(r"(?m)^(?P<title>[^\n<]{4,220}?)\s*<(?P<link>https?://[^>\n]+)>")
TLDR_ITEM_RE = re.compile(
    r"(?ms)^(?P<title>[^\n<]{8,180}?)\s+<(?P<link>https?://[^>\n]+)>\s*\n\n"
    r"(?P<body>.*?)(?=\n\n[^\n<]{8,180}?\s+<https?://|\Z)"
)
NUMBERED_ITEM_RE = re.compile(r"(?ms)^\s*(?P<num>\d+)\.\s+(?P<title>[^:\n]{8,220}):\s*(?P<body>.*?)(?=^\s*\d+\.\s+|\Z)")
QUOTED_LINK_RE = re.compile(r'(?m)^"?(?P<title>[A-Z\[][^\n<]{6,220}?)"?\s*<(?P<link>https?://[^>\n]+)>')
CURATED_SECTION_RE = re.compile(r"(?ms)^_{10,}\s*\n(?P<title>[^\n]{8,220})\n(?P<body>.*?)(?=^_{10,}\s*$|\Z)")
TRACKING_PATH_B64_RE = re.compile(r"^[A-Za-z0-9_-]{16,}$")
FW_PREFIX_RE = re.compile(r"^\s*(?:fw|fwd)\s*:\s*", re.I)
LINK_TITLE_NOISE_RE = re.compile(
    r"^(?:read in app|read online|read more|view(?: in browser| online)?|listen online|open in app|"
    r"sign up|get started|subscribe|watch now|get your guide|check it out|download your guide(?: today)?|"
    r"tweet screenshot|welcome back!?|privacy policy|terms of service|comments?$|"
    r"share(?: on [a-z]+)?|image removed by sender\.?)$",
    re.I,
)
SERIES_LINK_RE = re.compile(r"^(?:in\s+)?part\s+\d+\b", re.I)
LINK_TITLE_NOISE_CONTAINS = (
    "carlos santos",
    "advanced analytics & ai",
    "pluto analytics",
    "subscribe here",
    "click here to remain a subscriber",
    "click here to",
    "get your guide",
    "download your guide",
    "check it out",
    "tweet screenshot",
    "claim my free post",
    "upgrade to paid",
    "or upgrade your subscription",
    "watch on x",
    "watch on youtube",
    "listen on spotify",
    "listen on apple",
    "apple podcasts",
    "full notes and more episodes",
    "episode transcript",
    "work with us",
    "b2b training",
    "read louis's perspective",
    "full story",
    "available here",
    "youtube video by",
    "welcome back!",
    "you follow",
    "sponsor",
    "reach out to me",
    "any of my socials",
)
URL_NOISE_MARKERS = (
    "stay-subscribed",
    "submitlike=true",
    "comments=true",
    "disable_email",
    "web-version",
    "play_audio=true",
    "podcast-email",
    "unsubscribe",
    "privacy",
    "terms",
)
SOCIAL_NOISE_DOMAINS = frozenset({
    "twitter.com", "x.com", "instagram.com", "facebook.com",
    "linkedin.com", "youtube.com", "youtu.be", "tiktok.com", "threads.net",
})
FAMILY_TITLE_NOISE_EXACT: dict[str, set[str]] = {
    "substack": {"like", "comment", "restack", "read in app", "spotify", "youtube", "apple podcasts"},
    "every": {"x", "youtube", "spotify", "apple podcasts", "episode transcript"},
    "kit": {
        "master full-stack ai engineering",
        "master full stack ai engineering",
        "succeed in ai engineering roles",
        "open-source",
        "open source",
        "hands-on",
        "hands on",
        "tutorial",
        "research",
    },
    "beehiiv": {
        "like", "share", "comment", "follow", "subscribe", "refer",
        "upgrade", "read online", "view online", "open in browser",
    },
    "neuron": {
        "like", "share", "comment", "follow", "subscribe", "refer",
        "upgrade", "read online", "view online", "open in browser",
    },
}
FAMILY_TITLE_NOISE_CONTAINS: dict[str, tuple[str, ...]] = {
    "substack": (
        "claim my free post",
        "upgrade to paid",
        "or upgrade your subscription",
        "full notes and more episodes",
        "post voiceover",
        "subscribe to unlock",
        "become a paid subscriber",
        "this post is for paid subscribers",
        "restacking",
        "per month or",
        "per year",
        "usd per",
        "pledge your support",
        "support ai made simple",
        "upgrade your subscription",
    ),
    "every": ("watch on x", "watch on youtube", "listen on spotify", "listen on apple", "episode transcript"),
    "kit": (
        "course with",
        "this course",
        "you can read more about this here",
        "you can find the github repo here",
        "you can find the openpipe art github repo here",
    ),
    "beehiiv": (
        "refer a friend",
        "share this",
        "follow us on",
        "follow on ",
        "follow me on",
        "subscribe to ",
        "check out the newest",
        "reach out to me",
        "today's sponsor",
        "in partnership with",
        "powered by beehiiv",
        "upgrade to",
        "become a member",
        "advertise with",
        "advertise in",
        "partner with",
        "see all sponsors",
        "our sponsor",
    ),
    "neuron": (
        "refer a friend",
        "share this",
        "follow us on",
        "follow on ",
        "follow me on",
        "check out the newest",
        "reach out to me",
        "today's sponsor",
        "in partnership with",
        "upgrade to",
        "become a member",
        "advertise with",
        "advertise in",
        "partner with",
        "see all sponsors",
        "our sponsor",
    ),
}
FAMILY_URL_NOISE_MARKERS: dict[str, tuple[str, ...]] = {
    "substack": ("app-store", "/@", "/subscribe", "/referral", "substack.com/upgrade"),
    "every": ("/@", "/courses/", "x.com/", "youtu", "spotify.com/", "podcasts.apple.com/"),
    "kit": ("/membership",),
    "beehiiv": ("/subscribe", "/upgrade", "/refer", "share=true", "referral="),
    "neuron": ("/subscribe", "/upgrade", "/refer", "share=true", "referral="),
}

BOILERPLATE_PATTERNS = [
    "manage your subscriptions",
    "unsubscribe",
    "privacy policy",
    "terms of service",
    "powered by beehiiv",
    "forwarded this email? subscribe here",
    "this email was sent to",
    "open in browser",
    "view online",
    "read online",
    "listen online",
    "share on facebook",
    "share on twitter",
    "share on linkedin",
    "share on threads",
    "start writing",
    "update your profile",
    "partner with us",
    "advertise to",
    "you received this email because",
    "click here to unsubscribe",
    "need help? visit our help center",
    "sponsored by",
    "published with curated",
]
PUBLICATION_NAME_HINTS = [
    "substack",
    "lenny's newsletter",
    "lenny's podcast",
    "the palindrome",
    "context window",
    "every",
    "machine learning at scale",
    "artificial intelligence made simple",
]
SKIP_SENDER_DOMAINS = {"github.com", "linkedin.com", "google.com", "notifications.google.com"}
MATCH_STOPWORDS = {
    "the",
    "and",
    "for",
    "with",
    "from",
    "this",
    "that",
    "your",
    "more",
    "read",
    "today",
    "issue",
    "newsletter",
    "episode",
    "weekly",
}


@dataclass
class ParsedArticle:
    title: str
    text: str
    source_link: str
    published_date: str
    published_date_source: str
    confidence: float
    notes: list[str]
    family: str
    source_link_final: bool = True  # False = placeholder à espera de link definitivo do utilizador
    images: list[str] = field(default_factory=list)


@dataclass
class EmailParseResult:
    articles: list[ParsedArticle]
    family: str
    skipped: bool
    skip_notes: list[str]
    best_text: str
    links: list[dict[str, Any]]
    # Metadados do email (pontos 1-5 do objetivo)
    received_at: str = ""             # 1 - data de receção pelo padev
    sender_email: str = ""            # 2 - quem enviou ao padev
    is_forwarded: bool = False        # 3 - se é reencaminhado
    original_sent_at: str = ""        # 3 - data do email original
    original_sender_email: str = ""   # 4 - email do remetente original
    original_sender_name: str = ""    # 4 - nome do remetente original
    original_subject: str = ""        # 5 - assunto do email original


def _strip_accents(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


_INVISIBLE_CHARS_RE = re.compile(
    r"[\u00ad\u034f\u200b\u200c\u200d\u200e\u200f\u2028\u2029\u2060\ufeff]+"
)


def clean_text(text: str | None) -> str:
    value = unescape((text or "").replace("\r\n", "\n").replace("\r", "\n"))
    value = value.replace("\u00a0", " ")
    value = _INVISIBLE_CHARS_RE.sub("", value)
    value = re.sub(r"[ \t]+", " ", value)
    value = re.sub(r"\n{3,}", "\n\n", value)
    return value.strip()


def clean_heading(text: str | None) -> str:
    return re.sub(r"\s+", " ", clean_text(text)).strip()


def _normalize_url(url: str | None) -> str:
    value = unquote((url or "").strip())
    return value.strip("<>.,);]")


def _forwarder_emails() -> set[str]:
    values = [
        item.strip().lower()
        for item in os.getenv("ALLOWED_FORWARDER_EMAILS", "carlos.santos@plutoanalytics.com").split(",")
        if item.strip()
    ]
    return set(values)


def unwrap(url: str | None) -> str:
    value = _normalize_url(url)
    if not value:
        return ""
    if "/CL0/https:" in value:
        match = re.search(r"/CL0/(https:[^\s]+?)(?:/[0-9]/|$)", value)
        if match:
            return unquote(match.group(1))
    parsed = urlparse(value)
    if parsed.netloc.endswith("substack.com") and parsed.path.startswith("/redirect/2/"):
        tail = parsed.path.split("/redirect/2/", 1)[-1].split("/", 1)[0]
        try:
            raw = tail + "=" * (-len(tail) % 4)
            decoded = base64.urlsafe_b64decode(raw.encode("ascii")).decode("utf-8", errors="ignore").strip()
            if decoded.startswith("{"):
                obj = json.loads(decoded)
                if isinstance(obj, dict):
                    for key in ("url", "e"):
                        if obj.get(key):
                            return _normalize_url(str(obj[key]))
            if decoded.startswith(("https://", "http://")):
                return _normalize_url(decoded)
        except Exception:
            pass
    qs = parse_qs(parsed.query)
    for key in ("url", "u", "redirect", "dest", "destination", "next"):
        if key in qs and qs[key]:
            return _normalize_url(qs[key][0])
    if parsed.netloc.endswith("every.to") and "/emails/click/" in parsed.path:
        tail = parsed.path.rsplit("/", 1)[-1]
        try:
            raw = tail + "=" * (-len(tail) % 4)
            decoded = base64.urlsafe_b64decode(raw.encode("ascii")).decode("utf-8", errors="ignore")
            obj = json.loads(decoded)
            if isinstance(obj, dict) and obj.get("url"):
                return _normalize_url(str(obj["url"]))
        except Exception:
            pass
    for segment in reversed([part for part in parsed.path.split("/") if part]):
        candidate = segment.rstrip("=")
        if not TRACKING_PATH_B64_RE.fullmatch(candidate):
            continue
        try:
            raw = candidate + "=" * (-len(candidate) % 4)
            decoded = base64.urlsafe_b64decode(raw.encode("ascii")).decode("utf-8", errors="ignore").strip()
        except Exception:
            continue
        if decoded.startswith(("https://", "http://")):
            return _normalize_url(decoded)
        if decoded.startswith("{") and '"url"' in decoded:
            try:
                obj = json.loads(decoded)
            except Exception:
                continue
            if isinstance(obj, dict) and obj.get("url"):
                return _normalize_url(str(obj["url"]))
    return value


def html_to_text(html: str) -> str:
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    # Remover elementos ocultos usados por email clients para anti-clipping
    for tag in soup.find_all(style=True):
        if not tag.attrs:
            continue
        style = (tag.get("style") or "").replace(" ", "").lower()
        if "display:none" in style or "max-height:0" in style or "visibility:hidden" in style or "opacity:0" in style:
            tag.decompose()
    # Normalizar whitespace dentro de text nodes: elimina \n de formatação HTML
    # que causam quebras no meio de frases inline (ex: "An\n Agent Contacted Me").
    for node in soup.find_all(string=True):
        if node.parent and node.parent.name in ("script", "style", "pre"):
            continue
        normalized = re.sub(r"[ \t\r\n]+", " ", str(node))
        node.replace_with(normalized)
    # Após normalizar text nodes, br e block elements são as únicas fontes de \n.
    for br in soup.find_all("br"):
        br.replace_with("\n")
    _BLOCK = {"p", "div", "h1", "h2", "h3", "h4", "h5", "h6",
              "li", "tr", "td", "th", "blockquote",
              "section", "article", "header", "footer", "nav", "aside", "main"}
    for tag in soup.find_all(_BLOCK):
        tag.insert_before("\n\n")
    # Sem separador: espaçamento vem dos text nodes, não do get_text().
    # Isto evita que tags inline (b, a, span) introduzam \n entre si.
    return clean_text(soup.get_text(""))


def extract_text_links(text: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for rx in (URL_IN_ANGLE_RE, URL_RE):
        for match in rx.finditer(text or ""):
            url = match.group(1) if match.lastindex else match.group(0)
            normalized = _normalize_url(url)
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            out.append(
                {
                    "url_original": normalized,
                    "url_unwrapped": unwrap(normalized),
                    "anchor_text": None,
                }
            )
    return out


def extract_html_links(html: str) -> list[dict[str, Any]]:
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    out: list[dict[str, Any]] = []
    for anchor in soup.find_all("a", href=True):
        href = _normalize_url(anchor.get("href", ""))
        if not href:
            continue
        out.append(
            {
                "url_original": href,
                "url_unwrapped": unwrap(href),
                "anchor_text": clean_text(anchor.get_text(" ", strip=True)) or None,
                "context_text": _anchor_context_text(anchor) or None,
            }
        )
    return out


def dedupe_links(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for row in rows:
        url = row.get("url_unwrapped") or row.get("url_original") or ""
        anchor = row.get("anchor_text") or ""
        key = (url, anchor)
        if not url or key in seen:
            continue
        seen.add(key)
        item = dict(row)
        item["domain"] = (urlparse(url).netloc or "").lower()
        out.append(item)
    return out


def _anchor_context_text(anchor) -> str:
    direct = clean_text(anchor.get_text(" ", strip=True))
    if direct and len(re.findall(r"[A-Za-z0-9]+", direct)) >= 2 and not LINK_TITLE_NOISE_RE.match(direct):
        return direct[:500]

    img = anchor.find("img")
    if img is not None:
        alt = clean_text(str(img.get("alt", "")))
        if alt and len(re.findall(r"[A-Za-z0-9]+", alt)) >= 2 and not LINK_TITLE_NOISE_RE.match(alt):
            return alt[:500]
    return direct[:500]


def _strip_forward_prefix(text: str | None) -> str:
    value = clean_heading(text)
    previous = None
    while value and value != previous:
        previous = value
        value = FW_PREFIX_RE.sub("", value).strip()
    return value


def _is_sparse_heading(text: str | None) -> bool:
    value = clean_heading(text)
    return len(re.findall(r"[A-Za-z0-9]{2,}", value)) < 2


def _resolved_subject_hint(context: dict[str, Any], meta: dict[str, Any]) -> str:
    candidates = [
        meta.get("original_subject"),
        context.get("headers_parsed", {}).get("subject"),
    ]
    for candidate in candidates:
        subject = _strip_forward_prefix(candidate)
        if subject and not _is_sparse_heading(subject):
            return subject
    return _strip_forward_prefix(candidates[0] or candidates[1] or "")


def _token_set(text: str | None) -> set[str]:
    return {
        token
        for token in re.findall(r"[A-Za-z0-9]{2,}", _strip_accents(clean_text(text).lower()))
        if token not in MATCH_STOPWORDS
    }


def _title_match_score(title: str, target: str) -> int:
    title_clean = clean_heading(title)
    target_clean = clean_heading(target)
    if not title_clean or not target_clean:
        return 0
    title_low = _strip_accents(title_clean).lower()
    target_low = _strip_accents(target_clean).lower()
    if title_low == target_low:
        return 12
    if title_low in target_low or target_low in title_low:
        return 8
    title_tokens = _token_set(title_clean)
    target_tokens = _token_set(target_clean)
    if not title_tokens or not target_tokens:
        return 0
    overlap = title_tokens & target_tokens
    if not overlap:
        return 0
    score = len(overlap) * 2
    if len(overlap) >= max(2, min(len(title_tokens), len(target_tokens)) // 2):
        score += 4
    return score


def _looks_like_link_title_noise(text: str, *, subject_hint: str = "", allow_subject_match: bool = False) -> bool:
    value = clean_heading(text)
    if not value:
        return True
    if len(re.findall(r"[A-Za-z0-9]+", value)) < 2:
        return True
    if LINK_TITLE_NOISE_RE.match(value):
        return True
    if is_boilerplate(value):
        return True
    low = value.lower()
    flat = re.sub(r"\s+", " ", low).strip()
    if any(marker in flat for marker in LINK_TITLE_NOISE_CONTAINS):
        return True
    if SERIES_LINK_RE.match(value):
        return True
    if any(flat.startswith(prefix) for prefix in ("advertise to ", "partner with us", "sponsored by", "in partnership with")):
        return True
    if flat in {"preview", "sponsor", "today's issue", "in today's newsletter:", "full list here", "opt in/out", "opt in / out"}:
        return True
    if re.fullmatch(r"[\w.-]+\.(io|com|net|org|co)", flat):
        return True
    if subject_hint and not allow_subject_match and flat == clean_heading(subject_hint).lower():
        return True
    return False


def _title_from_context_text(value: str) -> str:
    text = clean_text(value)
    if not text:
        return ""
    first_line = clean_text(text.splitlines()[0])
    sentence = clean_text(re.split(r"(?<=[.!?])\s+", first_line, maxsplit=1)[0])
    candidate = sentence or first_line
    return candidate[:220]


def _title_from_link_row(row: dict[str, Any], *, subject_hint: str = "", allow_subject_match: bool = False) -> str:
    anchor_text = clean_heading(row.get("anchor_text") or "")
    if anchor_text.startswith(("http://", "https://")):
        anchor_text = ""
    if anchor_text and not _looks_like_link_title_noise(anchor_text, subject_hint=subject_hint, allow_subject_match=allow_subject_match):
        return anchor_text
    context_text = clean_text(row.get("context_text") or "")
    title = _title_from_context_text(context_text)
    if title.startswith(("http://", "https://")):
        return ""
    return title


def _looks_like_link_candidate_noise(
    title: str,
    url: str,
    family: str,
    *,
    context_text: str = "",
    subject_hint: str = "",
    allow_subject_match: bool = False,
) -> bool:
    if not url.startswith(("http://", "https://")):
        return True
    if _looks_like_link_title_noise(title, subject_hint=subject_hint, allow_subject_match=allow_subject_match):
        return True
    low_title = clean_heading(title).lower()
    low_context = re.sub(r"\s+", " ", clean_text(context_text).lower()).strip()
    low_url = url.lower()
    if any(marker in low_url for marker in URL_NOISE_MARKERS):
        return True
    if low_title in FAMILY_TITLE_NOISE_EXACT.get(family, set()):
        return True
    if any(marker in low_title for marker in FAMILY_TITLE_NOISE_CONTAINS.get(family, ())):
        return True
    if any(marker in low_context for marker in FAMILY_TITLE_NOISE_CONTAINS.get(family, ())):
        return True
    if any(marker in low_url for marker in FAMILY_URL_NOISE_MARKERS.get(family, ())):
        return True
    if "sponsor" in low_title or "sponsor" in low_context or "utm_medium=sponsored" in low_url:
        return True
    if family in {"beehiiv", "neuron"}:
        parsed_netloc = urlparse(url).netloc.lstrip("www.")
        if parsed_netloc in SOCIAL_NOISE_DOMAINS:
            return True
    if family == "substack" and _title_match_score(title, subject_hint) == 0:
        if re.fullmatch(r"[A-Z][\w'.-]+(?: [A-Z][\w'.-]+){1,2}", clean_heading(title)):
            return True
    return False


def _iter_scored_link_candidates(
    context: dict[str, Any],
    meta: dict[str, Any],
    family: str,
    *,
    match_hint: str = "",
    allow_subject_match: bool = False,
) -> list[dict[str, Any]]:
    subject_hint = clean_heading(match_hint or _resolved_subject_hint(context, meta))
    out: list[dict[str, Any]] = []
    for idx, row in enumerate(context.get("links", [])):
        url = clean_text(row.get("url_unwrapped") or row.get("url_original") or "")
        context_text = clean_text(row.get("context_text") or "")
        title = _title_from_link_row(row, subject_hint=subject_hint, allow_subject_match=allow_subject_match)
        if not title:
            continue
        if _looks_like_link_candidate_noise(
            title,
            url,
            family,
            context_text=context_text,
            subject_hint=subject_hint,
            allow_subject_match=allow_subject_match,
        ):
            continue
        score = _title_match_score(title, subject_hint)
        if context_text and len(context_text) >= 80:
            score += 2
        if len(title) >= 24:
            score += 1
        low_url = url.lower()
        if family == "substack" and ("substack.com/app-link/post" in low_url or "open.substack.com/" in low_url or "substack.com/redirect/" in low_url):
            score += 2
        elif family == "every" and "every.to/" in low_url:
            score += 2
        elif family == "kit" and "dailydoseofds.com" in low_url:
            score += 2
        out.append(
            {
                "title": title,
                "body": context_text if len(context_text) > len(title) + 20 else title,
                "url": url,
                "score": score,
                "order": idx,
            }
        )
    return out


def _best_link_candidate(
    context: dict[str, Any],
    meta: dict[str, Any],
    family: str,
    match_hint: str,
) -> dict[str, Any] | None:
    rows = _iter_scored_link_candidates(context, meta, family, match_hint=match_hint, allow_subject_match=True)
    if not rows:
        return None
    rows.sort(key=lambda item: (item["score"], -item["order"]), reverse=True)
    return rows[0]


def _extract_kit_issue_titles(forward_body: str) -> list[str]:
    titles: list[str] = []
    seen: set[str] = set()
    lines = [clean_heading(line) for line in forward_body.splitlines()]
    capture = False
    for line in lines:
        low = line.lower()
        if not capture:
            if low in {"in today's newsletter:", "in today’s newsletter:"}:
                capture = True
            continue
        if not line or line in {"·", "•", "-", "*"}:
            continue
        if low in {"today's issue", "today’s issue"}:
            break
        if low in {"open-source", "open source", "hands-on", "hands on", "tutorial", "research"}:
            continue
        if is_boilerplate(line):
            break
        if _looks_like_link_title_noise(line, allow_subject_match=True):
            continue
        key = line.casefold()
        if key in seen:
            continue
        seen.add(key)
        titles.append(line)
        if len(titles) >= 6:
            break
    return titles


def _extract_link_digest_items(
    context: dict[str, Any],
    meta: dict[str, Any],
    family: str,
    *,
    max_items: int = 8,
) -> list[ParsedArticle]:
    by_url: dict[str, dict[str, Any]] = {}

    for row in _iter_scored_link_candidates(context, meta, family, allow_subject_match=True):
        url = row["url"]
        current = by_url.get(url)
        if current is None or row["score"] > current["score"]:
            by_url[url] = dict(row)

    rows = sorted(by_url.values(), key=lambda item: item["order"])
    articles: list[ParsedArticle] = []
    seen_titles: set[str] = set()
    for row in rows:
        title = clean_heading(row["title"])
        if title.casefold() in seen_titles:
            continue
        seen_titles.add(title.casefold())
        articles.append(
            _make_article(
                context,
                meta,
                title,
                row["body"],
                row["url"],
                family,
                0.76,
                ["link_digest_fallback"],
            )
        )
        if len(articles) >= max_items:
            break
    return articles


def _safe_iso(year: int, month: int, day: int, hour: int = 0, minute: int = 0, second: int = 0) -> str:
    return datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc).isoformat()


def parse_possible_date(value: str | None, fallback_year: int | None = None) -> str | None:
    if not value:
        return None
    cleaned = clean_text(value)
    ascii_value = _strip_accents(cleaned).lower()

    iso = re.search(r"\b(20\d{2})-(\d{1,2})-(\d{1,2})\b", cleaned)
    if iso:
        try:
            return _safe_iso(int(iso.group(1)), int(iso.group(2)), int(iso.group(3)))
        except ValueError:
            pass

    pt = re.search(
        r"\b(\d{1,2})\s+de\s+([a-z]+)\s+de\s+(\d{4})(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?",
        ascii_value,
        flags=re.I,
    )
    if pt:
        day = int(pt.group(1))
        month = PT_MONTHS.get(pt.group(2).lower())
        year = int(pt.group(3))
        hour = int(pt.group(4) or 0)
        minute = int(pt.group(5) or 0)
        second = int(pt.group(6) or 0)
        if month:
            try:
                return _safe_iso(year, month, day, hour, minute, second)
            except ValueError:
                pass

    en = re.search(r"\b([A-Z][a-z]+)\s+(\d{1,2})(?:,\s*(\d{4}))?\b", cleaned)
    if en:
        month = EN_MONTHS.get(en.group(1).lower())
        day = int(en.group(2))
        year = int(en.group(3) or fallback_year or datetime.now(timezone.utc).year)
        if month:
            try:
                return _safe_iso(year, month, day)
            except ValueError:
                pass

    dotted = re.search(r"\b(\d{1,2})\.(\d{1,2})\.(\d{2,4})\b", cleaned)
    if dotted:
        a = int(dotted.group(1))
        b = int(dotted.group(2))
        year = int(dotted.group(3))
        if year < 100:
            year += 2000
        candidates: list[tuple[int, int]] = []
        if a > 12:
            candidates.append((b, a))
        elif b > 12:
            candidates.append((a, b))
        else:
            candidates.extend([(a, b), (b, a)])
        for month, day in candidates:
            try:
                return _safe_iso(year, month, day)
            except ValueError:
                continue
    return None


def choose_published_date(context: dict[str, Any], text: str | None = None, link: str | None = None) -> tuple[str, str]:
    fallback_year = None
    sent = ""
    forward_blocks = context.get("signals", {}).get("forward_blocks", [])
    if forward_blocks:
        sent = str(forward_blocks[0].get("sent_iso", "")).strip()
    if not sent:
        sent = str(context.get("headers_parsed", {}).get("date", "")).strip()
    if sent:
        try:
            fallback_year = datetime.fromisoformat(sent.replace("Z", "+00:00")).year
        except Exception:
            fallback_year = None
    for source_name, haystack in (("article_text", text or ""), ("source_link", link or "")):
        parsed = parse_possible_date(haystack, fallback_year=fallback_year)
        if parsed:
            return parsed, source_name
    for candidate in context.get("signals", {}).get("date_candidates", []):
        if candidate:
            return str(candidate), "forward_or_header_fallback"
    if sent:
        return sent, "forward_or_header_fallback"
    return "", ""


def is_boilerplate(text: str) -> bool:
    hay = clean_text(text).lower()
    return any(marker in hay for marker in BOILERPLATE_PATTERNS)


_SECTION_HEADER_RE = re.compile(
    r"^(?:part|section|chapter|step|phase|module|unit|appendix)\s+\d+",
    re.I,
)


def _is_section_header(text: str) -> bool:
    """Devolve True se o texto parece um cabeçalho de secção, não um título de artigo."""
    if not text:
        return True
    if _SECTION_HEADER_RE.match(text):
        return True
    words = text.split()
    # ALL CAPS com poucas palavras (ex: "PART 1: THE 101 GUIDE")
    if len(words) <= 6 and text == text.upper() and any(c.isalpha() for c in text):
        return True
    return False


def _extract_html_heading(raw_html: str, *, skip: str = "") -> str:
    """Extrai o primeiro H1/H2/H3 significativo do HTML, ignorando boilerplate.

    Se `skip` for fornecido, ignora headings que coincidam com esse texto
    (case/accent-insensitive) e continua para o seguinte.
    """
    if not raw_html:
        return ""
    skip_norm = _strip_accents(skip.casefold()) if skip else ""
    soup = BeautifulSoup(raw_html, "html.parser")
    for level in ("h1", "h2", "h3"):
        for tag in soup.find_all(level):
            text = clean_heading(tag.get_text(" ", strip=True))
            if not text or len(text) < 6:
                continue
            if len(re.findall(r"[A-Za-z0-9]+", text)) < 2:
                continue
            if is_boilerplate(text):
                continue
            if LINK_TITLE_NOISE_RE.match(text):
                continue
            low = text.lower()
            if any(low == hint.lower() for hint in PUBLICATION_NAME_HINTS):
                continue
            if skip_norm and _strip_accents(text.casefold()) == skip_norm:
                continue
            return text
    return ""


def find_first_link(text: str) -> str:
    match = URL_IN_ANGLE_RE.search(text or "")
    return match.group(1) if match else ""


def detect_family(
    sender_email: str | None,
    original_sender_email: str | None,
    subject: str,
    best_text: str,
    links: list[dict[str, Any]],
) -> list[str]:
    hay = "\n".join(filter(None, [sender_email or "", original_sender_email or "", subject or "", best_text or ""]))
    joined_links = "\n".join((row.get("url_unwrapped") or row.get("url_original") or "") for row in links)
    probes = (hay + "\n" + joined_links).lower()
    families: list[str] = []
    if any(x in probes for x in ("substack.com", "@substack.com", "substack-post-media", "publication_id=")):
        families.append("substack")
    if any(x in probes for x in ("newsletter.theneurondaily.com", "theneuron")):
        families.append("neuron")
    if any(x in probes for x in ("beehiiv", "mail.beehiiv.com", "link.mail.beehiiv.com")):
        families.append("beehiiv")
    if any(x in probes for x in ("tldrnewsletter.com", "tldr.tech", "links.tldrnewsletter.com", "a.tldrnewsletter.com")):
        families.append("tldr")
    if any(x in probes for x in ("every.to", "mg.every.to")):
        families.append("every")
    if any(x in probes for x in ("kit-mail3.com", "convertkit", "update your profile", "advertise to 950k+")):
        families.append("kit")
    if any(x in probes for x in ("technologyreview", "list-manage.com")):
        families.append("mailchimp")
    if any(x in probes for x in ("aiweekly.co", "faveeo.com", "published with curated", "curated.co")):
        families.append("curated")
    if any(x in probes for x in ("towardsdatascience.com", "newsletter.towardsdatascience.com", "_hsenc=", "_hsmi=")):
        families.append("tds")
    if not families:
        families.append("generic")
    return families


def build_signals(
    best_text: str,
    sender_email: str,
    original_sender_email: str,
    subject: str,
    links: list[dict[str, Any]],
    sent_iso: str,
) -> dict[str, Any]:
    subject_clean = clean_text(subject)
    top_sender = clean_text(sender_email).lower()
    original_sender = clean_text(original_sender_email).lower()
    families = detect_family(top_sender, original_sender, subject_clean, best_text, links)
    signals: dict[str, Any] = {
        "forward_blocks": [],
        "original_sender_candidates": [],
        "date_candidates": [],
        "newsletter_family_candidates": families,
        "newsletter_family_best": families[0] if families else None,
        "is_probable_forward": bool(subject_clean.lower().startswith(("fw:", "fwd:"))) or top_sender in _forwarder_emails(),
        "skip_reason": None,
    }
    match = FORWARD_HEADER_RE.search(best_text or "")
    if match:
        from_raw = clean_text(match.group("from"))
        sent_raw = clean_text(match.group("sent"))
        subject_raw = clean_text(match.group("subject"))
        sent_forward = parse_possible_date(sent_raw)
        signals["forward_blocks"].append(
            {
                "from_raw": from_raw,
                "sent_raw": sent_raw,
                "sent_iso": sent_forward or "",
                "subject": subject_raw,
                "to_raw": clean_text(match.group("to")),
            }
        )
        email_match = EMAIL_RE.search(from_raw)
        if email_match:
            signals["original_sender_candidates"].append({"raw": from_raw, "email": email_match.group(1).lower()})
        if sent_forward:
            signals["date_candidates"].append(sent_forward)
    elif original_sender and original_sender != top_sender:
        signals["original_sender_candidates"].append({"raw": original_sender, "email": original_sender})
    if sent_iso:
        signals["date_candidates"].append(sent_iso)
    sender_domain = top_sender.split("@")[-1] if top_sender else ""
    original_sender_domain = ""
    if signals["original_sender_candidates"]:
        original_sender_domain = str(signals["original_sender_candidates"][0].get("email", "")).split("@")[-1]
    body_low = (best_text or "").lower()
    if not signals["forward_blocks"] and sender_domain in SKIP_SENDER_DOMAINS:
        signals["skip_reason"] = f"direct_{sender_domain}"
    elif signals["forward_blocks"] and original_sender_domain in SKIP_SENDER_DOMAINS:
        signals["skip_reason"] = "known_non_newsletter_sender"
    elif top_sender and not signals["forward_blocks"] and top_sender not in _forwarder_emails() and families == ["generic"]:
        signals["skip_reason"] = "not_forwarded_generic"
    if "watch the live video" in body_low or "open.substack.com/live-stream" in body_low:
        signals["skip_reason"] = signals.get("skip_reason") or "substack_live_notification"
    return signals


def split_forward_meta(best_text: str, context: dict[str, Any]) -> tuple[dict[str, Any], str]:
    sender_fallback = str(context.get("headers_parsed", {}).get("sender_email", "")).strip().lower()
    meta = {
        "original_sender_email": clean_text(context.get("headers_parsed", {}).get("original_sender_email") or "").lower() or None,
        "original_sender_raw": clean_text(context.get("headers_parsed", {}).get("original_sender_raw") or "") or None,
        "original_subject": context.get("headers_parsed", {}).get("subject"),
        "original_sent_raw": context.get("headers_parsed", {}).get("date"),
        "top_sender_email": sender_fallback,
    }
    match = FORWARD_HEADER_RE.search(best_text or "")
    if match:
        from_raw = clean_text(match.group("from"))
        meta["original_sender_raw"] = from_raw
        email_match = EMAIL_RE.search(from_raw)
        meta["original_sender_email"] = email_match.group(1).lower() if email_match else meta["original_sender_email"]
        meta["original_subject"] = clean_text(match.group("subject"))
        meta["original_sent_raw"] = clean_text(match.group("sent"))
        remainder = best_text[match.end():].strip()
        return meta, remainder
    candidates = context.get("signals", {}).get("original_sender_candidates", [])
    if candidates:
        meta["original_sender_email"] = candidates[0].get("email") or meta["original_sender_email"]
        meta["original_sender_raw"] = candidates[0].get("raw") or meta["original_sender_raw"]
    return meta, best_text


def family_for(context: dict[str, Any], meta: dict[str, Any], forward_body: str) -> str:
    family_candidates = context.get("signals", {}).get("newsletter_family_candidates", [])
    if family_candidates:
        return str(family_candidates[0])
    sender = (meta.get("original_sender_email") or meta.get("top_sender_email") or "").lower()
    hay = (forward_body or "").lower() + "\n" + sender
    if "tldrnewsletter.com" in hay or "tldr ai" in hay:
        return "tldr"
    if "substack.com" in hay:
        return "substack"
    if "newsletter.theneurondaily.com" in hay:
        return "neuron"
    if "beehiiv" in hay:
        return "beehiiv"
    if "every.to" in hay:
        return "every"
    if "kit-mail3.com" in hay:
        return "kit"
    if "technologyreview" in hay or "list-manage.com" in hay:
        return "mailchimp"
    if "towardsdatascience.com" in hay or "newsletter.towardsdatascience.com" in hay or "_hsenc=" in hay or "_hsmi=" in hay:
        return "tds"
    if "aiweekly.co" in hay or "faveeo.com" in hay or "published with curated" in hay:
        return "curated"
    return "generic"


def trim_footer(text: str) -> str:
    lines = [line.rstrip() for line in clean_text(text).splitlines()]
    out: list[str] = []
    content_chars = 0
    for line in lines:
        # Só corta boilerplate depois de haver conteúdo real acumulado.
        # Isto evita que banners de topo (ex: "Forwarded this email? Subscribe here")
        # sejam confundidos com footers quando aparecem como primeira linha.
        if content_chars >= 300 and is_boilerplate(line):
            break
        if re.match(r"^\[[^\]]+\]$", line):
            continue
        out.append(line)
        content_chars += len(line.strip())
    return clean_text("\n".join(out))


_BOUNCE_SUBJECT_PREFIXES = (
    "não entregue:",
    "nao entregue:",
    "undeliverable:",
    "delivery status notification",
    "mail delivery failed",
    "mail delivery failure",
    "returned mail:",
    "failure notice",
)

_BOUNCE_SENDER_PREFIXES = (
    "postmaster@",
    "mailer-daemon@",
    "mail-daemon@",
    "delivery@",
    "noreply+bounce@",
)


def should_skip_email(context: dict[str, Any], meta: dict[str, Any], family: str, forward_body: str, *, raw_html: str = "") -> tuple[bool, list[str]]:
    notes: list[str] = []
    top_sender = (meta.get("top_sender_email") or "").lower()
    original_sender = (meta.get("original_sender_email") or "").lower()
    original_sender_raw = clean_text(meta.get("original_sender_raw") or "").lower()
    original_subject = clean_text(meta.get("original_subject") or "").lower()
    has_forward = bool(context.get("signals", {}).get("forward_blocks"))
    sender_domain = (original_sender or top_sender).split("@")[-1] if (original_sender or top_sender) else ""

    # --- bounce / NDR ---
    _any_sender = original_sender or top_sender
    if any(original_subject.startswith(p) for p in _BOUNCE_SUBJECT_PREFIXES):
        return True, ["bounce_ndr"]
    if any(_any_sender.startswith(p) for p in _BOUNCE_SENDER_PREFIXES):
        return True, ["bounce_ndr"]

    if top_sender and not has_forward and top_sender not in _forwarder_emails() and family == "generic":
        notes.append("not_forwarded_newsletter")
        if sender_domain in SKIP_SENDER_DOMAINS:
            notes.append("known_non_newsletter_sender")
        return True, notes

    if family == "generic" and sender_domain in SKIP_SENDER_DOMAINS:
        return True, ["known_non_newsletter_sender"]

    body_low = (forward_body or "").lower()
    if sender_domain == "github.com" or "github sudo authentication code" in body_low:
        return True, ["github_security_email"]
    if sender_domain == "linkedin.com" or "ver mais no linkedin" in body_low:
        return True, ["linkedin_notification"]
    if "security alert" in body_low and not has_forward:
        return True, ["security_alert"]

    # --- email clipped pelo cliente de mail (Gmail) ---
    if raw_html and ("[message clipped]" in raw_html.lower() or "view entire message" in raw_html.lower()):
        return True, ["email_clipped"]

    looks_like_substack_notification = (
        "substack" in sender_domain
        or "substack" in original_sender_raw
        or "substack" in body_low
    )
    if looks_like_substack_notification:
        if original_subject.startswith("live video with") or "watch the live video" in body_low or "open.substack.com/live-stream" in body_low:
            return True, ["substack_live_notification"]
        if " recommended " in f" {original_subject} " and ("subscribe to" in body_low or "explore what" in body_low):
            return True, ["substack_recommendation_notification"]
        # Substack "Your Weekly Stack" — digest curado sem links de artigos reais
        if "your weekly stack" in body_low:
            return True, ["substack_weekly_stack"]

    return False, notes


def _make_article(
    context: dict[str, Any],
    meta: dict[str, Any],
    title: str | None,
    text: str | None,
    link: str | None,
    family: str,
    confidence: float,
    notes: list[str],
) -> ParsedArticle:
    published_date, date_source = choose_published_date(context, text=text, link=link)
    return ParsedArticle(
        title=clean_heading(title or ""),
        text=trim_footer(text or ""),
        source_link=unwrap(link),
        published_date=published_date,
        published_date_source=date_source,
        confidence=confidence,
        notes=notes,
        family=family,
    )


def extract_tldr(forward_body: str, context: dict[str, Any], meta: dict[str, Any]) -> list[ParsedArticle]:
    subject_hint = _resolved_subject_hint(context, meta)
    articles: list[ParsedArticle] = []
    for match in TLDR_ITEM_RE.finditer(forward_body):
        title = clean_text(match.group("title"))
        if is_boilerplate(title) or len(title) < 8:
            continue
        body = clean_text(match.group("body"))
        notes: list[str] = []
        if "sponsor" in title.lower() or "together with" in body.lower():
            notes.append("sponsor_detected")
        articles.append(_make_article(context, meta, title, body, match.group("link"), "tldr", 0.90, notes))
    if articles:
        return articles
    link_articles = _extract_link_digest_items(context, meta, "tldr", max_items=5)
    if len(link_articles) >= 2:
        return link_articles
    return [_make_article(context, meta, subject_hint or meta.get("original_subject"), forward_body, None, "tldr", 0.55, ["tldr_fallback"])]


def find_best_substack_title(forward_body: str, context: dict[str, Any], meta: dict[str, Any]) -> tuple[str | None, str | None]:
    original_subject = _resolved_subject_hint(context, meta)
    best_link = _best_link_candidate(context, meta, "substack", original_subject)
    if best_link:
        return best_link["title"], best_link["url"]
    candidates: list[tuple[int, str, str]] = []
    for match in TITLE_LINK_RE.finditer(forward_body):
        title = clean_text(match.group("title"))
        link = match.group("link")
        if is_boilerplate(title) or len(title) < 4:
            continue
        lower = title.lower()
        score = 0
        if original_subject and title == original_subject:
            score += 100
        if original_subject and original_subject.lower() in lower:
            score += 70
        if any(hint in lower for hint in PUBLICATION_NAME_HINTS):
            score -= 20
        if "play_audio=true" in link or "#play" in link or "podcast" in lower:
            score -= 25
        if lower == "preview":
            score -= 30
        if "publication_id=" in link or "post_id=" in link:
            score += 10
        candidates.append((score, title, link))
    if candidates:
        candidates.sort(key=lambda row: row[0], reverse=True)
        best = candidates[0]
        if best[0] > -5:
            return best[1], best[2]
    return (original_subject or None), None


def extract_substack(forward_body: str, context: dict[str, Any], meta: dict[str, Any]) -> list[ParsedArticle]:
    subject_hint = _resolved_subject_hint(context, meta)
    title, link = find_best_substack_title(forward_body, context, meta)
    body = trim_footer(forward_body)
    if link and title:
        anchor = f"{title}<{link}>"
        pos = body.find(anchor)
        if pos >= 0:
            body = body[pos + len(anchor):].strip()
    if "cross-posted a post from" in forward_body.lower():
        match = TITLE_LINK_RE.search(body)
        if match:
            maybe_title = clean_text(match.group("title"))
            looks_like_author = maybe_title.count(" ") < 2 and maybe_title.lower() != subject_hint.lower()
            if maybe_title and not looks_like_author and not any(hint in maybe_title.lower() for hint in PUBLICATION_NAME_HINTS):
                title = maybe_title
                link = match.group("link")
                body = body[match.end():].strip()
    # Não activar digest se o post é paywalled (post único com "keep reading")
    body_low = forward_body.lower()
    is_paywalled = (
        "keep reading with a" in body_low
        or "subscribe to" in body_low and "to keep reading" in body_low
        or "this post is for paid subscribers" in body_low
    )
    if not is_paywalled:
        digest_articles = _extract_link_digest_items(context, meta, "substack", max_items=6)
        if len(digest_articles) >= 3:
            return digest_articles
    article = _make_article(context, meta, title or subject_hint, body, link, "substack", 0.87, [])
    if is_paywalled:
        article.source_link_final = False
        if "paywall" not in article.notes:
            article.notes.append("paywall")
    return [article]


def extract_mailchimp(forward_body: str, context: dict[str, Any], meta: dict[str, Any]) -> list[ParsedArticle]:
    link = ""
    for row in context.get("links", []):
        url = row.get("url_unwrapped") or row.get("url_original") or ""
        if url and ("technologyreview" in url or "list-manage.com/track/click" in url):
            link = str(url)
            break
    return [_make_article(context, meta, meta.get("original_subject"), trim_footer(forward_body), link, "mailchimp", 0.83, [])]


def extract_tds(forward_body: str, context: dict[str, Any], meta: dict[str, Any]) -> list[ParsedArticle]:
    title = meta.get("original_subject")
    title_low = clean_text(title or "").lower()
    link = ""
    for row in context.get("links", []):
        url = row.get("url_unwrapped") or row.get("url_original") or ""
        anchor = clean_text(row.get("anchor_text") or "").lower()
        if not url:
            continue
        low = str(url).lower()
        if "unsubscribe" in low or "view in browser" in anchor or "manage subscriptions" in low:
            continue
        if title_low and anchor == title_low:
            link = str(url)
            break
        if "towardsdatascience.com" in low or "newsletter.towardsdatascience.com" in low or "towards data science" in anchor:
            link = str(url)
            break
    notes: list[str] = []
    if not link:
        notes.append("tds_link_fallback")
    confidence = 0.82 if link else 0.72
    return [_make_article(context, meta, title, trim_footer(forward_body), link, "tds", confidence, notes)]


def extract_numbered_beehiiv(forward_body: str, context: dict[str, Any], meta: dict[str, Any], family: str) -> list[ParsedArticle]:
    articles: list[ParsedArticle] = []
    for match in NUMBERED_ITEM_RE.finditer(forward_body):
        title = clean_text(match.group("title"))
        body = clean_text(match.group("body"))
        if len(title) < 8 or is_boilerplate(title):
            continue
        link_match = URL_IN_ANGLE_RE.search(body)
        link = link_match.group(1) if link_match else None
        notes: list[str] = []
        if "sponsor" in title.lower() or "in partnership with" in body.lower() or "together with" in body.lower():
            notes.append("sponsor_detected")
        articles.append(_make_article(context, meta, title, body, link, family, 0.89, notes))
    return articles


_ARROW_CHARS = frozenset({"→", "->", "»", "▶"})


def _extract_beehiiv_paragraph_items(
    raw_html: str,
    context: dict[str, Any],
    meta: dict[str, Any],
    family: str,
) -> list[ParsedArticle]:
    """Extrai artigos do padrão 'um <p> por artigo' usado em digests beehiiv/neuron.

    Padrão detectado:
      [emoji] <b><i>Título completo</i></b> [→] descrição

    O título é tudo o que está antes do separador '→' no texto do parágrafo,
    com o prefixo de emoji/símbolo removido. Isto captura tanto o texto das
    âncoras como o texto bold adjacente fora delas.

    Discriminador chave: presença de '→' no texto do parágrafo — ausente nos
    parágrafos editoriais e nos blocos de Thought Loop.
    """
    if not raw_html:
        return []

    soup = BeautifulSoup(raw_html, "html.parser")
    subject_hint = _resolved_subject_hint(context, meta)
    articles: list[ParsedArticle] = []
    seen_urls: set[str] = set()

    for p in soup.find_all("p"):
        p_text = p.get_text(" ", strip=True)

        # Parágrafo deve conter seta → como separador título/descrição
        arrow_pos = -1
        for arrow in _ARROW_CHARS:
            pos = p_text.find(arrow)
            if pos > 0 and (arrow_pos < 0 or pos < arrow_pos):
                arrow_pos = pos
        if arrow_pos < 0:
            continue

        # Deve ter pelo menos um link HTTP
        links = [a for a in p.find_all("a", href=True) if a.get("href", "").startswith("http")]
        if not links:
            continue

        first_url = links[0].get("href", "")

        # Título = tudo antes do → (strip de prefixo emoji/não-alfanumérico)
        title_raw = p_text[:arrow_pos].strip()
        # Remove prefixo de emoji e símbolos não-alfanuméricos
        title = re.sub(r"^[^\w\"'(]+", "", title_raw).strip()
        title = clean_heading(title)

        # Descrição = tudo depois do →
        desc = clean_text(p_text[arrow_pos + 1:].strip())
        if not desc:
            desc = clean_text(p_text)

        # Filtros de ruído
        if len(title) < 8 or is_boilerplate(title):
            continue
        if _looks_like_link_title_noise(title, subject_hint=subject_hint):
            continue
        if any(m in title.lower() for m in FAMILY_TITLE_NOISE_CONTAINS.get(family, ())):
            continue
        if any(m in first_url.lower() for m in FAMILY_URL_NOISE_MARKERS.get(family, ())):
            continue
        if any(m in first_url.lower() for m in URL_NOISE_MARKERS):
            continue

        # Deduplicação por URL
        if first_url in seen_urls:
            continue
        seen_urls.add(first_url)

        articles.append(
            _make_article(context, meta, title, desc, first_url, family, 0.82, ["para_digest"])
        )

    return articles


def extract_beehiiv(forward_body: str, context: dict[str, Any], meta: dict[str, Any], family: str, *, raw_html: str = "") -> list[ParsedArticle]:
    # Tenta primeiro o padrão estrutural HTML (um parágrafo por artigo com seta →)
    if raw_html:
        para_articles = _extract_beehiiv_paragraph_items(raw_html, context, meta, family)
        if len(para_articles) >= 3:
            return para_articles

    subject_hint = _resolved_subject_hint(context, meta)
    articles = extract_numbered_beehiiv(forward_body, context, meta, family)
    if articles:
        return articles
    matches = []
    for match in TITLE_LINK_RE.finditer(forward_body):
        title = clean_text(match.group("title"))
        if is_boilerplate(title) or len(title) < 8:
            continue
        if _looks_like_link_title_noise(title, subject_hint=subject_hint):
            continue
        lower = title.lower()
        if any(marker in lower for marker in FAMILY_TITLE_NOISE_CONTAINS.get(family, ())):
            continue
        matches.append(match)
    if matches:
        title = clean_text(matches[0].group("title"))
        link = matches[0].group("link")
        body = forward_body[matches[0].end():]
        for marker in ("In partnership with", "Powered by beehiiv", "Terms of Service", "Copyright 2026", "Copyright 2025"):
            pos = body.find(marker)
            if pos > 0:
                body = body[:pos]
        notes = ["single_featured_item"]
        if "in today's email" in forward_body.lower() or "here's what happened in ai today" in forward_body.lower():
            notes.append("digest_not_fully_split")
        return [_make_article(context, meta, title, body, link, family, 0.74, notes)]
    link_articles = _extract_link_digest_items(context, meta, family, max_items=5)
    if len(link_articles) >= 2:
        return link_articles
    fallback_notes = [f"{family}_fallback"]
    if family in {"beehiiv", "neuron"} and (
        "in today's email" in forward_body.lower() or "here's what happened in ai today" in forward_body.lower()
    ):
        fallback_notes.append("digest_not_fully_split")
    return [_make_article(context, meta, subject_hint or meta.get("original_subject"), forward_body, None, family, 0.60, fallback_notes)]


def extract_every(forward_body: str, context: dict[str, Any], meta: dict[str, Any]) -> list[ParsedArticle]:
    articles: list[ParsedArticle] = []
    subject_hint = _resolved_subject_hint(context, meta)
    title = subject_hint or meta.get("original_subject")
    main_link = ""
    main_body = forward_body
    best_link = _best_link_candidate(context, meta, "every", title or "")
    if best_link:
        title = best_link["title"]
        main_link = best_link["url"]
    for marker in ("Knowledge base", "You received this email because", "No longer interested", "221 Canal St"):
        pos = main_body.find(marker)
        if pos > 0:
            main_body = main_body[:pos]
            break
    articles.append(_make_article(context, meta, title, main_body, main_link, "every", 0.80, []))
    kb_pos = forward_body.find("Knowledge base")
    if kb_pos >= 0:
        kb_body = forward_body[kb_pos + len("Knowledge base"):]
        for match in QUOTED_LINK_RE.finditer(kb_body):
            kb_title = clean_text(match.group("title")).strip('"')
            if len(kb_title) < 8 or is_boilerplate(kb_title):
                continue
            next_match = QUOTED_LINK_RE.search(kb_body, match.end())
            segment = kb_body[match.end(): next_match.start() if next_match else len(kb_body)]
            articles.append(_make_article(context, meta, kb_title, segment, match.group("link"), "every", 0.77, ["knowledge_base_item"]))
    return articles


def extract_kit(forward_body: str, context: dict[str, Any], meta: dict[str, Any]) -> list[ParsedArticle]:
    subject_hint = _resolved_subject_hint(context, meta)
    issue_titles = _extract_kit_issue_titles(forward_body)
    if issue_titles:
        primary_title = max(issue_titles, key=lambda item: _title_match_score(item, subject_hint)) if subject_hint else issue_titles[0]
        articles: list[ParsedArticle] = []
        seen_titles: set[str] = set()
        for title in issue_titles:
            key = title.casefold()
            if key in seen_titles:
                continue
            seen_titles.add(key)
            best_link = _best_link_candidate(context, meta, "kit", title)
            body = forward_body if title == primary_title else (best_link["body"] if best_link and best_link.get("body") else title)
            notes: list[str] = ["kit_issue_list"]
            if title != primary_title:
                notes.append("summary_only")
            articles.append(
                _make_article(
                    context,
                    meta,
                    title,
                    body,
                    best_link["url"] if best_link else None,
                    "kit",
                    0.82 if title == primary_title else 0.72,
                    notes,
                )
            )
        if articles:
            return articles
    best_link = _best_link_candidate(context, meta, "kit", subject_hint)
    if best_link:
        return [_make_article(context, meta, best_link["title"], forward_body, best_link["url"], "kit", 0.70, ["kit_link_fallback"])]
    return [_make_article(context, meta, subject_hint or meta.get("original_subject"), forward_body, None, "kit", 0.55, ["kit_fallback"])]


def extract_curated(forward_body: str, context: dict[str, Any], meta: dict[str, Any]) -> list[ParsedArticle]:
    articles: list[ParsedArticle] = []
    sponsor_match = re.search(
        r"(?ms)^Sponsor\s*\n\n(?P<body>.*?)(?=^In the News\s*$|^_{10,}\s*$|^Artificial Intelligence Weekly\s*$|\Z)",
        forward_body,
    )
    if sponsor_match:
        sponsor_body = clean_text(sponsor_match.group("body"))
        lines = [clean_text(line) for line in sponsor_body.splitlines() if clean_text(line)]
        sponsor_title = lines[0] if lines else "Sponsor"
        sponsor_link = find_first_link(sponsor_body)
        articles.append(_make_article(context, meta, sponsor_title, sponsor_body, sponsor_link, "curated", 0.82, ["sponsor_detected"]))

    news_body = forward_body
    news_pos = forward_body.find("In the News")
    if news_pos >= 0:
        news_body = forward_body[news_pos + len("In the News"):]
    if sponsor_match and sponsor_match.end() < len(news_body):
        news_body = news_body[sponsor_match.end():]
    for match in CURATED_SECTION_RE.finditer(news_body):
        title = clean_text(match.group("title"))
        body = clean_text(match.group("body"))
        if len(title) < 8 or is_boilerplate(title):
            continue
        link = find_first_link(body)
        articles.append(_make_article(context, meta, title, body, link, "curated", 0.88, []))
    if articles:
        return articles
    return [_make_article(context, meta, meta.get("original_subject"), forward_body, find_first_link(forward_body), "curated", 0.60, ["curated_fallback"])]


def extract_generic(forward_body: str, context: dict[str, Any], meta: dict[str, Any]) -> list[ParsedArticle]:
    return [_make_article(context, meta, meta.get("original_subject"), forward_body, None, "generic", 0.45, ["generic_fallback"])]


# ---------------------------------------------------------------------------
# Image enrichment (ponto 6.4) — busca og:image/twitter:image do source link
# ---------------------------------------------------------------------------

_OG_IMAGE_PROPS = ("og:image", "og:image:secure_url", "twitter:image")
_FETCH_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; plutonia-bot/1.0; +https://plutoanalytics.com)",
    "Accept": "text/html,application/xhtml+xml",
}


def fetch_og_images(url: str, *, timeout: float = 6.0) -> list[str]:
    """Devolve URLs de imagem OG/Twitter encontradas no source link. Falha silenciosamente."""
    if not url or not url.startswith(("http://", "https://")):
        return []
    try:
        req = urllib.request.Request(url, headers=_FETCH_HEADERS)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            charset = resp.headers.get_content_charset("utf-8") or "utf-8"
            html = resp.read(300_000).decode(charset, errors="replace")
    except Exception:
        return []
    soup = BeautifulSoup(html, "html.parser")
    images: list[str] = []
    seen: set[str] = set()
    for prop in _OG_IMAGE_PROPS:
        meta = soup.find("meta", property=prop) or soup.find("meta", attrs={"name": prop})
        if meta:
            img = str(meta.get("content", "")).strip()
            if img and img not in seen:
                seen.add(img)
                images.append(img)
    return images


def enrich_with_images(result: EmailParseResult, *, timeout: float = 6.0) -> None:
    """Preenche article.images in-place para todos os artigos com source_link."""
    for article in result.articles:
        if article.source_link and not article.images:
            article.images = fetch_og_images(article.source_link, timeout=timeout)


def _build_context(raw_html: str, email_meta: dict[str, Any]) -> dict[str, Any]:
    html_text = html_to_text(raw_html)
    links = dedupe_links(extract_html_links(raw_html) + extract_text_links(html_text))
    sender_email = clean_text(email_meta.get("sender_email") or "").lower()
    original_sender_email = clean_text(email_meta.get("original_sender_email") or "").lower()
    original_sender_raw = clean_text(email_meta.get("original_sender_name") or email_meta.get("original_sender_email") or "")
    sent_iso = clean_text(email_meta.get("original_sent_at") or email_meta.get("received_at") or "")
    subject = clean_text(email_meta.get("subject") or "")
    return {
        "headers_parsed": {
            "sender_email": sender_email,
            "original_sender_email": original_sender_email,
            "original_sender_raw": original_sender_raw,
            "subject": subject,
            "date": sent_iso,
        },
        "links": links,
        "bodies": {
            "html_text": html_text,
            "best_text": html_text,
        },
        "signals": build_signals(html_text, sender_email, original_sender_email, subject, links, sent_iso),
    }


def parse_email_articles(raw_html: str, email_meta: dict[str, Any] | None = None) -> EmailParseResult:
    meta_in = email_meta or {}
    context = _build_context(raw_html, meta_in)
    best_text = str(context.get("bodies", {}).get("best_text", ""))
    meta, forward_body = split_forward_meta(best_text, context)
    family = family_for(context, meta, forward_body)
    skipped, skip_notes = should_skip_email(context, meta, family, forward_body, raw_html=raw_html)

    # --- metadados do email (pontos 1-5) ---
    received_at = clean_text(meta_in.get("received_at") or "")
    sender_email = clean_text(meta_in.get("sender_email") or "").lower()
    is_forwarded = bool(context.get("signals", {}).get("forward_blocks"))
    original_sent_at = clean_text(meta.get("original_sent_raw") or meta_in.get("original_sent_at") or "")
    original_sender_email = clean_text(meta.get("original_sender_email") or "").lower()
    _sender_raw = clean_text(meta.get("original_sender_raw") or meta_in.get("original_sender_name") or "")
    original_sender_name = _sender_raw[:_sender_raw.index("<")].strip() if "<" in _sender_raw else _sender_raw
    original_subject = clean_text(meta.get("original_subject") or meta_in.get("subject") or "")

    _email_kwargs: dict[str, Any] = dict(
        received_at=received_at,
        sender_email=sender_email,
        is_forwarded=is_forwarded,
        original_sent_at=original_sent_at,
        original_sender_email=original_sender_email,
        original_sender_name=original_sender_name,
        original_subject=original_subject,
    )

    if skipped:
        return EmailParseResult([], family, True, skip_notes, best_text, context.get("links", []), **_email_kwargs)

    if family == "tldr":
        articles = extract_tldr(forward_body, context, meta)
    elif family == "substack":
        articles = extract_substack(forward_body, context, meta)
    elif family == "mailchimp":
        articles = extract_mailchimp(forward_body, context, meta)
    elif family == "tds":
        articles = extract_tds(forward_body, context, meta)
    elif family == "beehiiv":
        articles = extract_beehiiv(forward_body, context, meta, family="beehiiv", raw_html=raw_html)
    elif family == "neuron":
        articles = extract_beehiiv(forward_body, context, meta, family="neuron", raw_html=raw_html)
    elif family == "every":
        articles = extract_every(forward_body, context, meta)
    elif family == "kit":
        articles = extract_kit(forward_body, context, meta)
    elif family == "curated":
        articles = extract_curated(forward_body, context, meta)
    else:
        articles = extract_generic(forward_body, context, meta)

    cleaned_articles = [article for article in articles if article.title or article.text]

    # Fix 3 — se o único artigo usa o subject como título, tenta extrair o H1/H2/H3 real do HTML
    if len(cleaned_articles) == 1 and original_subject:
        article = cleaned_articles[0]
        if _strip_accents(article.title.casefold()) == _strip_accents(original_subject.casefold()):
            heading = _extract_html_heading(raw_html, skip=original_subject)
            if heading and not _is_section_header(heading):
                article.title = heading

    return EmailParseResult(cleaned_articles, family, False, [], best_text, context.get("links", []), **_email_kwargs)
