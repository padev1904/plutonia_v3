from __future__ import annotations

import base64
import json
import os
import re
import unicodedata
from dataclasses import dataclass
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
    r"(?ims)^(?:_{2,}\s*)?(?:from|de):\s*(?P<from>.+?)\s*$.*?"
    r"^(?:sent|date|enviado|data):\s*(?P<sent>.+?)\s*$.*?"
    r"^(?:to|para):\s*(?P<to>.+?)\s*$.*?"
    r"^(?:subject|assunto):\s*(?P<subject>.+?)\s*$"
)
TITLE_LINK_RE = re.compile(r"(?m)^(?P<title>[^\n<]{4,220}?)\s*<(?P<link>https?://[^>\n]+)>")
TLDR_ITEM_RE = re.compile(
    r"(?ms)^(?P<title>[^\n<]{8,180}?)\s+<(?P<link>https?://[^>\n]+)>\s*\n\n"
    r"(?P<body>.*?)(?=\n\n[^\n<]{8,180}?\s+<https?://|\Z)"
)
NUMBERED_ITEM_RE = re.compile(r"(?ms)^\s*(?P<num>\d+)\.\s+(?P<title>[^:\n]{8,220}):\s*(?P<body>.*?)(?=^\s*\d+\.\s+|\Z)")
QUOTED_LINK_RE = re.compile(r'(?m)^"?(?P<title>[A-Z\[][^\n<]{6,220}?)"?\s*<(?P<link>https?://[^>\n]+)>')
CURATED_SECTION_RE = re.compile(r"(?ms)^_{10,}\s*\n(?P<title>[^\n]{8,220})\n(?P<body>.*?)(?=^_{10,}\s*$|\Z)")

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


@dataclass
class EmailParseResult:
    articles: list[ParsedArticle]
    family: str
    skipped: bool
    skip_notes: list[str]
    best_text: str
    links: list[dict[str, Any]]


def _strip_accents(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def clean_text(text: str | None) -> str:
    value = unescape((text or "").replace("\r\n", "\n").replace("\r", "\n"))
    value = value.replace("\u00a0", " ")
    value = re.sub(r"[ \t]+", " ", value)
    value = re.sub(r"\n{3,}", "\n\n", value)
    return value.strip()


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
    return value


def html_to_text(html: str) -> str:
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    for br in soup.find_all("br"):
        br.replace_with("\n")
    return clean_text(soup.get_text("\n", strip=False))


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
    for line in lines:
        if is_boilerplate(line):
            break
        if re.match(r"^\[[^\]]+\]$", line):
            continue
        out.append(line)
    return clean_text("\n".join(out))


def should_skip_email(context: dict[str, Any], meta: dict[str, Any], family: str, forward_body: str) -> tuple[bool, list[str]]:
    notes: list[str] = []
    top_sender = (meta.get("top_sender_email") or "").lower()
    original_sender = (meta.get("original_sender_email") or "").lower()
    original_sender_raw = clean_text(meta.get("original_sender_raw") or "").lower()
    original_subject = clean_text(meta.get("original_subject") or "").lower()
    has_forward = bool(context.get("signals", {}).get("forward_blocks"))
    sender_domain = (original_sender or top_sender).split("@")[-1] if (original_sender or top_sender) else ""

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
        title=clean_text(title or ""),
        text=trim_footer(text or ""),
        source_link=unwrap(link),
        published_date=published_date,
        published_date_source=date_source,
        confidence=confidence,
        notes=notes,
        family=family,
    )


def extract_tldr(forward_body: str, context: dict[str, Any], meta: dict[str, Any]) -> list[ParsedArticle]:
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
    return [_make_article(context, meta, meta.get("original_subject"), forward_body, None, "tldr", 0.55, ["tldr_fallback"])]


def find_best_substack_title(forward_body: str, meta: dict[str, Any]) -> tuple[str | None, str | None]:
    original_subject = clean_text(meta.get("original_subject") or "")
    if original_subject.lower().startswith("fw:"):
        original_subject = clean_text(original_subject[3:])
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
    title, link = find_best_substack_title(forward_body, meta)
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
            looks_like_author = maybe_title.count(" ") < 2 and maybe_title.lower() != clean_text(meta.get("original_subject") or "").lower()
            if maybe_title and not looks_like_author and not any(hint in maybe_title.lower() for hint in PUBLICATION_NAME_HINTS):
                title = maybe_title
                link = match.group("link")
                body = body[match.end():].strip()
    return [_make_article(context, meta, title, body, link, "substack", 0.87, [])]


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


def extract_beehiiv(forward_body: str, context: dict[str, Any], meta: dict[str, Any], family: str) -> list[ParsedArticle]:
    articles = extract_numbered_beehiiv(forward_body, context, meta, family)
    if articles:
        return articles
    matches = []
    for match in TITLE_LINK_RE.finditer(forward_body):
        title = clean_text(match.group("title"))
        if is_boilerplate(title) or len(title) < 8:
            continue
        lower = title.lower()
        if lower.startswith("march ") or lower.startswith("welcome") or lower in {"sign up", "advertise", "read online", "listen online"}:
            continue
        if lower.startswith("in partnership with"):
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
    fallback_notes = [f"{family}_fallback"]
    if family in {"beehiiv", "neuron"} and (
        "in today's email" in forward_body.lower() or "here's what happened in ai today" in forward_body.lower()
    ):
        fallback_notes.append("digest_not_fully_split")
    return [_make_article(context, meta, meta.get("original_subject"), forward_body, None, family, 0.60, fallback_notes)]


def extract_every(forward_body: str, context: dict[str, Any], meta: dict[str, Any]) -> list[ParsedArticle]:
    articles: list[ParsedArticle] = []
    title = meta.get("original_subject")
    main_link = ""
    main_body = forward_body
    if title:
        escaped_title = re.escape(str(title))
        match = re.search(rf"(?m)^{escaped_title}\s*<(?P<link>https?://[^>\n]+)>", forward_body)
        if match:
            main_link = match.group("link")
            main_body = forward_body[match.end():]
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
    articles: list[ParsedArticle] = []
    title_links = []
    for match in TITLE_LINK_RE.finditer(forward_body):
        title = clean_text(match.group("title")).lstrip("*- ")
        lower = title.lower()
        if len(title) < 8 or is_boilerplate(title):
            continue
        if lower in {"today's issue", "in today's newsletter:"}:
            continue
        title_links.append(match)
    seen_titles: set[str] = set()
    idx = 1
    for match in title_links:
        title = clean_text(match.group("title")).lstrip("*- ")
        lower = title.lower()
        if any(lower.startswith(prefix) for prefix in ("master full-stack", "advertise to 950k+", "partner with us")):
            continue
        if lower in seen_titles:
            continue
        start = match.end()
        next_match = next((row for row in title_links if row.start() > start), None)
        body = forward_body[start: next_match.start() if next_match else len(forward_body)]
        if "open-source" in body[:40].lower() or "hands-on" in body[:40].lower():
            body = re.sub(r"(?mi)^(open-source|hands-on|tutorial|research)\s*$", "", body).strip()
        seen_titles.add(lower)
        confidence = 0.84 if idx <= 3 else 0.70
        notes: list[str] = []
        if idx > 3:
            notes.append("late_section_item")
        articles.append(_make_article(context, meta, title, body, match.group("link"), "kit", confidence, notes))
        idx += 1
    if articles:
        return articles
    return [_make_article(context, meta, meta.get("original_subject"), forward_body, None, "kit", 0.55, ["kit_fallback"])]


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
    skipped, skip_notes = should_skip_email(context, meta, family, forward_body)
    if skipped:
        return EmailParseResult([], family, True, skip_notes, best_text, context.get("links", []))
    if family == "tldr":
        articles = extract_tldr(forward_body, context, meta)
    elif family == "substack":
        articles = extract_substack(forward_body, context, meta)
    elif family == "mailchimp":
        articles = extract_mailchimp(forward_body, context, meta)
    elif family == "tds":
        articles = extract_tds(forward_body, context, meta)
    elif family == "beehiiv":
        articles = extract_beehiiv(forward_body, context, meta, family="beehiiv")
    elif family == "neuron":
        articles = extract_beehiiv(forward_body, context, meta, family="neuron")
    elif family == "every":
        articles = extract_every(forward_body, context, meta)
    elif family == "kit":
        articles = extract_kit(forward_body, context, meta)
    elif family == "curated":
        articles = extract_curated(forward_body, context, meta)
    else:
        articles = extract_generic(forward_body, context, meta)
    cleaned_articles = [article for article in articles if article.title or article.text]
    return EmailParseResult(cleaned_articles, family, False, [], best_text, context.get("links", []))
