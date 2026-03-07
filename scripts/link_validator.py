#!/usr/bin/env python3
"""Early Source Link Validation (migrated from local agent).

Validates source_link semantically before Telegram triage notification.
If invalid/uncertain, searches for better candidates via SearxNG + LLM.

Entry point: run_link_validation(cfg, article_data) -> dict
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from typing import Any, Optional

import requests
from bs4 import BeautifulSoup

LOG = logging.getLogger("link_validator")

# --- Thresholds (configurable via env) ---
LINK_VALID_THRESHOLD = float(os.getenv("LINK_VALID_THRESHOLD", "0.75"))
LINK_UNCERTAIN_THRESHOLD = float(os.getenv("LINK_UNCERTAIN_THRESHOLD", "0.45"))
LINK_MAX_SEARCH_CANDIDATES = int(os.getenv("LINK_MAX_SEARCH_CANDIDATES", "5"))
LINK_FETCH_TIMEOUT = int(os.getenv("LINK_FETCH_TIMEOUT", "10"))
LINK_FETCH_MAX_CHARS = int(os.getenv("LINK_FETCH_MAX_CHARS", "3000"))
LINK_VALIDATION_TIMEOUT = int(os.getenv("LINK_VALIDATION_TIMEOUT", "60"))

_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)

LINK_VALIDATION_PROMPT = """You are a link validation assistant. Your task is to determine whether a given URL is likely the original source for a piece of email content.

You will receive:
- The email subject
- A text preview from the email body
- The candidate URL
- The page content fetched from that URL (may be empty if fetch failed)

Analyze whether the URL corresponds to the same informative content described in the email. Consider:
- Do the topics match?
- Are the key entities (people, companies, products) the same?
- Is this likely the original article/resource the email is referencing?

If page content is empty, base your judgment on the URL structure and domain alone (lower confidence).

Respond ONLY with raw JSON (no markdown, no ```json). Use this exact structure:
{{
  "status": "valid|invalid|uncertain",
  "confidence": 0.0,
  "reason": "short explanation in Portuguese",
  "matched_signals": ["signal1", "signal2"],
  "contradictions": ["contradiction1"]
}}

Confidence ranges:
- 0.75-1.0: Strong match, clearly the right source
- 0.45-0.74: Partial match, some signals align but not conclusive
- 0.0-0.44: Weak or no match

Email subject: {email_subject}
Text preview: {text_preview}
Candidate URL: {candidate_url}
Page content: {page_content}
"""


def _classify_confidence(confidence: float) -> str:
    if confidence >= LINK_VALID_THRESHOLD:
        return "valid"
    if confidence >= LINK_UNCERTAIN_THRESHOLD:
        return "uncertain"
    return "invalid"


def _fetch_page_text(url: str, max_chars: int = 3000, timeout: int = 10) -> str:
    if not url or url == "NEEDS_USER_LINK" or "://" not in url:
        return ""
    try:
        resp = requests.get(
            url,
            timeout=timeout,
            headers={"User-Agent": _BROWSER_UA},
            allow_redirects=True,
        )
        if resp.status_code != 200:
            return ""
        soup = BeautifulSoup(resp.text[:max_chars * 3], "lxml")
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        text = " ".join(soup.get_text(" ", strip=True).split())
        return text[:max_chars]
    except Exception as e:
        LOG.debug("fetch_page_text error for %s: %s", url, e)
        return ""


def _call_ollama_for_validation(
    ollama_url: str,
    model: str,
    email_subject: str,
    text_preview: str,
    candidate_url: str,
    page_content: str,
    timeout: int = 120,
) -> Optional[dict]:
    """Use LLM to semantically validate a link against email content.

    NOTE: Calls Ollama directly (not via _llm_generate) intentionally.
    Link validation is a lightweight, fast task that should always use
    Ollama even when the main pipeline uses OpenClaw. This is consistent
    with the local agent's original behavior.
    """
    prompt = LINK_VALIDATION_PROMPT.format(
        email_subject=email_subject,
        text_preview=text_preview[:LINK_FETCH_MAX_CHARS],
        candidate_url=candidate_url,
        page_content=page_content[:LINK_FETCH_MAX_CHARS],
    )
    try:
        resp = requests.post(
            f"{ollama_url.rstrip('/')}/api/generate",
            json={"model": model, "prompt": prompt, "stream": False, "format": "json"},
            timeout=timeout,
        )
        if resp.status_code != 200:
            return None

        raw = resp.json().get("response", "{}")
        raw = re.sub(r"<think>.*?</think>", "", raw, flags=re.DOTALL).strip()
        result = json.loads(raw)

        status = result.get("status", "uncertain")
        if status not in ("valid", "invalid", "uncertain"):
            status = "uncertain"
        result["status"] = status

        conf = result.get("confidence", 0.0)
        try:
            conf = max(0.0, min(1.0, float(conf)))
        except (ValueError, TypeError):
            conf = 0.0
        result["confidence"] = conf
        result.setdefault("reason", "")
        result.setdefault("matched_signals", [])
        result.setdefault("contradictions", [])
        return result
    except Exception as e:
        LOG.warning("LLM link validation failed: %s", e)
        return None


def _search_candidates(
    searxng_url: str,
    email_subject: str,
    text_preview: str,
) -> list[dict[str, str]]:
    """Search for alternative links via SearxNG."""
    if not searxng_url:
        return []

    queries = []
    if email_subject:
        queries.append(email_subject.strip())
    first_words = " ".join(text_preview.split()[:10])
    if first_words and email_subject:
        combined = f"{email_subject} {first_words}".strip()
        if combined not in queries:
            queries.append(combined)

    seen_urls: set[str] = set()
    candidates: list[dict[str, str]] = []

    for query in queries[:3]:
        try:
            resp = requests.get(
                f"{searxng_url.rstrip('/')}/search",
                params={"q": query, "format": "json", "engines": "google,bing,duckduckgo"},
                timeout=15,
            )
            if resp.status_code != 200:
                continue
            for r in resp.json().get("results", [])[:LINK_MAX_SEARCH_CANDIDATES]:
                url = r.get("url", "")
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    candidates.append({
                        "url": url,
                        "title": r.get("title", ""),
                        "content": r.get("content", ""),
                    })
        except Exception as e:
            LOG.warning("SearxNG search failed: %s", e)

        if len(candidates) >= LINK_MAX_SEARCH_CANDIDATES:
            break

    return candidates[:LINK_MAX_SEARCH_CANDIDATES]


def run_link_validation(
    ollama_url: str,
    ollama_model: str,
    searxng_url: str,
    email_subject: str,
    text_preview: str,
    source_link: str,
) -> dict[str, Any]:
    """Run full link validation flow.

    Returns dict with:
        final_link, status, confidence, reason, origin, candidates
    """
    start_time = time.time()
    candidates_log: list[dict] = []

    # --- Step 1: Validate existing link ---
    if not source_link or source_link == "NEEDS_USER_LINK" or "://" not in source_link:
        initial = {
            "status": "invalid",
            "confidence": 0.0,
            "reason": "Sem link válido para validar.",
            "origin": "email",
        }
    else:
        page_content = _fetch_page_text(source_link, LINK_FETCH_MAX_CHARS, LINK_FETCH_TIMEOUT)
        llm_result = _call_ollama_for_validation(
            ollama_url, ollama_model,
            email_subject, text_preview, source_link, page_content,
        )
        if llm_result:
            effective = _classify_confidence(llm_result["confidence"])
            llm_result["status"] = effective
            llm_result["origin"] = "email"
            initial = llm_result
        else:
            initial = {
                "status": "uncertain",
                "confidence": 0.5,
                "reason": "Validação LLM falhou, resultado incerto por defeito.",
                "origin": "email",
            }

    elapsed = time.time() - start_time
    LOG.info(
        "Link validation initial: status=%s conf=%.2f (%.1fs)",
        initial["status"], initial["confidence"], elapsed,
    )

    if initial["status"] == "valid":
        return {
            "final_link": source_link,
            "status": "valid",
            "confidence": initial["confidence"],
            "reason": initial.get("reason", "Link original validado."),
            "origin": "email",
            "candidates": [],
        }

    # --- Step 2: Search alternatives ---
    if time.time() - start_time > LINK_VALIDATION_TIMEOUT:
        return _fallback(source_link, initial, "(timeout)")

    candidates = _search_candidates(searxng_url, email_subject, text_preview)
    if source_link and source_link != "NEEDS_USER_LINK":
        candidates = [c for c in candidates if c["url"] != source_link]
    candidates_log = candidates.copy()

    if not candidates:
        return _fallback(source_link, initial, candidates=candidates_log)

    # --- Step 3: Evaluate candidates ---
    best = None
    best_confidence = 0.0

    for candidate in candidates:
        url = candidate["url"]
        page_content = _fetch_page_text(url, LINK_FETCH_MAX_CHARS, LINK_FETCH_TIMEOUT)
        llm_result = _call_ollama_for_validation(
            ollama_url, ollama_model,
            email_subject, text_preview, url, page_content,
        )
        if not llm_result:
            continue

        conf = llm_result["confidence"]
        effective = _classify_confidence(conf)

        if conf > best_confidence:
            best_confidence = conf
            best = {
                "url": url,
                "status": effective,
                "confidence": conf,
                "reason": llm_result.get("reason", ""),
            }

        if effective == "valid":
            break

    original_confidence = initial.get("confidence", 0.0)
    if best and best["confidence"] > original_confidence and best["confidence"] >= LINK_UNCERTAIN_THRESHOLD:
        return {
            "final_link": best["url"],
            "status": best["status"],
            "confidence": best["confidence"],
            "reason": f"Link corrigido por pesquisa: {best['reason']}",
            "origin": "search",
            "candidates": candidates_log,
        }

    return _fallback(source_link, initial, candidates=candidates_log)


def _fallback(
    source_link: str,
    validation: dict,
    suffix: str = "",
    candidates: list | None = None,
) -> dict[str, Any]:
    if source_link and source_link != "NEEDS_USER_LINK" and "://" in source_link:
        reason = validation.get("reason", "Validação inconclusiva.")
        if suffix:
            reason = f"{reason} {suffix}"
        return {
            "final_link": source_link,
            "status": validation.get("status", "uncertain"),
            "confidence": validation.get("confidence", 0.0),
            "reason": reason,
            "origin": "email",
            "candidates": candidates or [],
        }
    return {
        "final_link": "NEEDS_USER_LINK",
        "status": "invalid",
        "confidence": 0.0,
        "reason": "Sem link válido encontrado.",
        "origin": "email",
        "candidates": candidates or [],
    }
