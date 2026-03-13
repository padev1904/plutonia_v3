#!/usr/bin/env python3
"""Apply second-stage editorial decision for one preview article."""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from pathlib import Path

import requests

from process_newsletter import (
    Config,
    _assert_required_summary_model,
    _get_source_snapshot,
    _llm_generate,
    _normalize_article_body_text,
    _normalize_keywords,
    _normalize_summary_text,
    _safe_json_object,
)


LOG = logging.getLogger("review_content_decision")

REVISION_PROMPT = """You are revising a newsroom article draft.

Use editor instructions as strict requirements while preserving factual accuracy.
Use source text as primary evidence.

ARTICLE INPUT:
- title: {title}
- summary: {summary}
- article_body: {article_body}
- source_url: {source_url}
- section: {section}
- category: {category}
- subcategory: {subcategory}
- categories: {categories}

EDITOR INSTRUCTIONS:
{instructions}

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
- Keep neutral, factual journalistic tone.
- `summary`: 2-3 complete sentences, no truncation markers.
- `article_body`: 5-8 paragraphs separated by blank lines, no bullet lists.
- section/category/subcategory in English.
- categories: 3-10 concise keywords in English.
- No markdown, no explanation outside JSON.
"""


def _api_get(cfg: Config, path: str, params: dict) -> dict:
    headers = {"X-API-Key": cfg.agent_api_key}
    resp = requests.get(
        f"{cfg.portal_api_url.rstrip('/')}/{path.lstrip('/')}",
        headers=headers,
        params=params,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def _api_post(cfg: Config, path: str, payload: dict) -> dict:
    headers = {"Content-Type": "application/json", "X-API-Key": cfg.agent_api_key}
    resp = requests.post(
        f"{cfg.portal_api_url.rstrip('/')}/{path.lstrip('/')}",
        headers=headers,
        json=payload,
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()


def _notify_next_article_review(cfg: Config, newsletter_id: int) -> dict:
    email_meta: dict | None = None
    try:
        raw_payload = _api_get(cfg, f"newsletter/{int(newsletter_id)}/raw/", {})
        email_meta = {
            "subject": raw_payload.get("subject"),
            "sender_name": raw_payload.get("sender_name"),
            "sender_email": raw_payload.get("sender_email"),
            "original_sender_name": raw_payload.get("original_sender_name"),
            "original_sender_email": raw_payload.get("original_sender_email"),
            "original_sent_at": raw_payload.get("original_sent_at"),
            "original_sent_at_raw": raw_payload.get("original_sent_at_raw"),
            "received_at": raw_payload.get("received_at"),
        }
    except Exception as exc:
        LOG.warning("failed to resolve newsletter metadata newsletter_id=%s err=%s", newsletter_id, exc)

    try:
        from gmail_monitor import send_next_review_notification  # Lazy import to avoid hard dependency.

        return send_next_review_notification(
            cfg,
            int(newsletter_id),
            include_intro=True,
            email_meta=email_meta,
        )
    except Exception as exc:
        LOG.warning("failed to send next article review notification newsletter_id=%s err=%s", newsletter_id, exc)
        return {"sent": False, "reason": "notify_error", "error": str(exc)}


def _read_source_text(source_text: str, source_text_file: str, read_stdin: bool) -> str:
    if source_text:
        return source_text.strip()
    if source_text_file:
        return Path(source_text_file).read_text(encoding="utf-8").strip()
    if read_stdin:
        return sys.stdin.read().strip()
    return ""


def _fallback_revision_source_text(article: dict, *, max_chars: int) -> str:
    parts: list[str] = []
    for key in ("title", "summary", "article_body"):
        value = str(article.get(key, "")).strip()
        if value:
            parts.append(value)
    categories = [str(c).strip() for c in article.get("categories", []) if str(c).strip()]
    if categories:
        parts.append("Categories: " + ", ".join(categories))
    for key in ("section", "category", "subcategory"):
        value = str(article.get(key, "")).strip()
        if value:
            parts.append(f"{key}: {value}")
    combined = "\n\n".join(part for part in parts if part)
    return combined[:max_chars].strip()


def _clean_revision_source_text(text: str) -> str:
    cleaned = " ".join(str(text or "").split()).strip()
    if not cleaned:
        return ""
    cleaned = re.sub(r"\s+([,.;:!?])", r"\1", cleaned)
    return cleaned


def _revision_context_limits(max_chars: int) -> list[int]:
    cap = max(180, int(max_chars))
    desired = [min(cap, 3200), min(cap, 2200), min(cap, 1500), min(cap, 1000)]
    limits: list[int] = []
    for value in desired:
        if value >= 180 and value not in limits:
            limits.append(value)
    if not limits:
        limits.append(cap)
    return limits


def _build_revision_payload(cfg: Config, article: dict, instructions: str, source_text_override: str, max_source_chars: int) -> dict:
    source_url = str(article.get("original_url", "")).strip()
    source_text = source_text_override.strip()
    if not source_text and source_url:
        snapshot = _get_source_snapshot(source_url, max_source_chars, max(120, cfg.source_open_min_chars))
        source_text = str(snapshot.get("text", "")).strip()
    if len(source_text) < 300:
        fallback_text = _fallback_revision_source_text(article, max_chars=max_source_chars)
        if len(fallback_text) > len(source_text):
            source_text = fallback_text
    cleaned_source_text = _clean_revision_source_text(source_text)
    if len(cleaned_source_text) >= 180:
        source_text = cleaned_source_text
    source_text = source_text[:max_source_chars]

    if len(source_text) < 180:
        raise SystemExit(
            "source text too short for revision even after preview fallback (need >=180 chars). "
            "Provide full text via --source-text/file/stdin."
        )

    prompt_summary = str(article.get("summary", "")).strip()
    prompt_article_body = str(article.get("article_body", "")).strip()
    if len(source_text) >= 180:
        prompt_summary = ""
        prompt_article_body = ""

    _assert_required_summary_model(cfg)
    parsed = None
    last_err: Exception | None = None
    limits = _revision_context_limits(min(max_source_chars, len(source_text)))
    for attempt, limit in enumerate(limits, start=1):
        prompt = REVISION_PROMPT.format(
            title=str(article.get("title", "")).strip()[:500],
            summary=prompt_summary,
            article_body=prompt_article_body,
            source_url=source_url or "unknown",
            section=str(article.get("section", "")).strip(),
            category=str(article.get("category", "")).strip(),
            subcategory=str(article.get("subcategory", "")).strip(),
            categories=", ".join([str(c).strip() for c in article.get("categories", []) if str(c).strip()]) or "None",
            instructions=instructions.strip(),
            source_text=source_text[:limit].strip(),
        )
        try:
            response = _llm_generate(cfg, prompt)
            parsed = _safe_json_object(response)
            if parsed:
                if attempt > 1:
                    LOG.info(
                        "revision succeeded after context backoff article_id=%s attempt=%s context_chars=%s",
                        article.get("id"),
                        attempt,
                        limit,
                    )
                break
            last_err = RuntimeError("revision failed: model did not return valid JSON")
        except Exception as exc:
            last_err = exc
            LOG.warning(
                "revision llm failed article_id=%s attempt=%s context_chars=%s err=%s",
                article.get("id"),
                attempt,
                limit,
                exc,
            )

    if not parsed:
        raise SystemExit(f"revision failed after context backoff: {last_err}")

    title = str(parsed.get("title", "")).strip()[:500]
    summary = _normalize_summary_text(str(parsed.get("summary", "")))
    article_body = _normalize_article_body_text(str(parsed.get("article_body", "")))
    section = str(parsed.get("section", "")).strip()[:120]
    category = str(parsed.get("category", "")).strip()[:120]
    subcategory = str(parsed.get("subcategory", "")).strip()[:120]
    categories = _normalize_keywords(parsed.get("categories", []), limit=10)

    payload: dict = {
        "decision": "revise",
        "title": title or str(article.get("title", "")).strip()[:500],
        "summary": summary or str(article.get("summary", "")).strip(),
        "article_body": article_body or str(article.get("article_body", "")).strip(),
        "section": section or str(article.get("section", "")).strip()[:120],
        "category": category or str(article.get("category", "")).strip()[:120],
        "subcategory": subcategory or str(article.get("subcategory", "")).strip()[:120],
        "categories": categories or article.get("categories", []),
        "comment": f"Revision requested: {instructions.strip()}",
    }
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(description="Apply editorial decision for one preview article")
    parser.add_argument("--article-id", type=int, required=True, help="Article id in portal DB")
    parser.add_argument(
        "--decision",
        choices=["approve", "revise", "hold", "changes", "request_changes", "reject"],
        required=True,
        help="approve=publish | revise=rewrite and return to pending | hold=on hold | changes/request_changes=changes requested",
    )
    parser.add_argument("--comment", default="", help="Optional comment")
    parser.add_argument("--instructions", default="", help="Required for decision=revise")
    parser.add_argument("--source-text-file", default="", help="Optional full source text file for revise flow")
    parser.add_argument("--source-text", default="", help="Optional inline source text for revise flow")
    parser.add_argument("--stdin", action="store_true", help="Read full source text from stdin")
    parser.add_argument("--max-source-chars", type=int, default=12000, help="Max source chars to send to LLM")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    cfg = Config()
    if not cfg.agent_api_key:
        raise SystemExit("AGENT_API_KEY missing")

    fetched = _api_get(cfg, "articles/editorial-data/", {"article_id": args.article_id})
    article = fetched.get("article") if isinstance(fetched, dict) else None
    if not isinstance(article, dict):
        raise SystemExit("failed to load article editorial data")

    decision = str(args.decision).strip().lower()
    decision_aliases = {
        "reject": "request_changes",
        "changes": "request_changes",
    }
    decision = decision_aliases.get(decision, decision)

    payload: dict = {
        "article_id": args.article_id,
        "decision": decision,
        "comment": args.comment.strip(),
    }

    if decision == "revise":
        instructions = args.instructions.strip()
        if not instructions:
            raise SystemExit("decision=revise requires --instructions")
        override_text = _read_source_text(args.source_text, args.source_text_file, bool(args.stdin))
        revision = _build_revision_payload(cfg, article, instructions, override_text, args.max_source_chars)
        payload.update(revision)

    result = _api_post(cfg, "articles/editorial-decision/", payload)

    next_review_notification = None
    if decision == "approve":
        newsletter_id = result.get("newsletter_id")
        if newsletter_id is not None:
            next_review_notification = _notify_next_article_review(cfg, int(newsletter_id))
        else:
            next_review_notification = {"sent": False, "reason": "missing_newsletter_id"}

    output = dict(result) if isinstance(result, dict) else {"status": "ok"}
    if next_review_notification is not None:
        output["next_review_notification"] = next_review_notification
    print(json.dumps(output, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
