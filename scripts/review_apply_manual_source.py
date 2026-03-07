#!/usr/bin/env python3
"""Apply reviewer-provided full source text to one draft article and regenerate content."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from process_newsletter import (
    Config,
    SOURCE_REWRITE_PROMPT,
    _assert_required_summary_model,
    _llm_generate,
    _normalize_article_body_text,
    _normalize_keywords,
    _normalize_summary_text,
    _safe_json_object,
    _taxonomy_defaults,
)


LOG = logging.getLogger("review_apply_manual_source")


def _resolve_draft_path(newsletter_id: int, explicit_path: str | None, review_dir: str) -> Path:
    if explicit_path:
        return Path(explicit_path)
    return Path(review_dir) / f"newsletter_{newsletter_id}_draft.json"


def _load_draft(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"draft not found: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("invalid draft payload")
    return payload


def _read_source_text(source_text: str, source_text_file: str, read_stdin: bool) -> str:
    if source_text:
        return source_text.strip()
    if source_text_file:
        return Path(source_text_file).read_text(encoding="utf-8").strip()
    if read_stdin:
        return sys.stdin.read().strip()
    return ""


def _rewrite_from_manual_text(cfg: Config, article: dict[str, Any], source_text: str) -> dict[str, Any]:
    prompt = SOURCE_REWRITE_PROMPT.format(
        existing_title=str(article.get("title", "")).strip()[:500],
        existing_summary=str(article.get("summary", "")).strip(),
        existing_categories=", ".join([str(c).strip() for c in article.get("categories", []) if str(c).strip()]) or "None",
        source_url=str(article.get("original_url", "")).strip() or "manual-source-text",
        source_published_at=str(article.get("published_at", "")).strip() or "unknown",
        source_text=source_text,
    )

    response = _llm_generate(cfg, prompt)
    parsed = _safe_json_object(response)
    if not parsed:
        raise RuntimeError("model returned invalid JSON for manual source text rewrite")

    out = dict(article)
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
        out["enrichment_context"] = article_body
    if section:
        out["section"] = section[:120]
    if category_value:
        out["category"] = category_value[:120]
    if subcategory:
        out["subcategory"] = subcategory[:120]
    if categories:
        out["categories"] = categories

    out["manual_review_required"] = False
    out["summary_source_mode"] = "manual_full_text_provided_by_reviewer"
    out["summary_source_url"] = str(out.get("original_url", "")).strip()
    out["review_note"] = "Manual full source text supplied by reviewer before publish."
    out["manual_source_text_applied"] = True
    out["manual_source_text_chars"] = len(source_text)
    out["manual_source_text_applied_at"] = datetime.now(tz=timezone.utc).isoformat()
    return _taxonomy_defaults(out)


def main() -> int:
    parser = argparse.ArgumentParser(description="Apply manual source text to one review draft article")
    parser.add_argument("--newsletter-id", type=int, required=True, help="Newsletter id")
    parser.add_argument("--article-index", type=int, required=True, help="1-based article index in draft")
    parser.add_argument("--draft-file", help="Explicit draft JSON path (optional)")
    parser.add_argument("--source-text-file", help="Path to file with full source text")
    parser.add_argument("--source-text", help="Inline full source text")
    parser.add_argument("--stdin", action="store_true", help="Read full source text from stdin")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

    cfg = Config()
    _assert_required_summary_model(cfg)

    draft_path = _resolve_draft_path(args.newsletter_id, args.draft_file, cfg.review_output_dir)
    draft = _load_draft(draft_path)
    articles = draft.get("articles", [])
    if not isinstance(articles, list) or not articles:
        raise SystemExit("draft has no articles")

    idx = args.article_index - 1
    if idx < 0 or idx >= len(articles):
        raise SystemExit(f"article index out of range: {args.article_index} (1..{len(articles)})")

    source_text = _read_source_text(args.source_text or "", args.source_text_file or "", bool(args.stdin))
    if len(source_text) < 300:
        raise SystemExit("source text too short; provide full source text (>=300 chars)")

    original = articles[idx]
    if not isinstance(original, dict):
        raise SystemExit("selected article is invalid")

    updated = _rewrite_from_manual_text(cfg, original, source_text)
    articles[idx] = updated
    draft["articles"] = articles
    draft["updated_at"] = datetime.now(tz=timezone.utc).isoformat()
    draft_path.write_text(json.dumps(draft, ensure_ascii=False, indent=2), encoding="utf-8")

    pending = sum(1 for row in articles if isinstance(row, dict) and bool(row.get("manual_review_required")))
    print(
        json.dumps(
            {
                "newsletter_id": args.newsletter_id,
                "draft_file": str(draft_path),
                "updated_article_index": args.article_index,
                "updated_title": str(updated.get("title", "")).strip(),
                "manual_review_remaining": pending,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
